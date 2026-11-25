//go:build !integration

package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/pandops/flowai-flow-registry/internal/broker"
	"github.com/pandops/flowai-flow-registry/internal/cursor"
	"github.com/pandops/flowai-flow-registry/internal/flows"
	"github.com/pandops/flowai-flow-registry/internal/mocks"
	"github.com/pandops/flowai-flow-registry/internal/outbox"
	"github.com/pandops/flowai-flow-registry/internal/store"
)

// Subjects of the services flow-registry calls.
const (
	subjTeamTags       = "executor-registry.service.team-tags.pub"
	subjValidateModels = "model-registry.service.validate-models.pub"
	subjApplyFlowTeam  = "model-registry.service.apply-flow-team.pub"
	subjRemoveFlowTeam = "model-registry.service.remove-flow-team.pub"
	subjFlowsStatus    = "model-registry.service.flows-status.pub"
	subjSourcesLookup  = "source-controller.service.sources-lookup.pub"
	subjBindingGuard   = "source-controller.service.binding-guard.pub"
)

// serviceDir is the standalone module root, found from this file's location.
func serviceDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(file))
}

func responderSchemaBase(t testing.TB, subject string) string {
	t.Helper()
	root := os.Getenv("FLOWAI_CONTRACT_FIXTURE_DIR")
	if !filepath.IsAbs(root) {
		t.Fatal("set FLOWAI_CONTRACT_FIXTURE_DIR to an absolute fixture directory")
	}
	parts := strings.Split(subject, ".")
	return filepath.Join(root, "svc", parts[0], "schemas", parts[1]+"."+parts[2])
}

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------

var (
	schemaOnce     sync.Once
	schemaCompiler *jsonschema.Compiler
	schemaCache    sync.Map
	schemaMu       sync.Mutex
)

func compileSchema(t testing.TB, name string) *jsonschema.Schema {
	t.Helper()
	if s, ok := schemaCache.Load(name); ok {
		return s.(*jsonschema.Schema)
	}
	schemaMu.Lock()
	defer schemaMu.Unlock()
	schemaOnce.Do(func() {
		schemaCompiler = jsonschema.NewCompiler()
		schemaCompiler.AssertFormat()
	})
	path := filepath.Join(serviceDir(), "schemas", name)
	s, err := schemaCompiler.Compile("file://" + filepath.ToSlash(path))
	if err != nil {
		t.Fatalf("compile schema %s: %v", name, err)
	}
	schemaCache.Store(name, s)
	return s
}

func validateAgainst(t testing.TB, name string, data []byte) error {
	t.Helper()
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("payload is not JSON: %w", err)
	}
	return compileSchema(t, name).Validate(doc)
}

func mustValidate(t testing.TB, name string, data []byte) {
	t.Helper()
	if err := validateAgainst(t, name, data); err != nil {
		t.Fatalf("payload does not validate against %s: %v\npayload: %s", name, err, data)
	}
}

// ---------------------------------------------------------------------------
// Fakes of the services flow-registry calls
// ---------------------------------------------------------------------------

type modelState struct {
	Availability string // active | paused | unavailable
	Selectable   bool
}

type usageState struct {
	Spend   string // decimal USD string, as model-registry sends it
	ResetAt string
}

type applyCall struct {
	TeamID       int64           `json:"team_id"`
	FlowID       int64           `json:"flow_id"`
	FlowRevision int             `json:"flow_revision"`
	FlowLimit    json.RawMessage `json:"flow_limit"`
}

type others struct {
	mu      sync.Mutex
	tags    map[int64][]string
	models  map[int64]map[string]modelState
	sources map[int64]map[int64]int
	usage   map[[2]int64]usageState

	modelsDown, applyDown, applyUnavailable, statusDown bool
	// applyHook runs inside apply-flow-team before it answers.
	applyHook func(applyCall)
	// validateHook runs inside validate-models before it reads the models,
	// as a model change in flight in model-registry would delay the reply.
	validateHook func()
	applied      []applyCall
	// removeUnavailable makes remove-flow-team answer 503; removed lists
	// the confirmed removals as [team_id, flow_id].
	removeUnavailable bool
	removed           [][2]int64
}

func newOthers() *others {
	return &others{
		tags:    map[int64][]string{},
		models:  map[int64]map[string]modelState{},
		sources: map[int64]map[int64]int{},
		usage:   map[[2]int64]usageState{},
	}
}

func (o *others) addModel(team int64, alias string, st modelState) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.models[team] == nil {
		o.models[team] = map[string]modelState{}
	}
	o.models[team][alias] = st
}

func (o *others) addSource(team, source int64, revision int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.sources[team] == nil {
		o.sources[team] = map[int64]int{}
	}
	o.sources[team][source] = revision
}

func (o *others) sourceRevision(team, source int64) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.sources[team][source]
}

func (o *others) set(f func(o *others)) {
	o.mu.Lock()
	defer o.mu.Unlock()
	f(o)
}

func (o *others) removedTeams() [][2]int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([][2]int64(nil), o.removed...)
}

func (o *others) appliedCalls() []applyCall {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]applyCall(nil), o.applied...)
}

type serviceRequest struct {
	Params json.RawMessage `json:"params"`
	Body   json.RawMessage `json:"body"`
}

// decodeBody decodes the request fields: body when present, else params.
// The contract wrapper has already validated the request.
func decodeBody(t testing.TB, data []byte, into any) {
	var env serviceRequest
	_ = json.Unmarshal(data, &env)
	src := env.Body
	if len(src) == 0 {
		src = env.Params
	}
	_ = json.Unmarshal(src, into)
}

// contracted makes a fake responder honor the responder's own published
// schemas: a request that violates svc/<svc>/schemas/service.<op>.request
// fails the test and gets 400; every fake reply is validated against
// service.<op>.reply before it is returned.
func contracted(t testing.TB, subject string, fn mocks.Responder) mocks.Responder {
	base := responderSchemaBase(t, subject)
	return func(m broker.Msg) (broker.Reply, error) {
		if err := validateFile(t, base+".request.schema.json", m.Data); err != nil {
			t.Errorf("%s request violates the responder's schema: %v\n%s", subject, err, m.Data)
			return errReply(400, "invalid_request")
		}
		r, err := fn(m)
		if err != nil {
			return r, err
		}
		if verr := validateFile(t, base+".reply.schema.json", r.Body); verr != nil {
			t.Errorf("fake %s reply violates the responder's schema: %v\n%s", subject, verr, r.Body)
		}
		return r, nil
	}
}

func jsonReply(status int, v any) (broker.Reply, error) {
	data, _ := json.Marshal(v)
	return broker.Reply{Status: status, Header: map[string]string{"status": strconv.Itoa(status)}, Body: data}, nil
}

func errReply(status int, code string) (broker.Reply, error) {
	return jsonReply(status, map[string]any{"code": code, "message": code, "request_id": 1})
}

func (o *others) install(t testing.TB, b *mocks.Broker) {
	b.Respond(subjTeamTags, contracted(t, subjTeamTags, func(m broker.Msg) (broker.Reply, error) {
		var req struct {
			TeamID int64 `json:"team_id"`
		}
		decodeBody(t, m.Data, &req)
		o.mu.Lock()
		defer o.mu.Unlock()
		tags := append([]string{}, o.tags[req.TeamID]...)
		return jsonReply(200, map[string]any{"tags": tags})
	}))
	b.Respond(subjValidateModels, contracted(t, subjValidateModels, func(m broker.Msg) (broker.Reply, error) {
		var req struct {
			TeamID  int64    `json:"team_id"`
			Aliases []string `json:"aliases"`
		}
		decodeBody(t, m.Data, &req)
		o.mu.Lock()
		hook := o.validateHook
		o.mu.Unlock()
		if hook != nil {
			hook()
		}
		o.mu.Lock()
		defer o.mu.Unlock()
		if o.modelsDown {
			return broker.Reply{}, broker.ErrNoResponse
		}
		out := []map[string]any{}
		for _, a := range req.Aliases {
			st, owned := o.models[req.TeamID][a]
			availability := "no_access"
			if owned {
				availability = st.Availability
			}
			out = append(out, map[string]any{"alias": a, "owned": owned, "availability": availability, "selectable": owned && st.Selectable})
		}
		return jsonReply(200, map[string]any{"aliases": out})
	}))
	b.Respond(subjApplyFlowTeam, contracted(t, subjApplyFlowTeam, func(m broker.Msg) (broker.Reply, error) {
		var req applyCall
		decodeBody(t, m.Data, &req)
		o.mu.Lock()
		hook := o.applyHook
		if o.applyDown {
			o.mu.Unlock()
			return broker.Reply{}, broker.ErrNoResponse
		}
		if o.applyUnavailable {
			o.mu.Unlock()
			return errReply(503, "model_gateway_unavailable")
		}
		o.applied = append(o.applied, req)
		o.mu.Unlock()
		if hook != nil {
			hook(req)
		}
		return jsonReply(200, map[string]any{"team_id": req.TeamID, "flow_id": req.FlowID, "flow_revision": req.FlowRevision, "budget_reset_at": nil})
	}))
	b.Respond(subjRemoveFlowTeam, contracted(t, subjRemoveFlowTeam, func(m broker.Msg) (broker.Reply, error) {
		var req struct {
			TeamID int64 `json:"team_id"`
			FlowID int64 `json:"flow_id"`
		}
		decodeBody(t, m.Data, &req)
		o.mu.Lock()
		defer o.mu.Unlock()
		if o.removeUnavailable {
			return errReply(503, "model_gateway_unavailable")
		}
		o.removed = append(o.removed, [2]int64{req.TeamID, req.FlowID})
		return jsonReply(200, map[string]any{"team_id": req.TeamID, "flow_id": req.FlowID})
	}))
	b.Respond(subjFlowsStatus, contracted(t, subjFlowsStatus, func(m broker.Msg) (broker.Reply, error) {
		var req struct {
			TeamID int64 `json:"team_id"`
			Flows  []struct {
				FlowID    int64           `json:"flow_id"`
				Aliases   []string        `json:"aliases"`
				FlowLimit json.RawMessage `json:"flow_limit"`
			} `json:"flows"`
		}
		decodeBody(t, m.Data, &req)
		o.mu.Lock()
		defer o.mu.Unlock()
		if o.statusDown {
			return broker.Reply{}, broker.ErrNoResponse
		}
		out := []map[string]any{}
		for _, f := range req.Flows {
			models := []map[string]any{}
			for _, a := range f.Aliases {
				st, owned := o.models[req.TeamID][a]
				av := "no_access"
				if owned {
					av = st.Availability
				}
				models = append(models, map[string]any{"model_alias": a, "availability": av})
			}
			var usage any
			if u, ok := o.usage[[2]int64{req.TeamID, f.FlowID}]; ok {
				var limit struct {
					Max    string `json:"max_budget_usd"`
					Period string `json:"budget_period"`
				}
				_ = json.Unmarshal(f.FlowLimit, &limit)
				usage = map[string]any{"spend_usd": u.Spend, "max_budget_usd": limit.Max, "budget_period": limit.Period, "budget_reset_at": u.ResetAt, "observed_at": "2026-11-07T10:00:00.000Z"}
				if limit.Period == "" {
					usage = nil
				}
			}
			out = append(out, map[string]any{"flow_id": f.FlowID, "models": models, "usage": usage})
		}
		return jsonReply(200, map[string]any{"flows": out})
	}))
	b.Respond(subjSourcesLookup, contracted(t, subjSourcesLookup, func(m broker.Msg) (broker.Reply, error) {
		var req struct {
			TeamID    int64   `json:"team_id"`
			SourceIDs []int64 `json:"source_ids"`
		}
		decodeBody(t, m.Data, &req)
		o.mu.Lock()
		defer o.mu.Unlock()
		out := []map[string]any{}
		for _, id := range req.SourceIDs {
			if _, ok := o.sources[req.TeamID][id]; ok {
				out = append(out, map[string]any{"source_id": id, "name": "source-" + strconv.FormatInt(id, 10), "mode": "webhook", "settings_status": "complete", "enabled": true, "polling_blocked": false, "revision": o.sources[req.TeamID][id]})
			}
		}
		return jsonReply(200, map[string]any{"sources": out})
	}))
	b.Respond(subjBindingGuard, contracted(t, subjBindingGuard, func(m broker.Msg) (broker.Reply, error) {
		var req struct {
			TeamID  int64 `json:"team_id"`
			Sources []struct {
				SourceID         int64 `json:"source_id"`
				ExpectedRevision int   `json:"expected_revision"`
			} `json:"sources"`
		}
		decodeBody(t, m.Data, &req)
		o.mu.Lock()
		defer o.mu.Unlock()
		for _, s := range req.Sources {
			rev, ok := o.sources[req.TeamID][s.SourceID]
			if !ok || rev != s.ExpectedRevision {
				return errReply(409, "source_revision_conflict")
			}
		}
		out := []map[string]any{}
		for _, s := range req.Sources {
			o.sources[req.TeamID][s.SourceID]++
			out = append(out, map[string]any{"source_id": s.SourceID, "revision": o.sources[req.TeamID][s.SourceID]})
		}
		return jsonReply(200, map[string]any{"sources": out})
	}))
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

type harness struct {
	t      *testing.T
	ctx    context.Context
	mem    *mocks.MemStore // nil when the harness runs on another store
	store  store.Store
	broker *mocks.Broker
	svc    *flows.Service
	relay  *outbox.Relay
	other  *others
	clock  *fakeClock
	reqID  atomic.Int64
	logs   *bytes.Buffer
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(time.Millisecond)
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

var testCursorSecret = []byte("0123456789abcdef0123456789abcdef")

func newHarness(t *testing.T, mods ...func(*flows.Options)) *harness {
	t.Helper()
	mem := mocks.NewMemStore()
	h := newHarnessWithStore(t, mem, mods...)
	h.mem = mem
	return h
}

func newHarnessWithStore(t *testing.T, st store.Store, mods ...func(*flows.Options)) *harness {
	t.Helper()
	clock := &fakeClock{now: time.Date(2026, 11, 7, 9, 0, 0, 0, time.UTC)}
	codec, err := cursor.New([]cursor.Key{{ID: 1, Secret: testCursorSecret}}, 1, 24*time.Hour, clock.Now)
	if err != nil {
		// Cursor construction failing is reported by the cursor tests; the
		// harness still runs so every other behavior reports its own failure.
		codec = nil
	}
	b := mocks.NewBroker()
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(lockedWriter{w: logs}, nil))
	opts := flows.Options{Store: st, Broker: b, Cursors: codec, Now: clock.Now, Logger: logger, RepairInterval: 5 * time.Millisecond}
	for _, m := range mods {
		m(&opts)
	}
	svc := flows.New(opts)
	if err := b.Serve(svc.Subjects(), svc.Handle); err != nil {
		t.Fatalf("serve: %v", err)
	}
	h := &harness{
		t: t, ctx: context.Background(), store: st, broker: b, svc: svc,
		relay: &outbox.Relay{Store: st, JS: b, Logger: logger, Interval: time.Millisecond},
		other: newOthers(), clock: clock, logs: logs,
	}
	h.other.install(t, b)
	// Team 3: tag openhands, two active models, Sources 12 and 13.
	h.other.tags[3] = []string{"openhands"}
	h.other.addModel(3, "3/gpt-4o", modelState{Availability: "active", Selectable: true})
	h.other.addModel(3, "3/claude-sonnet", modelState{Availability: "active", Selectable: true})
	h.other.addSource(3, 12, 5)
	h.other.addSource(3, 13, 2)
	// Team 4: tag gpu, one model, Source 20.
	h.other.tags[4] = []string{"gpu"}
	h.other.addModel(4, "4/m", modelState{Availability: "active", Selectable: true})
	h.other.addSource(4, 20, 1)
	t.Cleanup(svc.Wait)
	return h
}

type lockedWriter struct{ w io.Writer }

var logMu sync.Mutex

func (l lockedWriter) Write(p []byte) (int, error) {
	logMu.Lock()
	defer logMu.Unlock()
	return l.w.Write(p)
}

func (h *harness) logText() string {
	logMu.Lock()
	defer logMu.Unlock()
	return h.logs.String()
}

// reply is one answer flow-registry gave.
type reply struct {
	Status int
	Header map[string]string
	Body   []byte
}

func (r reply) json(t testing.TB) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(r.Body, &out); err != nil {
		t.Fatalf("reply body is not a JSON object (status %d): %v: %s", r.Status, err, r.Body)
	}
	return out
}

func (r reply) code(t testing.TB) string {
	t.Helper()
	var e struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(r.Body, &e)
	return e.Code
}

func (r reply) expect(t testing.TB, status int, code string) {
	t.Helper()
	if r.Status != status {
		t.Fatalf("status = %d, want %d; body %s", r.Status, status, r.Body)
	}
	if code != "" && r.code(t) != code {
		t.Fatalf("code = %q, want %q; body %s", r.code(t), code, r.Body)
	}
}

// reqOpt adjusts one request's headers.
type reqOpt func(map[string]string)

func withHeader(k, v string) reqOpt { return func(h map[string]string) { h[k] = v } }
func withoutHeader(k string) reqOpt { return func(h map[string]string) { delete(h, k) } }
func ifMatch(revision int) reqOpt {
	return withHeader("If-Match", fmt.Sprintf("%q", strconv.Itoa(revision)))
}
func withOperator(id int64) reqOpt   { return withHeader("operator_id", strconv.FormatInt(id, 10)) }
func withTeamHeader(v string) reqOpt { return withHeader("team", v) }
func withRequestID(id int64) reqOpt  { return withHeader("request_id", strconv.FormatInt(id, 10)) }

func (h *harness) nextRequestID() int64 { return 1000 + h.reqID.Add(1) }

func envelope(params map[string]any, body any) []byte {
	env := map[string]any{}
	if params != nil {
		env["params"] = params
	}
	if body != nil {
		env["body"] = body
	}
	data, _ := json.Marshal(env)
	return data
}

// deliver sends a raw request on a flow-registry ".pub" subject.
func (h *harness) deliver(subject string, header map[string]string, data []byte) reply {
	h.t.Helper()
	r := h.broker.Deliver(h.ctx, subject, header, data)
	out := reply{Status: r.Status, Header: r.Header, Body: r.Body}
	if r.Header != nil {
		if s, err := strconv.Atoi(r.Header["status"]); err == nil && s != r.Status {
			h.t.Fatalf("reply header status %q differs from status %d", r.Header["status"], r.Status)
		}
	}
	return out
}

// ui sends an operator request as API Gateway would.
func (h *harness) ui(team int64, op string, params map[string]any, body any, opts ...reqOpt) reply {
	h.t.Helper()
	header := map[string]string{
		"team":        strconv.FormatInt(team, 10),
		"operator_id": "7",
		"request_id":  strconv.FormatInt(h.nextRequestID(), 10),
		"team_name":   "Operations",
	}
	for _, o := range opts {
		o(header)
	}
	r := h.deliver("flow-registry.ui."+op+".pub", header, envelope(params, body))
	h.checkReplySchema("ui."+op, r)
	return r
}

// service sends a service request as another FlowAI service would.
func (h *harness) service(team string, op string, body any, opts ...reqOpt) reply {
	h.t.Helper()
	header := map[string]string{"team": team, "request_id": strconv.FormatInt(h.nextRequestID(), 10)}
	for _, o := range opts {
		o(header)
	}
	r := h.deliver("flow-registry.service."+op+".pub", header, envelope(asParams(body), nil))
	h.checkReplySchema("service."+op, r)
	return r
}

// admin sends an administrator request as admin-panel would.
func (h *harness) admin(op string, params map[string]any, body any, opts ...reqOpt) reply {
	h.t.Helper()
	header := map[string]string{"team": "admin", "admin_subject": "admin@example.test", "request_id": strconv.FormatInt(h.nextRequestID(), 10)}
	for _, o := range opts {
		o(header)
	}
	r := h.deliver("flow-registry.admin."+op+".pub", header, envelope(params, body))
	h.checkReplySchema("admin."+op, r)
	return r
}

// checkReplySchema validates every reply against its schema: success
// bodies against <class>.<op>.reply, error bodies against error.reply.
func (h *harness) checkReplySchema(name string, r reply) {
	h.t.Helper()
	switch {
	case r.Status == 0 || r.Status == 501:
		return // not served: the caller's assertions report it
	case r.Status == 204:
		if len(bytes.TrimSpace(r.Body)) != 0 {
			h.t.Fatalf("%s: 204 reply carries a body: %s", name, r.Body)
		}
	case r.Status >= 200 && r.Status < 300:
		mustValidate(h.t, name+".reply.schema.json", r.Body)
	default:
		mustValidate(h.t, "error.reply.schema.json", r.Body)
	}
}

// flush runs the outbox relay once and returns the newly acknowledged messages.
func (h *harness) flush() []mocks.Published {
	h.t.Helper()
	before := len(h.broker.Published())
	if _, err := h.relay.Flush(h.ctx); err != nil {
		h.t.Fatalf("outbox flush: %v", err)
	}
	all := h.broker.Published()
	out := all[before:]
	for _, m := range out {
		h.validatePublished(m)
	}
	return out
}

func (h *harness) validatePublished(m mocks.Published) {
	h.t.Helper()
	switch {
	case strings.HasPrefix(m.Subject, "flow-registry.audit."):
		action := strings.TrimSuffix(strings.TrimPrefix(m.Subject, "flow-registry.audit."), ".pub")
		mustValidate(h.t, "audit."+action+".schema.json", m.Data)
	case strings.HasPrefix(m.Subject, "flow-registry.event."):
		kind := strings.TrimSuffix(strings.TrimPrefix(m.Subject, "flow-registry.event."), ".pub")
		mustValidate(h.t, "event."+kind+".schema.json", m.Data)
	default:
		h.t.Fatalf("flow-registry published on an unexpected subject %q", m.Subject)
	}
}

// audits flushes and returns the published audit messages, decoded.
func (h *harness) audits() []map[string]any {
	h.t.Helper()
	var out []map[string]any
	for _, m := range h.flush() {
		if strings.HasPrefix(m.Subject, "flow-registry.audit.") {
			var v map[string]any
			_ = json.Unmarshal(m.Data, &v)
			v["_subject"] = m.Subject
			v["_team"] = m.Header["team"]
			v["_msgid"] = m.Header["Nats-Msg-Id"]
			out = append(out, v)
		}
	}
	return out
}

func (h *harness) requests(subject string) []mocks.SentRequest { return h.broker.Requests(subject) }

// callsTo counts requests sent to any of the given service prefixes.
func (h *harness) callsTo(prefixes ...string) int {
	n := 0
	for _, r := range h.broker.Requests("") {
		for _, p := range prefixes {
			if strings.HasPrefix(r.Subject, p) {
				n++
			}
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// Flow bodies
// ---------------------------------------------------------------------------

func flowBody(mods ...func(map[string]any)) map[string]any {
	b := map[string]any{
		"name":                 "Intake routing",
		"execution_tag":        "openhands",
		"model_aliases":        []any{"3/gpt-4o"},
		"per_task_limit":       nil,
		"flow_limit":           nil,
		"prompt_template":      "Solve {{external_id}}",
		"execution_parameters": map[string]any{},
		"default_image":        nil,
		"source_ids":           []any{},
		"source_revisions":     map[string]any{},
	}
	for _, m := range mods {
		m(b)
	}
	return b
}

func set(k string, v any) func(map[string]any) { return func(b map[string]any) { b[k] = v } }
func unset(k string) func(map[string]any)      { return func(b map[string]any) { delete(b, k) } }

// bindTo binds the body to the Sources with the given current revisions.
func bindTo(revisions map[int64]int, ids ...int64) func(map[string]any) {
	return func(b map[string]any) {
		list := []any{}
		for _, id := range ids {
			list = append(list, id)
		}
		revs := map[string]any{}
		for id, rev := range revisions {
			revs[strconv.FormatInt(id, 10)] = rev
		}
		b["source_ids"] = list
		b["source_revisions"] = revs
	}
}

// createFlow creates a Flow for the team and returns its view.
func (h *harness) createFlow(team int64, mods ...func(map[string]any)) map[string]any {
	h.t.Helper()
	body := flowBody(mods...)
	if team != 3 {
		if _, ok := body["execution_tag"]; ok && body["execution_tag"] == "openhands" {
			tags := h.other.tags[team]
			if len(tags) > 0 {
				body["execution_tag"] = tags[0]
			}
		}
		if aliases, ok := body["model_aliases"].([]any); ok && len(aliases) == 1 && aliases[0] == "3/gpt-4o" {
			for alias := range h.other.models[team] {
				body["model_aliases"] = []any{alias}
				break
			}
		}
	}
	r := h.ui(team, "flow-create", nil, body)
	r.expect(h.t, 201, "")
	return r.json(h.t)
}

func num(t testing.TB, v any) int64 {
	t.Helper()
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("value %v (%T) is not a JSON number", v, v)
	}
	return int64(f)
}

func flowID(t testing.TB, view map[string]any) int64 { return num(t, view["flow_id"]) }
func revision(t testing.TB, view map[string]any) int { return int(num(t, view["revision"])) }

func strs(v any) []string {
	list, _ := v.([]any)
	out := []string{}
	for _, x := range list {
		out = append(out, fmt.Sprint(x))
	}
	return out
}

func sortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

type mocksPublished = mocks.Published

func jsonUnmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// asParams turns a service request's fields into the params object (rule 9).
func asParams(body any) map[string]any {
	if body == nil {
		return nil
	}
	if m, ok := body.(map[string]any); ok {
		return m
	}
	var m map[string]any
	_ = json.Unmarshal(mustJSON(body), &m)
	return m
}
