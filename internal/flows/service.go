// Package flows implements flow-registry's operations: the operator ui
// operations, the service operations other FlowAI services call, and the
// administrator tag projection.
package flows

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/pandops/flowai-flow-registry/internal/broker"
	"github.com/pandops/flowai-flow-registry/internal/cursor"
	"github.com/pandops/flowai-flow-registry/internal/ids"
	"github.com/pandops/flowai-flow-registry/internal/store"
)

// Options wires a Service.
type Options struct {
	Store   store.Store
	Broker  broker.Requester
	Cursors *cursor.Codec
	Now     func() time.Time
	Logger  *slog.Logger
	// RepairInterval is the wait between background apply-flow-team
	// restoration attempts (default 1 s, doubling up to 5 min).
	RepairInterval time.Duration
	// RequestTimeout bounds every request to another service (default 5 s).
	RequestTimeout time.Duration
}

const (
	publisher       = "flow-registry"
	maxRepairDelay  = 5 * time.Minute
	defaultTimeout  = 5 * time.Second
	defaultInterval = time.Second
)

// Service answers flow-registry's request subjects.
type Service struct {
	o       Options
	schemas map[string]*jsonschema.Schema
	schemaE error
	bg      sync.WaitGroup
}

// New builds the service. A schema set that does not compile makes every
// request fail with 500 (the schemas are embedded, so this is a build defect).
func New(o Options) *Service {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.RepairInterval <= 0 {
		o.RepairInterval = defaultInterval
	}
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = defaultTimeout
	}
	s := &Service{o: o}
	s.schemas, s.schemaE = requestSchemas()
	if s.schemaE != nil {
		o.Logger.Error("request schemas do not compile", "error", s.schemaE)
	}
	return s
}

// Subjects are the ".sub" subjects flow-registry subscribes.
func (s *Service) Subjects() []string {
	return []string{"flow-registry.ui.*.sub", "flow-registry.service.*.sub", "flow-registry.admin.*.sub"}
}

// Ready reports whether PostgreSQL answers and the broker is connected.
func (s *Service) Ready(ctx context.Context) bool {
	if c, ok := s.o.Broker.(interface{ Connected() bool }); ok && !c.Connected() {
		return false
	}
	return s.o.Store.Ping(ctx) == nil
}

// Wait blocks until background restorations started so far have finished.
func (s *Service) Wait() { s.bg.Wait() }

// apiError is a non-2xx reply.
type apiError struct {
	status int
	code   string
	msg    string
	flows  []flowRef
}

func (e *apiError) Error() string { return e.code }

func fail(status int, code, msg string) *apiError {
	return &apiError{status: status, code: code, msg: msg}
}

func invalid(msg string) *apiError { return fail(400, codeInvalidRequest, msg) }

var errNotFound = fail(404, codeNotFound, "not found")

// request is one decoded request with its verified context.
type request struct {
	class, op    string
	team         string // header value
	teamID       int64  // 0 for team: admin
	operatorID   int64  // ui only
	requestID    int64
	adminSubject string
	header       map[string]string
	params       json.RawMessage
	body         json.RawMessage
}

// result is a successful reply plus what its audit message needs.
type result struct {
	status     int
	body       any
	etag       int
	resourceID *int64
	changes    map[string]any
	// audited means the audit row was committed with the change.
	audited bool
}

// Handle answers one request.
func (s *Service) Handle(ctx context.Context, m broker.Msg) broker.Reply {
	r := &request{header: m.Header}
	if r.header == nil {
		r.header = map[string]string{}
	}
	parts := strings.Split(m.Subject, ".")
	if len(parts) != 4 || parts[0] != publisher {
		return s.errorReply(r, errNotFound)
	}
	r.class, r.op = parts[1], parts[2]
	schema, known := s.schemas[r.class+"."+r.op]
	if !known {
		return s.errorReply(r, errNotFound)
	}
	if err := s.context(r); err != nil {
		return s.errorReply(r, err) // no verified actor: not audited
	}
	res, err := s.serve(ctx, r, schema, m.Data)
	// Every rejected ui or admin request is audited here. A successful write
	// is audited in its own transaction, a successful administrator read
	// here, and a successful operator read not at all (D15).
	if r.class != "service" && (err != nil || (!res.audited && r.class == "admin")) {
		var rid *int64
		if res != nil {
			rid = res.resourceID
		}
		if rid == nil {
			rid = r.paramFlowID()
		}
		var changes map[string]any
		if res != nil {
			changes = res.changes
		}
		if aerr := s.auditStandalone(ctx, r, rid, err, changes); aerr != nil {
			s.o.Logger.Error("audit append failed", "operation", r.class+"."+r.op, "error", aerr)
			if err == nil {
				err = fail(503, codeServiceDown, "audit could not be stored")
			}
		}
	}
	if err != nil {
		return s.errorReply(r, err)
	}
	return s.okReply(res)
}

func (s *Service) serve(ctx context.Context, r *request, schema *jsonschema.Schema, data []byte) (*result, error) {
	if s.schemaE != nil {
		return nil, fail(500, codeInternal, "schemas unavailable")
	}
	if !validDoc(schema, data) {
		return nil, invalid("request does not match its schema")
	}
	var env struct {
		Params json.RawMessage `json:"params"`
		Body   json.RawMessage `json:"body"`
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &env); err != nil {
			return nil, invalid("request is not JSON")
		}
	}
	r.params, r.body = env.Params, env.Body
	switch r.class {
	case "ui":
		return s.serveUI(ctx, r)
	case "service":
		return s.serveService(ctx, r)
	default:
		return s.adminTags(ctx, r)
	}
}

// context verifies the headers of the request's class.
func (s *Service) context(r *request) *apiError {
	rid, ok := ids.Parse(r.header[broker.HeaderRequestID])
	if !ok {
		return invalid("request_id header is required")
	}
	r.requestID = rid
	r.team = r.header[broker.HeaderTeam]
	switch r.class {
	case "ui":
		team, ok := ids.Parse(r.team)
		if !ok {
			return invalid("team header is required")
		}
		op, ok := ids.Parse(r.header[broker.HeaderOperatorID])
		if !ok {
			return invalid("operator_id header is required")
		}
		r.teamID, r.operatorID = team, op
	case "service":
		if r.team != broker.TeamAdmin {
			team, ok := ids.Parse(r.team)
			if !ok {
				return invalid("team header is required")
			}
			r.teamID = team
		}
	case "admin":
		if r.team != broker.TeamAdmin {
			return invalid("team header must be admin")
		}
		r.adminSubject = strings.TrimSpace(r.header[broker.HeaderAdminSubject])
		if r.adminSubject == "" {
			return invalid("admin_subject header is required")
		}
	}
	return nil
}

// paramFlowID returns params.flow_id when it is a valid identifier.
func (r *request) paramFlowID() *int64 {
	var p struct {
		FlowID json.RawMessage `json:"flow_id"`
	}
	if json.Unmarshal(r.params, &p) != nil || p.FlowID == nil {
		return nil
	}
	if id, ok := ids.FromJSON(p.FlowID); ok {
		return &id
	}
	return nil
}

func (s *Service) okReply(res *result) broker.Reply {
	h := map[string]string{broker.HeaderStatus: strconv.Itoa(res.status)}
	if res.etag > 0 {
		h[broker.HeaderETag] = strconv.Quote(strconv.Itoa(res.etag))
	}
	var body []byte
	if res.body != nil {
		body, _ = json.Marshal(res.body)
	}
	return broker.Reply{Status: res.status, Header: h, Body: body}
}

func (s *Service) errorReply(r *request, err error) broker.Reply {
	var e *apiError
	if !errors.As(err, &e) {
		s.o.Logger.Error("request failed", "error", err)
		e = fail(500, codeInternal, "internal error")
	}
	out := map[string]any{"code": e.code, "message": e.msg}
	// A missing or invalid request_id header leaves the field out (D12).
	if r != nil {
		if rid, ok := ids.Parse(r.header[broker.HeaderRequestID]); ok {
			out["request_id"] = rid
		}
	}
	if e.flows != nil {
		out["flows"] = e.flows
	}
	body, _ := json.Marshal(out)
	return broker.Reply{Status: e.status, Header: map[string]string{broker.HeaderStatus: strconv.Itoa(e.status)}, Body: body}
}

// ---------------------------------------------------------------------------
// Audit and events
// ---------------------------------------------------------------------------

func (s *Service) auditAction(r *request) string {
	if r.class == "admin" {
		return "admin-" + r.op
	}
	return r.op
}

// auditMessage builds one audit outbox message. A nil err is "succeeded".
func (s *Service) auditMessage(r *request, action string, actorID int64, resourceID *int64, err error, changes map[string]any) store.OutboxMessage {
	fields := map[string]any{
		"publisher":      publisher,
		"source_service": publisher,
		"action":         action,
		"request_id":     r.requestID,
		"outcome":        "succeeded",
	}
	if r.class == "admin" {
		fields["actor_type"] = "administrator"
		fields["actor_subject"] = r.adminSubject
		fields["resource_type"] = "flow-tags"
	} else {
		fields["actor_type"] = "operator"
		fields["actor_id"] = actorID
		fields["team_id"] = r.teamID
		fields["resource_type"] = "flow"
	}
	if resourceID != nil {
		fields["resource_id"] = *resourceID
	} else {
		fields["resource_id"] = nil
	}
	if err != nil {
		fields["outcome"] = "rejected"
		code := codeInternal
		var e *apiError
		if errors.As(err, &e) {
			code = e.code
		}
		fields["reason"] = code
	}
	if len(changes) > 0 {
		fields["changes"] = changes
	}
	team := r.team
	if r.class == "service" && r.teamID > 0 {
		team = strconv.FormatInt(r.teamID, 10)
	}
	return s.outboxMessage("flow-registry.audit."+action+".pub", team, r.requestID, fields)
}

func (s *Service) outboxMessage(subject, team string, requestID int64, fields map[string]any) store.OutboxMessage {
	occurred := ts(s.o.Now())
	return store.OutboxMessage{
		Subject: subject, Team: team, RequestID: requestID,
		Build: func(seq int64) ([]byte, error) {
			out := make(map[string]any, len(fields)+2)
			for k, v := range fields {
				out[k] = v
			}
			out["publisher_seq"] = seq
			out["occurred_at"] = occurred
			return json.Marshal(out)
		},
	}
}

func (s *Service) auditStandalone(ctx context.Context, r *request, resourceID *int64, err error, changes map[string]any) error {
	_, aerr := s.o.Store.AppendOutbox(ctx, s.auditMessage(r, s.auditAction(r), r.operatorID, resourceID, err, changes))
	return aerr
}

// ---------------------------------------------------------------------------
// Requests to other services
// ---------------------------------------------------------------------------

// call sends one request and returns the reply status and body. Transport
// failures map to 503 with the given code; a disconnected broker is always
// broker_unavailable.
func (s *Service) call(ctx context.Context, subject string, teamID, requestID int64, payload any, downCode string) (int, []byte, *apiError) {
	data, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, fail(500, codeInternal, "request encoding failed")
	}
	header := map[string]string{broker.HeaderTeam: strconv.FormatInt(teamID, 10), broker.HeaderRequestID: strconv.FormatInt(requestID, 10)}
	ctx, cancel := context.WithTimeout(ctx, s.o.RequestTimeout)
	defer cancel()
	type answer struct {
		r   broker.Reply
		err error
	}
	ch := make(chan answer, 1)
	go func() {
		r, err := s.o.Broker.Request(ctx, subject, header, data)
		ch <- answer{r, err}
	}()
	var a answer
	select {
	case a = <-ch:
	case <-ctx.Done():
		a.err = broker.ErrNoResponse
	}
	if errors.Is(a.err, broker.ErrDisconnected) {
		return 0, nil, fail(503, codeBrokerDown, "message broker unavailable")
	}
	if a.err != nil {
		return 0, nil, fail(503, downCode, "a required service did not answer")
	}
	status := a.r.Status
	if status == 0 {
		status, _ = strconv.Atoi(a.r.Header[broker.HeaderStatus])
	}
	return status, a.r.Body, nil
}
