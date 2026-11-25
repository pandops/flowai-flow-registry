//go:build !integration

package test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// Requirement: flow-registry serves Flow operations only over its broker subjects.
//
// "Service operation from a caller it does not list" is enforced by NATS
// subject permissions in the deployment; it is not applicable to
// flow-registry's service tests (no tests for infrastructure).

var uiOps = []string{"flows-list", "flow-create", "flow-get", "flow-replace", "flow-preview", "llm-settings-list", "llm-settings-get", "llm-settings-replace"}
var serviceOps = []string{"targets", "llm-settings", "alias-usage", "retire-alias", "restore-alias", "flow-owner"}

func TestV0048FlowRegistryServesFlowOperationsOnlyOverItsBrokerSubjects(t *testing.T) {
	t.Run("Subscriptions are exactly the ui, service and admin subjects", func(t *testing.T) {
		h := newHarness(t)
		got := append([]string(nil), h.svc.Subjects()...)
		sort.Strings(got)
		want := []string{"flow-registry.admin.*.sub", "flow-registry.service.*.sub", "flow-registry.ui.*.sub"}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("subjects = %v, want %v", got, want)
		}
		// Operation tokens come from the schema files flow-registry ships.
		files, _ := filepath.Glob(filepath.Join(serviceDir(), "schemas", "*.request.schema.json"))
		var ops []string
		for _, f := range files {
			parts := strings.Split(filepath.Base(f), ".")
			if len(parts) != 5 || (parts[0] != "ui" && parts[0] != "service" && parts[0] != "admin") {
				t.Fatalf("schema file name %s", filepath.Base(f))
			}
			if !regexp.MustCompile(`^[a-z]+(-[a-z]+)*$`).MatchString(parts[1]) {
				t.Fatalf("operation token %q", parts[1])
			}
			ops = append(ops, parts[0]+"."+parts[1])
		}
		var specOps []string
		for _, op := range uiOps {
			specOps = append(specOps, "ui."+op)
		}
		for _, op := range serviceOps {
			specOps = append(specOps, "service."+op)
		}
		specOps = append(specOps, "admin.tags")
		sort.Strings(ops)
		sort.Strings(specOps)
		if strings.Join(ops, ",") != strings.Join(specOps, ",") {
			t.Fatalf("request schemas %v, want the spec operations %v", ops, specOps)
		}
		// Unknown operations are not served.
		h.ui(3, "flow-delete", nil, nil).expect(t, 404, "not_found")
		h.service("3", "flows-list", map[string]any{"team_id": 3}).expect(t, 404, "not_found")
	})

	t.Run("Operator request arrives on the ui subject", func(t *testing.T) {
		h := newHarness(t)
		r := h.ui(3, "flow-create", nil, flowBody())
		r.expect(t, 201, "")
		v := r.json(t)
		if r.Header["ETag"] != `"1"` || r.Header["status"] != "201" {
			t.Fatalf("reply headers = %v", r.Header)
		}
		if f, err := h.store.GetFlow(h.ctx, 3, flowID(t, v)); err != nil || f.TeamID != 3 {
			t.Fatalf("Flow not owned by the team header: %+v %v", f, err)
		}
		// API Gateway always sends params, empty for create and preview.
		gw := func(op string, body any) reply {
			data := mustJSON(map[string]any{"params": map[string]any{}, "body": body})
			return h.deliver("flow-registry.ui."+op+".pub", map[string]string{"team": "3", "operator_id": "7", "request_id": "77"}, data)
		}
		gw("flow-create", flowBody(set("name", "gateway form"))).expect(t, 201, "")
		gw("flow-preview", map[string]any{"prompt_template": "Solve {{external_id}}", "external_id": "A"}).expect(t, 200, "")
		h.ui(3, "flow-create", map[string]any{"flow_id": 1}, flowBody(set("name", "with params"))).expect(t, 400, "invalid_request")
		// A body team_id never sets ownership.
		h.ui(3, "flow-create", nil, flowBody(set("name", "x"), set("team_id", 4))).expect(t, 400, "invalid_request")
		// Missing or malformed operator context.
		h.ui(3, "flow-create", nil, flowBody(set("name", "y")), withoutHeader("team")).expect(t, 400, "invalid_request")
		h.ui(3, "flow-create", nil, flowBody(set("name", "y")), withTeamHeader("admin")).expect(t, 400, "invalid_request")
		h.ui(3, "flow-create", nil, flowBody(set("name", "y")), withoutHeader("operator_id")).expect(t, 400, "invalid_request")
		h.ui(3, "flow-create", nil, flowBody(set("name", "y")), withHeader("operator_id", "abc")).expect(t, 400, "invalid_request")
		// team_name is display only.
		h.ui(3, "flow-create", nil, flowBody(set("name", "z")), withHeader("team_name", "Team Four")).expect(t, 201, "")
		if h.mem.FlowCount() != 3 {
			t.Fatalf("Flow count = %d", h.mem.FlowCount())
		}
		// Every ui operation is served.
		id := flowID(t, v)
		for op, params := range map[string]map[string]any{
			"flows-list": nil, "flow-get": {"flow_id": id}, "llm-settings-list": nil, "llm-settings-get": {"flow_id": id},
		} {
			h.ui(3, op, params, nil).expect(t, 200, "")
		}
		// The list is paged with a cursor bound to the operator.
		for i := 0; i < 3; i++ {
			h.createFlow(3, set("name", "page"+itoa(int64(i))))
		}
		first := h.ui(3, "flows-list", map[string]any{"limit": 2}, nil)
		first.expect(t, 200, "")
		cur, _ := first.json(t)["next_cursor"].(string)
		if len(first.json(t)["items"].([]any)) != 2 || cur == "" {
			t.Fatalf("first page = %s", first.Body)
		}
		h.ui(3, "flows-list", map[string]any{"limit": 2, "cursor": cur}, nil).expect(t, 200, "")
		h.ui(3, "flows-list", map[string]any{"limit": 2, "cursor": cur}, nil, withOperator(8)).expect(t, 400, "invalid_request")
	})

	t.Run("Operator previews a prompt through the ui subject", func(t *testing.T) {
		h := newHarness(t)
		r := h.ui(3, "flow-preview", nil, map[string]any{"prompt_template": "Solve {{external_id}}", "external_id": "ISSUE-1"})
		r.expect(t, 200, "")
		if h.mem.FlowCount() != 0 {
			t.Fatalf("preview wrote a Flow")
		}
	})

	t.Run("Published audit message matches its schema", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3)
		h.flush()
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("name", "renamed")), ifMatch(1)).expect(t, 200, "")
		msgs := h.flush() // validates every message against audit.<action>.schema.json
		if len(msgs) != 1 || msgs[0].Subject != "flow-registry.audit.flow-replace.pub" {
			t.Fatalf("published = %+v", msgs)
		}
		// Every published subject has a schema file.
		for _, m := range h.broker.Published() {
			name := "audit." + strings.TrimSuffix(strings.TrimPrefix(m.Subject, "flow-registry.audit."), ".pub") + ".schema.json"
			if strings.HasPrefix(m.Subject, "flow-registry.event.") {
				name = "event." + strings.TrimSuffix(strings.TrimPrefix(m.Subject, "flow-registry.event."), ".pub") + ".schema.json"
			}
			if _, err := os.Stat(filepath.Join(serviceDir(), "schemas", name)); err != nil {
				t.Fatalf("no schema for %s", m.Subject)
			}
		}
	})

	t.Run("Service request carries operator headers", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3, bindTo(map[int64]int{12: 5}, 12))
		r := h.service("3", "targets", map[string]any{"team_id": 3, "source_id": 12}, withOperator(7), withHeader("team_name", "x"))
		r.expect(t, 200, "")
		h.service("3", "targets", map[string]any{"team_id": 4, "source_id": 12}).expect(t, 400, "invalid_request")
		h.service("3", "llm-settings", map[string]any{"team_id": 4, "flow_id": 1}, withOperator(7)).expect(t, 400, "invalid_request")
		h.service("3", "flow-owner", map[string]any{"team_id": 4, "flow_id": 1}).expect(t, 400, "invalid_request")
		h.service("x", "flow-owner", map[string]any{"team_id": 3, "flow_id": 1}).expect(t, 400, "invalid_request")
		// Operator headers do not unlock ui operations on service subjects.
		h.service("3", "flow-create", flowBody(), withOperator(7)).expect(t, 404, "not_found")
		if h.mem.FlowCount() != 1 {
			t.Fatalf("service subject created a Flow")
		}
		// Unknown body fields are rejected.
		h.service("3", "targets", map[string]any{"team_id": 3, "source_id": 12, "operator_id": 7}).expect(t, 400, "invalid_request")
	})

	t.Run("Foreign Flow is not revealed", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3, set("name", "one"))
		h.createFlow(3, set("name", "Team three secret"))
		foreign := []reply{
			h.ui(4, "flow-get", map[string]any{"flow_id": 2}, nil),
			h.ui(4, "flow-replace", map[string]any{"flow_id": 2}, flowBody(set("execution_tag", "gpu"), set("model_aliases", []any{"4/m"})), ifMatch(1)),
			h.ui(4, "llm-settings-get", map[string]any{"flow_id": 2}, nil),
			h.ui(4, "llm-settings-replace", map[string]any{"flow_id": 2}, settingsBody([]any{"4/m"}, nil, nil), ifMatch(1)),
		}
		unknown := []reply{
			h.ui(4, "flow-get", map[string]any{"flow_id": 77}, nil),
			h.ui(4, "flow-replace", map[string]any{"flow_id": 77}, flowBody(set("execution_tag", "gpu"), set("model_aliases", []any{"4/m"})), ifMatch(1)),
			h.ui(4, "llm-settings-get", map[string]any{"flow_id": 77}, nil),
			h.ui(4, "llm-settings-replace", map[string]any{"flow_id": 77}, settingsBody([]any{"4/m"}, nil, nil), ifMatch(1)),
		}
		for i := range foreign {
			if foreign[i].Status != 404 || foreign[i].code(t) != unknown[i].code(t) || foreign[i].Status != unknown[i].Status {
				t.Fatalf("foreign %d: %d %s; unknown %d %s", i, foreign[i].Status, foreign[i].Body, unknown[i].Status, unknown[i].Body)
			}
			if strings.Contains(string(foreign[i].Body), "secret") {
				t.Fatalf("foreign reply reveals team 3: %s", foreign[i].Body)
			}
		}
		list := h.ui(4, "flows-list", nil, nil)
		if len(list.json(t)["items"].([]any)) != 0 {
			t.Fatalf("team 4 list shows team 3 Flows: %s", list.Body)
		}
		if f, _ := h.store.GetFlow(h.ctx, 3, 2); f.Revision != 1 {
			t.Fatalf("foreign replace changed team 3 Flow")
		}
	})

	t.Run("Non-integer Flow identifier is rejected", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3)
		calls := h.callsTo("")
		commits := h.mem.Commits()
		for _, id := range []any{"f-1", "0", "-2", "a0c4e8f2-1b3d-4e5f-8a9b-0c1d2e3f4a5b", "+1", "01", "1.5", "9223372036854775808", 0, -2, 1.5} {
			h.ui(3, "flow-get", map[string]any{"flow_id": id}, nil).expect(t, 400, "invalid_request")
			h.ui(3, "llm-settings-replace", map[string]any{"flow_id": id}, settingsBody([]any{"3/gpt-4o"}, nil, nil), ifMatch(1)).expect(t, 400, "invalid_request")
			h.service("3", "flow-owner", map[string]any{"team_id": 3, "flow_id": id}).expect(t, 400, "invalid_request")
			h.service("3", "targets", map[string]any{"team_id": 3, "source_id": id}).expect(t, 400, "invalid_request")
		}
		h.ui(3, "flow-create", nil, flowBody(bindTo(map[int64]int{12: 5}, 12), set("source_ids", []any{"12"}))).expect(t, 400, "invalid_request")
		h.ui(3, "flow-replace", map[string]any{"flow_id": 1}, flowBody(), withHeader("If-Match", `"abc"`)).expect(t, 400, "invalid_request")
		if h.callsTo("") != calls {
			t.Fatalf("invalid identifiers reached another service")
		}
		// Only rejected-request audit rows may be committed.
		for _, row := range h.mem.Outbox() {
			if !strings.HasPrefix(row.Subject, "flow-registry.audit.") {
				t.Fatalf("unexpected outbox row %q", row.Subject)
			}
		}
		_ = commits
	})
}
