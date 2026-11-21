//go:build !integration

package test

import "testing"

// Requirement: service-platform service stores team ownership in normalized form.
//
// The database constraints (composite team_id foreign keys) are proven by
// test/integration/v0048_flow_store_integration_test.go ("Execution
// revisions are immutable and keep the Flow's team"); this unit test proves
// the service rules.

func TestV0048ServicePlatformServiceStoresTeamOwnershipInNormalizedForm(t *testing.T) {
	t.Run("Team-owned parent keeps its team", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3)
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("prompt_template", "New {{external_id}}"), set("team_id", 4)), ifMatch(1)).expect(t, 400, "invalid_request")
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("prompt_template", "New {{external_id}}")), ifMatch(1), withHeader("team_name", "Team Four")).expect(t, 200, "")
		f, err := h.store.GetFlow(h.ctx, 3, flowID(t, v))
		if err != nil || f.TeamID != 3 || f.Execution.Revision != 2 {
			t.Fatalf("Flow = %+v, %v", f, err)
		}
		if _, err := h.store.GetFlow(h.ctx, 4, flowID(t, v)); err == nil {
			t.Fatalf("Flow visible under team 4")
		}
	})

	t.Run("Child references a foreign-team parent", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3)
		// A team 4 Source cannot be bound to a team 3 Flow.
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(bindTo(map[int64]int{20: 1}, 20)), ifMatch(1)).expect(t, 400, "invalid_request")
		// A team 4 model cannot be attached to a team 3 Flow.
		h.ui(3, "llm-settings-replace", map[string]any{"flow_id": flowID(t, v)}, settingsBody([]any{"4/m"}, nil, nil), ifMatch(1)).expect(t, 400, "invalid_request")
		// A team 4 operator cannot add a revision to a team 3 Flow.
		h.ui(4, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("execution_tag", "gpu"), set("model_aliases", []any{"4/m"})), ifMatch(1)).expect(t, 404, "not_found")
		if len(h.mem.Executions(3, flowID(t, v))) != 1 || h.other.sourceRevision(4, 20) != 1 {
			t.Fatalf("a foreign child was attached")
		}
	})

	t.Run("Display metadata never authorizes", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3, set("name", "three"))
		r := h.ui(4, "flow-get", map[string]any{"flow_id": 1}, nil, withHeader("team_name", "Operations"))
		r.expect(t, 404, "not_found")
		r = h.ui(3, "flow-get", map[string]any{"flow_id": 1}, nil, withHeader("team_name", "Renamed team"))
		r.expect(t, 200, "")
		if _, ok := r.json(t)["team_name"]; ok {
			t.Fatalf("Flow view carries team_name")
		}
	})
}
