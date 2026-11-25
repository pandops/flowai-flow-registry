//go:build !integration

package test

import (
	"encoding/json"
	"strings"
	"testing"
)

// Requirement: flow-registry stores Flow LLM settings with the Flow revision.

func settingsBody(aliases []any, perTask, flow any) map[string]any {
	return map[string]any{"model_aliases": aliases, "per_task_limit": perTask, "flow_limit": flow}
}

func TestV0048FlowRegistryStoresFlowLLMSettings(t *testing.T) {
	t.Run("Operator configures a Flow", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3)
		id := flowID(t, v)
		h.flush()
		r := h.ui(3, "llm-settings-replace", map[string]any{"flow_id": id}, settingsBody(
			[]any{"3/gpt-4o", "3/claude-sonnet"},
			map[string]any{"max_budget_usd": "2", "rpm": 20},
			map[string]any{"max_budget_usd": "50", "budget_period": "week"},
		), ifMatch(1))
		r.expect(t, 200, "")
		view := r.json(t)
		if revision(t, view) != 2 || r.Header["ETag"] != `"2"` {
			t.Fatalf("revision %v ETag %q, want 2", view["revision"], r.Header["ETag"])
		}
		if strings.Join(strs(view["model_aliases"]), ",") != "3/gpt-4o,3/claude-sonnet" {
			t.Fatalf("model order not kept: %v", view["model_aliases"])
		}
		for _, forbidden := range []string{"state", "status", "api_key", "litellm_key"} {
			if _, ok := view[forbidden]; ok {
				t.Fatalf("settings carry %q", forbidden)
			}
		}
		calls := h.other.appliedCalls()
		last := calls[len(calls)-1]
		var limit map[string]any
		_ = json.Unmarshal(last.FlowLimit, &limit)
		if last.TeamID != 3 || last.FlowID != id || last.FlowRevision != 2 || limit["max_budget_usd"] != "50" || limit["budget_period"] != "week" {
			t.Fatalf("apply-flow-team = %+v limit %v, want team 3 Flow %d revision 2 with 50 USD per week", last, limit, id)
		}
		audits := h.audits()
		if len(audits) != 1 || audits[0]["action"] != "llm-settings-replace" || audits[0]["outcome"] != "succeeded" {
			t.Fatalf("audits = %v, want one succeeded llm-settings-replace", audits)
		}
		// The write preserves bindings and execution fields: no execution revision.
		if ex := h.mem.Executions(3, id); len(ex) != 1 {
			t.Fatalf("settings-only write appended an execution revision: %d", len(ex))
		}
	})

	t.Run("Operator selects a foreign or unknown model", func(t *testing.T) {
		for name, aliases := range map[string][]any{
			"foreign":   {"4/m"},
			"unknown":   {"3/missing"},
			"duplicate": {"3/gpt-4o", "3/gpt-4o"},
		} {
			t.Run(name, func(t *testing.T) {
				h := newHarness(t)
				v := h.createFlow(3)
				applies := len(h.requests(subjApplyFlowTeam))
				h.ui(3, "llm-settings-replace", map[string]any{"flow_id": flowID(t, v)}, settingsBody(aliases, nil, nil), ifMatch(1)).expect(t, 400, "invalid_request")
				if len(h.requests(subjApplyFlowTeam)) != applies {
					t.Fatalf("rejected selection changed the LiteLLM team")
				}
				got := h.ui(3, "llm-settings-get", map[string]any{"flow_id": flowID(t, v)}, nil)
				got.expect(t, 200, "")
				if revision(t, got.json(t)) != 1 || strings.Join(strs(got.json(t)["model_aliases"]), ",") != "3/gpt-4o" {
					t.Fatalf("record changed: %s", got.Body)
				}
			})
		}
	})

	t.Run("Budget period is missing", func(t *testing.T) {
		for name, limit := range map[string]any{
			"budget without period": map[string]any{"max_budget_usd": "10"},
			"period without budget": map[string]any{"budget_period": "day"},
			"unknown period":        map[string]any{"max_budget_usd": "10", "budget_period": "year"},
			"empty object":          map[string]any{},
			"three decimals":        map[string]any{"max_budget_usd": "1.001", "budget_period": "day"},
			"below minimum":         map[string]any{"max_budget_usd": "0.00", "budget_period": "day"},
			"above maximum":         map[string]any{"max_budget_usd": "1000000.01", "budget_period": "day"},
		} {
			t.Run(name, func(t *testing.T) {
				h := newHarness(t)
				v := h.createFlow(3)
				h.ui(3, "llm-settings-replace", map[string]any{"flow_id": flowID(t, v)}, settingsBody([]any{"3/gpt-4o"}, nil, limit), ifMatch(1)).expect(t, 400, "invalid_request")
				h.ui(3, "flow-create", nil, flowBody(set("name", "other"), set("flow_limit", limit))).expect(t, 400, "invalid_request")
			})
		}
		for name, limit := range map[string]any{
			"rpm zero":   map[string]any{"rpm": 0},
			"tpm huge":   map[string]any{"tpm": 100000001},
			"budget str": map[string]any{"max_budget_usd": 2},
		} {
			t.Run("per task "+name, func(t *testing.T) {
				h := newHarness(t)
				h.ui(3, "flow-create", nil, flowBody(set("per_task_limit", limit))).expect(t, 400, "invalid_request")
			})
		}
	})

	t.Run("Settings list while model-registry is down", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3, set("name", "a"), set("flow_limit", map[string]any{"max_budget_usd": "50", "budget_period": "week"}))
		h.createFlow(3, set("name", "b"), set("model_aliases", []any{"3/gpt-4o", "3/claude-sonnet"}))
		h.createFlow(4, set("name", "foreign"))
		h.other.set(func(o *others) { o.statusDown = true })
		r := h.ui(3, "llm-settings-list", nil, nil)
		r.expect(t, 200, "")
		items, _ := r.json(t)["items"].([]any)
		if len(items) != 2 {
			t.Fatalf("items = %d, want the 2 Flows of team 3", len(items))
		}
		for _, it := range items {
			item := it.(map[string]any)
			if item["usage"] != nil {
				t.Fatalf("usage = %v, want null", item["usage"])
			}
			for _, m := range item["models"].([]any) {
				if m.(map[string]any)["availability"] != "unavailable" {
					t.Fatalf("availability = %v, want unavailable", m)
				}
			}
		}
		// While model-registry answers, availability and usage come from flows-status.
		h.other.set(func(o *others) {
			o.statusDown = false
			o.usage[[2]int64{3, 1}] = usageState{Spend: "12.4", ResetAt: "2026-11-09T00:00:00.000Z"}
			o.models[3]["3/claude-sonnet"] = modelState{Availability: "paused", Selectable: true}
		})
		r = h.ui(3, "llm-settings-list", nil, nil)
		r.expect(t, 200, "")
		items = r.json(t)["items"].([]any)
		first, second := items[0].(map[string]any), items[1].(map[string]any)
		usage, _ := first["usage"].(map[string]any)
		if usage == nil || usage["spend_usd"] != "12.4" || usage["max_budget_usd"] != "50" || usage["budget_reset_at"] != "2026-11-09T00:00:00.000Z" || len(usage) != 3 {
			t.Fatalf("usage = %v, want the flows-status spend", first["usage"])
		}
		models := second["models"].([]any)
		if models[1].(map[string]any)["availability"] != "paused" {
			t.Fatalf("availability = %v, want paused from flows-status", models)
		}
		reqs := h.requests(subjFlowsStatus)
		if !strings.Contains(string(reqs[len(reqs)-1].Body), `"team_id":3`) || strings.Contains(string(reqs[len(reqs)-1].Body), "foreign") {
			t.Fatalf("flows-status request is not team 3 only: %s", reqs[len(reqs)-1].Body)
		}
	})

	t.Run("model-registry reads current settings", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3, set("name", "one"))
		v := h.createFlow(3, set("name", "two"))
		h.createFlow(4, set("name", "two"))
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("name", "two"),
			set("model_aliases", []any{"3/claude-sonnet", "3/gpt-4o"}), set("per_task_limit", map[string]any{"tpm": 1000})), ifMatch(1)).expect(t, 200, "")
		r := h.service("3", "llm-settings", map[string]any{"team_id": 3, "flow_id": 2})
		r.expect(t, 200, "")
		got := r.json(t)
		if len(got) != 4 || num(t, got["revision"]) != 2 {
			t.Fatalf("llm-settings = %v", got)
		}
		if strings.Join(strs(got["model_aliases"]), ",") != "3/claude-sonnet,3/gpt-4o" {
			t.Fatalf("aliases = %v, want the committed list", got["model_aliases"])
		}
		if lim, _ := got["per_task_limit"].(map[string]any); lim == nil || lim["tpm"] != float64(1000) {
			t.Fatalf("per_task_limit = %v", got["per_task_limit"])
		}
		h.service("3", "llm-settings", map[string]any{"team_id": 3, "flow_id": 9}).expect(t, 404, "")
	})

	t.Run("Shared revision with the Flow resource", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3)
		id := flowID(t, v)
		h.ui(3, "flow-replace", map[string]any{"flow_id": id}, flowBody(set("name", "renamed")), ifMatch(1)).expect(t, 200, "")
		h.ui(3, "llm-settings-replace", map[string]any{"flow_id": id}, settingsBody([]any{"3/gpt-4o"}, nil, nil), ifMatch(1)).expect(t, 412, "")
		got := h.ui(3, "llm-settings-get", map[string]any{"flow_id": id}, nil)
		got.expect(t, 200, "")
		if got.Header["ETag"] != `"2"` {
			t.Fatalf("settings ETag = %q, want the shared revision \"2\"", got.Header["ETag"])
		}
	})
}
