//go:build !integration

package test

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

// Requirement: flow-registry removes and restores archived model aliases on request.

func retire(h *harness, alias string) reply {
	h.t.Helper()
	return h.service("3", "retire-alias", map[string]any{"team_id": 3, "alias": alias, "operator_id": 7})
}

func TestV0048FlowRegistryRemovesAndRestoresArchivedModelAliases(t *testing.T) {
	setup := func(t *testing.T) *harness {
		h := newHarness(t)
		h.other.addModel(3, "3/m1", modelState{Availability: "active", Selectable: true})
		return h
	}

	t.Run("Archive removes every Flow reference", func(t *testing.T) {
		h := setup(t)
		a := h.createFlow(3, set("name", "a"), set("model_aliases", []any{"3/gpt-4o", "3/m1", "3/claude-sonnet"}))
		b := h.createFlow(3, set("name", "b"), set("model_aliases", []any{"3/m1", "3/claude-sonnet"}))
		c := h.createFlow(3, set("name", "c"))
		h.createFlow(4, set("name", "a"))
		h.flush()
		usage := h.service("3", "alias-usage", map[string]any{"team_id": 3, "alias": "3/m1"})
		usage.expect(t, 200, "")
		if len(usage.json(t)["flows"].([]any)) != 2 || len(usage.json(t)["sole_alias_flows"].([]any)) != 0 {
			t.Fatalf("alias-usage = %s", usage.Body)
		}
		retire(h, "3/m1").expect(t, 204, "")
		ga := h.ui(3, "flow-get", map[string]any{"flow_id": flowID(t, a)}, nil).json(t)
		gb := h.ui(3, "flow-get", map[string]any{"flow_id": flowID(t, b)}, nil).json(t)
		gc := h.ui(3, "flow-get", map[string]any{"flow_id": flowID(t, c)}, nil).json(t)
		if strings.Join(strs(ga["model_aliases"]), ",") != "3/gpt-4o,3/claude-sonnet" || revision(t, ga) != 2 {
			t.Fatalf("Flow a = %v", ga)
		}
		if strings.Join(strs(gb["model_aliases"]), ",") != "3/claude-sonnet" || revision(t, gb) != 2 {
			t.Fatalf("Flow b = %v", gb)
		}
		if revision(t, gc) != 1 {
			t.Fatalf("unrelated Flow changed: %v", gc)
		}
		var retireAudits []map[string]any
		for _, a := range h.audits() {
			if a["action"] == "retire-alias" {
				retireAudits = append(retireAudits, a)
			}
		}
		if len(retireAudits) != 2 {
			t.Fatalf("retire audits = %v, want one per affected Flow", retireAudits)
		}
		for _, a := range retireAudits {
			if num(t, a["actor_id"]) != 7 || num(t, a["team_id"]) != 3 || a["outcome"] != "succeeded" || a["resource_id"] == nil {
				t.Fatalf("retire audit = %v", a)
			}
		}
		// The retired alias is rejected by every write.
		h.ui(3, "flow-create", nil, flowBody(set("name", "new"), set("model_aliases", []any{"3/m1"}))).expect(t, 400, "invalid_request")
		h.ui(3, "llm-settings-replace", map[string]any{"flow_id": flowID(t, c)}, settingsBody([]any{"3/gpt-4o", "3/m1"}, nil, nil), ifMatch(1)).expect(t, 400, "invalid_request")
		// A repeated retirement succeeds without another change.
		retire(h, "3/m1").expect(t, 204, "")
		if g := h.ui(3, "flow-get", map[string]any{"flow_id": flowID(t, a)}, nil).json(t); revision(t, g) != 2 {
			t.Fatalf("repeat retire changed Flow a: %v", g)
		}
		for _, a := range h.audits() {
			if a["action"] == "retire-alias" {
				t.Fatalf("repeat retire published an audit: %v", a)
			}
		}
		// Restore lets the alias be selected again without restoring references.
		h.service("3", "restore-alias", map[string]any{"team_id": 3, "alias": "3/m1"}).expect(t, 204, "")
		gb = h.ui(3, "flow-get", map[string]any{"flow_id": flowID(t, b)}, nil).json(t)
		if strings.Join(strs(gb["model_aliases"]), ",") != "3/claude-sonnet" {
			t.Fatalf("restore re-added references: %v", gb["model_aliases"])
		}
		h.ui(3, "flow-create", nil, flowBody(set("name", "new"), set("model_aliases", []any{"3/m1"}))).expect(t, 201, "")
		// Retirement is per team.
		h.service("4", "retire-alias", map[string]any{"team_id": 4, "alias": "4/m", "operator_id": 9}).expect(t, 409, "model_last_in_flow")
	})

	t.Run("Sole selected model blocks retirement", func(t *testing.T) {
		h := setup(t)
		a := h.createFlow(3, set("name", "only m1"), set("model_aliases", []any{"3/m1"}))
		b := h.createFlow(3, set("name", "two"), set("model_aliases", []any{"3/gpt-4o", "3/m1"}))
		usage := h.service("3", "alias-usage", map[string]any{"team_id": 3, "alias": "3/m1"}).json(t)
		if len(usage["sole_alias_flows"].([]any)) != 1 {
			t.Fatalf("alias-usage = %v", usage)
		}
		r := retire(h, "3/m1")
		r.expect(t, 409, "model_last_in_flow")
		mustValidate(t, "service.retire-alias.reply.schema.json", r.Body)
		var e struct {
			Flows []struct {
				FlowID int64  `json:"flow_id"`
				Name   string `json:"name"`
			} `json:"flows"`
		}
		_ = json.Unmarshal(r.Body, &e)
		if len(e.Flows) != 1 || e.Flows[0].FlowID != flowID(t, a) || e.Flows[0].Name != "only m1" {
			t.Fatalf("409 flows = %s", r.Body)
		}
		for _, v := range []map[string]any{a, b} {
			if g := h.ui(3, "flow-get", map[string]any{"flow_id": flowID(t, v)}, nil).json(t); revision(t, g) != 1 {
				t.Fatalf("blocked retirement changed Flow %v", g)
			}
		}
		// The alias was not recorded as retired.
		h.ui(3, "flow-create", nil, flowBody(set("name", "new"), set("model_aliases", []any{"3/m1"}))).expect(t, 201, "")
	})

	t.Run("Flow write races retirement", func(t *testing.T) {
		for i := 0; i < 20; i++ {
			h := setup(t)
			v := h.createFlow(3, set("model_aliases", []any{"3/gpt-4o", "3/m1"}))
			var wg sync.WaitGroup
			var write, ret reply
			wg.Add(2)
			go func() {
				defer wg.Done()
				write = h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("model_aliases", []any{"3/m1"})), ifMatch(1))
			}()
			go func() { defer wg.Done(); ret = retire(h, "3/m1") }()
			wg.Wait()
			got := h.ui(3, "flow-get", map[string]any{"flow_id": flowID(t, v)}, nil).json(t)
			aliases := strs(got["model_aliases"])
			if len(aliases) == 0 {
				t.Fatalf("Flow left without an alias")
			}
			switch {
			case write.Status == 200 && ret.Status == 409:
				// The write committed first and the check saw it.
				if strings.Join(aliases, ",") != "3/m1" {
					t.Fatalf("aliases = %v", aliases)
				}
			case ret.Status == 204 && (write.Status == 400 || write.Status == 412):
				if strings.Join(aliases, ",") != "3/gpt-4o" {
					t.Fatalf("aliases = %v", aliases)
				}
			default:
				t.Fatalf("race outcome write %d (%s) retire %d (%s)", write.Status, write.Body, ret.Status, ret.Body)
			}
		}
		// Deterministic order: retirement first, then the write.
		h := setup(t)
		v := h.createFlow(3, set("model_aliases", []any{"3/gpt-4o", "3/m1"}))
		retire(h, "3/m1").expect(t, 204, "")
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("model_aliases", []any{"3/m1"})), ifMatch(2)).expect(t, 400, "invalid_request")
	})
}
