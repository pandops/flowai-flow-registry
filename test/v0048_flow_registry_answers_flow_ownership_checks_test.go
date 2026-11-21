//go:build !integration

package test

import (
	"strings"
	"testing"
)

// Requirement: flow-registry answers Flow ownership checks.

func TestV0048FlowRegistryAnswersFlowOwnershipChecks(t *testing.T) {
	t.Run("env-registry checks a same-team Flow", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3, set("name", "one"))
		h.createFlow(3, set("name", "two"))
		commits := h.mem.Commits()
		r := h.service("3", "flow-owner", map[string]any{"team_id": 3, "flow_id": 2})
		r.expect(t, 200, "")
		got := r.json(t)
		if len(got) != 1 || num(t, got["team_id"]) != 3 {
			t.Fatalf("flow-owner = %s, want only team_id 3", r.Body)
		}
		if h.mem.Commits() != commits {
			t.Fatalf("ownership read wrote data")
		}
	})

	t.Run("env-registry checks an unknown Flow", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3, set("name", "one"))
		h.createFlow(3, set("name", "two"))
		for _, req := range []map[string]any{{"team_id": 4, "flow_id": 2}, {"team_id": 3, "flow_id": 99}} {
			team := itoa(int64(req["team_id"].(int)))
			r := h.service(team, "flow-owner", req)
			r.expect(t, 404, "flow_not_found")
			if strings.Contains(string(r.Body), "\"team_id\"") {
				t.Fatalf("404 reveals an owner: %s", r.Body)
			}
		}
	})

	t.Run("Lookup by flow_id alone is rejected", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3)
		h.service("3", "flow-owner", map[string]any{"flow_id": 1}).expect(t, 400, "invalid_request")
		h.service("3", "flow-owner", map[string]any{"team_id": 4, "flow_id": 1}).expect(t, 400, "invalid_request")
	})
}
