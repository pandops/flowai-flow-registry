//go:build !integration

package test

import (
	"strings"
	"testing"
)

// Requirement: service-platform service sends human actions to audit and others to journal.
//
// Not applicable to flow-registry: "Operator cancels a task" (task-manager)
// and "Executor event goes to journal" (Executors). flow-registry's analogs
// follow.

func TestV0048ServicePlatformServiceSendsHumanActionsToAudit(t *testing.T) {
	t.Run("Operator replaces a Flow", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3)
		h.flush()
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("name", "renamed")), ifMatch(1)).expect(t, 200, "")
		msgs := h.flush()
		if len(msgs) != 1 || !strings.HasPrefix(msgs[0].Subject, "flow-registry.audit.") {
			t.Fatalf("human edit published %+v, want one audit message and no event", msgs)
		}
	})

	t.Run("Forwarded administrator action is audited by its owner", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3)
		h.flush()
		h.admin("tags", nil, nil).expect(t, 200, "")
		msgs := h.flush()
		if len(msgs) != 1 || msgs[0].Subject != "flow-registry.audit.admin-tags.pub" || msgs[0].Header["team"] != "admin" {
			t.Fatalf("admin read published %+v", msgs)
		}
	})

	t.Run("Rejected human action is audited", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3)
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("name", "x")), ifMatch(1)).expect(t, 200, "")
		h.flush()
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("name", "y")), ifMatch(1)).expect(t, 412, "")
		a := h.audits()
		if len(a) != 1 || a[0]["outcome"] != "rejected" || a[0]["action"] != "flow-replace" {
			t.Fatalf("audits = %v", a)
		}
	})

	t.Run("Background restoration goes to events", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3)
		h.flush()
		h.mem.FailNextCommit(errInjected)
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("flow_limit", map[string]any{"max_budget_usd": "9", "budget_period": "day"})), ifMatch(1))
		h.svc.Wait()
		var events int
		for _, m := range h.flush() {
			if strings.HasPrefix(m.Subject, "flow-registry.event.") {
				events++
			}
		}
		if events != 1 {
			t.Fatalf("events = %d, want one flow_team_restored", events)
		}
	})
}
