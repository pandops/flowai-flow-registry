//go:build !integration

package test

import "testing"

// Requirement: message-broker carries the team in the team header.
// flow-registry rows: "ui request claims the admin team", plus the analogs
// for service and admin subjects and outgoing propagation. Executor rows are
// not applicable (flow-registry receives no Executor message).

func TestV0048MessageBrokerCarriesTheTeamInTheTeamHeader(t *testing.T) {
	t.Run("ui request claims the admin team", func(t *testing.T) {
		h := newHarness(t)
		for _, op := range uiOps {
			h.ui(3, op, map[string]any{"flow_id": 1}, flowBody(), withTeamHeader("admin")).expect(t, 400, "invalid_request")
		}
		if h.mem.FlowCount() != 0 || h.callsTo("") != 0 {
			t.Fatalf("team: admin ui request changed data or called another service")
		}
		if n := len(h.mem.Outbox()); n != 0 {
			t.Fatalf("unauthenticated requests stored %d outbox rows", n)
		}
	})

	t.Run("Payload team disagrees with the team header", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3, bindTo(map[int64]int{12: 5}, 12))
		h.service("4", "targets", map[string]any{"team_id": 3, "source_id": 12}).expect(t, 400, "invalid_request")
		h.service("admin", "targets", map[string]any{"team_id": 3, "source_id": 12}).expect(t, 200, "")
	})

	t.Run("Outgoing requests and messages carry the causing team", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(4)
		for _, r := range h.broker.Requests("") {
			if r.Header["team"] != "4" {
				t.Fatalf("%s team header = %q", r.Subject, r.Header["team"])
			}
		}
		for _, m := range h.flush() {
			if m.Header["team"] != "4" {
				t.Fatalf("%s team header = %q", m.Subject, m.Header["team"])
			}
		}
	})
}
