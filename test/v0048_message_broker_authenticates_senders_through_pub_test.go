//go:build !integration

package test

import "testing"

// Requirement: message-broker authenticates senders through publish permissions.
// flow-registry rows: "ui request without a context header" and "Context
// headers on a service subject". The permission rows are NATS configuration
// (no tests for infrastructure).

func TestV0048MessageBrokerAuthenticatesSendersThroughPublishPermissions(t *testing.T) {
	t.Run("ui request without a context header", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3)
		for _, drop := range []string{"request_id", "team", "operator_id"} {
			h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("name", "x")), ifMatch(1), withoutHeader(drop)).expect(t, 400, "invalid_request")
			h.ui(3, "flows-list", nil, nil, withoutHeader(drop)).expect(t, 400, "invalid_request")
		}
		if f, _ := h.store.GetFlow(h.ctx, 3, flowID(t, v)); f.Revision != 1 {
			t.Fatalf("Flow changed")
		}
	})

	t.Run("Context headers on a service subject", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3)
		r := h.service("3", "flow-owner", map[string]any{"team_id": 3, "flow_id": 1}, withOperator(99), withHeader("team_name", "Other"))
		r.expect(t, 200, "")
		h.service("3", "flow-owner", map[string]any{"team_id": 3, "flow_id": 1}, withoutHeader("request_id")).expect(t, 400, "invalid_request")
		h.service("3", "flow-owner", map[string]any{"team_id": 3, "flow_id": 1}, withoutHeader("team")).expect(t, 400, "invalid_request")
	})
}
