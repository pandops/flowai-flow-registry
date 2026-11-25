//go:build !integration

package test

import (
	"bytes"
	"testing"
)

// Requirement: service-platform service returns non-revealing not-found for
// foreign resources. The spec examples name task-manager and env-registry;
// these are flow-registry's analogs for Flows.

func TestV0048ServicePlatformServiceReturnsNonRevealingNotFound(t *testing.T) {
	t.Run("Operator reads a foreign Flow", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 10; i++ {
			h.createFlow(4, set("name", "four-"+itoa(int64(i))))
		}
		foreign := h.ui(3, "flow-get", map[string]any{"flow_id": 10}, nil, withRequestID(9))
		unknown := h.ui(3, "flow-get", map[string]any{"flow_id": 11}, nil, withRequestID(9))
		foreign.expect(t, 404, "not_found")
		if !bytes.Equal(foreign.Body, unknown.Body) {
			t.Fatalf("foreign %s differs from unknown %s", foreign.Body, unknown.Body)
		}
		fs := h.service("3", "llm-settings", map[string]any{"team_id": 3, "flow_id": 10}, withRequestID(9))
		us := h.service("3", "llm-settings", map[string]any{"team_id": 3, "flow_id": 11}, withRequestID(9))
		if fs.Status != 404 || !bytes.Equal(fs.Body, us.Body) {
			t.Fatalf("service read: foreign %d %s, unknown %s", fs.Status, fs.Body, us.Body)
		}
	})

	t.Run("Operator writes a foreign Flow", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 5; i++ {
			h.createFlow(4, set("name", "four-"+itoa(int64(i))))
		}
		applies := len(h.requests(subjApplyFlowTeam))
		foreign := h.ui(3, "flow-replace", map[string]any{"flow_id": 5}, flowBody(), ifMatch(1), withRequestID(9))
		unknown := h.ui(3, "flow-replace", map[string]any{"flow_id": 6}, flowBody(), ifMatch(1), withRequestID(9))
		foreign.expect(t, 404, "not_found")
		if !bytes.Equal(foreign.Body, unknown.Body) {
			t.Fatalf("foreign %s differs from unknown %s", foreign.Body, unknown.Body)
		}
		// The 404 comes before any request to another service.
		if len(h.requests(subjApplyFlowTeam)) != applies {
			t.Fatalf("foreign write reached model-registry")
		}
		if f, _ := h.store.GetFlow(h.ctx, 4, 5); f.Revision != 1 {
			t.Fatalf("team 4 Flow changed")
		}
	})
}
