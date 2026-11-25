//go:build !integration

package test

import (
	"errors"
	"testing"
)

// Requirement: service-platform service publishes changes through an outbox.
//
// "Synchronous publications without an outbox" is not applicable:
// flow-registry publishes only through its outbox.

func TestV0048ServicePlatformServicePublishesChangesThroughAnOutbox(t *testing.T) {
	t.Run("Rolled-back change publishes nothing", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3)
		h.flush()
		h.mem.FailNextCommit(errors.New("injected"))
		r := h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("name", "renamed")), ifMatch(1))
		if r.Status < 500 {
			t.Fatalf("failed commit answered %d", r.Status)
		}
		h.svc.Wait()
		for _, m := range h.flush() {
			if m.Subject == "flow-registry.audit.flow-replace.pub" {
				var a map[string]any
				_ = jsonUnmarshal(m.Data, &a)
				if a["outcome"] == "succeeded" {
					t.Fatalf("rolled-back replace published a succeeded audit")
				}
			}
		}
		if got := h.ui(3, "flow-get", map[string]any{"flow_id": flowID(t, v)}, nil).json(t); got["name"] == "renamed" {
			t.Fatalf("rolled-back change is visible")
		}
	})
}
