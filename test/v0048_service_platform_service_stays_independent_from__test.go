//go:build !integration

package test

import (
	"strings"
	"testing"
)

// Requirement: service-platform service stays independent from execution and identity.
//
// "Executor capacity is not decided centrally" is not applicable:
// flow-registry neither claims tasks nor tracks Executor capacity.

func TestV0048ServicePlatformServiceStaysIndependentFromExecutionAndIdentity(t *testing.T) {
	t.Run("Image equality across teams grants no authority", func(t *testing.T) {
		h := newHarness(t)
		a := h.createFlow(3, set("default_image", testImage))
		h.createFlow(4, set("default_image", testImage))
		h.ui(4, "flow-get", map[string]any{"flow_id": 2}, nil).expect(t, 404, "not_found")
		h.ui(4, "flow-replace", map[string]any{"flow_id": flowID(t, a)}, flowBody(set("execution_tag", "gpu"), set("model_aliases", []any{"4/m"}), set("name", "taken")), ifMatch(1)).expect(t, 200, "")
		if f, _ := h.store.GetFlow(h.ctx, 3, flowID(t, a)); f.Revision != 1 {
			t.Fatalf("team 4 write changed team 3's Flow with the same image")
		}
		r := h.admin("tags", map[string]any{"team_id": 4}, nil)
		if strings.Contains(string(r.Body), "ghcr.io") {
			t.Fatalf("tags reply carries the image")
		}
	})

	t.Run("Flow cannot be created from a Source path", func(t *testing.T) {
		h := newHarness(t)
		// task-manager asks for targets of a Source that no Flow binds.
		targetsOf(t, h, 3, 12).expect(t, 404, "not_found")
		for _, op := range []string{"flow-create", "flows-create", "flow-upsert"} {
			h.service("3", op, flowBody()).expect(t, 404, "not_found")
		}
		if h.mem.FlowCount() != 0 || len(h.requests(subjApplyFlowTeam)) != 0 {
			t.Fatalf("a Source-path request created a Flow or a LiteLLM team")
		}
	})
}
