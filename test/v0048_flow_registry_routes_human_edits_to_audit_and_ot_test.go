//go:build !integration

package test

import (
	"strconv"
	"strings"
	"testing"
)

// Requirement: flow-registry routes human edits to audit and other changes to events.

func TestV0048FlowRegistryRoutesHumanEditsToAuditAndOtherChangesToEvents(t *testing.T) {
	t.Run("Rejected Flow write is audited", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3)
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("name", "renamed")), ifMatch(1)).expect(t, 200, "")
		h.flush()
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("prompt_template", "Secret prompt {{external_id}}"), set("name", "stale")), ifMatch(1), withRequestID(555)).expect(t, 412, "")
		audits := h.audits()
		if len(audits) != 1 {
			t.Fatalf("audits = %v, want one", audits)
		}
		a := audits[0]
		if a["_subject"] != "flow-registry.audit.flow-replace.pub" || a["outcome"] != "rejected" || a["action"] != "flow-replace" {
			t.Fatalf("audit = %v", a)
		}
		if num(t, a["team_id"]) != 3 || num(t, a["actor_id"]) != 7 || a["actor_type"] != "operator" || num(t, a["resource_id"]) != flowID(t, v) || num(t, a["request_id"]) != 555 {
			t.Fatalf("audit identity = %v", a)
		}
		if a["_team"] != "3" || a["publisher"] != "flow-registry" {
			t.Fatalf("audit header/publisher = %v", a)
		}
		if strings.Contains(string(mustJSON(a)), "Secret prompt") {
			t.Fatalf("audit carries prompt text: %v", a)
		}
		if got := h.ui(3, "flow-get", map[string]any{"flow_id": flowID(t, v)}, nil).json(t); got["name"] != "renamed" || revision(t, got) != 2 {
			t.Fatalf("rejected write changed the Flow: %v", got)
		}
		// Every authenticated rejected request, read or write, publishes
		// exactly one audit message; a successful read publishes none
		// (D15 owner decision 2026-11-09).
		h.flush()
		h.ui(3, "flow-create", nil, flowBody(set("agent_id", "x"))).expect(t, 400, "invalid_request")
		h.ui(3, "flow-get", map[string]any{"flow_id": 99}, nil).expect(t, 404, "")
		h.ui(3, "llm-settings-get", map[string]any{"flow_id": 99}, nil).expect(t, 404, "")
		h.ui(3, "flows-list", nil, nil).expect(t, 200, "")
		h.ui(3, "flow-get", map[string]any{"flow_id": flowID(t, v)}, nil).expect(t, 200, "")
		h.ui(3, "llm-settings-list", nil, nil).expect(t, 200, "")
		h.ui(3, "llm-settings-get", map[string]any{"flow_id": flowID(t, v)}, nil).expect(t, 200, "")
		h.ui(3, "flow-preview", nil, map[string]any{"prompt_template": "x", "external_id": "1"}).expect(t, 200, "")
		audits = h.audits()
		var actions []string
		for _, a := range audits {
			actions = append(actions, a["action"].(string)+":"+a["outcome"].(string))
			if strings.Contains(string(mustJSON(a)), "api_key") {
				t.Fatalf("audit carries a key: %v", a)
			}
		}
		if strings.Join(actions, ",") != "flow-create:rejected,flow-get:rejected,llm-settings-get:rejected" {
			t.Fatalf("audit actions = %v", actions)
		}
		// Requests without verified operator context are not audited.
		h.ui(3, "flow-get", map[string]any{"flow_id": 1}, nil, withoutHeader("operator_id")).expect(t, 400, "invalid_request")
		if n := len(h.audits()); n != 0 {
			t.Fatalf("unauthenticated request published %d audits", n)
		}
	})

	t.Run("publisher_seq and Nats-Msg-Id come from the outbox sequence", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3, set("name", "a"))
		h.createFlow(3, set("name", "b"))
		msgs := h.flush()
		if len(msgs) != 2 {
			t.Fatalf("published %d", len(msgs))
		}
		var prev int64
		for _, a := range h.decode(msgs) {
			seq := num(t, a["publisher_seq"])
			if seq <= prev || a["_msgid"] != "flow-registry-"+strconv.FormatInt(seq, 10) {
				t.Fatalf("seq %d after %d, msg id %v", seq, prev, a["_msgid"])
			}
			if !strings.HasSuffix(a["occurred_at"].(string), "Z") {
				t.Fatalf("occurred_at = %v", a["occurred_at"])
			}
			prev = seq
		}
	})

}

func (h *harness) decode(msgs []mocksPublished) []map[string]any {
	var out []map[string]any
	for _, m := range msgs {
		var v map[string]any
		_ = jsonUnmarshal(m.Data, &v)
		v["_msgid"] = m.Header["Nats-Msg-Id"]
		out = append(out, v)
	}
	return out
}
