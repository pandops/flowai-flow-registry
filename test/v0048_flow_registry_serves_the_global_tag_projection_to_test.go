//go:build !integration

package test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// Requirement: flow-registry serves the global tag projection to admin-panel.
//
// "Another identity requests the admin tag read" is enforced by the NATS
// subject permissions of the deployment, not by flow-registry; it is not
// applicable to flow-registry's service tests (no tests for infrastructure).

func TestV0048FlowRegistryServesTheGlobalTagProjection(t *testing.T) {
	t.Run("admin-panel reads tags across teams", func(t *testing.T) {
		h := newHarness(t)
		h.other.tags[4] = append(h.other.tags[4], "openhands")
		h.createFlow(3, set("name", "a"), set("default_image", testImage))
		// createFlow maps team 4's default tag to "gpu"; send this one directly.
		h.ui(4, "flow-create", nil, flowBody(set("name", "b"), set("model_aliases", []any{"4/m"}))).expect(t, 201, "")
		h.createFlow(4, set("name", "c"))
		h.createFlow(3, set("name", "d"))
		h.flush()
		commits := h.mem.Commits()
		r := h.admin("tags", nil, nil)
		r.expect(t, 200, "")
		var got struct {
			Items []map[string]any `json:"items"`
			Next  any              `json:"next"`
		}
		_ = json.Unmarshal(r.Body, &got)
		var order []string
		for _, it := range got.Items {
			if len(it) != 3 {
				t.Fatalf("entry carries extra fields: %v", it)
			}
			order = append(order, fmt.Sprintf("%v/%v/%v", it["team_id"], it["execution_tag"], it["flow_id"]))
		}
		if strings.Join(order, ",") != "3/openhands/1,3/openhands/2,4/gpu/2,4/openhands/1" {
			t.Fatalf("order = %v", order)
		}
		if got.Next != nil {
			t.Fatalf("next = %v, want null", got.Next)
		}
		for _, forbidden := range []string{"default_image", "ghcr.io", "prompt", "name"} {
			if strings.Contains(string(r.Body), forbidden) {
				t.Fatalf("reply carries %q: %s", forbidden, r.Body)
			}
		}
		// Only the audit outbox row is committed; no Flow changes.
		if h.mem.Commits() != commits+1 || h.mem.FlowCount() != 4 {
			t.Fatalf("admin read changed Flow data: commits %d -> %d", commits, h.mem.Commits())
		}

		// Filters and pagination.
		r = h.admin("tags", map[string]any{"tag": "openhands", "limit": 1}, nil)
		r.expect(t, 200, "")
		_ = json.Unmarshal(r.Body, &got)
		if len(got.Items) != 1 || got.Next == nil {
			t.Fatalf("first page = %s", r.Body)
		}
		var all []string
		all = append(all, fmt.Sprint(got.Items[0]["team_id"], "/", got.Items[0]["flow_id"]))
		for got.Next != nil {
			r = h.admin("tags", map[string]any{"tag": "openhands", "limit": 1}, map[string]any{"after": got.Next})
			r.expect(t, 200, "")
			got.Next = nil
			_ = json.Unmarshal(r.Body, &got)
			for _, it := range got.Items {
				all = append(all, fmt.Sprint(it["team_id"], "/", it["flow_id"]))
			}
		}
		if strings.Join(all, ",") != "3/1,3/2,4/1" {
			t.Fatalf("paged openhands entries = %v", all)
		}
		r = h.admin("tags", map[string]any{"team_id": 4}, nil)
		_ = json.Unmarshal(r.Body, &got)
		if len(got.Items) != 2 {
			t.Fatalf("team 4 filter = %s", r.Body)
		}
		r = h.admin("tags", nil, nil)
		_ = json.Unmarshal(r.Body, &got)
		if len(got.Items) != 4 {
			t.Fatalf("default limit returned %d entries", len(got.Items))
		}
		for _, after := range []any{"not-a-position", map[string]any{"team_id": 3}, map[string]any{"team_id": "3", "execution_tag": "openhands", "flow_id": 1}} {
			h.admin("tags", nil, map[string]any{"after": after}).expect(t, 400, "invalid_request")
		}
		for _, bad := range []map[string]any{{"limit": 201}, {"limit": 0}, {"team_id": "3"}, {"tag": "bad tag"}} {
			h.admin("tags", bad, nil).expect(t, 400, "invalid_request")
		}
	})

	t.Run("Default page size is 50", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 51; i++ {
			h.createFlow(3, set("name", fmt.Sprintf("f%02d", i)))
		}
		r := h.admin("tags", nil, nil)
		r.expect(t, 200, "")
		var got struct {
			Items []any `json:"items"`
			Next  any   `json:"next"`
		}
		_ = json.Unmarshal(r.Body, &got)
		if len(got.Items) != 50 || got.Next == nil {
			t.Fatalf("default page = %d items, next %v", len(got.Items), got.Next)
		}
	})

	t.Run("Admin tag read is audited by flow-registry", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3)
		h.flush()
		h.admin("tags", map[string]any{"tag": "openhands"}, nil, withRequestID(4242)).expect(t, 200, "")
		// The audit row is committed before the reply.
		rows := h.mem.Outbox()
		if len(rows) == 0 || !strings.Contains(rows[len(rows)-1].Subject, "admin-tags") {
			t.Fatalf("no committed admin-tags outbox row before the reply: %+v", rows)
		}
		audits := h.audits()
		if len(audits) != 1 {
			t.Fatalf("audits = %v, want one", audits)
		}
		a := audits[0]
		if a["_subject"] != "flow-registry.audit.admin-tags.pub" || a["_team"] != "admin" {
			t.Fatalf("audit subject/team = %v/%v", a["_subject"], a["_team"])
		}
		if a["actor_type"] != "administrator" || a["actor_subject"] != "admin@example.test" || a["outcome"] != "succeeded" || num(t, a["request_id"]) != 4242 {
			t.Fatalf("audit = %v", a)
		}
		if _, ok := a["team_id"]; ok {
			t.Fatalf("admin audit carries team_id: %v", a)
		}
		if !strings.Contains(string(mustJSON(a["changes"])), "openhands") {
			t.Fatalf("audit lacks the filter: %v", a["changes"])
		}
		// A rejected admin read is audited as rejected.
		h.admin("tags", map[string]any{"limit": 500}, nil).expect(t, 400, "invalid_request")
		audits = h.audits()
		if len(audits) != 1 || audits[0]["outcome"] != "rejected" {
			t.Fatalf("rejected read audits = %v", audits)
		}
		// Without the administrator subject the read is refused.
		h.admin("tags", nil, nil, withoutHeader("admin_subject")).expect(t, 400, "invalid_request")
		h.admin("tags", nil, nil, withTeamHeader("3")).expect(t, 400, "invalid_request")
	})
}
