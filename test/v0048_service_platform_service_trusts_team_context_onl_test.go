//go:build !integration

package test

import "testing"

// Requirement: service-platform service trusts team context only from ui requests.

func TestV0048ServicePlatformServiceTrustsTeamContextOnlyFromUIRequests(t *testing.T) {
	t.Run("Gateway forwards verified operator context", func(t *testing.T) {
		h := newHarness(t)
		h.ui(3, "flow-create", nil, flowBody(set("name", "with name"))).expect(t, 201, "")
		h.ui(3, "flow-create", nil, flowBody(set("name", "without name")), withoutHeader("team_name")).expect(t, 201, "")
		audits := h.audits()
		if len(audits) != 2 {
			t.Fatalf("audits = %d", len(audits))
		}
		for _, a := range audits {
			if num(t, a["team_id"]) != 3 || num(t, a["actor_id"]) != 7 || a["request_id"] == nil {
				t.Fatalf("audit context = %v", a)
			}
		}
	})

	t.Run("Payload names another team", func(t *testing.T) {
		h := newHarness(t)
		h.ui(3, "flow-create", nil, flowBody(set("team_id", 4))).expect(t, 400, "invalid_request")
		h.ui(3, "flows-list", map[string]any{"team_id": 4}, nil).expect(t, 400, "invalid_request")
		h.ui(3, "flow-preview", nil, map[string]any{"prompt_template": "x", "external_id": "1", "team_id": 4}).expect(t, 400, "invalid_request")
		if h.mem.FlowCount() != 0 {
			t.Fatalf("a Flow was created")
		}
	})

	t.Run("Context is missing", func(t *testing.T) {
		h := newHarness(t)
		// D12 owner decision 2026-11-09: no valid request_id, no request_id
		// field; every other error keeps it.
		for _, drop := range []string{"team", "operator_id"} {
			r := h.ui(3, "flow-create", nil, flowBody(), withRequestID(77), withoutHeader(drop))
			r.expect(t, 400, "invalid_request")
			if rid, has := r.json(t)["request_id"]; !has || rid != float64(77) {
				t.Fatalf("without %s: %s, want request_id 77", drop, r.Body)
			}
		}
		for _, bad := range []reqOpt{withTeamHeader("admin"), withTeamHeader("0"), withTeamHeader("03"), withTeamHeader("x"), withOperator(0)} {
			r := h.ui(3, "flow-create", nil, flowBody(), withRequestID(77), bad)
			r.expect(t, 400, "invalid_request")
			if r.json(t)["request_id"] != float64(77) {
				t.Fatalf("bad context: %s, want request_id 77", r.Body)
			}
		}
		for _, bad := range []reqOpt{withoutHeader("request_id"), withHeader("request_id", "r-1"), withHeader("request_id", "0"), withHeader("request_id", "-1")} {
			r := h.ui(3, "flow-create", nil, flowBody(), bad)
			r.expect(t, 400, "invalid_request")
			if _, has := r.json(t)["request_id"]; has {
				t.Fatalf("invalid request_id: %s, want no request_id field", r.Body)
			}
		}
		r := h.service("3", "flow-owner", map[string]any{"team_id": 3, "flow_id": 1}, withoutHeader("request_id"))
		r.expect(t, 400, "invalid_request")
		if _, has := r.json(t)["request_id"]; has {
			t.Fatalf("service request without request_id: %s, want no request_id field", r.Body)
		}
		if h.mem.FlowCount() != 0 || h.callsTo("") != 0 {
			t.Fatalf("a request without context changed data or called another service")
		}
		// admin subjects require team: admin.
		h.admin("tags", nil, nil, withTeamHeader("3")).expect(t, 400, "invalid_request")
	})
}
