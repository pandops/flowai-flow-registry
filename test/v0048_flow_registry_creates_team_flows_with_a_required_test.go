//go:build !integration

package test

import (
	"encoding/json"
	"strings"
	"testing"
)

// Requirement: flow-registry creates team Flows with a required executor tag and model.

func TestV0048FlowRegistryCreatesTeamFlows(t *testing.T) {
	t.Run("Operator creates a fully configured Flow", func(t *testing.T) {
		h := newHarness(t)
		r := h.ui(3, "flow-create", nil, flowBody(set("name", "Intake routing")))
		r.expect(t, 201, "")
		view := r.json(t)
		if flowID(t, view) != 1 {
			t.Fatalf("first Flow of team 3 has flow_id %v, want 1", view["flow_id"])
		}
		if revision(t, view) != 1 || r.Header["ETag"] != `"1"` {
			t.Fatalf("revision %v ETag %q, want 1 and \"1\"", view["revision"], r.Header["ETag"])
		}
		if view["execution_tag"] != "openhands" || strings.Join(strs(view["model_aliases"]), ",") != "3/gpt-4o" || view["prompt_template"] != "Solve {{external_id}}" {
			t.Fatalf("saved settings differ: %v", view)
		}
		for _, forbidden := range []string{"state", "status", "agent_id"} {
			if _, ok := view[forbidden]; ok {
				t.Fatalf("Flow view carries %q: %v", forbidden, view)
			}
		}
		// Ready for intake without a second configuration step: the
		// execution revision and the LiteLLM team exist after this one call.
		got := h.service("3", "flow-owner", map[string]any{"team_id": 3, "flow_id": 1})
		got.expect(t, 200, "")
		if calls := h.other.appliedCalls(); len(calls) != 1 || calls[0].FlowID != 1 || calls[0].FlowRevision != 1 {
			t.Fatalf("apply-flow-team calls = %+v, want one for Flow 1 revision 1", calls)
		}
		if ex := h.mem.Executions(3, 1); len(ex) != 1 || ex[0].Revision != 1 {
			t.Fatalf("execution revisions = %+v, want revision 1", ex)
		}
	})

	t.Run("Flow write carrying agent_id is rejected", func(t *testing.T) {
		for _, value := range []any{"openhands", "", nil, 5} {
			h := newHarness(t)
			r := h.ui(3, "flow-create", nil, flowBody(set("agent_id", value)))
			r.expect(t, 400, "invalid_request")
			if n := h.callsTo("executor-registry.", "model-registry.", "source-controller."); n != 0 {
				t.Fatalf("agent_id=%v: %d requests to other services, want 0", value, n)
			}
			if h.mem.FlowCount() != 0 {
				t.Fatalf("agent_id=%v: a Flow was created", value)
			}
		}
		h := newHarness(t)
		created := h.createFlow(3)
		before := h.callsTo("executor-registry.", "model-registry.", "source-controller.")
		r := h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, created)}, flowBody(set("agent_id", "openhands"), set("name", "Renamed")), ifMatch(1))
		r.expect(t, 400, "invalid_request")
		if after := h.callsTo("executor-registry.", "model-registry.", "source-controller."); after != before {
			t.Fatalf("replacement with agent_id sent %d requests to other services", after-before)
		}
		got := h.ui(3, "flow-get", map[string]any{"flow_id": flowID(t, created)}, nil)
		got.expect(t, 200, "")
		if got.json(t)["name"] != "Intake routing" || revision(t, got.json(t)) != 1 {
			t.Fatalf("replacement with agent_id changed the Flow: %s", got.Body)
		}
		if strings.Contains(string(got.Body), "agent_id") {
			t.Fatalf("Flow response contains agent_id: %s", got.Body)
		}
	})

	t.Run("Flow identifiers are numbered per team", func(t *testing.T) {
		h := newHarness(t)
		for i := 1; i <= 4; i++ {
			v := h.createFlow(3, set("name", "flow "+string(rune('a'+i))))
			if flowID(t, v) != int64(i) {
				t.Fatalf("team 3 Flow %d got flow_id %v", i, v["flow_id"])
			}
		}
		fifth := h.createFlow(3, set("name", "fifth"))
		first4 := h.createFlow(4, set("name", "team four"))
		if flowID(t, fifth) != 5 || flowID(t, first4) != 1 {
			t.Fatalf("flow_id team3=%v team4=%v, want 5 and 1", fifth["flow_id"], first4["flow_id"])
		}
		own := h.ui(4, "flow-get", map[string]any{"flow_id": 1}, nil)
		own.expect(t, 200, "")
		if own.json(t)["name"] != "team four" {
			t.Fatalf("team 4 Flow 1 read returned another team's Flow: %s", own.Body)
		}
		mine := h.ui(3, "flow-get", map[string]any{"flow_id": 1}, nil)
		mine.expect(t, 200, "")
		if mine.json(t)["name"] == "team four" {
			t.Fatalf("team 3 Flow 1 read returned team 4's Flow")
		}
	})

	t.Run("Required executor tag or model is missing", func(t *testing.T) {
		cases := map[string]func(map[string]any){
			"no tag":          unset("execution_tag"),
			"empty tag":       set("execution_tag", ""),
			"no models":       unset("model_aliases"),
			"empty models":    set("model_aliases", []any{}),
			"six models":      set("model_aliases", []any{"3/a", "3/b", "3/c", "3/d", "3/e", "3/f"}),
			"duplicate model": set("model_aliases", []any{"3/gpt-4o", "3/gpt-4o"}),
			"no name":         set("name", "  "),
			"no prompt":       unset("prompt_template"),
		}
		for name, mod := range cases {
			t.Run(name, func(t *testing.T) {
				h := newHarness(t)
				h.ui(3, "flow-create", nil, flowBody(mod)).expect(t, 400, "invalid_request")
				if h.mem.FlowCount() != 0 || len(h.requests(subjApplyFlowTeam)) != 0 {
					t.Fatalf("rejected create persisted a Flow or requested a LiteLLM team change")
				}
			})
		}
		t.Run("no active model", func(t *testing.T) {
			h := newHarness(t)
			h.other.addModel(3, "3/paused", modelState{Availability: "paused", Selectable: true})
			h.ui(3, "flow-create", nil, flowBody(set("model_aliases", []any{"3/paused"}))).expect(t, 400, "invalid_request")
			if h.mem.FlowCount() != 0 || len(h.requests(subjApplyFlowTeam)) != 0 {
				t.Fatalf("Flow without an active model was created or applied")
			}
		})
		t.Run("replacement without models", func(t *testing.T) {
			h := newHarness(t)
			v := h.createFlow(3)
			h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("model_aliases", []any{})), ifMatch(1)).expect(t, 400, "invalid_request")
			if len(h.requests(subjApplyFlowTeam)) != 1 {
				t.Fatalf("rejected replacement requested a LiteLLM team change")
			}
		})
	})

	t.Run("Execution tag outside the allowed characters is rejected", func(t *testing.T) {
		for _, tag := range []string{"open.hands", "open*", "a/b", "open hands", strings.Repeat("a", 129), "ü"} {
			h := newHarness(t)
			h.other.tags[3] = append(h.other.tags[3], tag)
			h.ui(3, "flow-create", nil, flowBody(set("execution_tag", tag))).expect(t, 400, "invalid_request")
			if n := h.callsTo("executor-registry.", "model-registry."); n != 0 {
				t.Fatalf("tag %q: %d requests to executor-registry or model-registry", tag, n)
			}
			if h.mem.FlowCount() != 0 {
				t.Fatalf("tag %q created a Flow", tag)
			}
		}
		h := newHarness(t)
		v := h.createFlow(3)
		before := h.callsTo("executor-registry.", "model-registry.")
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("execution_tag", "bad tag")), ifMatch(1)).expect(t, 400, "invalid_request")
		if h.callsTo("executor-registry.", "model-registry.") != before {
			t.Fatalf("replacement with an invalid tag called another service")
		}
		long := strings.Repeat("a", 128)
		h.other.tags[3] = append(h.other.tags[3], long)
		h.ui(3, "flow-create", nil, flowBody(set("name", "long tag"), set("execution_tag", long))).expect(t, 201, "")
	})

	t.Run("Foreign executor tag is not a choice", func(t *testing.T) {
		h := newHarness(t)
		r := h.ui(3, "flow-create", nil, flowBody(set("execution_tag", "gpu")))
		r.expect(t, 400, "invalid_request")
		if strings.Contains(string(r.Body), "4") && strings.Contains(strings.ToLower(string(r.Body)), "team") {
			t.Fatalf("rejection reveals the foreign team: %s", r.Body)
		}
		reqs := h.requests(subjTeamTags)
		if len(reqs) != 1 {
			t.Fatalf("team-tags requests = %d, want 1", len(reqs))
		}
		// executor-registry's team-tags contract carries team_id in params.
		var env struct {
			Params struct {
				TeamID int64 `json:"team_id"`
			} `json:"params"`
		}
		_ = json.Unmarshal(reqs[0].Body, &env)
		if env.Params.TeamID != 3 || reqs[0].Header["team"] != "3" {
			t.Fatalf("team-tags asked for team %d (header %q), want 3", env.Params.TeamID, reqs[0].Header["team"])
		}
		if h.mem.FlowCount() != 0 {
			t.Fatalf("Flow with a foreign tag was created")
		}
	})

	t.Run("Unchanged tag stays editable without a serving Executor", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3)
		h.other.set(func(o *others) { o.tags[3] = nil })
		before := len(h.requests(subjTeamTags))
		r := h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)},
			flowBody(set("model_aliases", []any{"3/claude-sonnet"}), set("per_task_limit", map[string]any{"rpm": 20})), ifMatch(1))
		r.expect(t, 200, "")
		if len(h.requests(subjTeamTags)) != before {
			t.Fatalf("replacement keeping the tag asked executor-registry")
		}
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("execution_tag", "unregistered")), ifMatch(revision(t, r.json(t)))).expect(t, 400, "invalid_request")
	})

	t.Run("Stale replacement", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3)
		id := flowID(t, v)
		// A write through the LLM settings route advances the shared revision.
		h.ui(3, "llm-settings-replace", map[string]any{"flow_id": id}, map[string]any{"model_aliases": []any{"3/claude-sonnet"}, "per_task_limit": nil, "flow_limit": nil}, ifMatch(1)).expect(t, 200, "")
		before := h.callsTo("executor-registry.", "model-registry.", "source-controller.")
		h.ui(3, "flow-replace", map[string]any{"flow_id": id}, flowBody(set("execution_tag", "openhands"), set("name", "Stale")), ifMatch(1)).expect(t, 412, "")
		if after := h.callsTo("executor-registry.", "model-registry.", "source-controller."); after != before {
			t.Fatalf("stale replacement sent %d requests to other services", after-before)
		}
		h.ui(3, "flow-replace", map[string]any{"flow_id": id}, flowBody(), withoutHeader("If-Match")).expect(t, 400, "invalid_request")
	})

	t.Run("Same-team duplicate name ignoring case", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3, set("name", "Intake routing"))
		h.ui(3, "flow-create", nil, flowBody(set("name", "INTAKE ROUTING"))).expect(t, 409, "flow_name_conflict")
		h.createFlow(4, set("name", "Intake routing"))
	})
}
