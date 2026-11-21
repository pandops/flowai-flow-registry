//go:build !integration

package test

import (
	"strings"
	"testing"
)

// Requirement: flow-registry versions Flow execution configuration.

var testImage = map[string]any{"repository": "ghcr.io/flowai/agent", "digest": "sha256:" + strings.Repeat("ab", 32)}

func targetsOf(t *testing.T, h *harness, team, source int64) reply {
	t.Helper()
	return h.service(itoa(team), "targets", map[string]any{"team_id": team, "source_id": source})
}

func TestV0048FlowRegistryVersionsFlowExecutionConfiguration(t *testing.T) {
	t.Run("Operator changes Flow execution configuration", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3, bindTo(map[int64]int{12: 5}, 12))
		id := flowID(t, v)
		first := targetsOf(t, h, 3, 12)
		first.expect(t, 200, "")
		frozen := first.json(t)["flows"].([]any)[0].(map[string]any)["execution_revision"].(map[string]any)
		if num(t, frozen["revision"]) != 1 || frozen["prompt_template"] != "Solve {{external_id}}" {
			t.Fatalf("first revision = %v", frozen)
		}
		r := h.ui(3, "flow-replace", map[string]any{"flow_id": id}, flowBody(
			set("prompt_template", "Fix {{external_id}} now"), set("default_image", testImage),
			bindTo(map[int64]int{12: 6}, 12)), ifMatch(1))
		r.expect(t, 200, "")
		if num(t, r.json(t)["execution_revision"]) != 2 || num(t, r.json(t)["flow_id"]) != id {
			t.Fatalf("replacement view = %s", r.Body)
		}
		next := targetsOf(t, h, 3, 12)
		next.expect(t, 200, "")
		rev := next.json(t)["flows"].([]any)[0].(map[string]any)["execution_revision"].(map[string]any)
		if num(t, rev["revision"]) != 2 || rev["prompt_template"] != "Fix {{external_id}} now" || rev["default_image"] == nil {
			t.Fatalf("targets after replacement = %v", rev)
		}
		ex := h.mem.Executions(3, id)
		if len(ex) != 2 || ex[0].PromptTemplate != "Solve {{external_id}}" || ex[0].DefaultImage != nil {
			t.Fatalf("revision 1 was changed or not kept: %+v", ex)
		}
		// A replacement that changes no execution field appends nothing.
		h.ui(3, "flow-replace", map[string]any{"flow_id": id}, flowBody(
			set("prompt_template", "Fix {{external_id}} now"), set("default_image", testImage), set("name", "renamed"),
			bindTo(map[int64]int{12: 7}, 12)), ifMatch(2)).expect(t, 200, "")
		if len(h.mem.Executions(3, id)) != 2 {
			t.Fatalf("rename appended an execution revision")
		}
		// Changing the tag appends a revision too.
		h.other.tags[3] = append(h.other.tags[3], "gpu-large")
		r = h.ui(3, "flow-replace", map[string]any{"flow_id": id}, flowBody(set("execution_tag", "gpu-large"),
			set("prompt_template", "Fix {{external_id}} now"), set("default_image", testImage), set("name", "renamed"),
			bindTo(map[int64]int{12: 8}, 12)), ifMatch(3))
		r.expect(t, 200, "")
		if num(t, r.json(t)["execution_revision"]) != 3 {
			t.Fatalf("tag change did not append a revision: %s", r.Body)
		}
	})

	t.Run("Operator submits an empty additional parameter set", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3, set("execution_parameters", map[string]any{}))
		if p, ok := v["execution_parameters"].(map[string]any); !ok || len(p) != 0 {
			t.Fatalf("execution_parameters = %v, want {}", v["execution_parameters"])
		}
		w := h.createFlow(3, set("name", "omitted"), unset("execution_parameters"))
		if p, ok := w["execution_parameters"].(map[string]any); !ok || len(p) != 0 {
			t.Fatalf("omitted execution_parameters = %v, want {}", w["execution_parameters"])
		}
		if ex := h.mem.Executions(3, flowID(t, v)); len(ex) != 1 || string(ex[0].Parameters) != "{}" {
			t.Fatalf("stored parameters = %+v", ex)
		}
	})

	t.Run("Invalid prompt or parameters", func(t *testing.T) {
		for name, mod := range map[string]func(map[string]any){
			"unknown placeholder":   set("prompt_template", "Solve {{title}}"),
			"single brace":          set("prompt_template", "Solve {external_id}"),
			"image parameter":       set("execution_parameters", map[string]any{"image": "ghcr.io/x@sha256:" + strings.Repeat("0", 64)}),
			"default_image param":   set("execution_parameters", map[string]any{"default_image": testImage}),
			"other parameter":       set("execution_parameters", map[string]any{"verbose": true}),
			"parameters not object": set("execution_parameters", []any{}),
			"image without digest":  set("default_image", map[string]any{"repository": "ghcr.io/flowai/agent"}),
			"image tag only":        set("default_image", map[string]any{"repository": "ghcr.io/flowai/agent", "digest": "latest"}),
			"image string":          set("default_image", "ghcr.io/flowai/agent:latest"),
			"oversized prompt":      set("prompt_template", strings.Repeat("x", 16385)),
		} {
			t.Run(name, func(t *testing.T) {
				h := newHarness(t)
				h.ui(3, "flow-create", nil, flowBody(mod)).expect(t, 400, "invalid_request")
				if h.mem.FlowCount() != 0 || len(h.requests(subjApplyFlowTeam)) != 0 {
					t.Fatalf("invalid write created a Flow or requested a LiteLLM team change")
				}
				v := h.createFlow(3, set("name", "existing"))
				h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(mod), ifMatch(1)).expect(t, 400, "invalid_request")
				if len(h.mem.Executions(3, flowID(t, v))) != 1 || len(h.requests(subjApplyFlowTeam)) != 1 {
					t.Fatalf("invalid replacement appended a revision or changed the LiteLLM team")
				}
			})
		}
	})

	t.Run("task-manager asks for targets", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3, set("name", "unbound"))
		a := h.createFlow(3, set("name", "two"), bindTo(map[int64]int{12: 5}, 12))
		b := h.createFlow(3, set("name", "five"), set("failure_retry_count", 0), set("release_delay_seconds", 600), bindTo(map[int64]int{12: 6}, 12))
		r := targetsOf(t, h, 3, 12)
		r.expect(t, 200, "")
		list := r.json(t)["flows"].([]any)
		if len(list) != 2 {
			t.Fatalf("targets = %d Flows, want 2", len(list))
		}
		gotIDs := []int64{num(t, list[0].(map[string]any)["flow_id"]), num(t, list[1].(map[string]any)["flow_id"])}
		if gotIDs[0] != flowID(t, a) || gotIDs[1] != flowID(t, b) {
			t.Fatalf("targets flow ids = %v", gotIDs)
		}
		rev := list[1].(map[string]any)["execution_revision"].(map[string]any)
		for _, k := range []string{"revision", "execution_tag", "prompt_template", "execution_parameters", "default_image",
			"failure_retry_count", "failure_retry_wait_seconds", "pending_limit_seconds", "release_delay_seconds"} {
			if _, ok := rev[k]; !ok {
				t.Fatalf("revision lacks %q: %v", k, rev)
			}
		}
		if num(t, rev["failure_retry_count"]) != 0 || num(t, rev["release_delay_seconds"]) != 600 || rev["default_image"] != nil {
			t.Fatalf("revision content = %v", rev)
		}
		for _, k := range []string{"model_aliases", "per_task_limit", "flow_limit", "agent_id", "api_key"} {
			if strings.Contains(string(r.Body), `"`+k+`"`) {
				t.Fatalf("targets reply carries %q: %s", k, r.Body)
			}
		}
	})

	t.Run("Source of another team asks for targets", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3, bindTo(map[int64]int{12: 5}, 12))
		r := targetsOf(t, h, 4, 12)
		r.expect(t, 404, "not_found")
		if strings.Contains(string(r.Body), "execution_revision") || strings.Contains(string(r.Body), "flow_id") {
			t.Fatalf("not-found reply reveals a Flow: %s", r.Body)
		}
	})
}

func itoa(n int64) string {
	return strings.TrimSpace(strings.ReplaceAll(string(mustJSON(n)), `"`, ""))
}
