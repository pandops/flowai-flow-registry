//go:build !integration

package test

import (
	"strings"
	"testing"
)

// Requirement (task-sources-interface): Team operator can preview
// deterministic flow prompt rendering.

func TestV0048TeamOperatorCanPreviewDeterministicFlowPromptRendering(t *testing.T) {
	t.Run("Operator previews source interpolation", func(t *testing.T) {
		h := newHarness(t)
		commits := h.mem.Commits()
		cases := map[string][2]string{
			"plain":          {"Solve {{external_id}}", "Solve ISSUE-42"},
			"twice":          {"{{external_id}} and {{external_id}}", "ISSUE-42 and ISSUE-42"},
			"literal braces": {"Use {{{{x}}}} for {{external_id}}", "Use {{x}} for ISSUE-42"},
			"no placeholder": {"Static text", "Static text"},
		}
		for name, c := range cases {
			r := h.ui(3, "flow-preview", nil, map[string]any{"prompt_template": c[0], "external_id": "ISSUE-42"})
			r.expect(t, 200, "")
			if r.json(t)["rendered_prompt"] != c[1] {
				t.Fatalf("%s: rendered %q, want %q", name, r.json(t)["rendered_prompt"], c[1])
			}
		}
		// The external ID is data: it is not parsed again.
		r := h.ui(3, "flow-preview", nil, map[string]any{"prompt_template": "Solve {{external_id}}", "external_id": "{{external_id}}"})
		r.expect(t, 200, "")
		if r.json(t)["rendered_prompt"] != "Solve {{external_id}}" {
			t.Fatalf("external ID was interpolated recursively: %v", r.json(t)["rendered_prompt"])
		}
		if h.mem.FlowCount() != 0 || h.callsTo("model-registry.", "source-controller.", "executor-registry.") != 0 {
			t.Fatalf("preview created a Flow or called another service")
		}
		// A successful preview commits no outbox row: it is a read, and
		// successful reads are not audited (D15 owner decision 2026-11-09).
		for _, row := range h.mem.Outbox() {
			t.Fatalf("successful preview stored %q", row.Subject)
		}
		_ = commits
		// The rendered prompt is bounded to 32 KiB.
		big := strings.Repeat("{{external_id}}", 200)
		h.ui(3, "flow-preview", nil, map[string]any{"prompt_template": big, "external_id": strings.Repeat("x", 200)}).expect(t, 400, "invalid_request")
		h.ui(3, "flow-preview", nil, map[string]any{"prompt_template": strings.Repeat("x", 16384), "external_id": "a"}).expect(t, 200, "")
		h.ui(3, "flow-preview", nil, map[string]any{"prompt_template": strings.Repeat("x", 16385), "external_id": "a"}).expect(t, 400, "invalid_request")
	})

	t.Run("Preview input attempts a foreign team or unknown placeholder", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(4, set("prompt_template", "Team four secret {{external_id}}"))
		for name, body := range map[string]map[string]any{
			"caller team_id":      {"prompt_template": "Solve {{external_id}}", "external_id": "A", "team_id": 4},
			"unknown placeholder": {"prompt_template": "Solve {{title}}", "external_id": "A"},
			"single brace":        {"prompt_template": "Solve {x}", "external_id": "A"},
			"unbalanced":          {"prompt_template": "Solve {{external_id}", "external_id": "A"},
			"empty template":      {"prompt_template": "", "external_id": "A"},
			"missing external id": {"prompt_template": "Solve {{external_id}}"},
			"flow reference":      {"prompt_template": "Solve {{external_id}}", "external_id": "A", "flow_id": 1},
		} {
			r := h.ui(3, "flow-preview", nil, body)
			r.expect(t, 400, "invalid_request")
			if strings.Contains(string(r.Body), "Team four") {
				t.Fatalf("%s: reply discloses team 4 data: %s", name, r.Body)
			}
		}
		if h.mem.FlowCount() != 1 {
			t.Fatalf("rejected previews persisted a Flow")
		}
	})
}
