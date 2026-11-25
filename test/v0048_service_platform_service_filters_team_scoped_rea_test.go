//go:build !integration

package test

import (
	"fmt"
	"testing"
)

// Requirement: service-platform service filters team-scoped reads before shaping.
//
// "Concurrent team streams" is not applicable: flow-registry serves no stream.

func TestV0048ServicePlatformServiceFiltersTeamScopedReadsBeforeShaping(t *testing.T) {
	t.Run("Teams share filter values", func(t *testing.T) {
		h := newHarness(t)
		h.other.addSource(4, 12, 1)
		h.other.addModel(4, "4/gpt-4o", modelState{Availability: "active", Selectable: true})
		for i := 0; i < 3; i++ {
			h.createFlow(3, set("name", fmt.Sprintf("three-%d", i)), bindTo(map[int64]int{12: 5 + i}, 12))
		}
		for i := 0; i < 4; i++ {
			h.createFlow(4, set("name", fmt.Sprintf("four-%d", i)), set("model_aliases", []any{"4/gpt-4o"}), bindTo(map[int64]int{12: 1 + i}, 12))
		}
		// Source 12 exists in both teams; team 3's filtered pages hold only team 3 rows.
		var names []string
		params := map[string]any{"source_id": 12, "limit": 2}
		for {
			r := h.ui(3, "flows-list", params, nil)
			r.expect(t, 200, "")
			items := r.json(t)["items"].([]any)
			if len(items) > 2 {
				t.Fatalf("page has %d items", len(items))
			}
			for _, it := range items {
				names = append(names, it.(map[string]any)["name"].(string))
			}
			next, _ := r.json(t)["next_cursor"].(string)
			if next == "" {
				break
			}
			params = map[string]any{"source_id": 12, "limit": 2, "cursor": next}
		}
		if fmt.Sprint(names) != "[three-0 three-1 three-2]" {
			t.Fatalf("team 3 rows = %v", names)
		}
		// An exactly full page after filtering has no further cursor.
		r := h.ui(3, "flows-list", map[string]any{"source_id": 12, "limit": 3}, nil)
		if r.json(t)["next_cursor"] != nil {
			t.Fatalf("full last page has a cursor: %s", r.Body)
		}
		// Filter by model alias and the settings list follow the same rule.
		r = h.ui(3, "flows-list", map[string]any{"model_alias": "4/gpt-4o"}, nil)
		if len(r.json(t)["items"].([]any)) != 0 {
			t.Fatalf("team 3 sees team 4 rows by alias: %s", r.Body)
		}
		r = h.ui(3, "llm-settings-list", nil, nil)
		if len(r.json(t)["items"].([]any)) != 3 {
			t.Fatalf("settings list = %s", r.Body)
		}
		r = targetsOf(t, h, 3, 12)
		if len(r.json(t)["flows"].([]any)) != 3 {
			t.Fatalf("targets = %s", r.Body)
		}
	})
}
