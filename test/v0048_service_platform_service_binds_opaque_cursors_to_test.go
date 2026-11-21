//go:build !integration

package test

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pandops/flowai-flow-registry/internal/cursor"
	"github.com/pandops/flowai-flow-registry/internal/store"
)

// Requirement: service-platform service binds opaque cursors to their read.

var testCursorSecretB = []byte(strings.Repeat("k", 32))

func pagedHarness(t *testing.T, n int) *harness {
	h := newHarness(t)
	for i := 0; i < n; i++ {
		h.createFlow(3, set("name", fmt.Sprintf("p%02d", i)), bindTo(map[int64]int{12: 5 + i}, 12))
	}
	h.createFlow(4, set("name", "p00"))
	return h
}

func TestV0048ServicePlatformServiceBindsOpaqueCursorsToTheirRead(t *testing.T) {
	t.Run("Forward page advances", func(t *testing.T) {
		h := pagedHarness(t, 5)
		var names []string
		params := map[string]any{"source_id": 12, "limit": 2}
		for pages := 0; pages < 5; pages++ {
			r := h.ui(3, "flows-list", params, nil)
			r.expect(t, 200, "")
			for _, it := range r.json(t)["items"].([]any) {
				names = append(names, it.(map[string]any)["name"].(string))
			}
			next, _ := r.json(t)["next_cursor"].(string)
			if next == "" {
				break
			}
			params = map[string]any{"source_id": 12, "limit": 2, "cursor": next}
		}
		if strings.Join(names, ",") != "p00,p01,p02,p03,p04" {
			t.Fatalf("pages = %v", names)
		}
		// llm-settings-list pages the same way.
		r := h.ui(3, "llm-settings-list", map[string]any{"limit": 3}, nil)
		next, _ := r.json(t)["next_cursor"].(string)
		r = h.ui(3, "llm-settings-list", map[string]any{"limit": 3, "cursor": next}, nil)
		r.expect(t, 200, "")
		if len(r.json(t)["items"].([]any)) != 2 {
			t.Fatalf("second settings page = %s", r.Body)
		}
	})

	t.Run("Tampered cursor", func(t *testing.T) {
		h := pagedHarness(t, 3)
		first := h.ui(3, "flows-list", map[string]any{"limit": 1}, nil).json(t)["next_cursor"].(string)
		raw, err := base64.RawURLEncoding.DecodeString(first)
		if err != nil {
			t.Fatalf("cursor is not base64url: %v", err)
		}
		for i := 0; i < len(raw)*8; i += 7 {
			flipped := append([]byte(nil), raw...)
			flipped[i/8] ^= 1 << (i % 8)
			tok := base64.RawURLEncoding.EncodeToString(flipped)
			h.ui(3, "flows-list", map[string]any{"limit": 1, "cursor": tok}, nil).expect(t, 400, "invalid_request")
		}
		for _, bad := range []string{"x", "....", strings.Repeat("A", 4096)} {
			h.ui(3, "flows-list", map[string]any{"limit": 1, "cursor": bad}, nil).expect(t, 400, "invalid_request")
		}
		// A cursor under an unknown or non-integer key_id is rejected before any query.
		other, err := cursor.New([]cursor.Key{{ID: 2, Secret: testCursorSecretB}}, 2, time.Hour, time.Now)
		if err != nil {
			t.Fatalf("cursor.New: %v", err)
		}
		foreignKey, err := other.Encode(cursor.Binding{Operation: "flows-list", TeamID: 3, OperatorID: 7, Filters: map[string]string{}}, store.Position{})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		h.ui(3, "flows-list", map[string]any{"limit": 1, "cursor": foreignKey}, nil).expect(t, 400, "invalid_request")
		// An expired cursor is rejected.
		h.clock.Advance(25 * time.Hour)
		h.ui(3, "flows-list", map[string]any{"limit": 1, "cursor": first}, nil).expect(t, 400, "invalid_request")
	})

	t.Run("Cursor reused for another operation or team", func(t *testing.T) {
		h := pagedHarness(t, 3)
		cur := h.ui(3, "flows-list", map[string]any{"limit": 1}, nil).json(t)["next_cursor"].(string)
		h.ui(3, "llm-settings-list", map[string]any{"limit": 1, "cursor": cur}, nil).expect(t, 400, "invalid_request")
		h.ui(4, "flows-list", map[string]any{"limit": 1, "cursor": cur}, nil).expect(t, 400, "invalid_request")
		h.ui(3, "flows-list", map[string]any{"limit": 1, "cursor": cur}, nil, withOperator(8)).expect(t, 400, "invalid_request")
		h.admin("tags", map[string]any{"limit": 1}, map[string]any{"after": cur}).expect(t, 400, "invalid_request")
	})

	t.Run("Changed filter", func(t *testing.T) {
		h := pagedHarness(t, 3)
		cur := h.ui(3, "flows-list", map[string]any{"limit": 1, "source_id": 12}, nil).json(t)["next_cursor"].(string)
		for _, p := range []map[string]any{
			{"limit": 1, "cursor": cur},
			{"limit": 1, "cursor": cur, "source_id": 13},
			{"limit": 1, "cursor": cur, "source_id": 12, "model_alias": "3/gpt-4o"},
		} {
			h.ui(3, "flows-list", p, nil).expect(t, 400, "invalid_request")
		}
		h.ui(3, "flows-list", map[string]any{"limit": 2, "cursor": cur, "source_id": 12}, nil).expect(t, 200, "")
	})

	t.Run("Limit out of range", func(t *testing.T) {
		h := pagedHarness(t, 1)
		for _, lim := range []any{0, 201, 1.5, "10", -1} {
			h.ui(3, "flows-list", map[string]any{"limit": lim}, nil).expect(t, 400, "invalid_request")
			h.ui(3, "llm-settings-list", map[string]any{"limit": lim}, nil).expect(t, 400, "invalid_request")
		}
		h.ui(3, "flows-list", map[string]any{"limit": 200}, nil).expect(t, 200, "")
	})
}
