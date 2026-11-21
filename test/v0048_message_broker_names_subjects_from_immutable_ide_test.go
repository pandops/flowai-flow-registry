//go:build !integration

package test

import (
	"regexp"
	"strings"
	"testing"
)

// Requirement: message-broker names subjects from immutable identifiers.

func TestV0048MessageBrokerNamesSubjectsFromImmutableIdentifiers(t *testing.T) {
	t.Run("Team appears as a subject token only in the task queue and Source intake", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3, bindTo(map[int64]int{12: 5}, 12))
		h.createFlow(4)
		h.ui(3, "llm-settings-list", nil, nil)
		h.admin("tags", nil, nil)
		h.mem.FailNextCommit(errInjected)
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("name", "x")), ifMatch(1))
		h.svc.Wait()
		h.flush()
		var subjects []string
		for _, r := range h.broker.Requests("") {
			subjects = append(subjects, r.Subject)
		}
		for _, m := range h.broker.Published() {
			subjects = append(subjects, m.Subject)
		}
		subjects = append(subjects, h.svc.Subjects()...)
		token := regexp.MustCompile(`^[a-z]+(-[a-z]+)*$|^[a-z]+(_[a-z]+)*$|^\*$`)
		for _, s := range subjects {
			if strings.HasPrefix(s, "flowai.") {
				t.Fatalf("subject %s starts with flowai.", s)
			}
			if !strings.HasSuffix(s, ".pub") && !strings.HasSuffix(s, ".sub") {
				t.Fatalf("subject %s does not end in .pub", s)
			}
			for _, tok := range strings.Split(s, ".") {
				if !token.MatchString(tok) {
					t.Fatalf("subject %s has a non-name token %q (team or id)", s, tok)
				}
			}
		}
	})
}
