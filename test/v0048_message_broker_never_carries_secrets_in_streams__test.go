//go:build !integration

package test

import (
	"strings"
	"testing"
)

// Requirement: message-broker never carries secrets in streams or logs.
// Only "Stream content inspection" concerns flow-registry's messages.

func TestV0048MessageBrokerNeverCarriesSecretsInStreams(t *testing.T) {
	t.Run("Stream content inspection", func(t *testing.T) {
		h := newHarness(t)
		prompt := "Top secret prompt {{external_id}}"
		v := h.createFlow(3, set("prompt_template", prompt), set("execution_parameters", map[string]any{}), set("default_image", testImage))
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("prompt_template", "Other secret {{external_id}}")), ifMatch(1)).expect(t, 200, "")
		h.ui(3, "flow-preview", nil, map[string]any{"prompt_template": "Preview secret {{external_id}}", "external_id": "SECRET-ID"}).expect(t, 200, "")
		h.ui(3, "flow-create", nil, flowBody(set("api_key", "sk-live-123"))).expect(t, 400, "invalid_request")
		h.mem.FailNextCommit(errInjected)
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("prompt_template", "Third secret {{external_id}}")), ifMatch(2))
		h.svc.Wait()
		h.flush()
		for _, m := range h.broker.Published() {
			for _, bad := range []string{"secret", "SECRET-ID", "sk-live", "api_key", "litellm_key", "jwt", "seed"} {
				if strings.Contains(strings.ToLower(string(m.Data)), strings.ToLower(bad)) {
					t.Fatalf("%s carries %q: %s", m.Subject, bad, m.Data)
				}
			}
		}
		if strings.Contains(h.logText(), "secret") || strings.Contains(h.logText(), "sk-live") {
			t.Fatalf("logs carry secret values: %s", h.logText())
		}
	})
}
