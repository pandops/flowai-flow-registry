//go:build !integration

package test

import (
	"encoding/json"
	"os"
	"testing"
)

// Requirement: message-broker message payloads follow publisher-owned JSON Schemas.
//
// "Schema change selects its consumers" is owned by qa-tooling's runner
// (shared-test-inputs.json); not applicable to flow-registry's tests.

func TestV0048MessageBrokerMessagePayloadsFollowPublisherSchemas(t *testing.T) {
	t.Run("Publisher message violates its own schema", func(t *testing.T) {
		// Every message flow-registry publishes validates (h.flush checks it);
		// a message missing a required field fails validation.
		h := newHarness(t)
		h.createFlow(3)
		msgs := h.flush()
		if len(msgs) != 1 {
			t.Fatalf("published %d", len(msgs))
		}
		var m map[string]any
		_ = json.Unmarshal(msgs[0].Data, &m)
		for _, field := range []string{"publisher_seq", "occurred_at", "outcome", "request_id"} {
			broken := map[string]any{}
			for k, v := range m {
				broken[k] = v
			}
			delete(broken, field)
			if err := validateAgainst(t, "audit.flow-create.schema.json", mustJSON(broken)); err == nil {
				t.Fatalf("audit without %s validates", field)
			}
		}
		if err := validateAgainst(t, "audit.flow-get.schema.json", msgs[0].Data); err == nil {
			t.Fatalf("flow-create audit validates as flow-get")
		}
	})

	t.Run("Consumer contract test reads the publisher schema", func(t *testing.T) {
		// flow-registry consumes no stream; it is a requester of six
		// operations and reads each responder's own request schema, never
		// importing that service's packages. The harness fakes also validate
		// every request and reply against the responders' schemas.
		h := newHarness(t)
		h.createFlow(3, bindTo(map[int64]int{12: 5}, 12))
		h.ui(3, "llm-settings-list", nil, nil).expect(t, 200, "")
		for _, subj := range []string{subjTeamTags, subjValidateModels, subjApplyFlowTeam, subjFlowsStatus, subjSourcesLookup, subjBindingGuard} {
			reqs := h.requests(subj)
			if len(reqs) == 0 {
				t.Fatalf("no request on %s", subj)
			}
			schema := responderSchemaBase(t, subj) + ".request.schema.json"
			if _, err := os.Stat(schema); err != nil {
				t.Fatalf("responder schema %s is missing: %v", schema, err)
			}
			for _, r := range reqs {
				if err := validateFile(t, schema, r.Body); err != nil {
					t.Fatalf("%s request violates %s: %v\n%s", subj, schema, err, r.Body)
				}
			}
		}
		for _, r := range h.broker.Requests("") {
			if r.Header["team"] != "3" || r.Header["request_id"] == "" {
				t.Fatalf("%s request headers = %v", r.Subject, r.Header)
			}
			if _, ok := r.Header["operator_id"]; ok {
				t.Fatalf("%s carries operator_id on a service subject", r.Subject)
			}
		}
	})
}
