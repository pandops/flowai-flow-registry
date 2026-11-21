package flows

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/pandops/flowai-flow-registry/internal/ids"
	"github.com/pandops/flowai-flow-registry/internal/prompt"
	"github.com/pandops/flowai-flow-registry/internal/store"
	"github.com/pandops/flowai-flow-registry/schemas"
)

const schemaBase = "mem:///flow-registry/schemas/"

// requestSchemas compiles every "<class>.<op>.request.schema.json" file. The
// keys ("ui.flow-create", ...) are also the set of served operations.
func requestSchemas() (map[string]*jsonschema.Schema, error) {
	c := jsonschema.NewCompiler()
	files, err := fs.Glob(schemas.FS, "*.json")
	if err != nil {
		return nil, err
	}
	var requests []string
	for _, name := range files {
		data, err := schemas.FS.ReadFile(name)
		if err != nil {
			return nil, err
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("schema %s: %w", name, err)
		}
		if err := c.AddResource(schemaBase+name, doc); err != nil {
			return nil, fmt.Errorf("schema %s: %w", name, err)
		}
		if strings.HasSuffix(name, ".request.schema.json") {
			requests = append(requests, name)
		}
	}
	out := map[string]*jsonschema.Schema{}
	for _, name := range requests {
		s, err := c.Compile(schemaBase + name)
		if err != nil {
			return nil, fmt.Errorf("compile %s: %w", name, err)
		}
		out[strings.TrimSuffix(name, ".request.schema.json")] = s
	}
	return out, nil
}

func validDoc(s *jsonschema.Schema, data []byte) bool {
	if len(bytes.TrimSpace(data)) == 0 {
		data = []byte("{}")
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return false
	}
	return s.Validate(doc) == nil
}

// Task setting defaults of a new Flow.
const (
	defaultRetryCount     = 3
	defaultRetryWait      = 60
	defaultPendingLimit   = 900
	defaultReleaseDelay   = 300
	maxFlowsPerSource     = 32
	defaultListLimit      = 50
	minBudgetCents        = 1
	maxBudgetCents        = 100000000
	emptyParameters       = "{}"
	headerIfMatchQuotes   = `"`
	codeInvalidRequest    = "invalid_request"
	codeNotFound          = "not_found"
	codeStale             = "stale_revision"
	codeServiceDown       = "service_unavailable"
	codeBrokerDown        = "broker_unavailable"
	codeGatewayDown       = "model_gateway_unavailable"
	codeNameConflict      = "flow_name_conflict"
	codeFlowNotFound      = "flow_not_found"
	codeModelLastInFlow   = "model_last_in_flow"
	codeInternal          = "internal_error"
	maxBindingSourceCount = 64
)

// flowInput is a schema-valid Flow or LLM settings write body. Pointer and
// "has" fields are nil/false when the field was omitted.
type flowInput struct {
	name, tag, prompt  *string
	aliases            []string
	hasPerTask         bool
	perTask            json.RawMessage
	hasFlowLimit       bool
	flowLimit          json.RawMessage
	hasParams          bool
	hasImage           bool
	image              json.RawMessage
	sourceIDs          []int64
	hasSourceIDs       bool
	sourceRevisions    map[int64]int
	hasSourceRevisions bool
	retry, wait        *int
	pending, release   *int
}

// parseFlowInput reads a body the request schema has already accepted and
// applies the rules a schema cannot express.
func parseFlowInput(body json.RawMessage) (flowInput, error) {
	var in flowInput
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return in, err
	}
	str := func(k string) (*string, error) {
		raw, ok := fields[k]
		if !ok {
			return nil, nil
		}
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		return &s, nil
	}
	num := func(k string) (*int, error) {
		raw, ok := fields[k]
		if !ok {
			return nil, nil
		}
		var n int
		if err := json.Unmarshal(raw, &n); err != nil {
			return nil, err
		}
		return &n, nil
	}
	var err error
	if in.name, err = str("name"); err != nil {
		return in, err
	}
	if in.name != nil && strings.TrimSpace(*in.name) == "" {
		return in, fmt.Errorf("name is blank")
	}
	if in.tag, err = str("execution_tag"); err != nil {
		return in, err
	}
	if in.prompt, err = str("prompt_template"); err != nil {
		return in, err
	}
	if in.prompt != nil {
		if err := prompt.Validate(*in.prompt); err != nil {
			return in, err
		}
	}
	if raw, ok := fields["model_aliases"]; ok {
		if err := json.Unmarshal(raw, &in.aliases); err != nil {
			return in, err
		}
	}
	if raw, ok := fields["per_task_limit"]; ok {
		in.hasPerTask = true
		if in.perTask, err = canonicalLimit(raw); err != nil {
			return in, err
		}
	}
	if raw, ok := fields["flow_limit"]; ok {
		in.hasFlowLimit = true
		if in.flowLimit, err = canonicalLimit(raw); err != nil {
			return in, err
		}
	}
	_, in.hasParams = fields["execution_parameters"]
	if raw, ok := fields["default_image"]; ok {
		in.hasImage = true
		if string(bytes.TrimSpace(raw)) != "null" {
			var img struct {
				Repository string `json:"repository"`
				Digest     string `json:"digest"`
			}
			if err := json.Unmarshal(raw, &img); err != nil {
				return in, err
			}
			img.Digest = strings.ToLower(img.Digest)
			in.image, _ = json.Marshal(img)
		}
	}
	if raw, ok := fields["source_ids"]; ok {
		in.hasSourceIDs = true
		if err := json.Unmarshal(raw, &in.sourceIDs); err != nil {
			return in, err
		}
		sort.Slice(in.sourceIDs, func(i, j int) bool { return in.sourceIDs[i] < in.sourceIDs[j] })
	}
	if raw, ok := fields["source_revisions"]; ok {
		in.hasSourceRevisions = true
		var revs map[string]int
		if err := json.Unmarshal(raw, &revs); err != nil {
			return in, err
		}
		in.sourceRevisions = map[int64]int{}
		for k, v := range revs {
			id, ok := ids.Parse(k)
			if !ok {
				return in, fmt.Errorf("source_revisions key %q", k)
			}
			in.sourceRevisions[id] = v
		}
	}
	for k, p := range map[string]**int{"failure_retry_count": &in.retry, "failure_retry_wait_seconds": &in.wait, "pending_limit_seconds": &in.pending, "release_delay_seconds": &in.release} {
		if *p, err = num(k); err != nil {
			return in, err
		}
	}
	return in, nil
}

// canonicalLimit checks the budget range and returns the limit with sorted
// keys, or nil for null.
func canonicalLimit(raw json.RawMessage) (json.RawMessage, error) {
	if string(bytes.TrimSpace(raw)) == "null" {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	if v, ok := m["max_budget_usd"].(string); ok {
		cents, ok := parseCents(v)
		if !ok || cents < minBudgetCents || cents > maxBudgetCents {
			return nil, fmt.Errorf("max_budget_usd out of range")
		}
	}
	return json.Marshal(m)
}

// parseCents reads a schema-valid "D[.dd]" decimal as cents.
func parseCents(v string) (int64, bool) {
	whole, frac, _ := strings.Cut(v, ".")
	w, err := strconv.ParseInt(whole, 10, 64)
	if err != nil || len(frac) > 2 {
		return 0, false
	}
	frac = (frac + "00")[:2]
	f, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, false
	}
	return w*100 + f, true
}

// merge applies the input to the old Flow (nil on create).
func merge(old *store.Flow, in flowInput, teamID, flowID int64) store.Flow {
	var f store.Flow
	if old != nil {
		f = old.Clone()
	} else {
		f = store.Flow{TeamID: teamID, FlowID: flowID, SourceIDs: []int64{}, Execution: store.Execution{
			Parameters:              json.RawMessage(emptyParameters),
			FailureRetryCount:       defaultRetryCount,
			FailureRetryWaitSeconds: defaultRetryWait,
			PendingLimitSeconds:     defaultPendingLimit,
			ReleaseDelaySeconds:     defaultReleaseDelay,
		}}
	}
	if in.name != nil {
		f.Name = *in.name
	}
	if in.tag != nil {
		f.Execution.Tag = *in.tag
	}
	if in.prompt != nil {
		f.Execution.PromptTemplate = *in.prompt
	}
	if in.aliases != nil {
		f.ModelAliases = append([]string(nil), in.aliases...)
	}
	if in.hasPerTask {
		f.PerTaskLimit = in.perTask
	}
	if in.hasFlowLimit {
		f.FlowLimit = in.flowLimit
	}
	if in.hasParams {
		f.Execution.Parameters = json.RawMessage(emptyParameters)
	}
	if in.hasImage {
		f.Execution.DefaultImage = in.image
	}
	for _, p := range []struct {
		in  *int
		out *int
	}{{in.retry, &f.Execution.FailureRetryCount}, {in.wait, &f.Execution.FailureRetryWaitSeconds}, {in.pending, &f.Execution.PendingLimitSeconds}, {in.release, &f.Execution.ReleaseDelaySeconds}} {
		if p.in != nil {
			*p.out = *p.in
		}
	}
	return f
}

// parseIfMatch reads `"<revision>"`.
func parseIfMatch(v string) (int, bool) {
	if len(v) < 3 || !strings.HasPrefix(v, headerIfMatchQuotes) || !strings.HasSuffix(v, headerIfMatchQuotes) {
		return 0, false
	}
	n, ok := ids.Parse(v[1 : len(v)-1])
	if !ok || n > 1<<31-1 {
		return 0, false
	}
	return int(n), true
}
