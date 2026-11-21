package flows

import (
	"encoding/json"
	"time"

	"github.com/pandops/flowai-flow-registry/internal/store"
)

const timeLayout = "2006-01-02T15:04:05.000Z"

func ts(t time.Time) string { return t.UTC().Format(timeLayout) }

func rawOrNull(r json.RawMessage) json.RawMessage {
	if len(r) == 0 {
		return json.RawMessage("null")
	}
	return r
}

func params(r json.RawMessage) json.RawMessage {
	if len(r) == 0 {
		return json.RawMessage(emptyParameters)
	}
	return r
}

type flowView struct {
	FlowID                  int64           `json:"flow_id"`
	Name                    string          `json:"name"`
	ExecutionTag            string          `json:"execution_tag"`
	ModelAliases            []string        `json:"model_aliases"`
	PerTaskLimit            json.RawMessage `json:"per_task_limit"`
	FlowLimit               json.RawMessage `json:"flow_limit"`
	PromptTemplate          string          `json:"prompt_template"`
	ExecutionParameters     json.RawMessage `json:"execution_parameters"`
	DefaultImage            json.RawMessage `json:"default_image"`
	SourceIDs               []int64         `json:"source_ids"`
	FailureRetryCount       int             `json:"failure_retry_count"`
	FailureRetryWaitSeconds int             `json:"failure_retry_wait_seconds"`
	PendingLimitSeconds     int             `json:"pending_limit_seconds"`
	ReleaseDelaySeconds     int             `json:"release_delay_seconds"`
	Revision                int             `json:"revision"`
	ExecutionRevision       int             `json:"execution_revision"`
	CreatedAt               string          `json:"created_at"`
	UpdatedAt               string          `json:"updated_at"`
}

func viewOf(f store.Flow) flowView {
	aliases := f.ModelAliases
	if aliases == nil {
		aliases = []string{}
	}
	sources := f.SourceIDs
	if sources == nil {
		sources = []int64{}
	}
	return flowView{
		FlowID: f.FlowID, Name: f.Name, ExecutionTag: f.Execution.Tag, ModelAliases: aliases,
		PerTaskLimit: rawOrNull(f.PerTaskLimit), FlowLimit: rawOrNull(f.FlowLimit),
		PromptTemplate: f.Execution.PromptTemplate, ExecutionParameters: params(f.Execution.Parameters),
		DefaultImage: rawOrNull(f.Execution.DefaultImage), SourceIDs: sources,
		FailureRetryCount: f.Execution.FailureRetryCount, FailureRetryWaitSeconds: f.Execution.FailureRetryWaitSeconds,
		PendingLimitSeconds: f.Execution.PendingLimitSeconds, ReleaseDelaySeconds: f.Execution.ReleaseDelaySeconds,
		Revision: f.Revision, ExecutionRevision: f.Execution.Revision,
		CreatedAt: ts(f.CreatedAt), UpdatedAt: ts(f.UpdatedAt),
	}
}

type modelView struct {
	ModelAlias   string `json:"model_alias"`
	Availability string `json:"availability"`
}

type usageView struct {
	SpendUSD      string `json:"spend_usd"`
	MaxBudgetUSD  string `json:"max_budget_usd"`
	BudgetResetAt string `json:"budget_reset_at"`
}

type settingsView struct {
	flowView
	Models []modelView `json:"models"`
	Usage  *usageView  `json:"usage"`
}

type executionView struct {
	Revision                int             `json:"revision"`
	ExecutionTag            string          `json:"execution_tag"`
	PromptTemplate          string          `json:"prompt_template"`
	ExecutionParameters     json.RawMessage `json:"execution_parameters"`
	DefaultImage            json.RawMessage `json:"default_image"`
	FailureRetryCount       int             `json:"failure_retry_count"`
	FailureRetryWaitSeconds int             `json:"failure_retry_wait_seconds"`
	PendingLimitSeconds     int             `json:"pending_limit_seconds"`
	ReleaseDelaySeconds     int             `json:"release_delay_seconds"`
	CreatedAt               string          `json:"created_at"`
}

func executionOf(e store.Execution) executionView {
	return executionView{
		Revision: e.Revision, ExecutionTag: e.Tag, PromptTemplate: e.PromptTemplate,
		ExecutionParameters: params(e.Parameters), DefaultImage: rawOrNull(e.DefaultImage),
		FailureRetryCount: e.FailureRetryCount, FailureRetryWaitSeconds: e.FailureRetryWaitSeconds,
		PendingLimitSeconds: e.PendingLimitSeconds, ReleaseDelaySeconds: e.ReleaseDelaySeconds,
		CreatedAt: ts(e.CreatedAt),
	}
}

type flowRef struct {
	FlowID int64  `json:"flow_id"`
	Name   string `json:"name"`
}
