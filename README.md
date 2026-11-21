# flow-registry

flow-registry owns team Flows: their names, execution tags, model lists and
LLM limits, the immutable execution revisions task-manager freezes, the
Flow-owned Source bindings, and archived model aliases. It answers only over
NATS request subjects and publishes only through its outbox.

## Subjects

- `flow-registry.ui.<op>.sub` — operator operations forwarded by API Gateway:
  `flows-list`, `flow-create`, `flow-get`, `flow-replace`, `flow-preview`,
  `llm-settings-list`, `llm-settings-get`, `llm-settings-replace`.
- `flow-registry.service.<op>.sub` — other services: `targets`,
  `llm-settings`, `alias-usage`, `retire-alias`, `restore-alias`,
  `flow-owner`.
- `flow-registry.admin.tags.sub` — admin-panel's global tag projection.

Request and reply bodies follow the files in [schemas/](schemas/); the service
validates every request against its own schema. Every operator and
administrator operation publishes one `flow-registry.audit.<action>.pub`
message. A background restoration of a Flow's LiteLLM team after a failed
commit publishes `flow-registry.event.flow_team_restored.pub`. After a failed
create, whose new LiteLLM team has no committed Flow, flow-registry asks
model-registry to remove that team in the background until it confirms
(deviation D17); the `flow_id` is never reused.

flow-registry calls `executor-registry.service.team-tags`,
`model-registry.service.validate-models|apply-flow-team|flows-status|remove-flow-team` and
`source-controller.service.sources-lookup|binding-guard`. It consumes no
stream (`schemas/consumers.json` is `[]`).

## Running

The image runs `/flow-registry --config /etc/flowai/config.yaml`; see
[config.example.yaml](config.example.yaml) for every key. NATS needs
`tls://`, the stand CA and operator-mode user credentials; PostgreSQL needs
`sslmode: verify-full`. Migrations run at startup in the configured schema.
The HTTP listener serves only `/v1/livez` and `/v1/readyz`.

## Tests

Run these from the standalone module root with Go 1.26.2. The unit suite
loads publisher-owned schemas from an external directory. Set
`FLOWAI_CONTRACT_FIXTURE_DIR` to its absolute path. Use the exact publisher
files at the selected revisions and keep the following paths under the
fixture root. Each operation needs both
`service.<operation>.request.schema.json` and
`service.<operation>.reply.schema.json`.

| Fixture directory | Operations | Shared schema |
| --- | --- | --- |
| `svc/executor-registry/schemas/` | `team-tags` | |
| `svc/model-registry/schemas/` | `apply-flow-team`, `flows-status`, `remove-flow-team`, `validate-models` | `defs.schema.json` |
| `svc/source-controller/schemas/` | `binding-guard`, `sources-lookup` | |

```sh
export FLOWAI_CONTRACT_FIXTURE_DIR=/absolute/path/to/contract-fixtures
go test -mod=readonly -count=1 ./...
```

The integration suite starts and removes a disposable PostgreSQL 16
container using Docker or Podman. It has its own build tag and uses no
publisher fixtures.

```sh
go test -mod=readonly -count=1 -tags=integration ./...
```
