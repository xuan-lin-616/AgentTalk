# AgentTalk IPC v1 Core Freeze

## Baseline and additive revision

- Task baseline: `cc247ce07543e4089fe0b07f734bf594f660494b`.
- Core worktree branch: `codex/core-connector-runtime-v1`.
- Schema: `schemas/ipc/v1/protocol.schema.json`.
- Previously frozen schema SHA-256: `F42B813C971514E987DF4534AC3450B74BF107F5E09128922B4F8FC8F2FB2AFF`.
- Current additive schema SHA-256: `00DA8CBACCCDADA2C00A9CD406EA4B09AC29E60FDDBE6B88590ED7F1C4B791B3`.
- Protocol major remains `1`; this is an additive v1 minor-compatible extension.

No existing IPC field is removed, renamed, or repurposed. The extension adds
the `connector.models` query, `connector.models.v1` catalog projection,
`connector.discover` and `agent.scan_local` queries, `connector.started`
event, and `agent.model_binding.patch` command. Existing
`agent.model_binding.set` retains its complete-replacement semantics; the new
`agent.model_binding.patch` command is the additive field-presence-aware form.

## Runtime catalog responsibilities

`runtime.models` is retained for legacy callers. Its payload is **exactly**
`{}` and it returns the Core process's active/default Runtime catalog as
`runtime.models.v1`. It does not accept `connectorId`, does not look up a
Connector profile, and must not be used to populate a Connector-specific model
picker.

`connector.models` is the Connector-scoped catalog query. It resolves the
persisted Connector profile by its `scopeId` and `connectorId`, then resolves
the Runtime adapter named by that profile's `runtimeType`. It never falls back
to the default Runtime when the profile, adapter, health, runtime type, or
requested model is invalid.

### Legacy `runtime.models` request

```json
{
  "kind": "query",
  "protocol": { "major": 1, "minor": 0 },
  "requestId": "default-runtime-catalog-001",
  "sessionId": "the-authenticated-session-id",
  "query": "runtime.models",
  "payload": {}
}
```

The matching `runtime.models.v1` response has `connectorId`, `runtimeId`,
`runtimeVersion`, `runtimeOwned`, `availability`, `capabilities`, `models`, and
`modelMetadata` for the one active/default Runtime. Its `connectorId` is an
adapter identity, not a requestable Connector profile selector.

### `connector.models` request

```json
{
  "kind": "query",
  "protocol": { "major": 1, "minor": 0 },
  "requestId": "catalog-codex-001",
  "sessionId": "the-authenticated-session-id",
  "query": "connector.models",
  "payload": {
    "scopeId": "desktop",
    "connectorId": "connector.codex"
  }
}
```

Only `scopeId: "desktop"` and a non-empty `connectorId` are accepted. Unknown
payload fields and a `connectorId` passed to `runtime.models` are invalid IPC
requests, not a request to select a different Runtime.

### `connector.models.v1` response

```json
{
  "kind": "response",
  "protocol": { "major": 1, "minor": 0 },
  "requestId": "catalog-codex-001",
  "ok": true,
  "payload": {
    "schemaVersion": "connector.models.v1",
    "scopeId": "desktop",
    "connectorId": "connector.codex",
    "runtimeType": "codex",
    "catalogRevision": 123,
    "defaultModelId": "codex-model-a",
    "models": ["codex-model-a", "codex-model-b"],
    "modelMetadata": [
      {
        "modelId": "codex-model-a",
        "availability": "available",
        "capabilities": {
          "streaming": true,
          "cancel": true,
          "filesystem": true,
          "shell": true
        }
      },
      {
        "modelId": "codex-model-b",
        "availability": "available",
        "capabilities": {
          "streaming": true,
          "cancel": true,
          "filesystem": true,
          "shell": true
        }
      }
    ],
    "availability": "available"
  }
}
```

`catalogRevision` is a deterministic integer derived from the credential-free
catalog projection. `modelMetadata` is aligned to the returned model IDs; it
contains only availability and capability flags. The projection never includes
a token, API key, Authorization value, endpoint secret, or provider error body.
In `fixture-dual`, both the Codex and Kun catalog responses advertise
`streaming`, `cancel`, `filesystem`, and `shell` as `true`; this is the offline
test-adapter contract, not evidence of a live Provider permission grant.

## Local Agent Discovery

`connector.discover` and `agent.scan_local` are additive **query** aliases.
They do not create a Connector profile, Agent, project assignment, event, or
database record. Their payload is exactly `{}`; unknown fields are rejected as
`INVALID_QUERY`.

```json
{
  "kind": "query",
  "protocol": { "major": 1, "minor": 0 },
  "requestId": "local-discovery-001",
  "sessionId": "the-authenticated-session-id",
  "query": "connector.discover",
  "payload": {}
}
```

Replace the query string with `agent.scan_local` when the UI is presenting a
local Agent scan. Both return the same non-persisted candidate list:

```json
{
  "discoveries": [
    {
      "connectorId": "local.kun.shared-runtime",
      "runtimeType": "kun",
      "displayName": "Kun Shared Runtime",
      "availability": "available",
      "models": ["kun-model-a"],
      "catalogRevision": "42",
      "source": "kind=kun-shared-runtime;runtimeJson=C:\\...\\runtime.json;port=32123;version=0.2.34;build=example",
      "requiresConfiguration": false
    }
  ]
}
```

Each entry has exactly `connectorId`, `runtimeType`, `displayName`,
`availability`, `models`, `catalogRevision`, `source`, and
`requiresConfiguration`. `availability` is one of `available`, `unavailable`,
`unconfigured`, or `authentication_required`; entries are ordered by
`connectorId` for repeatable scans.

Codex discovery checks only an explicit/known executable path and its install
location. It never starts an app-server process, so it reports an empty model
list and `requiresConfiguration: true` until the user explicitly configures a
Connector. Kun discovery reads its `runtime.json` and, only when that record
contains a token, uses the existing bounded loopback runtime-info/catalog
checks to validate the port, process/listener ownership, build/version, and
health identity. The token is never copied into a candidate, IPC response,
event, log, or database record. No external Provider/model request is made.

## Connector routing and fail-closed errors

The Core registry may contain multiple adapters. The first registered adapter
remains the legacy/default Runtime for `runtime.models`; it is not a fallback
for Connector-bound catalog or execution work. For a Connector profile:

1. Core loads the exact `desktop` profile by `connectorId`.
2. The profile must be enabled.
3. Its `runtimeType` must name an available adapter whose adapter identity
   matches that type.
4. A pinned or frozen `modelId` must occur in that adapter's catalog.
5. Core persists the resolved Connector/runtime/model tuple before dispatch and
   decorates `connector.started`, `runtime.started`, output, and terminal
   events with the same route fields.

The stable resolver errors below are non-retryable at the IPC layer and carry a
safe `details.category`; their messages contain no provider diagnostic data.

| Code | `details.category` | Meaning |
| --- | --- | --- |
| `CONNECTOR_NOT_FOUND` | `connector_not_found` | The requested persisted profile does not exist. |
| `CONNECTOR_DISABLED` | `connector_disabled` | The profile is disabled. |
| `CONNECTOR_RUNTIME_UNAVAILABLE` | `connector_runtime_unavailable` | The named Runtime is absent, unhealthy, or not usable. |
| `CONNECTOR_SHARED_RUNTIME_UNAVAILABLE` | `shared_runtime_unavailable` | Kun's external Shared Runtime record or local listener is unavailable. |
| `CONNECTOR_RUNTIME_AUTHENTICATION_FAILED` | `runtime_authentication_failed` | Local Runtime authentication failed; no token or response body is included. |
| `CONNECTOR_RUNTIME_IDENTITY_MISMATCH` | `runtime_identity_mismatch` | The authenticated Kun Runtime does not match its rendezvous record. |
| `CONNECTOR_CATALOG_UNAVAILABLE` | `connector_catalog_unavailable` | The selected Runtime cannot provide a non-empty authoritative catalog. |
| `CONNECTOR_PROVIDER_AUTHENTICATION_FAILED` | `provider_authentication_failed` | A provider rejected the AgentTalk-created turn with 401/403. |
| `CONNECTOR_RUNTIME_MISMATCH` | `connector_runtime_mismatch` | The profile/frozen route does not match the selected adapter identity. |
| `CONNECTOR_MODEL_UNAVAILABLE` | `connector_model_unavailable` | The model is not in that Connector Runtime's catalog. |
| `CONNECTOR_BINDING_REQUIRED` | `connector_binding_required` | A model was supplied without a Connector binding. |

Malformed `connector.models` payloads return `INVALID_QUERY`. Resolver and
catalog failures return the classified error above instead of `QUERY_REJECTED`;
execution commands use the same safe classification for route-resolution
failures, and terminal Runtime events retain the matching safe reason. Core
never substitutes another Connector or model. A Runtime failure affects only
that Run and terminates it explicitly; Core must not silently retry it on a
different adapter.

## Shared v1 envelope, replay, and backpressure

Every command/query is correlated by `requestId` and bound to the authenticated
`sessionId`. Handshake requires the session credential, protocol major `1`, a
bounded `maxMessageBytes`, and, when present, a cursor from the current stream
epoch. Malformed JSON, unknown fields, mismatched sessions, and unsupported
protocol majors fail closed.

Other stable host error categories remain `INVALID_HANDSHAKE`,
`INVALID_ENVELOPE`, `INVALID_COMMAND`, `SESSION_MISMATCH`,
`UNSUPPORTED_PROTOCOL`, `REQUEST_ID_REUSE`, `COMMAND_IN_PROGRESS`,
`COMMAND_REJECTED`, `QUERY_REJECTED`, `REPLAY_GAP`, `INVALID_ACK`,
`SUBSCRIPTION_NOT_FOUND`, and `SUBSCRIPTION_OVERFLOW`. Runtime/provider
diagnostics are redacted before entering an event or response.

`events.subscribe` begins from a `StreamCursor`. Core retains a bounded window
and requires `events.ack`; a slow subscriber is isolated. `REPLAY_GAP` and
`SUBSCRIPTION_OVERFLOW` require a fresh `projection.snapshot` before a new
subscription. A cursor from another Core epoch is rejected with
`CURSOR_EPOCH_MISMATCH`.

## Binding, scope, and selection rules

`agent.model_binding.set` is the legacy complete-replacement command:

- omitted or `null` `connectorId` clears the Connector;
- omitted or `null` `modelId` clears the model;
- omitted `candidateModelListRevision` writes `0` (a supplied revision must be
  a non-negative integer);
- an attempted binding with a model but no Connector is rejected rather than
  persisted as an orphan.

`agent.model_binding.patch` is additive and uses three-state field semantics:

- an omitted field preserves its stored value;
- `null` clears that field (`candidateModelListRevision: null` writes `0`);
- a non-empty string sets `connectorId` or `modelId`; a non-negative integer
  sets `candidateModelListRevision`.

For either command, clearing the Connector also clears the model. A model cannot
remain bound without a Connector.

Identity model option requests (`identity_model_options.list`,
`identity_model_option.upsert`, and `identity_model_option.default`) must use a
legal `IdentityModelTarget`:

| `identityScope` | `projectId` | `conversationId` |
| --- | --- | --- |
| `base_agent` | absent or `null` | absent or `null` |
| `project_agent` | required | absent or `null` |
| `conversation_agent` | absent or `null` | required |

Each target is additionally isolated by `agentId` and `connectorId`. Defaults
are unique within that target/Connector pair. Option source (`runtime`,
`config`, or `manual`), availability (including `unverified`), catalog
revision, and the resolved-list revision/hash are persisted; malformed scopes
fail closed rather than being coerced to a neighbouring scope.

For selection, a conversation override has priority over a project override,
then the base-agent list/default. The selected Connector is filtered before the
candidate list is frozen, so a Codex list cannot supply a Kun model or vice
versa.

## Execution, events, retry, and rerun-current

For Connector-bound execution, `execution.start` supplies the frozen
`connectorId` and optional `modelId` in its existing payload. Core validates the
route before creating a dispatch and stores the Connector/runtime/model in the
Execution Run snapshot, model-selection snapshot, and Context Manifest. The
event sequence for a Context-bearing run is:

`execution.created` → `scope.frozen` → `context.assembled` → `context.sealed`
→ `execution.status_changed(assembling)` → `connector.started` →
`runtime.started` → zero or more `output.delta`/tool/handoff events → exactly
one terminal event.

`connector.started`, `runtime.started`, `output.delta`, and terminal execution
events include the frozen `connectorId`, `runtimeType`, and `modelId` route
fields. Independent Runs may interleave events, so clients must key every event
by `executionRunId` and must not infer the route from stream order.

- `execution.retry` creates a new Run from the source Run's frozen selection.
  If a request includes Connector/model fields, they must agree with that
  snapshot; Retry never reselects the currently active/default Runtime.
- `execution.rerun_current` creates a new Run after resolving the current
  Connector/binding/identity-option configuration and freezes that new result.
  It intentionally does not inherit the source Run's prior selection.
- `execution.cancelled`, `execution.completed`, `execution.failed`, and
  `execution.interrupted` are mutually exclusive terminal states. A Core
  restart marks each persisted non-terminal Run as `execution.interrupted`
  once.

## Flutter / Antigravity integration contract

1. Create or update a Connector profile first, then call `connector.models`
   with that profile's exact `connectorId` to populate its model picker.
2. Continue using `runtime.models` only for legacy/default-Runtime UI. Do not
   send a Connector ID to it and do not emulate Connector catalogs in a fake
   pipe.
3. Store a model choice with the binding patch semantics above. Never send
   credentials; `authEnvKey` is a non-secret reference only.
4. Send the selected `connectorId` and `modelId` with `execution.start`,
   subscribe to Core events, and render only route fields attached to the
   matching `executionRunId`.
5. On a classified resolver error, show the profile/catalog as unavailable and
   require an explicit user change or recovery; do not substitute another
   Connector or its default model.
6. Preserve the normal replay/backpressure flow: acknowledge `core-events`,
   load `projection.snapshot` after `REPLAY_GAP`, and do not synthesize a
   terminal event when the Named Pipe closes.

## Runtime and Owner-Gate boundary

Production Core keeps `unconfigured` as the first, unavailable, fail-closed
Runtime for legacy `runtime.models`. With no Runtime environment override, it
also lazily registers built-in `codex` and `kun` adapter types. Registration
does not start a process, open a Shared Runtime record, contact a Provider, or
read credentials; only a profile-bound health, catalog, or execution request
may perform bounded local discovery. A persisted Connector profile's
`runtimeType` selects its adapter exactly; its `connectorId` remains a profile
identifier and is never reinterpreted as the literal adapter name.

The Codex adapter owns only App Server children it starts itself. The Kun
adapter resolves and reuses an external Shared Runtime and never stops,
restarts, or kills it; shutdown may only interrupt an AgentTalk-created turn
and discard local bookkeeping. `mock` and `fixture-dual` require explicit
development mode. The dual fixture additionally requires
`AGENTTALK_CORE_DEV_MODE=1` and must be the only configured Runtime
(`AGENTTALK_CORE_RUNTIME=fixture-dual` or
`AGENTTALK_CORE_RUNTIMES=fixture-dual`); it exposes only deterministic local
`codex-model-a`/`codex-model-b` and `kun-model-a`/`kun-model-b` catalogs for
isolated Named Pipe tests. It is not evidence of a live Codex or Kun Provider
integration.

The following remain Owner-Gated and are deliberately outside this IPC change:

- real Provider credentials, real Codex/Kun process/turn Smoke, and credential
  lifecycle work;
- formal user-database migration or inspection beyond approved metadata gates;
- signing, installer publication/installation, release, tag, PR, or push.

Contract verification may use offline fixture adapters, isolated SQLite files,
and local Named Pipe tests only. No real Provider, credential, production
database, signing key, release, or remote push is part of this branch.
