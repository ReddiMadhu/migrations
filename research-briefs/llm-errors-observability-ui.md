# Model Adapters, Error Taxonomy, Observability & UI Surfaces

HEAD `00102833` (2026-09-22). All citations are current code unless labelled DOC or NOTE.

## Top verified findings (most surprising first)

1. **The OTel telemetry backend emits log records, not spans.** `session-telemetry-otel` composes a `LoggerProvider` + `BatchLogRecordProcessor` + OTLP/HTTP **log** exporter (`packages/session/session-telemetry-otel/src/index.ts:31-38`). There is no tracer, no span surface. "OTel spans/events" in the usual sense does not exist here.
2. **`llmRetryPolicyOf(stream)` is a doc ghost.** `docs/subsystems/llm-streaming.md:311` documents it; grep across `packages/` returns **zero** occurrences. The stream sidecar was removed (NOTE `.agents/notes/implemented/architecture/2026-07-29-terminal-llm-stream-failures.md`: "The public `isLlmAdapterFailure`, `llmFailureOf`, and `llmRetryPolicyOf` sidecar APIs are absent"). Retry policy now rides the `agent/request-error` payload (`packages/core/agent/src/runtime-types.ts:353`).
3. **There is no content hash in streaming.** No `contentHash`/`fingerprint` anywhere in `packages/llm`, `packages/sdk`, or `packages/session-query`. Attempt identity is `(LlmAttemptId, index, revision)`. The only digest in the content path is the attachment's own `sha256:` id (`packages/llm/llm/src/content.ts:77`). Marked **UNVERIFIED / absent** against the assignment's premise.
4. **No circuit breaker exists** in token or cost accounting (grep for `circuit`/`breaker` finds only short-circuit comments). Backpressure is the bounded retry policy plus `IMAGE_OFFLOAD_REQUIRED`.
5. **The CLI has no chat UI.** `apps/cli` is a profile launcher (`apps/cli/src/bin.ts:30-76`); bundles are `acp-app, base, headless, sdk-app, sdk-minimal, web-app`. Interactive conversation is **web-only**.

---

## 1. The ModelAdapter contract

`LlmAdapter` is an abstract class at `packages/llm/llm/src/index.ts:204`. The **only required method** is `abstract stream(options: GenerateOptions): AsyncIterable<StreamChunk>` (`:285`). Everything else has a default:

- `providerInfo(provider)` (`:210`), `providerRetryPolicy(provider)` (`:219`, default `undefined` → normal defaults), `imageRequestPricing(provider, model)` (`:232`, default none), `listModels(provider)` (`:243`), `resolveModel(provider, model, signal)` (`:256`), `prepareCall(provider, model, signal)` (`:273`) — the latter binds exact model metadata and dispatch to **one adapter generation**, "so settings changes between preparation and dispatch cannot combine one generation's capabilities with another's endpoint" (`:264-267`).

**Responsible for:** translating provider wire messages into the neutral `StreamChunk` vocabulary; honoring `options.signal`; classifying failures into stable codes; supplying provider-owned retry policy and image pricing; mandatory app attribution headers on every provider HTTP request (`:200-202`).

**Must NOT decide:**

- **Routing/validation.** `listModels` is "advisory: an adapter may accept unlisted model ids, and consumers must not turn absence into request rejection" (`:237-239`); `resolveModel` "is independent of the advisory catalog and does not validate request routing" (`:248-249`). Registration key is the **provider**, not the model (NOTE `2026-07-14-provider-routed-llm-adapters.md`; duplicate provider → `DUPLICATE_ADAPTER`).
- **Recovery.** Adapters classify facts (`LlmFailure`); the *policy* is provider-owned and *execution* belongs to `dsh-llm-retry` on `agent/request-error`. Adapters disable library retries — "One adapter call is one provider attempt" (DOC `llm-streaming.md:302`).
- **Durability/surface shape.** The loop, not the adapter, decides `assistant/message` vs `assistant/attempt`.
- **Presentation.** The content union "is SEMANTIC, never visual… Colors, icons, ordering, and collapse defaults are the consumer's business" (`packages/llm/llm/src/message.ts:48-53`).

`LlmRuntime` is the **normalization boundary**: it catches only final-adapter selection, synchronous dispatch, iterator construction, and `next()` failures, converting them to a terminal `finish`; caller cancellation or `ABORTED` → `aborted`, everything else → `error` (`packages/llm/llm/src/index.ts:1130-1138`, `adapterFailureChunk`).

---

## 2. Streaming semantics

**Chunk vocabulary** (`packages/llm/llm/src/types.ts:440-452`): `block-start`, `text-delta`, `reasoning-delta`, `tool-call-delta`, `block-end`, `usage`, `finish{reason, replayState?}`. Ordering contract: `usage` before `finish`, nothing after (DOC `llm-streaming.md:299`); tool `arguments` stay raw JSON strings end-to-end (`:300`).

**Ephemeral** = process-local `AssistantStreamFrame`s (`packages/core/agent/src/runtime-types.ts:128-161`): `start | chunk | end`, keyed by `LlmAttemptId` = `${sessionId}:${attempt}` (`packages/core/agent-loop/src/assistant-stream.ts:45`) plus a monotone `revision`; `end` carries `outcome: {kind:'committed', eventType, seq} | {kind:'abandoned'}`. Published on `agent/assistant-stream`. A reconnecting Web follower refolds these via `SessionAssistantStreamAccumulator`, which **drops the attempt on any revision or index gap** (`packages/api/session-controller/src/assistant-stream.ts:43-47, 61-71`).

**Durable** = the session log. `assistant/message` embeds `{message, stream, usage?, interrupted?}`; `assistant/attempt` embeds `{stream}` only (`packages/core/session/src/types.ts:341-355`). The embedded stream is `AssistantStreamRecord` — lossless but *compacted* into runs (`text-chunks`, `reasoning-chunks`, `tool-call-chunks` with `dt` timestamp deltas) plus raw `chunk` records (`packages/llm/llm/src/assistant-stream.ts:20-44`).

**The message/attempt decision** (`packages/core/agent-loop/src/agent.ts:433-511`): a normal finish commits `assistant/message` with `usage` (`:502-511`); an `error`/`aborted` finish commits `assistant/attempt` then fires `agent/request-error` (`:471-491`); an abort *with* delivered content commits `assistant/message` with `interrupted: true` (`:433-447`); an abort with no content commits `assistant/attempt` (`:449-453`).

**Usage**: `TokenUsage` buckets are **disjoint** — `inputTokens` is uncached input only, cache reported separately (`packages/llm/llm/src/types.ts:168-190`). `BlockAssembler.usage` keeps the last `usage` chunk (`packages/llm/llm/src/assembler.ts:85-88`).

**Finish reason**: `FinishReasonMap` = `stop | tool-calls | max-tokens | aborted{failure} | error{failure}` (`packages/llm/llm/src/types.ts:157-166`); `assembler.finish` defaults to `{kind:'stop'}` when the stream omitted one (`assembler.ts:187-189`).

**Content hash**: **not present** (UNVERIFIED). Identity/dedup is by `attemptId`/`revision`/`index`; the client swaps transient rows for the durable settlement by `attemptId` (`packages/api/session-controller/src/client/contract/events.ts:181-196`, `settleAssistant`).

---

## 3. Error / retry classification — the closed vocabularies

**`TurnEndReason`** (`packages/core/session/src/types.ts:201-229`, merge-extensible): `completed`, `aborted{reason: TurnEndCancelCause}`, `blocked`, `error{error: LlmFailure}`, `max-tokens`, `interrupted` (crash-orphaned, synthesized on cold read), `forked` (fork-seed only). Deliberately **omits** ACP's `refusal`/`max_turn_requests` — producer-gated (`packages/core/session/README.md:189`).

**`ApprovalOutcome`** (`packages/interaction/user-approval/src/types.ts:32`): `'allowed-once' | 'rejected' | 'cancelled' | 'unavailable'` — closed; "Callers fail closed on `unavailable`". Logged on `approval/decided` (`:55-58`). Mapped 1:1 into tool decisions at `packages/core/tools/src/index.ts:1750-1764` (each non-grant → a distinct `deny` reason; `assertNever` exhaustiveness).

**`PreToolDecision`** (`packages/core/tools/src/index.ts:606-610`): `allow | deny{reason, info?} | cancel | ask{reason?}`. Input rewriting is explicitly excluded from the union ("arguments are already logged and presented").

**`PostToolDecision`** (`:616-619`): `accept{content? | value?, additionalContexts?} | block{feedback, additionalContexts?}`. `block` materializes an `isError` result (`:1785-1792`).

**LLM-level retry categories** — `DEFAULT_RETRYABLE_CODES` (`packages/llm/llm-retry/../llm/src/retry-policy.ts:18-24`): `EMPTY_RESPONSE`, `RATE_LIMIT`, `SERVER`, `TIMEOUT`, `TRANSPORT`. Non-retryable codes emitted by adapters: `AUTH`, `QUOTA`, `CONTEXT_WINDOW_EXCEEDED`, `INVALID_REQUEST`, `INVALID_CREDENTIAL`, `MALFORMED_RESPONSE`, `UNSUPPORTED_CONTENT`, `ABORTED`, `IMAGE_OFFLOAD_REQUIRED`, and fallback `HTTP_<status>`.

**Where retry is classified:** the `agent/request-error` waterfall carries `{turn, step, provider, failure, retryPolicy, signal}` (`packages/core/agent/src/runtime-types.ts:353`); `dsh-llm-retry` listens and executes provider policy (`packages/llm/llm-retry/src/index.ts:243-252`). Modes: `normal` (maxRetries default 5, bounded exponential backoff with jitter) or `always` (unbounded until success/cancel/disposal) (`retry-policy.ts:36-57`). `localDelay` (`llm-retry/src/index.ts:59-64`) honors `providerRetryAfterMs` over local backoff.

**Classifier implementations:** DeepSeek `providerError()` maps status/type → codes (`packages/llm/llm-deepseek/src/transport.ts:22-43`); `translate.ts:149` raises `EMPTY_RESPONSE` on a contentless `stop`; `adapter.ts:64-67` maps watchdog expiry → `TIMEOUT`, caller abort → `ABORTED`, else `TRANSPORT`. `normalizeLlmFailure` trusts only Harness-owned codes — third-party SDK codes collapse to `'UNKNOWN'` (`packages/llm/llm/src/adapter-failure.ts:104-107`).

---

## 4. Token and cost accounting

Measured, in three layers:

1. **Provider-reported usage** — `tokenUsageProjection` fold (`packages/llm/token-meter/src/usage-projection.ts:14-43`): `uncachedInputTokens, outputTokens, cacheReadTokens, cacheWriteTokens`, with `addReplacing` subtracting a re-folded event's previous buckets to avoid double-counting on replay. `pressureFrom` = input + cacheRead + cacheWrite, **excluding output** (`:78-79`).
2. **Surface (heuristic) cost** — `contextBreakdownProjection` (`breakdown-projection.ts:48-76`) prices the retained surface into `systemTokens / toolsTokens / messageTokens` using a fixed estimator; tools are repriced from each `request/header`.
3. **Route-projected cost** — `priceSurface()` (`packages/llm/token-meter/src/route-pricing.ts:32-75`) replaces image/file structural heuristics with what the route actually sends: provider visual tokens + the model-visible handle text. It throws if the pricing answers a mismatched occurrence count (`:48-52`).

**usage-projection vs surface-projection** (the two folds named in the assignment): `usage-projection.ts` is the durable provider-usage fold; `surface-projection.ts` (`packages/llm/token-meter/src/surface-projection.ts:64-96`) is the **O(1) occupancy fold** built on the *shadow-price protocol*: `compaction/summary`/`compaction/prune` arms a `ShadowPriceClaim`, the immediately-following surface `replace` consumes it (`:68-95`). A replace with no armed claim folds **zero delta** — "historical replay degrades to drift instead of failing" (`:84-88`); a claim for a different range **fails loud** (`:89-94`).

**Per-turn accounting**: `TurnTokenUsage` (`packages/llm/token-meter/src/turn-usage.ts:12-27`) aggregates across attempts, exposing cache/reasoning buckets only "when every attempt reported the bucket" and `routes` only when every attempt has provider/model attribution.

**Circuit breakers: none.** The only backpressure is `maxRetries` (default 5) and the `always` mode; `IMAGE_OFFLOAD_REQUIRED` with `offloadImages` is a bounded recovery signal, not a breaker (`packages/llm/llm/src/error.ts:166-172`).

---

## 5. Telemetry

**Capture** (`packages/session/session-telemetry/src/index.ts`): one logical record per canonical Session event. Two channels — `ledger` (log mirror) and `ops` (`agent-error`, `shutdown`, deliberately carrying no `event.seq` so they can't be mistaken for ledger rows) (`:64-70`). Severity is **pre-mapped at capture** so a receiver can alert with zero config: `error` for tool `isError`, `turn/end` error reasons, and `agent-error`; others `info` (`:46-55`). `session-telemetry/record` is the redaction waterfall — ships no rules of its own; "exported data is exactly as clean as the rules a deployment mounts" (`:24-44`). Capture mode is `live` (firehose) or `on-demand` (explicit canonical-log replay) (`coordinator.ts:29-38`).

**OTel backend** (`session-telemetry-otel/src/index.ts`): `LoggerProvider` + `BatchLogRecordProcessor` + `OTLPLogExporter` (`:31-38`) — **log records only, no spans/traces**. Modes `FEEDBACK_ONLY` (default) / `DISABLED` (`:46-52`). Feedback gating: only the session's own `feedback/*` events authorize replay (`:58-66`). Owns an outer shutdown deadline because the SDK's export timeout doesn't bound `forceFlush()` (`:294-309`).

**session-stats** (`packages/session/session-stats/src/projection.ts`): a pure fold to `turns, steps, llmMs, toolMs, ttftMs, ttftSteps, decodeMs, decodeTokens`. `step/end` — not `assistant/message` — is the counted step event "because it is the step lifecycle authority" (`:6-11`), avoiding overcount of max-tokens empty messages and undercount of cancelled steps.

**Queryability**: stats and token projections reach the UI via the session-projection wire (`viewSchema` + `view`) and `useProjection`; the Web composer's `StatsPills` reads `sessionStats` + `tokenUsage`, with a client-side `deriveStats` fallback "for assemblies without the `sessionStats` projection" (`packages/client/ui-chat/src/client/chat/StatsPills.tsx:43-53`).

---

## 6. UI surfaces

**What exists.** The `dsh` CLI is a launcher: `profile | plugin | dump-config | dump-config-schema` (`apps/cli/src/bin.ts:34-75`); `dsh --profile web` boots the web app (`apps/cli/src/args.ts:10-11`). There is **no terminal chat UI** in this checkout (no `ink`/`blessed` dependency anywhere in `apps`). Interactive surfaces: the **Web client** (`apps/web` + `packages/client/ui-*`) and **ACP** (`packages/acp`). `packages/host/webserver/src/index.ts:1-7` is a deliberate non-harness route registrar: "It knows no harness concepts and serves no files." The Gateway's WS mux is `/api/remote.mux` (`packages/api/gateway/src/stream-protocol.ts:7`).

**Model-facing vs UI-card separation.** Two distinct projections per tool:
- `ToolOutputDefinition = { schema, render, presentationMeta? }` (`packages/extensions/tool-cordis/src/api-catalog.ts:7254`). `render` produces the **model-facing** `ContentBlock[]`; `presentationMeta` is a separate `JsonValue` persisted on the durable `tool/result` `meta` and read back by `presentResult` — "the structured `lines`/`path`/`lang` fields cannot be reconstructed from the model-facing result text alone" (`packages/core/tools/src/presentation.ts:270-308`).
- `projectContent` installs execution-prepared content **before** `tools/post-execute`, so "retention sees real images" while policies remain authoritative (`packages/mcp/mcp-client/README.md:111`).

**UI cards (representative sample):**
- Tool rows: `ui-tool/src/client/tool/models/tool-call-model.ts` classifies tool name → `ToolRowVariant` (`search|read|bash|write|edit|code|others`, `:47-74`) with `ToolRowState = 'running'|'ok'|'error'|'stopped'` (`:21`). Keyed `tool.call.toolview` entries *replace* the generic row; `cordis_define` is deliberately absent from the table because a keyed entry already owns it (`:40-46`).
- Assistant nodes: `ui-chat/.../conversation-nodes/assistant.ts` folds `StreamChunk`s into `AssistantBlock`s, resets on retry while preserving `firstTokenTime` (`:82-87`).
- Approval: `ui-approval/.../ApprovalPanel.tsx` is a composer takeover answering exactly `'allowed-once' | 'rejected'` (`:26-49`) — narrower than the four-value `ApprovalOutcome`.

---

## 7. The session-query API and UI state reconstruction

**Service** (`packages/session-query/session-query/src/index.ts:98`): `SessionQueryEngine` is the "unified live-preferred session query service" — exact reads/filters/traces are backend-independent concrete behavior; a backend implements full-text observation, reconciliation, ranking, and cursors. `extractSessionEventText` (`extraction.ts:15-44`) defines the searchable text per event type; notably `reasoning` blocks and `assistant/attempt` contribute **no** search text. Host-side request types live in `packages/api/session-controller/src/types.ts:262-485` (`SessionListRequest`, `SessionSearchRequest`, `SessionPageRequest`, `SessionFollowRequest`, `SessionProjectionsRequest`).

**Client reconstruction** (`packages/api/session-controller/src/client/`): a `Session` owns a contiguous event window (`MutableSessionEventSource`, `contract/events.ts:120-204`) supporting `replace | prepend | append | settle-assistant`. The window mixes durable events with client-only **transient** `assistant/live-chunk` rows (`:6-17`); `settleAssistant(attemptId, entry)` filters the attempt's transient rows and splices in the committed `assistant/message`/`assistant/attempt` (`:181-196`). `SessionSnapshot` (`contract/snapshot.ts:69-91`) exposes `running`, `openState ('cold'|'loading'|'open'|'error')`, `hasMore`, `pendingSubmissions`, `lastAgentError` — echoes are "Client-memory only — reload and reconnect rebuild the conversation from durable events alone" (`:44-46`). `ui-conversation`'s `ConversationNodeAssembler` (`assembler.ts:180`) then folds that window into business contexts and view snapshots.

---

## Guarantees vs Conventions

| Aspect | Guaranteed (enforced in code) | Convention (documented, not enforced) |
|---|---|---|
| Adapter stream contract | `stream()` is the only required method; `LlmRuntime` normalizes throws → terminal finish (`index.ts:1130`) | `usage` before `finish`; attribution headers (DOC `llm-streaming.md:299,306`) |
| Error codes | `LlmFailure.code` is the routing key; `assertNever` exhaustiveness on `ApprovalOutcome` (`tools/index.ts:1764`) | Adapters reuse canonical codes rather than inventing new ones |
| Retry | `retryableCodes` closed set + `maxRetries`; provider `retry-after` honored (`llm-retry/index.ts`) | Adapters disable library retries (DOC `:302`) |
| Token accounting | Disjoint usage buckets (`types.ts:168-190`); shadow-price mismatch fails loud (`surface-projection.ts:89`) | Fixed estimator agreement across producers (`surface-projection.ts:11-13`) |
| Durability | Every event JSON-validated at append; one durable settlement per attempt (`session/types.ts:276-279, 355`) | Live frames are best-effort; reconnect refolds |
| Telemetry | Redaction waterfall ships no default rules (`session-telemetry/index.ts:24-44`) | Severity pre-mapping for zero-config alerting |

## Implications for a regulated SAS→PySpark migration verification platform

1. **Route on codes, never messages.** `LlmFailure.code` is the stable, machine-routable taxonomy (`error.ts:14`); audit and alert on it, since `normalizeLlmFailure` deliberately discards third-party SDK codes (`adapter-failure.ts:104-107`).
2. **The durable log is the only audit source.** Transient streaming frames are process-local and dropped on any gap (`session-controller/assistant-stream.ts:43-47`); a compliance replay must read `assistant/message`/`assistant/attempt` + `approval/decided`, which are one-record-per-event.
3. **Telemetry is opt-in and policy-empty by default.** `FEEDBACK_ONLY` is the default mode and the redaction waterfall ships no rules — a regulated deployment must mount its own `session-telemetry/record` rules or export nothing.
4. **No content hash ⇒ no built-in tamper-evidence.** If you need content-integrity attestation for generated migration artifacts, add it; DSH only guarantees lossless-JSON event validation at append time.
5. **Cost is heuristic, not billed.** Only `TokenUsage` is provider-authoritative; surface/context figures are estimator-derived and can drift on pre-shadow-price logs (`surface-projection.ts:84-88`). Never bill from the meter.
6. **UI is web-only.** Any human-review station for reviewers/approvers must be the Web client (or ACP); there is no terminal approval UI to fall back on.
