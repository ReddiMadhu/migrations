# Guards, Approval, Permission Presets, and Credentials — Source Brief

HEAD `00102833dfaee1da9f48a3a8eae9d34005a75218` (2026-09-22).

**Path aliases** (all under `C:\Users\madhu\deepseek-harness\`): `tools` = `packages/core/tools/src/index.ts` · `tools-spec` = `packages/core/tools/tests/scoped.spec.ts` · `store` = `packages/core/scope/src/store.ts` · `approval` = `packages/interaction/user-approval/src/index.ts` · `approval-types` / `approval-inv` / `approval-spec` = its `types.ts` / `invariant.ts` / `tests/approval.spec.ts` · `esc` = `packages/sandbox/sandbox/src/escalation.ts` · `loop` = `packages/core/agent-loop/src/tool-calls.ts` · `sess-types` = `packages/core/session/src/types.ts` · `cred` / `cred-types` = `packages/credentials/credentials/src/{index,types}.ts` · `local` = `packages/credentials/credentials-local/src/index.ts` · `authz` = `packages/credentials/authorization/src/index.ts` · `telemetry` = `packages/session/session-telemetry/`.

## Lead findings

1. **`packages/guard/` contains ZERO `ToolGuard` implementations** — both packages are waterfall listeners (`tools/post-execute` + `agent/pre-step`; `tools/execute`). The only non-test `ctx.tools.guard()` call in the repo is the structured-output "run is complete" seal at `packages/subagent/subagent-in-process-driver/src/structured.ts:109`.
2. **A guard denial leaves no structured trace in the durable log** — it becomes a `tool/result` whose text is `Error: <reason>`, and the event's `error` field comes only from `result.error.info` (`loop:285`). Registration does not even fire `tools/change` (`tools:1139`).
3. **No approval store, no memoization, no reuse — by construction.** Every `request()` mints a fresh id (`approval:224`); `allowed-once` is literal and per-action.
4. **Doc/code divergence:** `docs/subsystems/approval.md:33` documents `ctx.approval.effectivePolicy(session)` for consumers; it is `private` (`approval:243`).
5. **Credentials are never injected into subprocess environments**, and **DSH ships no PII redaction and no secret detection** — the telemetry redaction waterfall "ships NO rules of its own" (`telemetry/README.md:116`).

---

## 1. ToolGuard

**Type** — `tools:730`: `export type ToolGuard = (execution: Readonly<ToolExecution>) => string | undefined`. Its doc comment is the contract: "Because guards have no allow result, listener ordering cannot turn a denial back into permission."

**Registration, storage, disposal, inheritance.** `ToolRuntime.guard()` (`tools:1135`) runs `this.layers.effect(this.ctx, layer => layer.guards.append(guard), { label: 'tools.guard()', notify: false })`. Each append is a separately keyed entry with an idempotent undo (`store:122-133`), and the registration is a Cordis `ctx.effect` — so a guard's lifetime is the registering fiber's and the returned disposer is the exact lift. Guards sit in `ToolLayer.guards` (`tools:736`), one layer per scope plus one global layer (`store:159-171`). `scopeOf(this.ctx)` picks the layer — a root context registers globally, `agent.ctx` for that agent only (`tools-spec:300-315`: a scoped guard denies agent `a` but not `other` and not agentless calls). `chainLayers` returns overlays farthest-ancestor-first, exact scope last (`store:192-199`), so ancestor guards apply to descendants and the nearest scope is consulted last; disposing a fiber lifts its entries and reclaims an emptied layer (`store:257-262`), and two registrations from one context unwind independently (`tools-spec:311-315`).

**When evaluated.** In `prepareExecution`, only after the whole `tools/pre-execute` waterfall returned `allow` (or an `ask` resolved to `allowed-once`): the waterfall defaults to `{ kind: 'allow' }` (`tools:1504-1507`); `ask` is routed through `serviceAsk` **before** guards (`tools:1508-1511`); `tools:1518` then computes `denialReason` only when the decision was `allow`. So an approval grant does not bypass guards and a `deny` short-circuits before them. A mode-collapsed call is denied even earlier, so policy listeners never observe — let alone approve — a call that can only fail (`tools:1399-1404`).

**Composition — first denial wins.** `guardReason` (`tools:1144-1153`) checks the global layer, then the chain layers farthest-first, returning the first non-`undefined` reason; within a layer `ToolLayer.guardReason` iterates insertion order (`tools:766-772`). Agentless calls check only the global layer (`tools:1147`). No priority, weight, or override — a guard can only abstain or deny.

**Monotonicity WITHIN a call — guaranteed.** The return type has no allow result and the stage runs after the entire extensible waterfall: `tools-spec:302-308` shows a listener registered later with `prepend: true` can force the pre-execute decision to allow "but cannot bypass the owner-level monotonic guard that runs after the waterfall."

**ACROSS time — not guaranteed, not claimed.** Guards are in-memory and effect-bound; nothing durable records that one existed, registered, or denied. A guard registered by an earlier guard is live-iterated on the same call (`tools-spec:335-354`); one that disposes itself and registers a replacement defers to the next call (`tools-spec:356-376`).

**How a denial reaches three audiences.** At `tools:1520-1530` a denial builds `{ content: [{ type:'text', text: 'Error: <reason>' }], isError: true, error: { message: reason, ...info } }` and routes it as `post-result`:

- **(a) Model:** the text becomes the tool result message (`loop:277-289`) and rides the next request as an `isError` result. Routing through `post-result` means `tools/post-execute` listeners still see denials — what `repeat-tool-reminder` exploits to count a model hammering a denied call.
- **(b) Human/UI:** `tool/result` is a `SurfaceEventType` (`sess-types:439-447`) reaching the ordered surface with `surfaceOp: 'append'` — there is no dedicated "denied" card, the UI renders the error text. Only the sandbox family adds markers: `[sandbox: file access denied under <mode> mode]` plus the escalation hint (`esc:71-86`).
- **(c) Events:** `tools/result` is emitted with the frozen exec and result, scope-filtered by agent, observer failures contained (`tools:1694-1712`). But the durable `tool/result` stores `error: result.error.info` — i.e. `{ name, code }` from a `HarnessError` — **not** the message. A guard denial (or a bare `deny` without `info`) leaves no structured failure identity in the log.

---

## 2. CONFIRMED: `packages/guard/` ships no `ToolGuard`

- **`repeat-tool-reminder`** — "Advisory per-agent repeat-call detector … **without vetoing or rewriting calls**" (`packages/guard/repeat-tool-reminder/src/index.ts:3`). Registers `tools/post-execute` (count → delegate → fold the reminder onto whatever came back, `:220-231`) and an `agent/pre-step` chain reset (`:236-239`). State is a `WeakMap<Agent, Chain>`; untracked tools are transparent to the chain; denied calls still count.
- **`timeout-policy`** — a `tools/execute` around-wrapper arming a cooperative deadline, replacing the result only when its own timer fired, restoring the caller's signal in `finally` so post-execute never sees its timeout signal (`packages/guard/timeout-policy/src/index.ts:56-80`).

**Design intent.** The reorderable waterfalls are the policy plane; the monotonic guard is reserved for *owner policy that must not be reordered* — "Use `ctx.tools.guard()` when an invariant needs a monotonic final denial" (`docs/cookbook/extension-cookbook.md:33`, `docs/cookbook/adding-a-tool.md:59`). `guard/` hosts optional behavioral-hygiene leaves — and the group is still unsettled: `timeout-policy/src/index.ts:6-9` carries an open **FIXME** to rename to `dsh-timeout-guard` "before the first tagged release", while the archived note `.agents/notes/archived/feature/2026-07-08-repeat-tool-guard.md` describes `packages/guard/repeat-tool-guard/`, an `agent/prompt-submit` reset hook, and source `{kind:'plugin', plugin:'repeat-tool-guard'}` — all superseded (the package is `repeat-tool-reminder`, the hook is `agent/pre-step`, the source is `{ kind: 'repeat-tool-reminder' }`, `src/index.ts:64`, `:237`). **The note is stale; trust the code.**

---

## 3. The approval system

**Identity and lifecycle** (`approval:215-234`): `hasOpenTurn` → append `approval/asked {id, toolName, callId?, reason?}` → `decide()` → append `approval/decided {id, outcome}` → return. `ApprovalRequestId` is a `Branded<'ApprovalRequestId'>` ("Service-issued — one fresh id per request", `approval-types:17`) that pairs the two audit events without becoming interchangeable with tool-call or session ids. Both events are log-only, never in the model transcript (`approval-types:38-59`).

**`hasOpenTurn` and why** (`approval:84-92`, scanning back for `turn/start` vs `turn/end`): "the turn is the durable log's commit/replay boundary, so a bare event appended between turns is indistinguishable from a crash tail and silently dropped on reload." Violating it throws before anything is appended. The invariant companion enforces the same independently — either event outside an open turn (`approval-inv:33, 39`), a repeated open id (`:35`), an unmatched `decided` (`:40`), or a non-vocabulary outcome (`:41-43`).

**`decide()` — every fail-closed path** (`approval:267-307`): already-aborted signal → `cancelled` (`:269`); effective policy `never` → `rejected` **before any dispatch** (`:275`); unclaimed request → `unavailable` (waterfall default, `:283`); throwing answerer → `unavailable` whether sync or async (`:291`); non-vocabulary return → normalized to `unavailable` (`:288`); abort winning the race → `cancelled` with the late answer discarded as a settled-promise no-op (`:294-306`). The promise chain is entered *before* dispatch so a synchronous throw lands in the same rejection path as an async one (`:276-279`).

**Closed vocabulary.** `'allowed-once' | 'rejected' | 'cancelled' | 'unavailable'` (`approval-types:32`) — "Callers fail closed on `unavailable`." `allowed-once` grants only the asked-about action; `ToolRuntime.serviceAsk` maps the three non-grants to distinct deny reasons so "the model can tell a human 'no' from an absent approval channel" (`tools:1750-1765`), while `approveEscalation` throws on them (`esc:177-185`).

**`ask` vs `never` and where `never` is enforced.** Inside the service's own request path, not as a listener — a listener registered with `prepend: true` after the service mounts "would sit ahead of any gate LISTENER, so a listener-shaped gate cannot keep the documented promise that 'never' rejects deterministically regardless of registration order" (`approval:270-274`). Proven both ways: a prepended answerer that would bypass a listener gate is never consulted (`approval-spec:422-442`). The effective value is the last `approval/policy` fold ?? config default ?? `'ask'` (`approval:243-245`); the single write path is `setApprovalPolicy`, which validates and throws before the log changes (`approval:100-105`).

**Turn-enclosed audit pair.** `approval/asked` + `approval/decided` are the *sole* durable record of an ask. Model-visible policy travels separately: a `systemPrompt.context` section placed after retained history so a switch does not rewrite the stable cache prefix (`approval:162-174`), plus `setPolicy` injecting a `user/message` notice on a live switch (`approval:184-195`).

**Divergences.** `docs/subsystems/approval.md:33` documents `ctx.approval.effectivePolicy(session)` for consumers; it is `private`. The same page also omits that the `ask`→`allow` mapping runs *before* monotonic guards (`tools:1508-1518`), which `docs/tool-execution-pipeline.md:63` states correctly.

---

## 4. The stale-approval question

**No approval store, memoization, or reuse exists.** `request()` always allocates a new id; nothing durable records a grant — only the *decision* of one ask.

**Closest real behaviors:**

- **`allowed-once` is per-action, not per-(tool, args).** An identical call re-asks; a resumed session re-asks. There is no "remember this approval" knob in the API.
- **`permission/preset` records intent, never a grant.** The durable event writes two *knobs* (`sandbox/mode`, `approval/policy`) through their canonical setters; `derive()` re-derives the preset name from the current knob folds on every read, a still-matching last selection winning shared-bundle ties (`packages/interaction/permission-presets/src/index.ts:343-355, 397-412`). `custom` is a derived not-a-preset state, never an event payload (`:79`, `:410`).
- **Sandbox escalation and the ACP bridge are both one-shot by design** — "A strictly wider mode requires approval and applies only to this call" (`esc:144-152`), and the bridge "offers one-shot choices only and never infers a durable grant from an unknown client response" (`packages/acp/acp/src/index.ts:152-154`).
- **The structural analogue in credentials** is per-operation re-resolution — "that per-operation read is what makes a changed credential reach the next operation without a restart" (`cred:176-183`).

A deployment wanting "ask once per session for this class of action" must implement it as an `approval/request` answerer with its own state; the seam will not do it.

---

## 5. Credentials

**Two disjoint key spaces** (`cred-types:14-29`): `CredentialRef` (a POSIX-identifier brand, i.e. an environment-variable name) and `CredentialKey` (`<scope>/<id>`, where scope is the owning plugin's registered name — "a record's payload is written in its owner's format", `cred-types:20-28`). The `/` keeps the grammars from colliding (`cred:19-22`).

**What is stored.** `$DSH_HOME/.credentials.yaml`, version 1, with `refs:` and `records:` sections (`local:166-227`). Refs map POSIX names to non-empty strings; records are `ApiKeyRecord { kind:'api-key', key?, env? }` or `GrantRecord { kind:'grant', payload: unknown }`. An `api-key` carrying neither field "states that its owner confirmed ambient authentication, which is configured, not blank" (`cred-types:31-44`).

**Layering, resolution, hot reload.** Precedence (`local:609-617`, documented at `local:1-35`): inherited process environment (read-only, **wins**) > managed YAML (writable) > invocation-cwd `.env` (`project-env`) > `$DSH_HOME/.env` (`user-env`) — the inherited environment wins because a CI secret or container `-e` "is this run's explicit intent" (`local:12-15`). A seam-wide rule binds it: an empty stored value is absent everywhere. `LocalCredentialProvider` resolves synchronously from in-memory snapshots that a chokidar watcher replaces wholesale on external edit, so a deleted entry never lingers (`local:569-607`, `:869-891`). Record writes take a cross-process file lock and re-read from disk first (`local:666-699`) — what makes a token refresh safe.

**Environment injection — none.** Credential values are never injected into a subprocess environment. `ApiKeyRecord.env` (e.g. `AWS_PROFILE`) is consumed only in-process, translated into the pi-ai library's own credential object by `toPiCredential` (`packages/llm/llm-pi-ai/src/auth.ts:74-84`). Without the seam composed, consumers fall back to the ambient launch environment, read-only (`packages/llm/llm-deepseek/src/index.ts:72-79`). The model can *name* a credential (`apiKeyEnv`) but never read one: the README's Model Experience is "Indirectly, through the consuming adapter, which resolves each credential reference and owns every model-facing use a value authorizes", and resolved values never enter a request prefix.

**Redaction (credential-specific).** `describe`, `describeRecord`, and `listRecords` return views with no slot a value could ride in (`cred-types:63-74`, `cred:125-146`) — "which is what lets the whole read half cross the Remote wire." YAML parse errors report only code + line/column because "the parser quotes the offending source line and in this document that line is a secret" (`local:159-164`). The document is created `0600` and rejected if group/other bits are set (`local:96-146`, `:872`), and writes are refused when the inherited environment would shadow them into apparent no-effect (`local:780-787`). Note `apiKeyEnv` uses `role('credential-ref')` (`packages/llm/llm-deepseek/src/config.ts:93`), *not* `role('secret')` — the reference name is safe to display.

**Failure handling.** An invalid document fails plugin activation rather than being treated as empty — "a credentials document that exists but cannot be trusted must never be treated as 'no credentials stored'" (`local:797-811`); a failed hot-reload warns and keeps the last good snapshot (`local:851-860`); the `credentials/*-updated` events contain listener failures, rethrowing only `INVARIANT`-coded sync ones (`cred:286-306`); and a named-but-missing ref fails loud with `MISSING_CREDENTIAL` rather than letting the provider pick up an unrelated ambient key — "billing another tenant for a request the deployment meant to authenticate differently" (`packages/llm/llm-pi-ai/src/index.ts:188-206`).

**Authorization and account layers.** `AuthorizationService` allows one flow per `CredentialKey` and one attempt per key at a time (`authz:209-225`, `:294-297`). Success requires observing a `credentials/record-updated` for that key *during* the attempt **and** a post-commit `describeRecord` still configured — "on a re-auth the record already exists, so presence alone would let a flow that wrote nothing report the stale credential as freshly authorized" (`authz:385-392`, `:442-453`). `DeepSeekAccount` marks `PlatformSession` "Host-only … never expose through account RPC" (`packages/credentials/deepseek-account/src/index.ts:12-20`), and `resolveToken` is origin-bound, refusing development grants on production origins and deleting at boot a grant whose issuer mismatches the configured origin (`packages/credentials/deepseek-account-platform/src/index.ts:156-165, 225-240`).

---

## 6. Credential redaction vs data/PII redaction vs secret detection

Only the first is built in.

- **Credential redaction — yes, structural.** `role('secret')` walking in `packages/settings/settings/src/redact.ts`, applied to value, base, user, and schema defaults (`packages/settings/settings/src/index.ts:323-330`); credential `describe*` views that cannot carry a value; error-text discipline in `local`. Scope: config surfaces, diagnostics, and the Remote wire.
- **Data / PII redaction — no built-in capability.** The seam is deployment-owned: it "ships NO rules of its own … records reach the backend as captured, so exported data is exactly as clean as the rules a deployment mounts" (`telemetry/src/index.ts:27-37`) — "including any credentials embedded in file contents or command output" (`telemetry/README.md:116`). Scope is outbound telemetry copies only; **the canonical session log is never rewritten** (`telemetry/README.md:52`).
- **Secret detection — none.** No scanner exists in `packages/fs`, `packages/spill`, or `packages/deliverables`.

Two things easily misread as redaction: **`spill-policy`** replaces oversized results with a preview + locator — *size*-based, not content-based (`packages/spill/spill-policy/src/notice.ts:15-19`); and **`session-query`'s "workspace authority and lineage redaction"** is authorization-based masking of out-of-workspace ancestors (`[outside workspace boundary]`), not content classification, while `ui-chat`/`ui-trajectory` "redacts auth failures" is error sanitization.

UNVERIFIED: whether any deployment ships a telemetry rule with secret detection. The fixture `packages/session/session-telemetry/tests/fixtures/telemetry-redact-rule.ts` is a scrub rule, not a detector. The experimental inspector explicitly "does not redact credentials, cookies, query values, or payloads" in fetch capture (`packages/experimental/inspector/README.md:114`) — a live leak path if composed alongside telemetry export.

---

## Guarantees vs Conventions

| Property | Status | Evidence |
|---|---|---|
| A guard can only abstain or deny; a later listener cannot undo a denial | **Guaranteed** (type-level) | `tools:722-730`; `tools-spec:302-308` |
| Guards run after the whole `tools/pre-execute` waterfall and after the approval `ask` | **Guaranteed** | `tools:1504-1518` |
| `never` rejects deterministically regardless of registration order | **Guaranteed** (service-owned, not a listener) | `approval:270-275`; `approval-spec:422-442` |
| Missing/throwing/rogue answerer fails closed | **Guaranteed** | `approval:280-292` |
| Exactly one `approval/decided` per `approval/asked`, both turn-enclosed | **Guaranteed** (+ invariant companion) | `approval-inv:32-50, 79-102` |
| `allowed-once` grants only the asked-about action | **Guaranteed** | `approval-types:17, 32`; `esc:180` |
| Guard registration is disposable and layer-scoped | **Guaranteed** (effect-owned undo) | `store:122-133, 226-266` |
| Guard denials are auditable *as denials* | **Convention only** — prose in `tool/result`; no `error` field without `info` | `loop:285` |
| Guard registration/removal is observable | **Not at all** — `notify: false`, no event | `tools:1139` |
| Approval grants persist or are reusable | **Not at all** — by design | `approval:224` |
| Credential values never reach the model or a subprocess env | **Convention + architecture** — no injection path exists in shell/terminal; model exposure is delegated to adapters | `packages/credentials/credentials/README.md:160-168` |
| Outbound data is free of secrets | **Convention only** — deployment-owned rules, none shipped | `telemetry/README.md:48-52, 116` |

## Implications for a regulated SAS->PySpark migration verification platform

1. **Build the permission model on `tools/pre-execute` + the approval seam, not on guards.** Guards are a monotonic backstop with no audit identity: a defensible denial needs either a `deny` carrying `info` (so `tool/result` records `{ name, code }`) or an explicit approval ask, or the only evidence is the string `Error: <reason>`.
2. **Every regulated action will re-prompt.** If the control framework expects "approved once for this project," implement an `approval/request` answerer owning that state, and accept that the audit pair still logs one ask per action — arguably desirable, since the trail is complete by construction. The audit pair is also turn-enclosed: never call `approval.request()` from outside the agent loop (it throws, correctly — a detached ask would be crash-tail garbage on reload).
3. **`never` is the only headless-safe policy and it is unbypassable.** For CI verification runs, seed `approval/policy: never` (or the `danger-full-access` preset, which bundles `never`); the rejection reason reaches the model deterministically and no answerer can override it. Assert on the knob folds, not the preset name — `custom` is a derived value that can never be written.
4. **Credential governance is strong; data governance is your job.** Reference-based configuration, `0600` documents, value-free `describe` views, origin-bound tokens, and per-operation re-resolution give a defensible secret-handling story. Nothing scans migrated SAS source or PySpark output for secrets before it is logged, exported to telemetry, or shown to the model — mount a `sessionTelemetry/record` rule and consider a pre-emit scanner; the seam is designed for exactly that insertion point.
5. **Guard disposal is a hazard for long-running verification agents.** A guard registered on a plugin fiber disappears when that plugin unloads, with no durable trace. Register policy that must outlive plugins as a `tools/pre-execute` listener on a composition-level fiber, and verify it at runtime rather than assuming it is mounted.
