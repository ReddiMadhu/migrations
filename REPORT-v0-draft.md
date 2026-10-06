# DeepSeek Harness Reverse Engineering & SAS→PySpark Architecture

> **Historical draft only.** This file targets an older DSH commit (`0d1f500…`). For the requested current-source audit, use [`REPORT.md`](REPORT.md)'s target appendix and [`docs/dsh-target-audit.md`](docs/dsh-target-audit.md).

**A source-level investigation of DeepSeek Harness, translated into an architecture for a production-grade SAS→PySpark migration verification platform.**

---

## Evidence Basis and Method

| Item | Value |
|---|---|
| Repository | `github.com/deepseek-ai/deepseek-harness` (local checkout `C:\Users\madhu\deepseek-harness`) |
| Branch | `master` |
| Commit analyzed | `0d1f50007f9bca3f52b06e1c3074fa14d5fb0720` |
| Commit date | 2026-09-15 ("Merge pull request #4192 from deepseek-harness/worktree-bootfast2") |
| Scale | pnpm monorepo; ~200 workspace packages under `packages/<group>/<pkg>`; ~50 client UI packages; vendor, native, python, apps, benchmarks, website trees |
| Method | Direct source inspection of `packages/**/src/**/*.ts`, generated docs (`docs/`), agent decision notes (`.agents/notes/implemented/`), and postmortems (`docs/postmortem/`). Claims cite file path + symbol. Where documentation and code disagree, both are stated. |

**Verification convention used throughout:**

```
Repository:  packages/core/tools/src/index.ts
Symbol:      ToolRuntime.execute()
Observed:    ...
Implication: ...
```

Unverified speculation is explicitly labeled. I do not assert behavior I did not read.

---

## 1. Executive Summary

DeepSeek Harness (DSH) is an **all-plugin Cordis agent harness**: a microkernel in which the model adapter, the tool registry, the session log, the agent loop itself, the sandbox, the approval policy, and the persistence backend are all swappable plugins composed at boot from ordered YAML layers. There is no privileged core to patch.

The single most important finding for our purposes is **not** any individual subsystem. It is the combination of four properties that together make the LLM *replaceable rather than authoritative*:

1. **The durable event log is the only source of truth.** `packages/core/session/src/index.ts` — `Session` is an append-only `SessionEvent` log; the LLM message history is *derived* by `deriveMessages()`. The design note `.agents/notes/implemented/architecture/2026-06-11-event-sourced-sessions.md` states the rejected alternative plainly: *"A mutable message array with events fired as notifications — simpler, but state and log can diverge; with event-sourcing the log IS the state, so divergence is structurally impossible."* Nothing the model claims is real until it is an appended event.

2. **"Model-visible ⟺ logged" is an enforced invariant**, not a convention. `AGENTS.md`: *"anything that reaches a model request must be reconstructable from the session log; a new model-visible input requires a session event."* The system asserts this at runtime.

3. **Tool results are canonicalized, frozen, and schema-validated before anyone — including the tool author — can mutate them.** `ToolRuntime` deep-freezes arguments *before* policy runs, validates every returned value against a declared JSON Schema, re-validates any value fabricated by middleware, and emits exactly one authoritative frozen result through `tools/result`.

4. **Failure is a typed, closed vocabulary, not an exception blob.** `TurnEndReason`, `ApprovalOutcome`, `PreToolDecision`/`PostToolDecision`, `ManualCompactionErrorCode`, and `SessionForkErrorCode` are closed unions that a replayer, a billing system, or an auditor can switch on exhaustively.

These four properties are exactly what a **migration verification system** needs, because they invert the usual dependency: instead of trusting the LLM and checking its output, the harness *records everything the LLM was allowed to see and do*, and every downstream artifact (prompt, context, tool call, result, approval, cost) becomes replayable evidence.

The second most important finding is a **warning**. DSH's tool sandbox is a *capability-layer fence over model-controlled paths in trusted code*, not a kernel-grade security boundary. Its own JSDoc says so (`packages/fs/fs-sandbox/src/index.ts:10-18`). A tool that performs I/O through anything other than the injected capability seams (`ctx.fs`, `ctx.shell`, `ctx.subprocess`) is unconstrained. For a regulated migration platform that executes SAS and Spark workloads, this is the single most important thing **not** to copy naively: the seam discipline must be enforced by isolation, not by convention.

Third: DSH is a **developer-facing, single-tenant, single-session-per-process** product. Its concurrency model (per-agent scope keys, WeakMaps keyed on live execution objects, single-writer file leases, in-process guard registries) is *excellent* for one operator at one machine and *untested* for a multi-tenant batch farm. We borrow its event-sourcing, capability-seam, and guard designs; we **redesign** its concurrency, persistence, and UI layers for batch.

**The core architectural recommendation of this report:** build the migration platform as an **event-sourced capability-harness in which the LLM is one of several replaceable "translation engines"** — each producing patches that are validated by *deterministic* reconciliation against dual execution, and every step (source parse, IR, translation, execution, reconciliation, repair, approval) is an appended event in an immutable log. Parity is decided by the reconciliation engine, never by the model's self-report.

---

## 2. Repository Snapshot

### 2.1 Top-level layout

```
deepseek-harness/
├── vendor/        Vendored Cordis (cordis, cosmokit, schemastery, cordis-plugin-*)
├── packages/      @deepseek-ai/dsh-<name> workspaces at packages/<group>/<pkg>/
├── native/        @deepseek-ai/node-addon-system: Landlock launcher, native build
├── python/        Python SDK/runtime (ships dsh CLI as a wheel)
├── apps/          Product assemblies: web, desktop(Electron), cli(=dsh), headless, sdk, acp
├── benchmarks/    Performance gates
├── .agents/       Agent workflows + implemented/architecture decision notes
├── docs/          ~250 markdown docs incl. subsystems/, cookbook/, postmortem/, persistence-changes/
├── scripts/       Gates and generators (verify-*, gen-doc-graphs, gen-cordis-catalog)
├── snapshots/     Keyless recorded-session replay fixtures
├── website/       VitePress docs projection
└── (config)       pnpm-workspace.yaml, tsconfig.*, vitest.*, tsdown.config.ts
```

`pnpm-workspace.yaml` globs `packages/*/*`, `vendor/*`, `apps/*`, `native/system*`, `benchmarks`, `website`, `python/sdk-runtime`. Notably it pins `@deepseek-ai/cosmokit` and `@deepseek-ai/schemastery` to vendored `link:` copies — confirming **Cordis lineage** (Cordis is the Koishi-community framework; cosmokit and schemastery are its utility and schema libraries).

### 2.2 Package groups (package map)

| Group | Purpose | Representative packages |
|---|---|---|
| `core/` | Kernel: agent/session/tools/prompt/scope | `session`, `agent`, `agent-loop`, `tools`, `system-prompt`, `scope`, `agent-default-model`, `agent-tool-presentation` |
| `llm/` | Model providers + token metering | `llm`, `llm-deepseek`, `llm-pi-ai`, `llm-retry`, `token-meter`, `deepseek-llm-api-extensions` |
| `shell/`, `subprocess/`, `terminal/` | Execution worlds | `bash-local`, `bash-sandbox`, `pwsh-*`, `subprocess-local`, `win32-process`, `terminal-bash` |
| `ssh/`, `e2b/` | Remote execution worlds | `ssh`, `fs-ssh`, `subprocess-ssh`, `sandbox-ssh`; `e2b`, `fs-e2b`, `subprocess-e2b` |
| `sandbox/` | Process confinement | `sandbox`, `sandbox-local`, `sandbox-policy`, `sandbox-windows-acl` |
| `fs/` | Filesystem capability | `fs`, `fs-local`, `fs-sandbox`, `fs-observation-policy`, `tool-fs`, `tool-fs-search`, `tool-str-replace-editor` |
| `session/` | Durable sessions | `session-persistence`, `-jsonl`, `session-projection`, `-cache`, `session-format-v0-to-v1…v2-to-v3`, `session-checkpoint-policy`, `session-stats`, `session-telemetry[-otel]`, `session-title-*`, `session-turn-outline`, `session-query[-sqlite]`, `session-log-deepseek` |
| `compaction/` | Context compaction | `compaction`, `compaction-basic`, `command-compact`, `compaction-image-offload`, `compaction-tool-result-pruner` |
| `context/` | Model context sources | `agent-instructions`, `file-reference[-local]`, `session-reference`, `time-context`, `tmux-context` |
| `skill/` | Skill system | `skill`, `skill-filesystem`, `skill-badge`, `tool-skill` |
| `subagent/` | Delegation | `subagent`, `subagent-fork-in-process`, `-spawn-in-process`, `-in-process-driver`, `-acp`, `-claude-code`, `-codex`, `-dsh-sdk`, `tool-subagent[-control]` |
| `ptc-runtime/`, `typert/` | Programmatic tool use + type graphs | `ptc-runtime`, `ptc-runtime-node`, `typert/{registry,loader,protocol,generator}` |
| `interaction/` | Human-in-the-loop | `user-approval`, `user-questions`, `tool-ask-user`, `permission-presets`, `commands` |
| `credentials/` | Secrets | `credentials`, `credentials-local`, `authorization`, `deepseek-account[-platform]` |
| `guard/` | Loop/tool guards | `repeat-tool-reminder`, `timeout-policy` (**no `guard/guard` package exists**) |
| `storage/` | Non-session storage | `storage`, `storage-json`, `storage-sqlite`, `storage-domain` |
| `jobs/`, `workflow/`, `goal/`, `plan/`, `todo/`, `schedule/` | Autonomy | `jobs[-local]`, `workflow`, `workflow-ptc`, `tool-workflow`, `tool-ralph`, `goal`, `goal-round-driver`, `plan-mode`, `tool-todo`, `schedule` |
| `extensions/` | Runtime self-modification | `cordis-host-runner`, `tool-cordis`, `ui-cordis`, `cordis-client-runner` |
| `hooks/` | External-agent bridges | `hooks-claude-code`, `hooks-codex` |
| `bundle/`, `boot/`, `preset/` | Composition | `base`, `web-app`, `headless`, `sdk-app`, `sdk-minimal`, `acp-app`; `app-boot`, `cmdline`, `hmr`, `plugin-manager`; `agent-presets`, `persona` |
| `api/`, `host/`, `client/`, `sdk/` | Surfaces | `api/{gateway,session-controller,…}`; `host/{webserver,frontend-static,…}`; ~50 `client/ui-*`; `sdk/{protocol,client,server}` |
| `experimental/` | Pre-stable prototypes | `agent-team[-*]`, `auto-review`, `code-runtime-python`, `ptc-runtime-python`, `inspector`, `webworker-*` |
| `spill/`, `attachment/`, `feedback/`, `workspace/`, `web/`, `mcp/`, `lsp/`, `webhook/`, `acp/`, `identity/`, `settings/`, `runtime-diagnostics/`, `util/`, `test-support/` | Supporting | (see capability graph) |

### 2.3 Dependency / capability graph

DSH classifies every `ctx.<key>` into four roles (`docs/capability-seams.md`, generated by `scripts/gen-doc-graphs.ts`):

- **`core`** — spine services owned by exactly one package, not swappable by configuration: `ctx.sessions`, `ctx.systemPrompt`, `ctx.tools`, `ctx.agents`, `ctx.agentLoop`, `ctx.scope` (library), `ctx.invariants`, `ctx.storageDomain`, `ctx.workspaceChanges`, `ctx.sessionProjections`.
- **`seam`** — swappable capability with a Service Definition + one-or-more Providers + Consumers: `ctx.llm`, `ctx.fs`, `ctx.shell`, `ctx.subprocess`, `ctx.sandbox`, `ctx.sandboxPolicy`, `ctx.approval`, `ctx.credentials`, `ctx.sessionPersistence`, `ctx.sessionQuery`, `ctx.compaction`, `ctx.spillStore`, `ctx.skills`, `ctx.subagents`, `ctx.ptcRuntime`, `ctx.workflowEngine`, `ctx.terminals`, `ctx.web`, `ctx.lsp`, `ctx.mcpResources`, `ctx.browserUse`, `ctx.computerUse`, `ctx.storage`, `ctx.sessionTelemetry`, `ctx.authorization`, `ctx.directoryPicker`, `ctx.userQuestions`.
- **`bundle`** — composition points: `ctx.agentLoop` (the one concrete loop plugin).
- **`service`** — standalone: `ctx.productTelemetry`.

The capability graph is generated and gated — `docs/capability-seams.md` is machine-produced and its completeness is checked in CI. This is itself a borrowable practice: **the architecture diagram is a build artifact, not a mural.**

A representative capability chain (from the graph):

```
Model-facing shell tool (tool-bash)
   ↓ depends on
ctx.shell  (Bash executor seam)
   ↓ providers
bash-local | bash-sandbox | pwsh-local
   ↓ bash-sandbox depends on
ctx.subprocess (subprocess seam) + ctx.sandboxPolicy
   ↓ providers
subprocess-local | subprocess-ssh
   ↓ consumers hand argv to
ctx.sandbox (sandbox seam)
   ↓ providers
sandbox-local (Landlock/bwrap/Seatbelt) | sandbox-ssh
   ↓ runtime effect
confined child process
```

The key structural property: **swapping one provider moves a whole execution world.** `docs/architecture.md`: *"Filesystem and subprocess providers share one execution world, so pointing them at a remote sandbox moves Bash, PTY, and LSP with them, with no provider forks."* This is precisely the property our migration platform needs: a "Spark execution world" and a "SAS execution world" should be capability seams, not hard-coded integrations.

### 2.4 Layer classification for our purposes

| Class | DSH examples | Migration analogue |
|---|---|---|
| Kernel-like | `session`, `scope`, `tools`, `agent`, `system-prompt`, cordis context/registry | Event log, scope, capability registry, prompt assembler |
| Framework-like | cordis `Context`/`Service`/`Events`, `ScopedLayers`, waterfall dispatch | Our DI + event bus + scope primitive |
| Capability providers | llm-*, fs-*, shell-*, subprocess-*, sandbox-*, session-persistence-jsonl, credentials-local | SAS executor, Spark executor, LLM adapter, artifact store, reconciliation engine |
| Policy layers | guard/*, permission-presets, user-approval, sandbox-policy, fs-observation-policy, session-checkpoint-policy | Parity policy, approval policy, guard registry, budget policy |
| Application behavior | tool-* (model-facing tools), workflow, goal, plan, todo, schedule, agent presets | Translation/diagnosis/repair tools, migration orchestration |

---

## 3. Actual Runtime Architecture

### 3.1 Boot and composition

A running `dsh` is a **plugin tree composed at boot from ordered layers** (`docs/architecture.md`):

- A **profile** (named composition stored in Harness home) lists **bundles** it stacks, holds out-of-tree plugins, and keeps the user's `cordis.patch.yml`. Shipped templates: `web`, `headless`, `sdk`, `sdk-minimal`, `acp`.
- A **bundle** is a distribution format for Cordis config rows and the code they mount, declared via the `dsh.bundle` field in its `package.json`.
- Layers apply to an empty entry list in order: each bundle in profile order → profile `cordis.patch.yml` → home-level patch → any `--patch` overlay. A patch targets a row by id and **replaces its whole config**, or inserts new rows.

**YAML controls HMR.** `dsh-base` enables config-only `dsh-hmr`; headless/SDK/ACP disable it. This means the *same* codebase supports a hot-reloading developer GUI and a frozen CI runner purely by composition — a property we want for migration-platform dev vs. production control planes.

**Application launch is restricted.** Only `dsh` profiles may launch supported Node applications; `scripts/verify-application-entrypoints.ts` rejects any Node application path that bypasses `dsh`. This is a deliberate anti-sprawl control worth copying: **one launcher, composition-only extension.**

### 3.2 The turn — actual call chain

The documented turn flow (`docs/architecture.md`) and the code agree. A **step** is one model request plus the tools it calls; a **turn** is zero or more steps, opened before its first input is claimed and closed once nothing is owed.

```
turn/start
  claim next-step input + one queued message
  assemble prompt sections + tool schemas; project runtime context
  → agent/pre-step          (waterfall)   reject | enter(messages, startsRequestSeries?)
     reject or empty first enter → close turn with no step
     step/start
     agent/request          (waterfall)   → prepareCall()
        cancellation during this async phase commits neither system nor user messages
     reconcile system/message via prepared-call capability
     append entered messages as user/message; log request/header (+ request/context)
     derive + freeze model history from the log
     stream bound prepared call → llm/stream → agent/assistant-stream start
        agent/assistant-stream chunk* (transient, process-local)
        assistant/message | assistant/attempt → agent/assistant-stream end
     tool/call*  (logged BEFORE execution)
        tools/pre-execute → (approval ask) → monotonic guards → tools/execute
          → tool body → normalize → projectContent → tools/post-execute
          → finalizeContent → tools/result (frozen)
     tool/result*  (single model-facing outcome)
     step/end
     tools owe another request, or next input arrived → next step
  → agent/turn-stopping      (serial, no next())
turn/end
```

**Ownership and mutability at each step** (verified):

| Step | Owner | Mutable state | Immutable/frozen | Events emitted | Persisted |
|---|---|---|---|---|---|
| claim input | `AgentLoop` inbox (`agent-loop/src/inbox.ts`) | inbox queue | — | inbox transitions (emit) | durable inbox projection |
| `agent/pre-step` | loop + listeners | claimed messages may be rewritten/rejected | — | `agent/pre-step` (waterfall) | — |
| `agent/request` → `prepareCall` | loop + LLM seam | route resolution | — | `agent/request` (waterfall) | `request/header`, `request/context` |
| prompt assembly | `ctx.systemPrompt` | — | rendered assembly | `system-prompt/assemble` (waterfall) | `system/message` |
| history derivation | `Session.deriveMessages()` | derivation cache | log events, derived messages (shared, deep-frozen) | — | derived from log |
| streaming | `ctx.llm` adapter | stream state | — | `llm/stream` (waterfall), `agent/assistant-stream` (emit) | `assistant/message` (+ embedded stream) or `assistant/attempt` |
| tool dispatch | `ToolRuntime` | `exec.signal` only | args (frozen pre-policy), identity fields, final result | `tools/pre-execute`, `tools/execute`, `tools/post-execute`, `tools/result` | `tool/call` (pre-policy), `tool/result` |
| turn close | loop | — | — | `agent/turn-stopping` (serial), `turn/end` | `turn/end` (durable) |

### 3.3 Where middleware intercepts, where policy denies, where extensions transform

- **Interception points (waterfalls):** `agent/pre-step`, `agent/request`, `agent/request-error`, `llm/stream`, `system-prompt/assemble`, `tools/pre-execute`, `tools/execute`, `tools/post-execute`, `tools/ptc-dispatch-log`, `approval/request`, `compaction/summary-error`.
- **Denial points (ordered):** (1) PTC-mode collapse → `UNKNOWN_TOOL` **before** the pipeline; (2) `tools/pre-execute` deny/cancel/ask; (3) approval refusal (ask); (4) monotonic guards; (5) `tools/post-execute` block; (6) output-schema validation failure; (7) lossless-JSON materialization failure.
- **Transformation points:** `tools/execute` (may replace signal; may fabricate a result), `projectContent` (definition-owned, pre-post-execute), `tools/post-execute` accept-with-content/value/block, `finalizeContent` (definition-owned, last-mile, must be total), `tools/ptc-dispatch-log` (durable-log copy only).

### 3.4 Failure, cancellation, timeout, crash

- **Tool failure** → `toolErrorResult`, still routed through `tools/post-execute`, emitted as `tool/result` with `error: { name, code, reason? }`.
- **Cancellation** is cooperative and *never abandons a started body*. The registry distinguishes `ABORTED` (body invoked) from `ABORTED_BEFORE_DISPATCH` (`core/tools/src/index.ts:483-486`). `fuseToolSignals` re-fuses the caller signal after any wrapper swap so an around-middleware cannot detach caller cancellation. The `ToolDefinition.execute` JSDoc admits: *"it cannot hard-kill same-process code."*
- **Timeout** is a `tools/execute` wrapper (`guard/timeout-policy`), cooperative via `exec.signal`, and deliberately *does not race or abandon* the tool promise.
- **Crash** — a process death mid-turn leaves a durable log ending inside an open turn. **Persistence does not truncate or repair it** (`docs/subsystems/persistence.md`); the *reader* repairs on resume by computing `interruptedTurnClosers` (`core/session/src/repair.ts`) and appending synthetic closers: error results for unmatched tool calls (`TOOL_OUTCOME_UNKNOWN` / `TOOL_NOT_STARTED`), any open `step/end`, and `turn/end { reason: 'interrupted' }`. The wording given to the model is deliberately conservative: *"Its outcome is unknown… retry only if the operation is read-only or idempotent; if it may have side effects, first verify external state… Do not retry blindly."*

**Implication for us:** this crash model is *exactly* right for dual execution. A SAS or Spark run that was started but whose result was never durably recorded is **outcome-unknown**, not "failed" and not "succeeded" — and the correct response is to verify external state, never to blindly re-run a side-effecting job.

---

## 4. Plugin / Capability Architecture

### 4.1 Is "everything is a plugin" actually true?

**Substantially yes, with a small, well-defined privileged set.** `docs/architecture.md`: *"There is no privileged core to patch: you extend dsh by mounting a plugin beside the others, and registrations are effects that unwind when their plugin unloads."*

Verified replaceable via seams (Service Definition + Provider + Consumer roles, each a separate package):

| Capability | Seam | Providers observed |
|---|---|---|
| Model adapter | `ctx.llm` | `llm-deepseek`, `llm-pi-ai`, `llm-replay` (deterministic replay provider!) |
| Tool registry | `ctx.tools` | one package, but tools/restrictions/guards/mode are per-scope registrations |
| Filesystem | `ctx.fs` | `fs-local`, `fs-sandbox`, `fs-ssh`, `fs-e2b` |
| Shell | `ctx.shell` | `bash-local`, `bash-sandbox`, `pwsh-local` |
| Subprocess | `ctx.subprocess` | `subprocess-local`, `subprocess-ssh` |
| Sandbox | `ctx.sandbox` | `sandbox-local`, `sandbox-ssh` (+e2b via fs/subprocess) |
| Event store | `ctx.sessionPersistence` | `session-persistence-jsonl` |
| Session query | `ctx.sessionQuery` | `session-query-sqlite` |
| Compaction | `ctx.compaction` | `compaction-basic` |
| Subagents | `ctx.subagents` | 6 providers (fork-in-process, spawn-in-process, acp, codex, claude-code, dsh-sdk) |
| Skills | `ctx.skills` | `skill-filesystem`, `skill-badge`, `skill-office` |
| PTC runtime | `ctx.ptcRuntime` | `ptc-runtime-node`, `experimental-ptc-runtime-python` |
| Credentials | `ctx.credentials` | `credentials-local` |
| Approval answerers | `approval/request` listeners | UI answerers, ACP bridge |
| Web/search | `ctx.web` | 4 providers |
| LSP | `ctx.lsp` | `lsp-stdio` |
| Spill | `ctx.spillStore` | `spill-local` |
| Storage | `ctx.storage` | `storage-json`, `storage-sqlite` |
| Telemetry | `ctx.sessionTelemetry` | `session-telemetry-otel` |
| Workflow | `ctx.workflowEngine` | `workflow-ptc` |
| Jobs | `ctx.jobs` | `jobs-local` |
| Session titles | `ctx.sessionTitle` | 2 LLM providers + deterministic fallback |

**The agent loop itself is a plugin** (`ctx.agentLoop`, package `agent-loop`) and *"nothing outside it may depend on it"* (decision note `2026-06-11-microkernel-event-taxonomy.md`).

### 4.2 What remains privileged

1. **The Cordis `Context`/registry** — vendored, not a plugin. Root context is process-global.
2. **`Session`/`SessionStore`** — core; persistence is pluggable but the in-memory log and its `append()` contract are not.
3. **`ctx.tools` pipeline ordering** — tools are plugins, but the *pipeline* (pre→guard→execute→post→finalize→result) is kernel.
4. **`ctx.systemPrompt`** — assembly registry is core; sections are plugins.
5. **`ctx.scope`** — the scope primitive is a library (`scope`), not replaceable.
6. **`ctx.invariants`** — assertion registry is core.
7. **`dsh-base` bundle** — the shared first layer defines the deployment default tool set, persistence, sandbox and approval policy; profiles stack above it. (Exception: `sdk-minimal` deliberately owns its complete explicit tree and does not apply `dsh-base`.)

### 4.3 Hidden coupling (verified)

- **Concrete imports across package boundaries are the norm** for types, but runtime coupling is channeled through `inject` and `ctx.get`. Two genuinely hidden couplings:
  - **Opportunistic service reads.** `dsh-tools` reads `ctx.get('approval')`, `ctx.get('ptcRuntime')`, and `ctx.get('sandboxPolicy')` **at use time rather than via static `inject`** — deliberately. The JSDoc on `requirePtcRuntime` explains why: *"an inject entry would hold `ctx.tools` — and every tool plugin behind it — hostage to a PTC runtime existing even under `mode: 'native'`."* This is a real coupling trade-off: **eager `inject` creates load-order dependency; opportunistic `ctx.get` creates optional capability.** Both are used deliberately and the choice is documented.
  - **Type-only augmentation imports.** `core/tools/src/index.ts:19-21` does `import type {} from '@deepseek-ai/dsh-user-approval'` *purely* so `ctx.get('approval')` resolves the augmented type. The seam stays optional at runtime. This is a clean pattern for optional capabilities.
- **Registration-order dependence.** Waterfall listeners run in registration order; `prepend: true` exists to jump the queue. DSH's answer is to make *policy that must not be reordered* a **monotonic guard** instead of a listener (see §8).
- **Scoped-restriction exemption.** `tools.restrict()` filters only *inherited* tools, never the scope's own registrations. The code comment (`core/tools/src/index.ts:1169-1173`) documents a **real past bug**: *"Reading the exempt set as 'the global layer' instead of 'not mine' held only while every model-facing tool sat in the host composition. Once presets moved them onto the agent plane they became an ANCESTOR contribution, so a child's filter silently stopped constraining anything it was given."* — a security-relevant regression caught by reasoning, not by types.
- **Config interpolation boundary.** Postmortem `0002`: `!!js` expressions are interpolated only under a plugin's `config` and its `disabled` field, **not** other entry metadata. An ACP composition put `disabled: !!js …` on filesystem entries; the raw expression object was truthy, so **the filesystem stack was permanently disabled in every mode**, and the snapshot suite *"proved deterministic replay of the regression rather than successful behavior."* Guardrails now include `verify-cordis-config` (rejects expression nodes in entry metadata) and `dsh-session-snapshot` rejecting structured `UNKNOWN_TOOL` results in fixtures.

### 4.4 Multi-tenant production fitness (analysis)

DSH's plugin/service registry is **single-context, single-process**. Verified concurrency-sensitive structures:

- `ToolRuntime` holds per-execution `WeakMap<ToolRunContext, …>` (deferred contexts, cancellation states, finalizers, projectors, canonical results) and `ScopedLayers` per scope. These are safe for *interleaved* calls in one process, not for cross-process tenants.
- `SessionStore` is a `Map<SessionId, SessionEntry>` with a module-private `attachments` WeakMap; single-writer ownership is enforced per-id, and `session/end-seed` explicitly *"is NOT a liveness signal about other writers — a concurrently live session holds its own boundary elsewhere, so tolerating concurrent writers needs a signal beyond the log."* (`core/session/src/types.ts:419-426`).
- Session persistence uses a **cross-process write lease** (`session-persistence-jsonl/src/lease.ts`) and single-writer `open(id,'write')` rejection — so *persistence* is multi-process-safe; the *live registry* is not.

**Conclusion:** DSH's model is "one live session per id per process; many readers." For a multi-tenant batch platform we must add: tenant-scoped contexts, per-tenant capability registries, and a distributed lease. **Borrow the seam design; do not borrow the in-process registry.**

---

## 5. Context / Scope / Dependency Injection

### 5.1 The Cordis model

Cordis (vendored, `vendor/cordis`; upstream SHAs in `vendor/README.md`) provides:

- **`Context`** — a repository of services. A service claims a stable `ctx.<key>`; plugins find services by key, not by import.
- **`Service`** — plugin base class; lifecycle mounted into the current context.
- **`inject`** — declared service dependencies; load order is expressed through service requirements, not manual boot sequencing. A plugin naming required services *waits until those services exist*.
- **Typed events** via TypeScript declaration merging on `interface Events`.
- **Effects** — every registration goes through `ctx.effect()` / `ctx.on()`; the registry's `register()` returns the disposer. Registrations unwind when the plugin unloads.
- **Fibers** — plugin-scoped lifecycles with async teardown (`fiber.dispose()`, `fiber.inertia`).

`docs/cordis-primer.md` distills it to five ideas: plugin = Service implementation; context = repository of services; `inject` declares dependency; typed events for communication; registrations are reversible effects.

### 5.2 Scope — the sharpest idea in the codebase

`packages/core/scope/src/index.ts` implements a **dual-direction inheritance** over one parent relation (`scopeParents` WeakMap):

- **Registration views inherit DOWN the chain.** A child scope sees its ancestors' layers (`ScopedLayers` in `core/scope/src/store.ts`): tools, restrictions, guards, prompt sections.
- **Event admission extends UP the chain.** `scopeTarget(base, key)` builds a routing-only carrier whose `filter` admits untagged listeners globally **and** tagged listeners whose key matches the dispatch key **or any ancestor** (`index.ts:170-185`). *"Events flow up the chain, never down."*

This one relation powers both directions:

```
createScope(ctx, key)          → Context tagged with opaque ScopeKey (fresh object)
bindScopeParent(key, parent)   → one-time; returns the sole ScopeParentBinding that may rebind
                                 cycle-checked (every chain consumer walks parents to root)
scopeOf(ctx)                   → nearest scope tag
scopeTarget(base, key)         → event carrier with ancestor admission
```

The scope key is an **opaque, identity-compared object** — a capability, not a name. The `Scope` returned by `createScope` exposes `ctx` (a context through which scope-owned registrations are made), `rawDispose`, and `dispose()` which **follows the fiber through async teardown** (`quiesceFiber`: `await fiber.dispose(); while (fiber.inertia) await fiber.inertia`).

**Why this matters for us:** this is the cleanest possible primitive for **repair-candidate isolation**. A repair candidate is a *scope*: it inherits the shared evidence prefix and capability set from its parent, can be additionally restricted (never widened) per candidate, and its registrations and observers unwind deterministically on disposal. An ancestor-scope observer (the orchestrator) sees every descendant's events automatically.

### 5.3 What to reproduce vs. what is over-engineered

**Reproduce:**
- Service registry keyed by `ctx.<name>` with declaration-merged typing (gives us compile-time capability graphs).
- `inject` for load-order-free bootstrapping.
- Scope with the dual-direction parent relation (registration down, events up).
- Registrations-as-effects with exact disposers and ordered composite teardown.
- Opportunistic `ctx.get` for optional capabilities (documented as such).

**Redesign / drop:**
- **HMR / dynamic plugin load-unload** (`extensions/cordis-host-runner`, `ctx.hmr`, `ctx.dynamicCordisRunner` with a `vm` sandbox for host halves) is essential to a *developer tool* and a liability in a *regulated platform*. Live plugin mutation changes the effective policy set mid-run; guards registered via effects can be disposed, **weakening policy for subsequent calls**. For migration, we want *immutable, pinned policy per migration run*, with policy version recorded in the certificate.
- **Fiber inertia / async teardown chains** are subtle; a batch platform prefers explicit, staged shutdown with a hard timeout.
- The `vm`-sandboxed "dynamic Cordis" runner is an attack surface we do not need.

---

## 6. Event System

### 6.1 Dispatch modes (verified from `docs/cordis-primer.md` + `vendor/cordis/src/events.ts`)

| Mode | Awaited? | Dispatch order | Return value? | DSH use |
|---|---|---|---|---|
| `emit` | No | listeners observe in registration order | No | `session/event`, `session/created`, `session/disposed`, `tools/result`, `tools/change`, `agent/assistant-stream`, inbox transitions |
| `waterfall` | No | listeners in registration order; **`next()` delegates** | Yes | `agent/pre-step`, `agent/request`, `agent/request-error`, `llm/stream`, `system-prompt/assemble`, `tools/pre-execute`, `tools/execute`, `tools/post-execute`, `tools/ptc-dispatch-log`, `approval/request`, `compaction/summary-error` |
| `parallel` | Yes | all listeners in parallel | No | `session/flush` (the durability checkpoint) |
| `serial` | Yes | listeners in registration order | Yes | `agent/turn-stopping` (no `next()` — it observes and may stop the turn) |
| `bail` | No | listeners in order until one bails | Yes | (framework mechanism; used sparingly) |

The dispatch mode is **part of each event's public contract**, documented with a `@mode` JSDoc tag, and `scripts/gen-cordis-catalog.ts` checks declarations against dispatch sites. **Copy this: the dispatch mode is a compile-time-checked interface property, not a comment.**

### 6.2 Waterfall semantics and their traps

`vendor/cordis/src/events.ts:234` — `waterfall()` shifts listeners off a queue; `next()` either invokes the next listener or the built-in inner default. Observed rules:

- **Not calling `next()` short-circuits the whole chain, including the built-in behavior.** For single-decision events this *is* the design (a policy listener that owns the decision returns without delegating).
- **A throwing listener propagates out of the enclosing `next()`.** Each pipeline stage contains this (`prepareExecution`, `dispatchScheduledExecution`, `finalizeScheduledExecution` in `core/tools/src/index.ts` wrap in try/catch → `toolErrorResult`).
- **Callback exceptions are contained in the dispatcher** for emit/parallel events (`invokeContainedSessionObservers` in `core/session/src/index.ts`; `notifyResult` in `core/tools/src/index.ts`). `docs/defensive-patterns.md`: *"A user-supplied listener that throws must not reject the promise it runs inside or starve the listeners after it."*
- **Hidden ordering dependencies are real.** Because listeners compose by registration order, two policy listeners that both `deny`/`block` produce different observable behavior depending on order. DSH's mitigation: put non-reorderable policy in **monotonic guards** (which run after *all* pre-execute listeners), and enforce invariants that the *never* policy is decided in-service before dispatch so `prepend` cannot defeat it.

### 6.3 Event table for our migration harness (recommended)

| Event mode | Execution | Error behavior | Mutation allowed | Best use in migration harness |
|---|---|---|---|---|
| `emit` | sync, registration order, fire-and-forget | contained per listener (logged, never fails the producer) | none (frozen payloads) | artifact-written, execution-finished, reconciliation-settled, guard-denied, approval-decided, telemetry |
| `waterfall` | around-middleware, `next()` to delegate | propagates; stage-level containment | request/signal/decision only, per documented contract | translate-request, patch-validate-pre, execute-around (timeout/retry/meter), patch-validate-post, reconcile-policy |
| `serial` | awaited, in order | propagates | decision | migration-stopping, budget-checkpoint |
| `parallel` | awaited fan-out | aggregated (`AggregateError` naming each failure) | none | durability flush, multi-artifact materialization, fan-out notifications |
| `bail` | in order until first bail value | propagates | bail value | cheap short-circuit predicates (e.g. "already certified?") |

**Deterministic ordering model for our system (recommendation):** policy that must be order-independent becomes a **monotonic guard** (deny-only, no allow result, evaluated after all waterfall policy); waterfall policy is reserved for *transformation and advisory* concerns; durability barriers use `parallel` with aggregated errors; and every policy decision is recorded as an event with the policy version that produced it, so ordering is *reconstructable* even when it isn't fixed.

---

## 7. ToolRuntime — the Execution Pipeline

This is the highest-value subsystem in the codebase. All citations: `packages/core/tools/src/index.ts` unless noted.

### 7.1 Exact sequence (verified)

```
0. agent-loop appends tool/call to the session log      (agent-loop/src/tool-calls.ts:264)
   -- the call and its raw arguments are durable BEFORE policy runs
1. ToolRuntime.execute(exec: ToolExecutionInput)        (:1368)
2. createExecution                                       (:1390)
   - mint token = Symbol('dsh.tool.execution')          (:1903)  callers cannot choose it
   - resolve rootCallId (root or nested)
   - PTC-collapse check: model-direct call under mode:'ptc' naming
     anything but run_code → final-result UNKNOWN_TOOL
     BEFORE the policy pipeline (pre-execute/approval/guards
     never observe, and cannot approve, a call that can only fail)  (:1399-1473)
   - snapshotJsonValue(exec.arguments) + deepFreeze     (:1440-1444)
   - capture finalizeContent / projectContent callbacks (:1435-1436)
   - → 'ready' | 'final-result'
3. caller-cancelled check → ABORTED_BEFORE_DISPATCH     (:1499)
4. tools/pre-execute waterfall  (default inner = allow) (:1504)
5. if decision.kind === 'ask' → serviceAsk              (:1726)
   - ctx.get('approval') missing → deny
   - exec.agent missing → deny
   - approval.request({agent, toolName, callId, reason, signal})
6. caller-cancelled && approvalCancelled → ABORTED_BEFORE_DISPATCH  (:1512)
7. decision.kind === 'cancel' → ABORTED_BEFORE_DISPATCH (:1515)
8. guardReason(exec)  — ONLY when decision.kind === 'allow'  (:1518)
   global layer first, then scope chain farthest-first; first denial wins
9. caller-cancelled recheck                             (:1531)
10. tools/execute waterfall (default inner = dispatchToolBody)  (:1604)
11. dispatchToolBody                                    (:1563)
    - fuseToolSignals(callerSignal, wrapperSignal)     (:1926)
    - resolveExecution(name, scope, nested)
    - state.bodyInvoked = true
    - tool.execute(args, exec)
    - createSuccessResult → snapshot + schema-validate + render + presentationMeta
12. normalizeDispatchResult                             (:1863)
    - canonicalResults WeakMap token check: a wrapper-authored value is
      RE-validated and RE-rendered through the owning output contract
13. caller-cancelled && !isError → cancellationResult (ABORTED vs ABORTED_BEFORE_DISPATCH)
14. projectContent (definition-owned) → tools/post-execute waterfall  (:1640-1780)
15. finishScheduledExecution                            (:1668)
    - materializeFinalResult  (snapshot + deepFreeze; lossless-JSON or TypeError)
    - applyFinalContent (finalizeContent; must be total; contained on throw)
    - materializeFinalResult again
16. notifyResult: Object.freeze(exec) → emit tools/result (contained)  (:1694)
17. agent-loop appends tool/result to the session log   (agent-loop/src/tool-calls.ts:282)
```

### 7.2 Identity

- `callId` — caller-supplied, branded `ToolCallId`.
- `rootCallId` — the model-requested call owning the execution tree; resolved for root and nested.
- `token` — registry-minted `ToolExecutionToken` (a branded `symbol`); shared with nested calls *only* as their opaque `parent`. This is the correlation capability that lets a commit-style observer wait for the outer `run_code` outcome without receiving its live mutable execution.
- `parent` token marks a **transport sub-dispatch** (PTC). Under `mode: 'ptc'`, only calls *with* a parent may execute a native tool name.

### 7.3 Request mutation — the precise answer

- **Arguments**: deep-frozen *before* policy. Lossy arguments (BigInt, symbol, undefined, -0, non-finite, circular, sparse, Map/Set/Date) are rejected at the append site with a `TypeError`.
- **Identity fields**: `readonly`; `Object.freeze(exec)` happens at `notifyResult`, so during the pipeline the live object is readonly-*typed*, and only `signal` is actually mutable.
- **Signal**: mutable only through the `ToolDispatchExecution` view; the registry **re-fuses** the caller signal before the body and restores the wrapper's signal in `finally`, so a wrapper cannot detach caller cancellation — but the *body's* cancellation remains cooperative.

### 7.4 Result normalization guarantees

- Every successful value is validated against the tool's declared `output.schema` (`validateJsonSchemaValue`), rendered through `output.render`, and snapshotted to lossless JSON. A violation throws `ToolOutputError` (`INVALID_TOOL_OUTPUT`).
- `output.presentationMeta` is projected **only for top-level calls** (`exec.parent === undefined`) and persisted verbatim on `tool/result` so Host presenters and Client renderers narrow independently — and so **the durable log reproduces the identical card on replay**.
- Exactly **one** authoritative result: `finishScheduledExecution` materializes once immediately before `tools/result`; observers receive `Readonly<...>` frozen lossless JSON; observer failures are caught and logged and cannot change the outcome.
- `canonicalResults` WeakMap (`:1821`) distinguishes registry-produced results from wrapper-authored ones — a wrapper cannot smuggle through an unvalidated value, though its authored value is then validated and rendered *as if the tool had produced it*.

### 7.5 Retries, timeouts, cancellation

- **Retries** are a `tools/execute` wrapper concern (e.g. `llm-retry` at the model layer; `timeout-policy` at the tool layer). The registry itself does not retry tool bodies.
- **Timeouts** are cooperative: a tool declares `timeoutMs` (which *"is NEVER sent to the model — `schemas()` whitelists only name/description/parameters"*), and `timeout-policy` arms a `deadline`, swaps the signal, delegates, restores, and substitutes a `TOOL_TIMEOUT` error **only if its own scoped timer fired** (`timeoutOf(d.signal, TOOL_TIMEOUT)` — so a nested outer deadline is not misattributed).
- **Cancellation** semantics as in §3.4. `TOOL_ABORTED` vs `TOOL_ABORTED_BEFORE_DISPATCH` is the model-visible distinction between "started, outcome lost" and "never started".

### 7.6 Model-visible vs human-visible errors

- **Model-visible**: the `tool/result` content blocks (text). For a denial: `Error: <denialReason>`.
- **Human-visible**: the UI card derived from the same event + `meta`. There is **no dedicated human-facing guard-denial channel**; the human sees the tool-result card. Approval denials *do* have a separate audit channel (`approval/asked` / `approval/decided`, log-only, never in the model transcript).
- **Durable diagnostics**: `error.info = { name, code, reason? }` travels on `tool/result` *outside* the model-facing content; `reason` is *"raw user-facing detail; durable projections preserve it but model-facing content does not include it."* **Copy this separation verbatim** — it is exactly how a migration platform should keep an internal error code out of the model's context while keeping it in the audit trail.

### 7.7 Guarantees vs conventions (verified table)

| Guarantee | Enforced how | Bypassable? |
|---|---|---|
| Args immutable + lossless JSON before policy | `snapshotJsonValue`+`deepFreeze` in `createExecution` | No |
| Only `run_code` model-direct under PTC | `resolveExecution`/`collapses` **before** policy | No |
| Guard denial cannot be undone by a later listener | `ToolGuard` has no allow result; first reason wins | Not within a call; **guards can be disposed, weakening future calls** |
| Body value obeys declared output schema | `createSuccessResult`; wrapper values re-validated in `normalizeDispatchResult` | No (a wrapper that skips `next()` fabricates a result, but it is still schema-validated; the body never runs) |
| Caller cancellation survives wrapper signal swap | `fuseToolSignals` | Not detachable; but body cancellation is cooperative — cannot hard-kill same-process code |
| Exactly one authoritative result | single materialization + freeze before emit | No for the artifact; **`tools/post-execute` and `projectContent` are skipped on the `final-result` path** |
| Approval fails closed | no service / no agent / throwing / rogue answerer → deny/unavailable | No; `never` enforced in-service before dispatch |
| `tools/result` observers cannot mutate outcome | frozen Readonly args; observer throws contained | No |
| Schema subset enforced | `assertSupportedJsonSchema` at registration | No |
| Tool honors `exec.signal` / `isConcurrencySafe` honesty / `finalizeContent` total | **CONVENTION ONLY** | Yes by a misbehaving tool (classifier fail-closes to `exclusive`) |
| Tool uses only confined capabilities | **CONVENTION ONLY** | **YES — direct Node I/O skips the sandbox** |

**Which failures can bypass middleware?** The `final-result` path bypasses `tools/post-execute` and `projectContent`. It is reached by: PTC-mode collapse; non-lossless arguments; pre-dispatch abort of a collapsed call; **and any exception thrown by the pre-execute / approval / guard / execute-waterfall stages.** `finalizeContent` still runs on every path.

**Can one plugin alter another's behavior?** Yes, in three documented ways: (a) `tools/execute` wrapper returning without `next()` — the body never runs; (b) `tools/post-execute` `accept`+content/value or `block`; (c) `tools/pre-execute` deny/cancel/ask. All are recorded in the durable log as the resulting `tool/result`.

**Can a malicious/buggy tool circumvent the policy pipeline?** Not by calling tools — there is no API to run a tool body outside the registry. But **yes** by performing side effects through non-seam channels (direct `node:fs`, `child_process`), because the sandbox is a capability-layer fence, not a process boundary.

---

## 8. Guards / Policy / Approval

### 8.1 Monotonic guard semantics (verified)

```ts
export type ToolGuard = (execution: Readonly<ToolExecution>) => string | undefined
```
`core/tools/src/index.ts:730` — *"Because guards have no allow result, listener ordering cannot turn a denial back into permission."*

| Property | Observed behavior |
|---|---|
| Where registered | `ToolRuntime.guard(guard)` → `layers.effect(ctx, layer => layer.guards.append(guard), { notify: false })` |
| When they run | After the **entire** `tools/pre-execute` waterfall **and** the approval ask; before dispatch (`:1518`). Skipped when pre-execute already denied (deny is final) |
| Scope | The **agent** (ScopeKey), not the Session. Plain-`ctx` guard = global; registered through `agent.ctx` = that agent only |
| Inherited | Yes — global layer first, then `chainLayers(exec.agent)` farthest-first |
| Removable | Yes, via the effect disposer. Guards are **re-read live per call** (`guards.values()` at call time) |
| Can they weaken | Monotonic *within* a call; **not across time** — disposing a guard weakens subsequent calls |
| Composition | First denial in insertion order wins (`ToolLayer.guardReason`) |
| Denial to model | `tool/result` text `Error: <reason>` |
| Denial to human | the tool-result card only (no dedicated channel) |
| Denial in events | the `tool/result` event with `error.info`; still flows through `tools/post-execute` |

**Surprising finding (verified):** `packages/guard/` contains **zero** `ToolGuard` implementations. `guard/guard` does not exist as a package. Both shipped "guard" packages are waterfall listeners:
- `guard/repeat-tool-reminder/src/index.ts:220` — a **`tools/post-execute`** listener, explicitly *"Observe-and-enrich, never veto."* It counts consecutive identical calls (arguments canonicalized by deep key-sort + stringify), delegates with `next()`, and prepends a reminder onto `additionalContexts` — including on a `block`. It **counts denied calls too**, because a deny flows through post-execute; it resets the chain when a genuine user message intervenes (`agent/pre-step`).
- `guard/timeout-policy/src/index.ts:56` — a **`tools/execute`** wrapper.

The pattern this reveals: **DSH reserves the monotonic guard API for policy that must not be reordered, and uses waterfalls for advisory/around concerns.** The two shipped guards are the advisory and around archetypes.

### 8.2 Approval system (verified)

`packages/interaction/user-approval/src/index.ts`. `ApprovalService.request(req)`:

1. **`hasOpenTurn(session)` precondition** — else it **throws**. The JSDoc is the most important sentence in the file: *"the approval/asked + approval/decided audit pair must be turn-enclosed (a bare event between turns is crash-tail garbage on reload)."* **The turn is the commit/replay boundary.**
2. Append `approval/asked` with a fresh `ApprovalRequestId` (branded; *not* interchangeable with tool-call or agent ids).
3. `decide()` → append `approval/decided` → return outcome.

`decide()` semantics — every one of these is a deliberate fail-closed design:

- aborted signal → `'cancelled'` (and a **late answer after abort is discarded by construction**).
- **`'never'` policy → `'rejected'` decided inside the service BEFORE waterfall dispatch**, so *"even an answerer registered later with `prepend` cannot bypass it."*
- `approval/request` waterfall with default inner `'unavailable'`.
- **A rogue non-vocabulary return is normalized to `'unavailable'`** so it cannot leak into callers' closed-union switches.
- **A throwing answerer fails the question closed, not the caller's call open.**
- *"A failure that prevents either audit append from committing still rejects because returning an unlogged decision would violate the pair."* **Integrity over availability — copy this.**

**Outcome vocabulary is closed:** `'allowed-once' | 'rejected' | 'cancelled' | 'unavailable'`. Only `allowed-once` is a grant.

**One-shot and uncached.** Every request mints a fresh id and re-asks; there is **no approval store, no memoization, no reuse**. "Stale approval" is *not a feature* of `dsh-user-approval` (the only `stale-approval` symbol in the repo is an unrelated build-approval guard in `boot/plugin-manager`). The closest real behaviors are: the one-shot grant; late-answer discard; and the `overrideOf` policy fold where the **last** `approval/policy` event wins.

**Policy:** `'ask'` (default; fail-closed with no answerer) | `'never'` (deterministic reject; the CI/unattended stance). Per-**session**, folded from the log; single write path `setApprovalPolicy`; live switch `setPolicy` also `agent.inject()`s a model-visible notice. The model learns policy through the runtime-context snapshot (`approval:policy` prompt section) — **never the audit events, which are log-only and never enter the model transcript.**

**Approval requests deliberately omit tool arguments** — an answerer attaches the prompt to the already-streamed tool call via `callId` *"instead of rendering a second copy that could drift."*

**Tool consumption** (`serviceAsk`): no `ctx.approval` → deny; no agent → deny; `allowed-once` → allow; `rejected` → deny "the user rejected…"; `cancelled` → deny + `approvalCancelled: true` (which, combined with caller cancellation, yields `ABORTED_BEFORE_DISPATCH` instead of a denial text); `unavailable` → deny "no approval channel is available."

### 8.3 Permission presets and sandbox escalation

- `packages/interaction/permission-presets` bundles the two *independent* knobs — sandbox mode and approval policy — writing each only when it changed through its canonical setter. `permission/preset` records user intent and survives shared-bundle ties. Unmatched knobs derive to `custom` (never a switch target). A fixed `auto` preset (`danger-full-access` + `never`, current-session-only) is gated by a **synchronous** `admit` before any mutation.
- `packages/sandbox/sandbox/src/escalation.ts` `approveEscalation`: the strictly-wider ladder `WIDER_MODES` is checked **at execution** against the call's effective mode — *"deliberately not a schema constraint"*; `sandbox_permissions` + `justification` must pair; approval is resolved **before anything executes**; the grant applies to **one call only**; non-grants throw.

**Postmortem 0002's lesson for policy design** is the most transferable sentence in the repo: *"Permission controls must describe only the capabilities they actually govern. Composition-time filesystem access cannot follow a runtime bash-only preset safely."* For us: **a migration approval must be bound to the exact artifact hash and capability set it was reasoned about — anything else is a policy that describes a capability it does not govern.**

### 8.4 Guard taxonomy for our migration harness

| DSH mechanism | Our analogue | Monotonic? |
|---|---|---|
| `ToolGuard` (deny-only, post-policy) | `MigrationGuard` — deny-only, evaluated after all advisory policy | yes |
| `repeat-tool-reminder` (post-execute advisory + canonical-arg keying) | **duplicate-patch guard** + **repair-oscillation detector** (canonical patch key, consecutive count, user-intervention reset) | advisory |
| `timeout-policy` (execute-wrapper, cooperative deadline, scoped code attribution) | SAS/Spark execution budget; per-phase deadlines with scoped error codes | around |
| `approval/request` (one-shot, fail-closed, turn-enclosed audit pair) | artifact-bound approval (production write, patch promotion, credential use) | decision |
| `sandbox escalation` (strictly-wider ladder, one call) | capability escalation for repair branches | decision |
| `session-checkpoint-policy` (fail-closed at side-effect boundaries) | durability barrier before any engine side effect | checkpoint |

---
