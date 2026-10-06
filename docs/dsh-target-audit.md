# Target source audit: DeepSeek Harness `46a7f68`

**Audit basis:** merge commit `46a7f68b0922371ce7144b668b90e377d8e799f4` (`master`, release `0.1.7-rc.1`, 2026-09-23). The local DSH checkout used for inspection is the older `00102833…` tree; every target-specific statement below is rebased to `git show 46a7f68:<path>` rather than the working tree.

## Executive classification

- DeepSeek Harness is a **developer-preview release candidate**, not a stable platform contract. The root and selected public `@deepseek-ai/dsh-*` packages are `0.1.7-rc.1`; public metadata does not make the composition production-stable.
- The workbench uses upstream `cordis@4.0.0-rc.10` directly. DSH's `vendor/cordis` is upstream `4.0.0-rc.7` at `56b3d4f725681cf4556c1a8695a709cc3b6eed74`, locally published as `@deepseek-ai/cordis@4.0.4`, with documented Harness patches. Local vendor behavior must not be attributed to upstream Cordis.
- `Context.root` is explicitly experimental. DSH has kernel-like core authorities (`Context`/reflection, `Fiber`, `Session`, `ToolRuntime`, scope layers, system prompt, and agent registry) around which plugins/providers are composed. “Everything is a plugin” is too strong; `bundle/base` is a patch composition, not the absence of privileged core code.
- A process can host many Web sessions. `SessionStore`/`AgentRegistry` maps and caches are process-local and are **not tenant boundaries**.

## Target measurements

| Measurement | Target value | Correction to older report basis |
|---|---:|---|
| Tracked files | 13,302 | Rebased from `00102833` |
| Package manifests | 307 | Rebased |
| Tracked package groups | 54 | Older report's 57/54 framing is stale |
| Service-role graph | 90 = 55 core + 33 seam + 1 bundle + 1 service | Older 89/54 table is stale |
| Docs | 347 Markdown files / 137,476 lines | File count happened to remain; line count is rebased |
| `.agents/notes` | 3,539 = 1,457 implemented + 1,919 archived + 117 proposed + 42 rejected, plus translations | Older counts are stale |
| `pnpm.patchedDependencies` | 7 | Older report's 12 is stale |
| Runtime invariant companion files | 38 `src/invariant.ts` paths | Older 39 count included a moved/renamed file |

Package-group count and service-role graph count are different measurements and must not be conflated.

## Core runtime findings

### Cordis and Fiber

- `Context`, `Context.extend`, `isolate`, and `intercept` are proxy-based composition primitives.
- `Service` plus `ReflectService`/`RegistryService` own provider identity, activation, and lookup. Dependency refresh is reactive; it is not a general cycle-detecting DAG scheduler.
- `Fiber.effect`, `update`, and `restart` own reversible registrations and configuration effects.
- `DisposableList.clear()` returns disposers in reverse registration order. `Fiber._unload()` starts that reversed top-level list concurrently with `Promise.all`; nested effect disposers are sequential reverse. Errors are contained/logged rather than silently swallowed.
- The local vendor README documents extra lifecycle/config/include/Loader/HMR patches. Those are DSH fork behavior, not upstream Cordis guarantees.

### Events and tools

Cordis exposes `emit`, `parallel`, `serial`, `bail`, and `waterfall`. `parallel` uses `Promise.allSettled` and raises `AggregateError` for rejected listeners. `emit` is synchronous; `serial` and `bail` stop on Cordis bail values; `waterfall` sequences `next()` and does not itself transform payloads.

`ToolRuntime`'s policy/approval/pre-execute/guard/body/post-execute/result seam is the meaningful safety boundary. `TOOL_RUNTIME_SCHEDULER` and staged scheduler types are internal contracts, not an arbitrary public plugin extension point. Tool results are lossless snapshots, schema-validated and deep-frozen. A normal guard denial may appear only as a generic tool error unless the guard supplies structured `error.info`; there is no universal durable guard-decision event.

Visibility/bindability is capability reachability, not authorization. A visible or programmatically callable operation can still be denied by dynamic policy, approval, or a guard. Conversely, a PTC program's escalation applies to its own direct-effect world; nested tool calls retain their own policy/approval checks.

### Session and persistence

- `Session.append` validates, snapshots, and deep-freezes an event, then commits the in-memory log synchronously before post-commit observers.
- JSONL persistence is write-behind for live appends (`LIVE_WRITE_BATCH_MAX_DELAY_MS = 200`); `session/flush` and the shipped checkpoint policy establish explicit durability barriers. Therefore “the event log is the only source of truth everywhere” is too absolute: the canonical session-visible state is the log, but durability and external side effects require barriers/evidence.
- Normal `finally` paths close turns. A process crash can leave a durable open tail; resume repair uses `openTurnClosers`/`interruptedTurnClosers` and deterministic `TOOL_NOT_STARTED` versus `TOOL_OUTCOME_UNKNOWN` outcomes.
- `deriveMessages` is incremental/cached; there is no durable `assistant/chunk` event. Session format v4 and the v0→v4 migration chain are present.
- Generic low-level fork seeds do not themselves rewrite all session metadata. The shipped in-process subagent fork path persists `childSessionMeta` (parent lineage, `origin: 'subagent'`, composed preset, and monotonic `delegationDepth`) and adds delegated policy after the seed. Claims that normal forks reset these fields are false.
- Session logs have contiguous sequence validation but no native event hash chain, MAC, or Merkle root. The workbench adds those guarantees rather than claiming DSH already has them.

### Scopes, subagents, and approvals

- Scope registrations/layers flow down; ancestor event admission flows up; siblings are isolated. Nearer scoped layers win within the inherited model.
- `ToolGuard` is a core seam. Shipped `packages/guard/*` middleware is not a comprehensive policy suite; the in-process subagent structured-output seal is an actual guard registration. “Zero guards” is only true for a narrower claim about shipped guard packages.
- `SubagentRuntime` bounds depth/active children by default, but local registries are not distributed tenant safety. `subagent-dsh-sdk` advertises limited `agentOptions: true`; ACP, Claude Code, and Codex use `NO_START_CAPABILITIES`. The external-child claim must be provider-specific.
- `ApprovalService` fails closed for missing/throwing answerers, creates fresh request IDs, and emits turn-enclosed asked/decided events. An ephemeral escalation grant is not a standing session/deployment policy. Approval events do not inherently bind artifact/input hash, policy version, or environment fingerprint; the workbench adds that binding.
- The in-process child composition can enforce delegated approval `never`; external ACP/Codex/SDK providers do not inherit a universal parent-enforced start policy.

### Credentials, sandbox, PTC, and telemetry

- `LocalCredentialProvider` exposes structural, value-free descriptions and does not automatically inject credential values. Generic subprocess/file/SDK paths can still explicitly merge env or read files when a provider bypasses that seam; “the model can never read one” must be narrowed to “cannot obtain a value through the credential describe/provider seam.”
- `credentials-local` enforces POSIX owner-only mode where meaningful and explicitly skips POSIX bit inspection on Windows; Windows protection depends on ACLs and create/replace APIs. A scrubbed parent environment is a default for the DSH subprocess seam, not a universal guarantee for every SDK/provider child.
- `NodePtcRuntime` launches a fresh managed Node process per run with host-owned bindings, sandbox-policy-backed direct file effects, an empty environment, and V8/output/message/pending/time budgets. It is not an AsyncFunction in-process sandbox. `PythonPtcRuntime` uses a separate bounded process with frame/output/pending/address-space/time/process-group controls but has no shipped filesystem confinement profile. The generic sandbox seam still lacks a universal network/process/resource vocabulary and is not a kernel-grade SAS/Spark boundary.
- The base composition ships checkpoint policy, but custom compositions must mount it; “DSH always flushes before every side effect” is not a universal runtime law.
- Shipped OTel defaults to `FEEDBACK_ONLY`. The session telemetry redaction waterfall has no built-in content rules and captured records can pass through; this is different from exporting every record by default.
- `llmRetryPolicyOf` in `docs/subsystems/llm-streaming.md` is a documentation ghost. Target source captures retry policy on prepared routes/adapters and exposes provider retry policy; the public provider-neutral seam remains `llm/src`.
- Public LLM adapters include `llm-deepseek` and `llm-pi-ai`, with durable retry policy in `llm-retry` and provider usage versus heuristic pressure in `token-meter`; DeepSeek is not the only shipped adapter.

### Boot and composition

`packages/boot/app-boot` adds a material target-only invariant absent from the older report:

- `compatibility-preflight.ts` (`prepareProfileEntries`, `prepareProfilePatches`)
- `plugin-compatibility.ts` (`evaluatePluginCompatibility`, `getDshRuntimeVersion`)
- `profile-compatibility.ts` exact plugin/runtime exemptions

The preflight audits manifests before Include/Loader composition, disables ordinary incompatible rows, and denies native Includes that reach a denied plugin. The workbench treats profile/plugin/runtime pinning and startup attestation as deployment requirements, while keeping its own direct upstream Cordis composition independent.

## DSH → workbench difference matrix

| Concern | Target DSH behavior | Workbench decision | Reason |
|---|---|---|---|
| Runtime dependency | Harness-owned patched Cordis vendor (`4.0.0-rc.7` lineage) | Direct upstream `cordis@4.0.0-rc.10`; no DSH package | Independent business runtime and reproducible dependency provenance |
| Session authority | Canonical in-memory log plus write-behind provider and explicit flush/checkpoint barriers | JSONL/PostgreSQL session provider persists before projection mutation and flushes before external submission | Make the evidence barrier explicit |
| Integrity | Contiguous sequence; no native event hash chain/MAC | Per-event SHA-256 chain and evidence root implemented; Merkle/KMS sealing planned | Detect mutation now and bind certificates when sealing is added |
| Scope/process model | Process-local contexts, registries, leases | Worker-local Cordis scopes plus PostgreSQL/object authority and distributed leases | Multi-worker and multi-tenant safety |
| Executor boundary | Provider seams, PTC variants, internal scheduler types | Stable `SparkExecutor`/`SasExecutor` contracts; Databricks/Lakeflow outside control plane | Keep generated code and data out of the harness |
| Policy approval | Ephemeral grants and provider-specific child capability | Exact action, source/semantic hash, policy, environment, input, scope binding | Reject stale or substituted approval |
| Reconciliation | No SAS parity verifier in DSH | Independent schema/aggregate/row/statistical strategies; model cannot decide | Prevent false parity |
| Repair | DSH subagent/tool composition, not a SAS repair authority | Harness-owned isolated branches, monotonic guards, deterministic winner/escalation | Bound model influence and cost |
| Model boundary | Replaceable LLM adapters and context projections | Advisory provider only; absent from Milestone 1 | No model authority over evidence or execution |
| Credentials | Value-free provider seam, but arbitrary direct paths can bypass it | Reference-only model view plus provider-bound resolution and path/content controls | Explicit threat boundary |
| PTC/sandbox | Provider-specific process budgets; generic seam lacks universal network/resource vocabulary | Separate attested SAS/Spark execution worlds with OS/container/microVM controls | Do not mistake PTC containment for kernel isolation |
| Deployment | Developer-preview composition and local storage | PostgreSQL, UC Volume/object storage, Databricks Jobs, immutable audit sink | Production authority and recoverability |
## Corrections that remain relevant to the workbench

1. Preserve the canonical contiguous lossless event log, but distinguish in-memory commit, persistence barriers, and external side-effect evidence.
2. Keep capability seams and scoped lifecycle ownership, but add immutable tenant/execution-world identity to every service, artifact, policy, credential, telemetry, and worker record.
3. Keep frozen canonical tool arguments/results, explicit approval/guard decisions, bounded concurrency, and unknown-outcome handling; do not equate reachability with authorization.
4. Treat Node/Python PTC budgets as provider-specific containment, not proof of kernel/network isolation. Put SAS and Spark in separately attested execution worlds.
5. Use PostgreSQL/object storage, distributed leases, idempotency mappings, and immutable audit sinks for production; process-local Cordis contexts and JSONL are worker/test mechanisms only.
6. Treat DSH's normal in-process fork metadata behavior, SDK subagent capability advertisement, and boot compatibility preflight as provider/composition facts—not universal agent guarantees.

## Evidence paths

- [Target commit](https://github.com/deepseek-ai/deepseek-harness/commit/46a7f68b0922371ce7144b668b90e377d8e799f4)
- [`vendor/cordis/src/fiber.ts`](https://github.com/deepseek-ai/deepseek-harness/blob/46a7f68b0922371ce7144b668b90e377d8e799f4/vendor/cordis/src/fiber.ts)
- [`packages/core/session/src/index.ts`](https://github.com/deepseek-ai/deepseek-harness/blob/46a7f68b0922371ce7144b668b90e377d8e799f4/packages/core/session/src/index.ts)
- [`packages/session/session-persistence-jsonl/src/storage.ts`](https://github.com/deepseek-ai/deepseek-harness/blob/46a7f68b0922371ce7144b668b90e377d8e799f4/packages/session/session-persistence-jsonl/src/storage.ts)
- [`packages/core/tools/src/index.ts`](https://github.com/deepseek-ai/deepseek-harness/blob/46a7f68b0922371ce7144b668b90e377d8e799f4/packages/core/tools/src/index.ts)
- [`packages/ptc-runtime/ptc-runtime-node/src/index.ts`](https://github.com/deepseek-ai/deepseek-harness/blob/46a7f68b0922371ce7144b668b90e377d8e799f4/packages/ptc-runtime/ptc-runtime-node/src/index.ts)
- [`packages/experimental/ptc-runtime-python/src/index.ts`](https://github.com/deepseek-ai/deepseek-harness/blob/46a7f68b0922371ce7144b668b90e377d8e799f4/packages/experimental/ptc-runtime-python/src/index.ts)
- [`packages/boot/app-boot/src/compatibility-preflight.ts`](https://github.com/deepseek-ai/deepseek-harness/blob/46a7f68b0922371ce7144b668b90e377d8e799f4/packages/boot/app-boot/src/compatibility-preflight.ts)
