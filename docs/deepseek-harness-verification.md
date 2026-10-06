# DeepSeek Harness source verification

**Verification basis:** [`46a7f68b0922371ce7144b668b90e377d8e799f4`](https://github.com/deepseek-ai/deepseek-harness/commit/46a7f68b0922371ce7144b668b90e377d8e799f4), the requested `master` merge/release commit (`0.1.7-rc.1`, 2026-09-23).  
**Prior uploaded report basis:** `00102833dfaee1da9f48a3a8eae9d34005a75218`.  
**Method:** source-level comparison using target Git objects/API; the local DSH checkout is older and was not treated as current. The detailed correction ledger is [`dsh-target-audit.md`](dsh-target-audit.md).

## Cordis identity and dependency boundary

| Item | Verified value |
|---|---|
| DSH vendored framework | `vendor/cordis/` |
| Upstream Cordis version in vendor | `4.0.0-rc.7` |
| Upstream Cordis commit | `56b3d4f725681cf4556c1a8695a709cc3b6eed74` |
| Harness-scoped package | `@deepseek-ai/cordis@4.0.4` |
| Upstream current npm release observed | `cordis@4.0.0-rc.10` |
| Upstream current `main` observed | `f8ea3cd50f1a5724e8e715995bcde131c9c12b2c` |
| Workbench dependency | exact `cordis@4.0.0-rc.10` |

The workbench consumes upstream Cordis directly. It does not consume, fork, or depend on `@deepseek-ai/dsh-*`, the vendored Cordis, or DeepSeek business packages. The local vendor README documents Harness-owned lifecycle/config/include/Loader/HMR patches; those behaviors are not silently attributed to upstream Cordis. Cordis 4 remains a release candidate, so the exact version and composition are pinned.

## Target delta from the uploaded report

The target tree is not a small documentation-only update. It is a release-candidate tree with boot compatibility preflight, package/metadata changes, and a number of source/documentation corrections. Target measurements are:

- 13,302 tracked files and 307 package manifests.
- 54 tracked package groups, distinct from the 90 service-role graph rows (55 core, 33 seam, 1 bundle, 1 service).
- 347 Markdown documentation files / 137,476 lines.
- 3,539 `.agents/notes` files: 1,457 implemented, 1,919 archived, 117 proposed, 42 rejected, plus translations.
- 7 `pnpm.patchedDependencies` entries.
- 38 runtime `src/invariant.ts` companion files.

The older report's `00102833` counts, old `§21` table, and 57/54 package framing must not be used as current evidence. `master` remains explicitly a developer preview with compatibility-breaking changes; selected public `0.1.7-rc.1` packages are not automatically production-stable.

## Source paths inspected

### Cordis runtime

- `vendor/cordis/src/context.ts` — `Context`, `extend`, `isolate`, `intercept`, experimental `root`
- `vendor/cordis/src/registry.ts` and `reflect.ts` — provider identity, lookup, optional resolution
- `vendor/cordis/src/service.ts` — `Service`
- `vendor/cordis/src/events.ts` — five dispatch modes
- `vendor/cordis/src/fiber.ts` — dependency activation, effects, updates, disposal

### Harness core and persistence

- `packages/core/agent/src/index.ts` — registry/ownership and initiator context
- `packages/core/agent-loop/src/{agent,assistant-stream,tool-calls,runtime-context}.ts` — loop, streams, tool barriers, projections
- `packages/core/session/src/{index,surface,fork,repair,invariant}.ts` — session log, derived surface, fork/repair
- `packages/session/session-persistence/src/*` and `session-persistence-jsonl/src/*` — persistence contract, write-behind/local JSONL, leases, torn-tail recovery
- `packages/session/session-checkpoint-policy/src/*` — explicit flush barriers
- `packages/session/session-format-catalog/src/*` and `session-format-v3-to-v4/src/*` — format migration

### Capability and safety seams

- `packages/core/scope/src/*` — inherited layers and ancestor event admission
- `packages/core/tools/src/{index,ptc,types}.ts` — frozen args/results, policy/approval/guards, internal scheduler
- `packages/interaction/user-approval/src/*` — fail-closed approval
- `packages/credentials/credentials/src/*` and `credentials-local/src/*` — references, provider precedence, local protection
- `packages/sandbox/sandbox/src/*`, `sandbox-policy/src/*`, `sandbox-local/src/*` — policy/provider split and confinement facts
- `packages/ptc-runtime/ptc-runtime-node/src/*` and `packages/experimental/ptc-runtime-python/src/*` — provider-specific PTC runtimes
- `packages/guard/*` — shipped middleware examples, not a complete guard suite
- `packages/storage/*`, `packages/spill/*` — storage seams and local spill behavior

### Composition and providers

- `packages/llm/llm/src/*`, `llm-deepseek`, `llm-pi-ai`, `llm-retry`, `token-meter` — provider-neutral LLM and retry/metering
- `packages/skill/skill/src/*` and providers — registry/progressive skill seam
- `packages/subagent/subagent/src/*` plus ACP/Claude/Codex/SDK providers — provider-specific capability manifests
- `packages/compaction/compaction/src/*` and related providers — model-surface compaction
- `packages/bundle/base/*` and `packages/boot/app-boot/src/*` — patch composition and startup preflight

## Decisions preserved for the workbench

1. **Cordis is the worker-local composition kernel.** `Context` is a service repository/proxy; child contexts, `Service`, `Fiber`, and registry/reflection provide dependency and lifecycle composition. DSH also has kernel-like core authorities; this is not a claim that all privileged code is absent.
2. **Lifecycle is fiber-based and reversible.** Dependency activation waits for injected services; effects own registrations/configuration cleanup. Top-level unload starts reverse-collected disposers concurrently, while nested effect cleanup is sequential reverse; failures are contained/logged.
3. **Events are a family of contracts.** `emit` is synchronous notification; `parallel` aggregates listener rejection; `serial`/`bail` use bail values; `waterfall` sequences `next()` without implicit payload transformation. The workbench adds a typed facade because the upstream RC root declaration does not merge its internal event interface reliably.
4. **Capability seams are service contracts plus providers.** Consumers do not import concrete SAS, Spark, database, object-store, or model SDK implementations.
5. **The canonical session-visible state is the log, with explicit durability barriers.** `Session.append` commits the validated in-memory event before observers; JSONL live appends can lag up to 200 ms. `flush`/checkpoint policy must precede model/top-level side effects. The workbench's production session provider must make the barrier and external evidence explicit.
6. **Crash tails are repaired, not assumed impossible.** A normal `finally` path closes turns, while a process crash can leave an open tail. Resume repair distinguishes `TOOL_NOT_STARTED` from `TOOL_OUTCOME_UNKNOWN` and requires external verification before retry.
7. **Fork lineage is provider/composition-specific.** The shipped in-process subagent fork preserves `childSessionMeta` lineage/origin/preset/delegation depth; a generic low-level seed is not evidence that those fields reset.
8. **Tool results are canonical and frozen.** Policy, approval, deny-only guards, body middleware, normalization, and result validation are separated. A visible/PTC-reachable call is not automatically authorized. The internal scheduler is not a general public extension point.
9. **Model-visible context is a projection.** Compaction, summaries, prompts, and tool presentation never replace durable evidence.
10. **Credentials are references at the provider seam.** They are not automatically injected into model context; generic subprocess/file/provider paths still require explicit environment and content controls.
11. **PTC containment is provider-specific.** Node PTC is a fresh managed process with budgets; Python PTC has bounded process controls but no shipped filesystem confinement. Neither is a universal SAS/Spark kernel boundary.
12. **Repair and external side effects are harness-controlled.** DSH's local registries/leases and JSONL persistence are not distributed tenant authority.
13. **Boot compatibility is a deployment invariant.** Target app-boot audits plugin/runtime compatibility before Include/Loader composition; the workbench pins and attests its own composition rather than inheriting DSH's profile machinery.

## Important target-only corrections

- The target app-boot path contains `compatibility-preflight.ts`, `plugin-compatibility.ts`, and `profile-compatibility.ts`; these are absent from the older report.
- `NodePtcRuntime` is not an AsyncFunction sandbox. It launches a managed process with host-owned bindings, empty environment, and V8/output/message/pending/time budgets. Python PTC has separate process-group/RLIMIT-style controls and no file sandbox.
- The base composition ships checkpoint policy, but custom compositions must mount it; “DSH always flushes” is not universal.
- Shipped OTel defaults to `FEEDBACK_ONLY`; absence of built-in content-redaction rules does not mean every record is exported.
- `llmRetryPolicyOf` is a documentation ghost; target source exposes provider retry policy on prepared routes/adapters.
- `permission-presets.AUTO_PRESET` is explicitly experimental and combines danger-full-access with approval `never`.
- `Context.root` is explicitly experimental. DSH's target public package graph includes `llm-pi-ai`; DeepSeek is not the only shipped adapter.
- The target `subagent-dsh-sdk` provider advertises limited `agentOptions: true`; ACP, Claude Code, and Codex advertise no start capabilities. External child approval/policy is not universally parent-enforced.
- The generic sandbox seam has no universal network/process/resource vocabulary. Add cgroup/Job-object/container/microVM and egress controls for regulated SAS/Spark execution.
- The target report's older “no resource confinement anywhere” claim is false for the PTC providers; the accurate gap is the absence of a universal OS/network boundary.

## What is preserved, adapted, or rejected

### Preserved

- Service contracts and composition-selected providers
- Cordis dependency/effect lifecycle
- Explicit event dispatch ownership and ordering
- Canonical frozen tool results
- Policy/provider separation
- Credential references and value-free model views
- Scope inheritance and ancestor observation
- Safe-prefix forking and independently replayable candidate branches
- Unknown external outcomes after ambiguous crashes

### Adapted

- One worker-local Context composes migration/candidate capabilities; PostgreSQL/object storage are cross-worker authority.
- Scope chains add tenant/execution identity, immutable policy snapshots, workspace prefixes, environment fingerprints, and provider identities.
- Monotonic guard sets and decisions become durable evidence; a model cannot select a repair winner.
- Per-event SHA-256 chaining is implemented now; Merkle roots, signed checkpoints, and certificate sealing are planned extensions to the workbench's native contiguous log.
- Databricks Jobs is the default Spark execution provider; generated code never runs in the control process.
- Reconciliation consumes independently materialized artifacts and a versioned policy, with exact strategies gating certification.

### Rejected as production authority

- DSH's local process maps/registries as tenant isolation
- JSONL/local leases as a distributed scheduler or consensus service
- PTC budgets as proof of kernel-grade security
- Conversation as the primary operator interface
- Model-written arbitrary programs as the primary repair mechanism
- A generic autonomous coding-agent boundary

## Source links

- [Target commit](https://github.com/deepseek-ai/deepseek-harness/commit/46a7f68b0922371ce7144b668b90e377d8e799f4)
- [Target audit correction ledger](dsh-target-audit.md)
- [Cordis context](https://github.com/deepseek-ai/deepseek-harness/blob/46a7f68b0922371ce7144b668b90e377d8e799f4/vendor/cordis/src/context.ts)
- [Cordis fiber](https://github.com/deepseek-ai/deepseek-harness/blob/46a7f68b0922371ce7144b668b90e377d8e799f4/vendor/cordis/src/fiber.ts)
- [Session](https://github.com/deepseek-ai/deepseek-harness/blob/46a7f68b0922371ce7144b668b90e377d8e799f4/packages/core/session/src/index.ts)
- [Tools](https://github.com/deepseek-ai/deepseek-harness/blob/46a7f68b0922371ce7144b668b90e377d8e799f4/packages/core/tools/src/index.ts)
- [Scope](https://github.com/deepseek-ai/deepseek-harness/blob/46a7f68b0922371ce7144b668b90e377d8e799f4/packages/core/scope/src/index.ts)
- [App boot compatibility preflight](https://github.com/deepseek-ai/deepseek-harness/blob/46a7f68b0922371ce7144b668b90e377d8e799f4/packages/boot/app-boot/src/compatibility-preflight.ts)
- [Upstream Cordis](https://github.com/cordiverse/cordis)
