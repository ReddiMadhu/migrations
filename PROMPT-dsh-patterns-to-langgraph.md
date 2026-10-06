# Task prompt: port DeepSeek Harness architectural patterns into the sas-pyspark LangGraph backend

> **Prompt type:** design + implementation, for an autonomous coding agent.
> **Effort:** large. **Discipline:** Python only. **Hard constraint:** the project's deterministic invariants survive or the work is rejected.

---

## 0. Mission

Study the DeepSeek Harness (DSH) TypeScript codebase and extract four architectural patterns — **(1) LLM caching, (2) file editing, (3) agent running, (4) LLM advisory-only guards** — then bring those patterns into the `sas-pyspark` LangGraph backend as a **concrete design + working implementation in Python**, without breaking any deterministic invariant.

You are porting *architectural thinking*, not code. DSH is TypeScript/Cordis; this project is Python/LangGraph. Do not transliterate. Find the Python/LangGraph idiom that carries the same guarantee, or write it if no library provides it.

**The one-sentence correctness test:** after your work, a reviewer can point at each of the four pattern areas and say which DSH file the pattern came from, which Python module implements it, and which test proves it — and every existing test still passes.

---

## 1. The two repos

| Role | Path | Rule |
|---|---|---|
| **Study target (read-only)** | `C:\Users\madhu\deepseek-harness` | Read and reason about it. **Never edit, never run builds.** It is a large Cordis plugin monorepo: `packages/<group>/<pkg>/src/`. Skip `node_modules/`. |
| **Your codebase (editable)** | `C:\Users\madhu\Desktop\sas-pyspark\backend` | The single authoritative implementation. Python package `backend/src/sas2pyspark/`, tests `backend/tests/`. |

Read these **before** touching anything in `backend/`:

1. `AGENTS.md` (repo root) — project rules and the deterministic invariants.
2. `docs/langgraph-migration-spec.md` — the full design spec: graph topology, DI, persistence, reconciliation, Databricks provider, LLM/HITL, UI, milestones, and the LangGraph interrupt gotchas.
3. `docs/dsh-target-audit.md` and `docs/deepseek-harness-verification.md` — a rigorous, already-completed source audit of DSH. These documents are **authoritative** about what DSH actually does, and they contain an explicit list of what this project deliberately **rejected**. Do not contradict them (see §4).

---

## 2. Non-negotiable deterministic invariants

These are the project's reason for existing. If any of them regresses, the entire change is rejected regardless of how elegant the new patterns are. Every one of them must hold *through* your new code.

1. **Hash-chained event log** — sessions append ordered events; each event hashes the previous; `GENESIS_HASH` is the chain root. Tamper detection depends on this.
2. **Content-addressed artifacts** — SHA-256 over exact bytes; same digest deduplicates; overwrite rejected.
3. **Canonical JSON before hashing/freezing** — `json.dumps(..., sort_keys=True, separators=(",", ":"))`.
4. **Frozen inputs/policies** — arguments and policies are immutable before execution. No in-place mutation of event payloads.
5. **Deterministic clock** — tests inject a fixed clock; production uses ISO-8601 UTC. **Never call `time.time()` / `datetime.now()` directly in domain code.** Go through the injected clock service.
6. **Five-step rule** — model proposes → harness executes → reconciler verifies → event log proves → operator decides.
7. **LLM advisory-only** — the model never writes to the event log, never certifies results, and never enters the authoritative loop unguarded.

Extra project rules:

- Python tooling is **venv + pip** only. Do not introduce poetry, uv, or conda.
- Pure deterministic helpers (hashing, canonical JSON, reconciliation) stay pure: **no I/O inside pure nodes.**
- Tests use pytest with `tmp_path`; fixtures come from `backend/tests/conftest.py` (`test_dir`, `fixed_clock`, `workbench`, `make_scopes`, `make_session`).
- Do not commit real Databricks credentials or live workspace config.

---

## 3. What "study DSH" actually means here

DSH is a **developer-preview release candidate** (`0.1.7-rc.1`), built on vendored Cordis (`4.0.0-rc.7` lineage). The audit docs establish three facts that discipline this whole task:

1. **This project does not depend on `@deepseek-ai/dsh-*`.** It consumes patterns, not packages. You are not allowed to add a DSH dependency — there is none to add; the reference here is TypeScript and the project's own implementation is Python.
2. **DSH has no native event hash chain.** Its session logs have contiguous-sequence validation but *no* MAC/Merkle root. The audit says so explicitly: *"the workbench adds those guarantees rather than claiming DSH already has them."* **Do not attribute the hash chain to DSH and do not weaken it to look like DSH.** It is ours, it stays, and it is stricter than DSH's.
3. **DSH's safety seams are seams, not complete suites.** `packages/guard/*` ships *middleware examples*, not a policy suite. `packages/sandbox/*` has no universal network/process/resource vocabulary. Port the *seam* (the extension point + fail-closed default), not a claim of comprehensive coverage.

So for each of the four areas, extract the **pattern** (the guarantee + the mechanism + where it plugs in), and leave the TS/Cordis machinery behind.

---

## 4. Do-not-port ledger (from the authoritative audit)

`docs/deepseek-harness-verification.md` lists what this project **rejected as production authority**. Your implementation must not reintroduce any of them:

- DSH's local process maps/registries as tenant isolation.
- JSONL/local leases as a distributed scheduler or consensus service.
- PTC process budgets as proof of kernel-grade security.
- Conversation as the primary operator interface.
- Model-written arbitrary programs as the primary repair mechanism.
- A generic autonomous coding-agent boundary.

And these specific corrections, which are easy to get wrong:

- **`llmRetryPolicyOf` is a documentation ghost** — it does not exist in the target source. Retry policy is captured on prepared routes/adapters, and the public provider-neutral seam is `packages/llm/llm/src`. Cite the real path.
- **`permission-presets.AUTO_PRESET` is explicitly experimental** and combines `danger-full-access` with approval `never`. Do not use it as a default.
- **A normal guard denial appears only as a generic tool error** unless the guard supplies structured `error.info`; there is **no universal durable guard-decision event** in DSH. If you want durable guard decisions, that is a workbench addition — build it explicitly.
- **Approval events do not inherently bind artifact/input hash, policy version, or environment fingerprint** — the workbench adds that binding. Keep it.
- **Capability reachability ≠ authorization.** A visible or programmatically callable operation can still be denied by policy, approval, or a guard.
- **DSH does not always flush.** JSONL live appends lag up to 200 ms; flush/checkpoint barriers must be explicit and mounted. Any durability guarantee you add must be an explicit barrier, not an assumption.

---

## 5. Study area 1 — LLM caching

**Read this first: DSH has no prompt/response cache.** A negative grep across `packages/` for `responseCache|promptCache|cachedResponse|llmCache|CacheStore` returns **zero matches**. Every "cache" hit in `packages/llm` is *provider prompt-cache token accounting* (`cacheReadTokens`/`cacheWriteTokens`), not a DSH-side response store. DSH deliberately routes **through** the provider's cache instead of caching responses itself — e.g. `packages/compaction/compaction-basic/src/summarizer.ts:30` *"so the provider's KV cache is reused instead of invalidated"*, and `packages/llm/llm/src/types.ts:394` *"a changed prompt can follow the cached history instead of rewriting message 0"*.

> **Do not build an `functools.lru_cache`-style prompt→response cache.** It does not exist in DSH, and a stale-response cache would violate the five-step rule: the model must *propose* fresh, the harness executes, the reconciler verifies. If response memoization is ever needed it must be **content-addressed** (SHA-256 over canonical JSON) with explicit invalidation — never an in-process LRU.

So "LLM caching" in DSH is really five smaller patterns. Port these:

### 5.1 Disjoint token buckets with exact-total validation

`TokenUsage` (`packages/llm/llm/src/types.ts:175`) uses **disjoint buckets**: `inputTokens` counts *uncached* input only; billed input = `input + cacheRead + cacheWrite`. The DeepSeek wire mapping lives in `packages/llm/llm-deepseek/src/translate.ts:36` (`cache_read_input_tokens` → `cacheReadTokens`).

Port verbatim as a frozen dataclass with a validator that refuses to infer a total when it is not provable or contradictory (see `normalizeUsage()` in `packages/llm/token-meter/src/turn-usage.ts:81`). Pair with LangGraph's own usage accounting for the graph-level totals.

### 5.2 Canonical request header + field-wise equality (not a cache key)

No request fingerprint is ever hashed *for caching*. The only canonicalization is the request **header**, used for logging/equality: `canonicalHeader()` (`packages/core/session/src/request-header.ts:21`) plus `headerEquals()` (`:43`) → `callConfigEquals()` (`packages/llm/llm/src/call-config.ts:49`) over provider/model/reasoningEffort/temperature/maxTokens/`stop`, with tool schemas compared by ordered `JSON.stringify` (`sameSchema` :33).

**This maps directly onto this project's existing canonical-JSON invariant** — `json.dumps(..., sort_keys=True, separators=(",", ":"))`. Use it to decide when a request is *identical* (so a reprice/re-log can be skipped) — not to serve a cached response.

The one hashed key in the LLM layer is retry state: `retryPolicyKey()` (`packages/llm/llm-retry/src/index.ts:66`) = `JSON.stringify([mode, maxRetries, [...retryableCodes].sort(), initialDelayMs, maxDelayMs, jitterRatio])`. Reuse this idea for a *policy* fingerprint.

### 5.3 Durable retry — log the retry *before* the wait

Defaults (`packages/llm/llm/src/retry-policy.ts`): `maxRetries=5, initialDelayMs=500, maxDelayMs=10_000, jitterRatio=0.1`; retriable codes `[EMPTY_RESPONSE, 'RATE_LIMIT', 'SERVER', 'TIMEOUT', 'TRANSPORT']`; error classification in `packages/llm/llm-deepseek/src/transport.ts:29-35` — **AUTH/QUOTA/CONTEXT_WINDOW_EXCEEDED/INVALID_REQUEST are NOT retriable.** Backoff with symmetric jitter (`llm-retry/src/index.ts:59`), honoring the provider's `Retry-After`.

The part worth porting is the **durability**: `llm/retry` is appended to the log *before* the cancellable wait and `llm/retry-started` after (`llm-retry/src/index.ts:188-190`); the counter lives in a session projection keyed `[provider, policyKey]`, so it survives an interruption. The backend's existing retry (`llm/translation.py:79`, 3 attempts) is in-memory only.

**Python note:** `tenacity.wait_exponential_jitter` matches DSH's symmetric jitter, but tenacity is stateless — the durability must be ported by hand onto the hash-chained event log. Use the injected clock for all backoff timing so tests stay deterministic.

### 5.4 Stream assembly invariants (pure reducer)

`BlockAssembler` (`packages/llm/llm/src/assembler.ts:38`) is the single canonical assembly algorithm over the `StreamChunk` protocol (`types.ts:452`): **first `block-end` wins**, straggler deltas ignored (a misbehaving adapter cannot corrupt a closed block); when max-token truncation drops a tool call, the replay entry at the same position is dropped, and a length mismatch discards the whole envelope (`:135-149`). `interruptedBlocks()` (`:169`) yields the **cancellation-safe prefix** — text/reasoning only, tool calls omitted, because *"interruption precedes dispatch; retaining one would require a fabricated result."*

Port this as a **pure reducer** over stream chunks. This is the highest-value pattern for the backend's current *synchronous* `model.invoke()` (`llm/translation.py:32`) if/when it moves to streaming — and the "never fabricate a tool result" rule is a direct expression of the LLM-advisory-only invariant.

### 5.5 Log-replay determinism (the real "cache")

"Deterministic replay" in DSH is **log replay**, not response replay: `Session.create(id, session.snapshotEvents()).deriveMessages()` reproduces identical messages, and `TokenMeter._sync()` (`packages/llm/token-meter/src/index.ts:219-240`) folds events forward from a stored offset. The natural Python target is **`langgraph.checkpoint`** (replay state from durable events) — reinforcing the project's existing checkpointer, not adding a response cache.

`packages/llm/llm-deepseek/src/replay.ts` is **provider-native replay metadata**, not offline response replay: a `ReplayEnvelope` (`packages/llm/llm/src/types.ts:430`) carries adapter-private state on the terminal `finish` chunk; `readReplay()` (`:39`) **degrades rather than throws** on kind/version mismatch, model mismatch (*"cross-model signatures are not portable"*), or block-count mismatch. Keep this only if the backend uses DeepSeek thinking blocks — it is opaque metadata echoed back for provider cache continuity. The *degrade, don't throw* posture is worth porting regardless.

---

## 6. Study area 2 — File editing

**The DSH pattern.** DSH never lets an agent edit a file it has not read, and it makes every write conditional on a version token. The pieces, all verified in the target tree:

- **Observation gate** — `packages/fs/fs-observation-policy/src/index.ts` (`ObservedStateGate`). State is a `WeakMap<object, Map<string, FsObservation>>`: outer key = the owning session, inner key = the realpath of the target. An `FsObservation` is either `{kind:'present', version}` or `{kind:'absent'}` — an **opaque version token**, not mtime, not content hash (`packages/fs/fs/src/types.ts`).
- **Read records observation** — `packages/fs/tool-fs/src/read.ts` emits `fs/observed` with `{kind:'present', version}`; `read-target.ts` emits `{kind:'absent'}` for missing files.
- **Intent, not effect** — edit dispatches `fs/edit-intent`; `editIntent()` throws `FS_NOT_OBSERVED` ("edit requires `<path>` first") if the path was never read, and `FS_NOT_FOUND` if it was read-but-absent. write dispatches `fs/write-intent` resolving to `createIfAbsent` or `replaceIfVersion` at the observed version. The provider re-checks **under a per-target lock** in `packages/fs/fs-local/src/index.ts`, returning `FS_STALE_VERSION` on mismatch.
- **Version token** — stat identity `${dev}:${ino}:${size}:${mtimeNs}:${ctimeNs}` (`fs-local/src/fsio.ts`).
- **Edit semantics** — `applyLiteralEdit` normalizes CRLF→LF on both sides, counts occurrences, raises `FS_EDIT_NOT_FOUND` on zero matches and `FS_AMBIGUOUS_EDIT` on >1 without `replaceAll`. The full `editText` path is probe → stale-guard → LF-normalized read → apply → restore line endings → atomic write, **all inside a per-target lock**. Diffs use structuredPatch with 3 lines of context (`tool-fs/src/diff.ts`).
- **Atomic writes** — `writeFileAtomic`: `mkdir -p` parent → private `0o700` staging dir → temp file opened `wx` at `0o600` → write + `fsync` → publish by hard link (create) or rename/`ReplaceFileW` (replace). **No backups** — the "before" text lives in memory for the diff only.
- **Sandbox** — `packages/fs/fs-sandbox/src/index.ts` fences only writes; `workspace-write` mode **re-resolves the target fresh** (so a swapped symlink ancestor cannot smuggle a write outside the root) and mutates that fresh target — no check-here-write-there TOCTOU. Containment is a lexical prefix fast path plus an ancestor walk comparing `dev+ino` (`fs-sandbox/src/containment.ts`). Denials surface as the shared marker `[sandbox: file access denied under <mode> mode]`.
- **Windows** — `packages/fs/fs-local/src/win32.ts` copies the target DACL onto the temp file (blocking parent inheritance) and uses `ReplaceFileW` to preserve ACL/metadata on publish.

**What to build in Python.** Map each piece to a stdlib/LangGraph idiom:

| DSH piece | Python equivalent |
|---|---|
| `ObservedStateGate` (WeakMap by session) | An injected `ObservationGate` service: `dict[SessionId, dict[Path, Observation]]`, or scoped to a work-unit if the backend has no session-scoped store |
| `FsObservation` `{present, version}` / `{absent}` | Frozen dataclass; version from `os.stat_result` → `f"{st_dev}:{st_ino}:{st_size}:{st_mtime_ns}:{st_ctime_ns}"` |
| `fs/edit-intent` / `fs/write-intent` events | An explicit injected policy/intent service (a LangGraph node boundary or a DI service) — **not** an event bus |
| `FS_NOT_OBSERVED` / `FS_STALE_VERSION` / `FS_AMBIGUOUS_EDIT` | `enum.Enum` error codes raising a domain exception; model-facing message kept actionable ("read the file, then retry") |
| `applyLiteralEdit` | `str.replace`-based apply with occurrence counting; CRLF normalization via `str.splitlines`/`os.linesep` handling |
| `computeHunkDiffs` | `difflib.unified_diff` / `difflib.SequenceMatcher` for hunk-level diffs |
| per-target lock | `threading.Lock` per realpath key (or `asyncio.Lock` if the graph is async) |
| `writeFileAtomic` | `tempfile.mkdtemp`/`NamedTemporaryFile(delete=False)` + `os.open(path, O_EXCL|O_CREAT)` + `os.fsync` + `os.replace` |
| sandbox containment | `pathlib.Path.resolve()` + `os.stat` ancestor walk comparing `st_dev`/`st_ino`; deny by raising the domain denial exception |
| Win32 DACL copy | Acceptable to drop; note the fidelity loss (`os.replace` does not preserve custom ACLs) |

**Where it lands in the backend.** This backend already has a content-addressed artifact store (`services/artifacts.py`: SHA-256 over exact bytes, exclusive-create `"xb"`, dedup on identical digest, `read_reference()` re-verifying digest/size/identity) — the file-editing pattern is a natural sibling: the same content-addressing discipline applied to mutable workspace files instead of immutable artifacts. Propose a new module (e.g. `backend/src/sas2pyspark/services/file_editing.py`) exposing `read` → `edit` / `write` behind the observation gate, with the error enum, composed in `workbench.py`.

**Keep it pure where possible:** the gate logic, occurrence counting, and diff computation are pure and must do no I/O; only the atomic-write step touches disk. Every timestamp goes through the injected `Clock` (`services/clock.py`); every hashed or serialized structure goes through `canonical_json` (`core/json.py`). Tests use `tmp_path` and the existing `conftest.py` fixtures — see §9.6.

---

## 7. Study area 3 — Agent running

**The DSH pattern.** DSH's agent loop is a `ReactLoopAgent` (`packages/core/agent-loop/src/agent.ts`) with a strict three-level structure — `kick()` → `while (await this.turn()) {}` → `step()` — where **every level is recorded in the durable log**. That is the pattern worth porting: the loop's *control flow is itself evidence*.

### 7.1 The loop, and why it stops

- One turn: `session.append('turn/start')` → `preStep()` (claims an inbox batch, assembles the system prompt, runs an `agent/pre-step` waterfall that may `reject` → reason `blocked`) → `session.append('step/start')` → `step()`.
- One step: `prepareRequest` (an `agent/request` waterfall resolves provider/model) → `buildRequest` (logs `request/header`, **freezes messages**, derives `messages`/`toolHistory` from the session surface) → stream the LLM → append `assistant/message` (or `assistant/attempt` on failure) → `executeToolCalls()` if tool-call blocks exist.
- Stopping: a step returns a `StepEndReason` — `max-tokens` (**sticky**), `completed` (no tool calls, or a tool set `concludesTurn`), or `null` → continue. `turn/end` **always** records a `TurnEndReason` (`completed|max-tokens|blocked|aborted|error`). Another turn runs only if the inbox has pending work.

**Port to LangGraph:** a `StateGraph` with `pre_step → llm → tools → router` nodes, and the `completed`/`max-tokens`/`turnEnds` logic as a **conditional edge** (a ReAct-style router). This is a direct fit. The backend already has the skeleton — `propose` → `guard` → `approve` → `execute` → `reconcile` — plus a `messages` channel in `graph/state.py`; what it lacks is the **turn/step envelope as durable events** and the explicit stop-reason recording. Add those.

### 7.2 Tool dispatch — the staged scheduler

`packages/core/tools/src/index.ts` runs tool calls through a staged scheduler — `prepare → dispatch → finalize → finish`:

- `prepareExecution` runs a `tools/pre-execute` waterfall returning `allow|deny|cancel|ask`, then `guardReason(exec)` over a synchronous `ToolGuard` list plus scoped `restrict` filters. A denial materializes as an error result.
- `dispatchToolBody` fuses caller + wrapper signals, resolves the tool, calls `tool.execute(args, exec)`, and normalizes output through the tool's declared `output.schema`/`render`.
- `finalize` runs `tools/post-execute` (`accept|replace|block`); `finish` applies `finalizeContent` and emits `tools/result`.
- **Ordering and parallelism** (`packages/core/agent-loop/src/tool-calls.ts`): exclusive calls form barriers; parallel calls run in a bounded pool (`maxParallelToolCalls`) with results **committed strictly in model order**.

**Port to LangGraph:** there is **no pre/post-tool hook seam** in LangGraph. Implement the waterfalls as a **middleware chain wrapping the tool node** — a custom `Tool` wrapper or a wrapper around `ToolNode`. The backend already has a deny-only `GuardService` (`services/guards.py`); the DSH addition is the *staged* shape (pre-execute → execute → post-execute → result) and the **model-ordered commit** discipline.

### 7.3 Cancellation — and the durability of partial work

`cancel(cause)` clears the inbox and aborts the phase's `AbortController`; the loop observes it via `signal.throwIfAborted()` at every boundary, and an aborted signal is mapped to `turn/end` reason `aborted`.

The remarkable part is what happens to **partial work**: streamed blocks already received settle as an `interrupted: true` `assistant/message`, and tool calls skipped after the cancel get **synthetic `tool/call` + `tool/result` error events** (`appendSkippedToolCall`) so the log stays replayable. Cancellation is cooperative — tools must observe `exec.signal`; DSH notes it *"cannot hard-kill same-process code."*

**Port to LangGraph:** **no direct equivalent.** `interrupt()` suspends for human input (resumable) — closer to DSH's `ask` pre-execute decision than to cancel. Signal propagation must be hand-rolled (via `RunnableConfig` signal or a token in state). The partial-work durability (interrupted assistant message, synthetic skipped-tool results) has no equivalent either — if the backend needs replay-after-cancel, persist it yourself, onto the hash-chained log.

### 7.4 Background jobs

Seam `packages/jobs/jobs/src/index.ts` (`JobRegistry`); in-memory impl `packages/jobs/jobs-local/src/index.ts`. Start resolves the owner agent, requires an attached job controller, enforces a **per-owner concurrency cap**, issues a `<kind>-N` id, and runs `spec.run(handle) → {done, cancel}`.

The output model is the part worth studying: a bounded **`OutputRing`** (`packages/jobs/jobs-local/src/ring.ts`) with UTF-8 byte retention, head eviction, and absolute offsets. `read()` advances a registry-kept `modelCursor`; `readAt(from)` is a non-consuming observer read; **a reader behind the window gets a lossy read, never an error.** Settlement is **first-wins** (`completed|killed|failed`), then the ring trims to `settledRetainBytes`. Teardown force-cancels with cause `teardown`; `remove()` only after settlement.

**Port to LangGraph:** **no equivalent.** Closest is an `asyncio.Task` registry; the bounded ring + model-cursor + pump model must be rebuilt wholesale in Python. Note the backend's `translate()` is currently a **blocking `invoke()` inside an async node** — this is the natural place to introduce job-style non-blocking execution if the port closes that gap.

### 7.5 Subagent delegation

`packages/subagent/subagent-in-process-driver/src/index.ts`: the child gets its **own `Session`** (own id/log) and its own Cordis `Scope`. The parent's message context is **not copied** — the child inherits only provider/model plus a delegation-scope statement. `applyChildComposition` installs a per-child persona and a `tools.restrict(toolFilter)` *"owned by the child's scope and therefore invisible to its parent and siblings."* Policy is **pinned at delegation** (approval → `never`). Parent abort propagates as `child.cancel({kind:'parent'})`. The result is read from the **child's own session events past the activation boundary** → `SubagentResult {output, structured?, stopReason}`, and **partial output survives cancel**. Depth and active-children counts are bounded.

**Port to LangGraph:** `Send` for fan-out, or a subgraph invoked as a node. DSH's isolation is **stronger** than a plain subgraph — separate log, separate scope, fixed inherited permission policy, depth budget, parent-cancel propagation. `subagent-fork-in-process` (seed = parent log prefix) is the closest analogue to a state-sharing subgraph. Workflow fan-out (`parallel`/`pipeline`, where a child failure yields a per-item `null` rather than a fatal re-throw) maps to `Send` + map-reduce with a join reducer — encode the fatal-vs-item-null discipline **in the reducer**.

⚠️ Per §4, do **not** treat DSH's local registries as tenant isolation — the audit rejects that.

### 7.6 Session durability and state restore

- `Session.append` (`packages/core/session/src/index.ts:718-769`) is the **single mutator**: validates lossless JSON, snapshots, assigns `seq`/`time`, pushes to the in-memory log, dispatches `session/event` synchronously. `SessionStore.flush()` is the explicit durability barrier.
- Persistence is JSONL **generation files** named by format version; a historical generation is migrated on write-open (`session-format-v0-to-v1 … v3-to-v4`).
- Per-turn checkpointing is a **policy, not the loop**: `packages/session/session-checkpoint-policy/src/index.ts` flushes before `llm/stream`, before each top-level `tools/execute`, and at `agent/pre-step` — **fail-closed** (the body is not invoked if the flush fails).
- **Resume after a crash:** `AgentLoop.resumeWith` cold-reads the events and appends **synthetic `interruptedTurnClosers`** (missing tool errors, `step/end`, `turn/end`) for the crashed final turn. This is the pattern for the audit's `TOOL_NOT_STARTED` vs `TOOL_OUTCOME_UNKNOWN` distinction.
- **State restore** (`packages/session/session-projection/src/index.ts`): units register `{key, init, apply, wire, stateVersion, stateSchema}`; cells **fold lazily over the log from `init`**. A persisted watermark cache is a shortcut only — `restore` **discards it on version mismatch and refolds**. Cold read = persisted state + forward tail replay.

**Port to LangGraph:** the append-only log maps to `BaseCheckpointSaver`, but DSH's log is **strictly richer** — projections are pure folds you would reimplement as **channel reducers** in `graph/state.py`, and `stateVersion` maps to checkpointer schema versioning. `session-checkpoint-policy` maps to the checkpointer's automatic per-node persist. **The lazy-fold-with-version-gated-waterfall idea is worth porting** into the backend's state reducers.

### 7.7 Deterministic clock — DSH does not have one

> **There is no `Clock`/`TestClock`/`fixedClock` abstraction anywhere in DSH's `packages/`.** Domain code calls `Date.now()` directly — session `append`, header `createdAt`, job `startedAt`, schedule `drive`. Tests inject *LLM responses* (`packages/test-support/llm-mock-server`, `llm-replay`), not time.

**This invariant is the workbench's own, not a DSH pattern.** Do not go looking for it in DSH, and do not weaken the backend's existing `Clock` Protocol (`services/clock.py`: `now()` / `uuid()`, `SystemClock`, `FixedClock`) to match DSH. Any new loop, job, retry, or turn envelope code must route timestamps through the injected clock and be testable with the `fixed_clock` fixture.

---

## 8. Study area 4 — LLM advisory-only guards

**The DSH pattern.** Every safety boundary in DSH is an **ordered waterfall where listeners either claim or call `next()`** — and the whole system is built so that the model can never escalate its own permissions. This is the most directly relevant area for this project, because "LLM advisory-only" is its reason for existing.

### 8.1 The guard seams — and the deny-only invariant

- **Turn guard** — `agent/pre-step` → `PreStepDecision = reject | enter{messages}` (`packages/core/agent/src/runtime-types.ts:112-119`). A `reject` drops the step **before any model call** — the cheapest possible denial.
- **Tool guards** — `tools/pre-execute` → `PreToolDecision = allow | deny{reason} | cancel | ask{reason}` (`packages/core/tools/src/index.ts:607-611`). Ordering is significant: pre-execute → `ask` via the approval seam → the monotonic `ToolGuard` list → dispatch.
- **`ToolGuard` is deny-only by construction**: *"guards have no allow result, listener ordering cannot turn a denial back into a permission"* (`:723-731`). This is the single most important sentence in the area — **a guard can never authorize anything.**
- **Post-body guard** — `tools/post-execute` → `PostToolDecision = accept | block{feedback}` (`:617-620`) turns an already-finished call into an error. This is the seam that lets you *retroactively* reject a completed effect.
- Hook bridges (Claude Code/Codex) compile into the same allow/deny/ask vocabulary with precedence **deny > ask > allow**.

**Port to LangGraph:** a guard is a **node + conditional edge**. A `pre_tool` node returns `ALLOW/DENY/CANCEL/ASK`; `ASK` routes to an approval node, `DENY`/`CANCEL` route to a synthesized error result. The monotonic guard is a **deny-only check applied after the extensible policy** — it never returns allow. The backend's `GuardService` (`services/guards.py`, deny-only, `fail_closed=True`) already carries exactly this invariant; extend it, don't replace it.

### 8.2 The approval flow — one-shot, turn-enclosed, fail-closed

`ApprovalService` (`packages/interaction/user-approval/src/index.ts`) gates **before** dispatch:

- Policy is `'ask' | 'never'`. Crucially, `'never'` is decided **in-service, before any listener**, so it rejects **deterministically regardless of registration order** (`:270-275`).
- An ask appends a **turn-enclosed audit pair** — `approval/asked`, then `approval/decided` — to the session log (`:215-234`); outside an open turn it **throws** rather than silently dropping the audit.
- Answer modes are a **closed union** `ApprovalOutcome = 'allowed-once' | 'rejected' | 'cancelled' | 'unavailable'` (`types.ts:32`). **Only `allowed-once` proceeds**, and it is **one-shot, scoped to that exact request** — never a standing grant.
- **No answerer available** → the waterfall default is `'unavailable'`; a *throwing or rogue* (non-vocabulary) answerer is normalized to `'unavailable'` (`:280-292`), which `serviceAsk` maps to a **deny with a distinct reason**. A missing `ApprovalService` degrades to deny too (`packages/core/tools/src/index.ts:1731-1767`).
- Sandbox **escalation** reuses the same flow: `approveEscalation` checks the request is *strictly wider*, routes through `ctx.approval.request` **before anything executes**, and **every non-grant throws** (`packages/sandbox/sandbox/src/escalation.ts:171-207`).
- `permission-presets` bundles sandbox mode + approval policy into presets set by the **human-run `/permission` command**.

**Port to LangGraph:** `ApprovalService.request` becomes **`interrupt({"kind":"approval","tool","call_id","reason"})`**, resumed with the closed `ApprovalOutcome` union. `'never'` and no-answerer skip the interrupt and `raise PermissionDenied` directly — this preserves the backend's existing `interrupt()` discipline at `approve.py:108` / `migration_graph.py:59` / `subgraphs/repair.py:191`. **Keep `asked`/`decided` as two separate hash-chained events inside a turn** — this is how an approval becomes *evidence* rather than a side-channel.

⚠️ Per §4: DSH's approval events **do not bind** artifact/input hash, policy version, or environment fingerprint — the workbench's `ApprovalBinding` (8-field fingerprint, `assert_approved()` exact-match) is strictly stronger. **Do not weaken it to match DSH.**

### 8.3 Fail-closed defaults — quote these in the design doc

- Sandbox: *"`mode: 'read-only'` is the fail-safe default; a deployment that wants a workspace-writable agent opts in explicitly"* (`packages/sandbox/sandbox-policy/src/index.ts:66-73`).
- Approval: default `'ask'`, *"with none composed the chain falls through to the fail-closed `'unavailable'"* (`user-approval/src/index.ts:62`).
- Sandbox runner: *"must return enforcing argv or fail closed … silent unconfined passthrough is forbidden"* (`packages/sandbox/sandbox/src/index.ts:155-157`).
- Policy switches validate against **closed vocabularies** and **throw before the log changes** (`user-approval/src/index.ts:101-104`).
- A throwing telemetry-redaction listener **withholds its record** fail-closed (`packages/session/session-telemetry/src/index.ts:36`); storage **refuses unknown event vocabulary** (`session-persistence/src/index.ts:121`).

**Port to LangGraph:** fail-closed means **raise, never return `None`**. Unknown vocabulary, a missing answerer, or a throwing listener must `raise` so the error edge yields a *denial*, never an implicit allow. This is a direct match for the backend's `fail_closed=True` on `GuardService` and `resolve_checkpointer()`.

### 8.4 Why the model cannot self-authorize

This is the mechanism that makes "advisory-only" real rather than aspirational:

- **No model-facing tool changes policy.** `setSandboxMode` / `setApprovalPolicy` are *host functions* appending log-only events; the only write path is the **human `/permission` command** and session seeding (`permission-presets/src/index.ts:264-274`, `:398-418`) — **never registered as tools**.
- The model's **only** lever is `sandbox_permissions` + `justification` on an already-denied call; the grant comes back **one-shot for that exact call** (`escalation.ts:202`).
- Answerers are **scope-filtered to the owning agent**; the model registers **no `approval/request` listener**.
- **Misconfiguration throws at load** rather than silently degrading (`repeat-tool-reminder/src/index.ts:135-148`; `permission-presets/src/index.ts:219-221`).

**Port to LangGraph:** policy lives in **frozen dataclasses resolved per call** (`override ?? default`, default `read-only`); the model sees only a *rendered context string*, never the policy object. The backend's equivalent today is the system-prompt text in `llm/prompts.py` ("you do not certify equivalence, approve execution, or write evidence") — keep the text, but **add the structural enforcement**: no tool that mutates policy may exist in `llm/tools.py`'s read-only set, and policy edits must be rev-checked and non-model-writable. (Useful analogue: `packages/settings/src/schema.ts:69-75` gates live config edits to schema-`volatile` fields with **revision-conflict refusal**.)

### 8.5 Budgets and loop limits

- Per-call `timeoutMs`, deployment-capped; **zero does not disable it** (`packages/core/tools/src/ptc.ts:110-121`), enforced by `timeout-policy` (wraps `tools/execute`, arms a deadline, substitutes a structured `TOOL_TIMEOUT` error).
- `maxGoalRounds` bounds continuation rounds (**default 256**, positive-int validated; `packages/goal/goal/src/index.ts:74`, `:201`, `:375-377`); the driver **refuses past the cap**.
- **Repeat detection** breaks stuck loops: `repeat-tool-reminder` counts consecutive identical calls (canonicalized args) on `tools/post-execute` and injects an advisory message at thresholds `[3,5,8]` — explicitly *"observe-and-enrich, never veto"* (`packages/guard/repeat-tool-reminder/src/index.ts:220`). Note the posture: **advisory, not a block.**
- `maxParallelSubCalls` default 10 (`packages/core/tools/src/index.ts:693`); `delegationDepth` recursion budget (`packages/core/agent/src/index.ts:71`).

**Port to LangGraph:** budgets become **graph state** — `rounds_started` checked in a conditional edge, recursion depth in state. Repeat detection is **state counters keyed on canonicalized args** that injects an advisory message past thresholds without vetoing. ⚠️ The backend's repair budget is currently **hardcoded** (`guard.py:118`, `repair_iteration > 3`) though the spec says configurable — this is the natural place to make it policy-driven.

### 8.6 Shipped guard packages are examples, not a suite

`packages/guard/` ships exactly two middleware examples in the base bundle — `repeat-tool-reminder` and `timeout-policy` — plus hook bridges. Per the audit (§3), these are **middleware examples, not a comprehensive policy suite**. Port the *seam* (the deny-only guard extension point + fail-closed default + the observe-and-enrich-vs-veto distinction), and build the project-specific checks on top — which is what `graph/nodes/guard.py`'s AST checks already do.

### 8.7 Read `SAFETY.md` in the DSH root — and inherit its posture

Condensed: DSH is **experimental, unaudited, and not production-secure**. It executes model code and commands, loads third-party plugins, and reaches network/processes/credentials/files. Sandbox, prompts, and permission controls **reduce risk but never guarantee isolation** and never protect resources the project can access. Run least-privilege in a disposable VM/container, keep backups, don't expose credentials, review plugins/config/commands before allowing them. MIT, no warranty.

Carry this honesty into the design doc: **a guard seam is risk reduction, not a proof of isolation.** Do not claim the port makes anything "secure"; claim exactly what mechanism enforces what invariant, and cite the test that proves it.

---

## 9. Backend integration map

This is the backend as it exists today. New code plugs into these exact spots — do not invent parallel structures.

### 9.1 The LLM call site (where caching/retry/metering land)

- `graph/nodes/propose.py:43` calls `translate(source=, ir_snapshot=, macro_resolution=, model_context=)`.
- The only implementation is `LangChainSasTranslator.translate()` (`llm/translation.py:32`) — a **synchronous** `model.invoke(messages)` with a **hand-rolled tool loop** (`max_tool_rounds=4`, `translation.py:59`) and `_invoke_with_retry` (`:79` — 3 attempts, exponential backoff on 429/5xx/Timeout/Connection/`OSError`). Model via `langchain.chat_models.init_chat_model` (`:99`).
- Prompts: `llm/prompts.py` — `ChatPromptTemplate.from_messages([("system", SYSTEM_PREFIX), ("human", "{model_context}/{source}/{reconciliation}")])`. `SYSTEM_PREFIX` explicitly disavows authority (*"you do not certify equivalence, approve execution, or write evidence"*). `prompt_payload()` (`:24`) canonical-JSON-dumps the context. **Keep this text — it is part of the advisory-only enforcement.**
- Tools: `llm/tools.py` — 5 **read-only** `StructuredTool`s (inspect_ir_nodes, inspect_macro_definition, inspect_dependency, diagnostics with an AST check that rejects `eval`/`exec`/`compile`/`__import__`, search_translation_examples), bound via `model.bind_tools`, dispatched by a **manual loop** (no `ToolNode`, no prebuilt agent).
- **There is no caching, memoization, or LLM replay anywhere.** The only "cache" string in the codebase is `TokenMeter.prompt_cache_hit_tokens` accounting (`core/logging.py:121`). Every call re-hits the model.

### 9.2 Advisory-only enforcement (where guards land)

- `services/guards.py` `GuardService` is **deny-only**: `evaluate()` (`:85`) raises `POLICY_DENIED` on the first denial, logs to `_denial_log`, and with `fail_closed=True` **denies when no guards are registered**. Guards can never authorize. This is the direct analogue of DSH's `ToolGuard` seam.
- `graph/nodes/guard.py` is the deterministic gate between propose and approve — AST checks for forbidden imports, dynamic exec, UDFs, f-string SQL, secrets, hallucinated PySpark APIs, and repeat digest/hypothesis.
- `services/approvals.py` — 8-field `ApprovalBinding` fingerprint; `assert_approved()` (`:108`) exact-match, else `STALE_APPROVAL` / `APPROVAL_REQUIRED`. This is the workbench's *addition* over DSH (DSH binds nothing) — preserve it.
- **HITL `interrupt()` is used at exactly 3 sites:** `graph/nodes/approve.py:108`, `graph/migration_graph.py:59`, `graph/subgraphs/repair.py:191`. Side effects are placed *after* the interrupt per spec §4.5. Any new interrupt must follow the same rule.
- **The five-step rule has no named construct in code** (grep finds nothing in `src/`). It is enforced structurally by the graph topology (`migration_graph.py:257-321`) plus the system-prompt text (`prompts.py:11-16`). Cite those; do not claim an enforcement function exists. **Opportunity:** give the rule an executable representation (a node/edge contract or a state field), but do not break the existing topology to do it.

### 9.3 Graph topology (where agent-running lands)

`graph/migration_graph.py` — 17 nodes: discover, parse, resolve_macros, create_work_unit, assemble_model_context, translation_started, propose, guard, approval_request, approve, execute, reconcile, operator_decision_request, operator_decision, certify, human_required, and the repair subgraph. Every stage routes via `_route_or_continue(next)` → `next | human_required`. `reconcile` → `{certify → operator_decision_request, repair, human_required}`; `certify → END`; `human_required → END`.

- State: `graph/state.py` — `MigrationGraphState(TypedDict, total=False)`, ~30 channels, reducers `_replace` / `_append_list` / `_merge_dict` / `add_messages` (a `messages` channel **already exists**).
- Checkpointer: `graph/checkpointer.py` `resolve_checkpointer()` → `MemorySaver` for dev; production requires `SqliteSaver` and **fails closed** (`CONFIG_ERROR`) if the module is missing. ⚠️ **`langgraph-checkpoint-sqlite` is not in `pyproject.toml`** — production checkpointing mode currently raises rather than falls back. Decide deliberately whether to add the dep.
- `graph/subgraphs/repair.py` is where bounded repair iteration lives (with its own `interrupt()` at `:191`).

### 9.4 Deterministic invariants (signatures you must not break)

- `core/hash.py` — `GENESIS_HASH = "sha256:" + "0"*64` (`:10`), `sha256_bytes(data: bytes) -> str` (`:13`), `sha256_json(value) -> str` (`:21`).
- `core/json.py` — `canonical_json(value) -> str` (`:67`), `snapshot_json(value, path="$")` (`:23`, rejects NaN/Inf, normalizes `-0.0`).
- Chain extension: `session/service.py:248` `append_event(session, type, data)` — `seq = lastSeq+1`, `previousHash = lastHash`, `hash = evidence.hash(EventHashInput(...))`, then persist + `_apply_event`. `EvidenceService.verify()` runs on every open/replay.
- Artifacts: `services/artifacts.py` `create(*, scope, data, media_type, created_by, ...)` → `"sha256:" + digest`, path `objects/<hh>/<digest>`, exclusive-create mode `"xb"`, dedup on identical digest, `DUPLICATE_ARTIFACT` otherwise; `read_reference()` re-verifies digest/size/identity.
- Clock: `services/clock.py` — `Clock` Protocol with `now() -> str` / `uuid() -> str`; `SystemClock` (ISO-8601 Z); `FixedClock` (tick sequence, base epoch 2026-01-01). **All new timestamps go through this.**

### 9.5 Composition

`workbench.py:103` — `async create_workbench(WorkbenchConfig) -> WorkbenchRuntime`: plain **constructor injection in dependency order** (no container, no registry, no providers). Pluggable slots: translator, spark_executor, reconciler, repairer, checkpointer, clock, token_budget_policy. ⚠️ **`runtime.translator` defaults to `None`** → the propose node hard-fails to `HUMAN_REQUIRED` with no default model anywhere. New services get composed here, never imported as globals.

### 9.6 Tests

`backend/tests/conftest.py` fixtures: `fixed_clock() -> FixedClock` (`:116`); `test_dir() -> Iterator[Path]` (`:121`, `.test_tmp/run-N`, `rmtree` after); `async workbench(test_dir, fixed_clock) -> AsyncIterator[WorkbenchRuntime]` (`:133`); `make_scopes(runtime, suffix="")` (`:142` → tenant/migration/attempt/candidate); `async make_session(runtime, scope=None, session_id="session-test")` (`:150`). **Reuse these; do not create a second fixture layer.**

### 9.7 Dependencies

`pyproject.toml` — all lower-bound ranges, **nothing pinned exact**: `langgraph>=0.2.0,<0.3` · `langchain>=0.3.0,<0.4` · `langchain-core>=0.3.0,<0.4` · `fastapi>=0.115.0,<0.116` · `pydantic>=2.9.0,<3` · `databricks-sdk>=0.30.0,<0.40` · `uvicorn` · `structlog` · `sse-starlette`. Dev: pytest, pytest-asyncio, httpx, ruff, mypy (strict). **Python ≥ 3.12.** Any new dependency (e.g. `tenacity`, `langgraph-checkpoint-sqlite`) must be added here with a bounded range and justified in the design doc.

### 9.8 Known gaps the port should address

1. No LLM cache/memo/replay (by design — see §5).
2. No subagents or multi-agent orchestration.
3. No background jobs — `translate()` is a blocking `invoke()` inside an async node; no `astream`/`stream_events`.
4. Retry exists only at the model-call level; no graph-level retry.
5. Token metering is **passive accounting** (`TokenMeter.record` + `check_budget` → `BudgetExceeded`), not enforced per-scope against `policy.maximum_cost_units`; tool rounds are unmetered.
6. `runtime.translator` defaults to `None`.
7. No durable checkpointer package in deps.
8. A real Databricks provider exists (`providers/databricks.py`) but the workbench ships dummies.
9. Repair budget is hardcoded (`guard.py:118`, `repair_iteration > 3`) though the spec says configurable.
10. Five-step rule has no executable representation.

Pick from this list deliberately; the design doc must state which gaps the port closes and which it deliberately leaves open.

---

## 10. Implementation discipline

- **Read before write.** Read `backend/src/sas2pyspark/workbench.py` and the modules you intend to touch before editing them. The composition style is constructor/dependency injection; new services must be composed there, not imported as globals.
- **Python only, venv + pip.** No poetry/uv/conda. No new runtime dependency may be added without justification; prefer the stdlib (`pathlib`, `difflib`, `tempfile`, `os`, `hashlib`, `enum`, `dataclasses`).
- **Deterministic clock everywhere.** Any new timestamp goes through the injected clock service. Tests use the `fixed_clock` fixture.
- **Canonical JSON before freezing.** Any new persisted/hashed structure goes through the canonical-JSON helper.
- **No I/O in pure nodes.** Hashing, canonicalization, gate logic, diff computation, and reconciliation stay pure.
- **Tests first, tests last.** Every new behavior gets a pytest test in `backend/tests/` using `tmp_path` and the existing `conftest.py` fixtures. Add fixtures only if strictly necessary.

**The gate (run before claiming done):**

```bash
cd backend && PYTHONPATH=src .venv/Scripts/python.exe -m pytest tests/ -q
```

The full suite must pass. If a test fails because an invariant now *correctly* fires, that is a bug in your change, not in the test.

---

## 11. Deliverables

Produce, in this order:

1. **A design doc** — `docs/dsh-patterns-port.md`. For each of the four areas: the DSH source (file paths), the extracted pattern (one paragraph), the chosen Python/LangGraph idiom, the target module in `backend/`, and the invariant interactions (which of §2's seven invariants this touches and how they are preserved). Include the do-not-port reasoning from §4 where relevant.
2. **The implementation** — new/edited modules under `backend/src/sas2pyspark/`, composed in `workbench.py`.
3. **The tests** — `backend/tests/`, named after the area (`test_file_editing.py`, `test_llm_cache.py`, `test_agent_run.py`, `test_llm_guards.py` or merged into existing files where they extend existing coverage).
4. **A run of the gate** — paste the passing pytest summary into your final message.

**Definition of done:** all four pattern areas are implemented in Python, the pytest gate is green, no invariant regressed, no rejected DSH pattern was reintroduced, and the design doc's file pointers all resolve to real files in both repos.

---

## 12. Failure modes to avoid

**Pattern traps (things DSH does *not* have — do not invent them):**

- **Building an `lru_cache`-style prompt→response cache.** It does not exist in DSH, and a stale response would violate the five-step rule (the model must propose fresh). If memoization is ever needed it must be content-addressed with explicit invalidation — never an in-process LRU. (§5)
- **Looking for a deterministic clock in DSH.** There is no `Clock`/`TestClock` abstraction; DSH calls `Date.now()` directly in domain code. The deterministic clock is *this project's* invariant — do not weaken `services/clock.py` to match DSH, and never call `time.time()`/`datetime.now()` in domain code. (§7.7)
- **Treating `interrupt()` as cancellation.** DSH's cancel is cooperative abort with durable partial work; LangGraph's `interrupt()` suspends for human input and resumes — closer to DSH's `ask` decision than to cancel. Do not conflate them. (§7.3)
- **Treating `packages/guard/` as a complete policy suite.** It ships two middleware *examples*. Port the seam, build the checks. (§8.6)

**Attribution traps (claiming DSH has something it does not):**

- Attributing the **hash chain**, **durable guard decisions**, or **approval-hash binding** to DSH. They are workbench additions; DSH has none of them, and the audit says so explicitly. (§3, §4)
- Citing `llmRetryPolicyOf` — it is a documentation ghost. Retry policy lives on prepared routes/adapters; the provider-neutral seam is `packages/llm/llm/src`. (§4)

**Discipline traps:**

- Transliterating TypeScript. If your Python code has `Promise`, `WeakMap`, or Cordis event-waterfall equivalents, stop and find the idiom.
- Using `AUTO_PRESET` or any `danger-full-access`-adjacent default — it is explicitly experimental.
- Adding a caching layer that makes the **deterministic clock** or **canonical JSON** path non-deterministic (e.g. caching a serialization that includes a timestamp).
- Letting the model call any new tool without it passing through the guard/approval seam, or adding any tool that can mutate policy (DSH makes policy-mutating functions host-only, never tools). (§8.4)
- Weakening the 8-field `ApprovalBinding` to match DSH's unbound approval events. (§8.2)
- Declaring done without running the gate.
