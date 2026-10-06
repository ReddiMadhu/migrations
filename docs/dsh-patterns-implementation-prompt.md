# Implementation prompt — imbibe DeepSeek Harness patterns into the SAS→PySpark LangGraph backend

> Hand this whole document to a coding agent. It is self-contained: it states the mission, the
> invariants that cannot move, the current code to reuse (with file:line), the DSH patterns being
> ported (with DSH file:line citations), the concrete build target, the tests to write, and how to
> verify. Read [`docs/langgraph-migration-spec.md`](langgraph-migration-spec.md) §19, §20.8 and
> [`AGENTS.md`](../AGENTS.md) before writing code — this prompt is consistent with both.

---

## 0. Standing rules (apply to every decision)

1. **Port patterns, not code.** DSH is TypeScript/Cordis. Translate to Python idioms: pydantic v2
   frozen models, `enum`, `asyncio`, pure functions. Do not recreate Cordis `Context`, waterfalls,
   `Fiber`, WeakMap, or zod/schemastery dual schemas — those are DSH-specific noise (explicitly
   ranked "skip" in the research).
2. **Find-similar-first.** For every DSH pattern below, first search `backend/src/sas2pyspark/` and
   the Python standard library / installed deps (pydantic, langchain-core, langgraph) for an
   existing equivalent. **Reuse it.** Only write new code when no equivalent exists. Cite what you
   reused in your commit notes.
3. **The Python backend is authoritative and already complete.** `backend/` is the single
   implementation (the TS/Cordis reference has been deleted). You are *adding capabilities to a
   working system*, not rebuilding it. Do not restructure the graph, rename services, or change
   public signatures that existing tests depend on. Additive + backward compatible.
4. **Invariants are load-bearing** (AGENTS.md / spec §3):
   - Hash-chained event log; `GENESIS_HASH` chain root; events appended in order, each hashing the
     previous. The LLM **never** appends to it directly — nodes do, guarded.
   - Content-addressed artifacts: SHA-256 over exact bytes, same digest dedups, overwrite rejected.
   - Canonical JSON before hashing/freezing (`canonical_json` in `core/json.py`).
   - Frozen inputs/policies (`WorkbenchModel`, `core/models.py`).
   - Deterministic clock: **never** `time.time()` / `uuid.uuid4()` in domain code — always the
     injected `Clock`; inject `sleep` where a delay is needed (the existing translator does this).
   - Five-step rule: model proposes → harness executes → reconciler verifies → event log proves →
     operator decides. **The LLM is advisory only.** Every LLM-node exit passes through the
     deterministic guard before any side effect.
   - Deny-only guards; policy monotonic narrowing (`assert_policy_no_wider`).
5. **No I/O inside pure nodes** (AGENTS.md). `apply_patch`, cache-plan building, descriptor
   snapshots, token normalization are all pure.
6. **Tooling: venv + pip only.** No poetry, uv, or conda (AGENTS.md).

---

## 0b. Your workflow (execute in this order, report after each)

1. **Read before writing.** Read `AGENTS.md`, `docs/langgraph-migration-spec.md` §3/§19/§20.8, and
   every file cited in §1 below. Do not design from assumptions.
2. **Reconnaissance table.** For each pattern in §3A/3B/3C, produce one row: *DSH pattern → existing
   Python equivalent found (path:line) → new code needed (file)*. If you cannot find an existing
   equivalent, say so explicitly and cite what you searched. This table is your first deliverable and
   the basis of the "find similar things in Python, else write it" rule.
3. **Implement 3A → 3B → 3C** in that order (3A unblocks nothing else; 3B is self-contained and pure;
   3C depends on tool-filtering from 3A and scope narrowing that already exists). Keep each area in a
   reviewable commit. Pure logic first, wiring second.
4. **Write the tests in §4 alongside each area, not after.** They are the acceptance criteria.
5. **Verify** per §5. If the pytest blocker in §5 bites, say so plainly and keep building pure logic.
6. **Final report must state, per area:** what DSH pattern was ported, what existing Python code was
   reused (path:line), what is new, which invariant could have been violated and how you prevented
   it, and the test result. If you skipped or rejected any pattern in §3, say why.

Definition of done: §3A/3B/3C implemented, §6 tests written and green (or the §5 blocker documented),
`ruff` + `mypy --strict` clean on touched files, no existing test weakened, and the report above
delivered.

---

## 1. Where you are working (current state — reuse these, do not rebuild)

```
backend/src/sas2pyspark/
  core/
    errors.py     WorkbenchErrorCode (L9-40), WorkbenchError(code, message, details) (L43-60)
    events.py     MigrationEventType (L13-40), ArtifactReferenceData (L55-65), MigrationEvent (L68-84)
    json.py       canonical_json (L67-74), snapshot_json (L23-49), deep_freeze (L77-83)
    logging.py    TokenMeter (L118-237), TokenBudgetPolicy (L21-29), BudgetExceeded (L32-35),
                  log_model_usage (L106-115), redact_value (L38-48)
    models.py     WorkbenchModel — frozen, extra-forbid, NaN/Inf-rejecting base (L11-20)
  services/
    artifacts.py  FileSystemArtifactService — content-addressed, overwrite rejected
    scopes.py     create_child (L136), require_by_id (L187)
    policy.py     assert_policy_no_wider(parent, child) (L69), policy_fingerprint
    guards.py / baseline_guards.py   GuardService — deny-only
    evidence.py / session/   hash-chained event log + JSONL persistence
  llm/
    prompts.py    SYSTEM_PREFIX + TRANSLATION_PROMPT (L18-21) + prompt_payload (L24-31)
    translation.py LangChainSasTranslator; _invoke_with_retry (L79-93); create_sas_translator (L96-101)
    tools.py      make_translation_tools — 5 read-only tools (inspect_ir_nodes,
                  inspect_macro_definition, inspect_dependency, diagnostics,
                  search_translation_examples) (L69-75)
  graph/
    migration_graph.py  node list L257-273; conditional edges L282-322
    nodes/propose.py    metering: records usage_metadata into token_meter, budget check (L52-61)
    nodes/guard.py      make_guard_node — static analysis, secret scan, repeat-digest/hypothesis
                        rejection, deny-only (L113-195)
    subgraphs/repair.py propose_patch returns {candidateSource, hypothesis, expectedEffect}
                        (L66-123); guard_patch (L125-143); reconcile w/ signature_seen +
                        near_oscillation (L238-294)
  workbench.py    WorkbenchConfig (L32-48); WorkbenchRuntime slots (L54-70);
                  create_workbench composes everything; token_meter created L154
```

Read those files before designing. The three capabilities plug into `llm/`, `services/patches.py`
(new), `services/delegation.py` + `graph/delegation.py` (new).

---

## 2. What DSH does (research findings, with citations)

Citations are absolute paths under `C:\Users\madhu\deepseek-harness`. **Read-only reference** — do
not modify that checkout.

### 2A. LLM caching + call layer

- **No client-side response cache exists in DSH** (grep for `responseCache|cacheHit` across
  `llm/`/`core/`/`compaction/`/`context/` `src`: zero hits). All caching is **provider-side prompt
  caching**. Do not build a response cache here either — it would hide cache accounting and risk
  serving stale proposals past the reconciler.
- **DeepSeek adapter** (`packages/llm/llm-deepseek/src/translate.ts:34-43,159`): *automatic prefix
  caching*, no explicit markers. Maps `cache_read_input_tokens`→`cacheReadTokens`,
  `cache_creation_input_tokens`→`cacheWriteTokens`.
- **Disjoint `TokenUsage` buckets** (`packages/llm/llm/src/types.ts:168-189`): `inputTokens` is
  **uncached input only**; billed input = `input + cacheRead + cacheWrite`; `reasoningTokens` ⊆
  output. Optional buckets appear only when *every* attempt reported them.
- **Turn aggregation discipline** (`packages/llm/token-meter/src/turn-usage.ts:178-273`): a strict
  fold with an attempt state machine; any missing boundary, non-safe-integer, or contradictory
  total → the **whole disclosure becomes `undefined`** rather than silently wrong.
- **Prompt-side pressure** (`usage-projection.ts:79`) = `input + cacheRead + cacheWrite` (no output).
- **Structural cache reuse** (`packages/llm/llm/src/types.ts:396-407`): `SystemPromptUpdate='in-history'`
  and `ToolUpdate='in-history'|'addition-only'` let mid-conversation system/tool changes *follow*
  cached history instead of rewriting message 0 — this is DSH's real cache-reuse lever.
- **Explicit markers are delegated to the transport** (`packages/llm/llm-pi-ai/src/catalog.ts:425-435`):
  per-profile flags `cacheControlFormat`, `supportsLongCacheRetention`, `supportsCacheControlOnTools`,
  plus `cacheRetention: 'none'|'short'|'long'` (`config.ts:340`). DSH never computes breakpoints itself.
- **Retry** (`packages/llm/llm/src/retry-policy.ts:14-24`, `packages/llm/llm-retry/src/index.ts:59-64`):
  maxRetries 5, initialDelay 500 ms, maxDelay 10 000 ms, jitterRatio 0.1; retryable codes
  `EMPTY_RESPONSE | RATE_LIMIT | SERVER | TIMEOUT | TRANSPORT`; modes `normal` (bounded + code-filtered)
  and `always`; backoff `min(initial·2^min(retry-1,1024), maxDelay)` × symmetric jitter
  `1−r+2r·random()`; provider `Retry-After` honored when ≤ maxDelay (`llm-retry:227-238`); retry
  count is **durable** via `llm/retry` events (`llm-retry:125-138`).
- **Error taxonomy** (`packages/llm/llm/src/error.ts:13-22,83-103`, `types.ts:41-59`): `HarnessError`
  base with stable `code`; regex classifiers (`isContextWindowExceededError`, `isQuotaExceededError`)
  are a **last-resort fallback** — routing must use the code, never the message.
- **Stream assembler invariants** (`packages/llm/llm/src/assembler.ts:137-139,169-179`): on
  `max-tokens`, tool calls are dropped; an interrupted prefix keeps only non-empty text/reasoning.

### 2B. File editing (str_replace stack)

- **Tool surface** (`packages/fs/tool-str-replace-editor/src/index.ts:436`): one tool, four commands
  `view | create | str_replace | insert`. **No undo.**
- **Matching is exact and literal** — plain `indexOf`, case-sensitive, byte-exact, **no whitespace
  normalization** (`index.ts:44-53`). Reads decode "byte-for-byte (no normalization)"
  (`fs-local/src/fsio.ts:392`). `old_str` must be non-empty (`index.ts:290`).
- **Uniqueness is enforced by counting all offsets** (`index.ts:297-311`): 0 matches →
  `FS_EDIT_NOT_FOUND` ("did not appear verbatim"); >1 → `FS_AMBIGUOUS_EDIT` **listing the line
  number of every occurrence** (`lineNumbersAt`, `index.ts:55-65`). That line list is the only hint.
- **insert** bounds-checked to `[0, lines.length]` (`index.ts:349-353`); **create** refuses to
  overwrite ("Cannot overwrite files using command `create`", `index.ts:252`).
- **Observation gate** (`packages/fs/fs-observation-policy/src/index.ts:28,78-94`): a three-state
  per-(session,target) gate unseen / confirmed-absent / present@version. `fs/observed` is the *only*
  way to declare observation. Edit without read → `FS_NOT_OBSERVED`. Blind overwrite is rejected
  atomically *in the provider* via a hard-link-no-replace publication so a concurrent creator's file
  survives (`fs-local/src/fsio.ts:633-638`).
- **Version CAS is checked *before* literal matching** (`fs-local/src/index.ts:264-275`): a stale read
  reports `FS_STALE_VERSION`, never a misleading match failure. A per-target FIFO lock serializes
  read→guard→write (`fs-local/src/index.ts:101-131`).
- **Sandbox** (`packages/fs/fs-sandbox/src/index.ts:80-109`): `read-only` denies with
  `FS_SANDBOX_DENIED`; `workspace-write` **re-canonicalizes via `resolve` and delegates with the
  fresh target** (kills check-here-write-there TOCTOU); containment is lexical-then-`dev+ino`
  identity (`packages/fs/fs/.../containment.ts:58-75`). Atomic publication: private staging dir +
  exclusive temp + `fsync` + rename (`fsio.ts:588-670`).

### 2C. Agent running (subagent runtime)

- **Descriptor is a separate, versioned, model-hidden record**
  (`packages/subagent/subagent/src/descriptor.ts:48,267-303`): `SUBAGENT_DESCRIPTOR_VERSION = 3`;
  `snapshotSubagentDescriptor` validates **lossless JSON before any Task exists** — validate-and-freeze
  first, work later. It is log-only, **never enters model history** (`descriptor.ts:34-38`).
- **Fail-loud capability preflight** (`subagent/src/index.ts:642-658`): unsupported options are
  rejected, never silently degraded.
- **Isolation** (`subagent/src/child-agent.ts:172-176,221-280`): the child gets its own session log
  and context scope; a fixed **delegation-scope statement** ("scope fixed at start, cannot be
  widened"); **tool allowlist is one-visibility** — `toolFilter` removes tools from the prompt *and*
  refuses execution (`types.ts:186-192`); approval is pinned `'never'`; policy is captured
  **synchronously before the first await** so later parent switches don't leak (`child-agent.ts:239-245`).
- **Depth is monotone from the persisted header**: `resolveChildDepth` = parent+1, cap-enforced
  (`child-agent.ts:50-59`, `depth.ts:28-36`); `maxDepth` default 1, `maxActiveSubagents` default 8
  (`index.ts:191,194-202`); caps validated as non-negative safe integers (`depth.ts:42-50`).
- **Settle, don't throw** (`types.ts:271-297`): `SubagentResult{output, structured, diagnostic,
  stopReason}`. `result` **resolves, never rejects**, even on child failure — it resolves
  `stopReason: 'error'` so a downstream layer treats model failure as data (`types.ts:294-297`);
  `diagnostic` is secret-scrubbed and ≤4096 bytes.
- **Log-authoritative outcome** (`lifecycle.ts:237-263`): `epochStopReason` derives the stop reason
  from the child's own event suffix, never from the disposal path. Teardown failure overrides the
  outcome to `error` and **withholds output** (`lifecycle.ts:191-194`).

---

## 3. The build target (concrete, file-level)

### 3A. LLM caching — `llm/cache.py` (new) + edits to `llm/prompts.py`, `llm/translation.py`, `core/logging.py`, `graph/nodes/propose.py`, `graph/subgraphs/repair.py`

**3A-1. Split the prompt into a cacheable prefix + variable suffix (spec §19.5).**
Today `llm/prompts.py:18-21` puts `SYSTEM_PREFIX`, then a **single human message** containing
`model_context` + `source` + `reconciliation`. Only the tiny system prompt is a cacheable prefix;
the **large** model context is bundled into the changing message, so repair iterations re-send it
uncached. Fix:

- `TRANSLATION_PROMPT` becomes `[system(SYSTEM_PREFIX), system(model_context), human(source +
  reconciliation)]` — **the cached prefix ends at the model-context message**; the variable tail is
  source + reconciliation evidence.
- New `llm/cache.py`:
  - `PromptCachePlan(WorkbenchModel)`: `prefix_messages`, `suffix_messages`, `prefix_digest`
    (SHA-256 over `canonical_json` of the prefix message contents — deterministic; used for
    observability and as the DSH "anchored call header" equality key).
  - `build_cache_plan(*, system, model_context, source, reconciliation) -> PromptCachePlan` — **pure**.
    `model_context` stays `canonical_json(...)` so identical context ⇒ identical prefix across
    repair iterations.
  - `ModelCacheCapabilities(WorkbenchModel)`: `automatic_prefix_cache: bool`,
    `cache_control_format: Literal['none','anthropic']`, `cache_control_on_tools: bool`,
    `cache_retention: Literal['none','short','long']`. A small registry keyed by provider name
    (DeepSeek → `automatic_prefix_cache=True`; anthropic → `cache_control_format='anthropic'`).
  - `apply_cache_markers(plan, capabilities) -> list[message]`: delegates marker insertion to the
    transport, exactly like DSH — adds Anthropic-style `cache_control` on the **last prefix
    message** when the provider supports it; a no-op for automatic providers. Never computes
    breakpoints itself.
  - **Structural cache-reuse flag** (port DSH `SystemPromptUpdate`/`ToolUpdate='in-history'`): when
    the propose node re-invokes within one migration, the tools/system must be *appended*, not
    rewritten, so the cached prefix survives. Expose a `ToolUpdateMode` on the plan.

**Backward compatibility:** `test_llm_propose.py:11-20` asserts `prompt_payload(...)` yields keys
`source` / `model_context` / `reconciliation` with the expected substrings, and that `SYSTEM_PREFIX`
contains "you do not certify". Keep that contract — `prompt_payload` may keep its three keys; the
*message split* is what changes.

**3A-2. Align `TokenMeter` with DSH's disjoint buckets.**
`core/logging.py:118-237` already tracks `prompt_cache_hit_tokens` / `prompt_cache_miss_tokens` /
`completion_tokens` / `cost_units`, and `propose.py:52-61` already records `usage_metadata`. Do:

- Fix semantics to DSH's model: `input_tokens` is **uncached input only**; billed prompt input =
  `input + cacheRead + cacheWrite` (`usage-projection.ts:79`). Add `cache_read_tokens` and
  `cache_write_tokens` buckets. Preserve the existing public field names and `snapshot()` shape so
  current tests keep passing; document the mapping in a docstring: DeepSeek
  `prompt_cache_hit_tokens`→cacheRead, `prompt_cache_miss_tokens`→input(uncached),
  `cache_creation_input_tokens`→cacheWrite; OpenAI-style `prompt_tokens` must have any cached portion
  subtracted out (DSH does exactly this, `types.ts:168-189`).
- Port the validation discipline from `deriveTurnTokenUsage` (`turn-usage.ts:178-273`): add a pure
  `normalize_usage(usage) -> dict | None` returning **`None` on any contradiction, missing boundary,
  or non-safe-integer**, and only emitting optional buckets when every attempt reported them. Wire
  `record()` through it.
- Meter the **repair** path too: `graph/subgraphs/repair.py` `propose_patch` currently records
  nothing. Record + budget-check there as well (spec §19.4: log hit/miss across repair iterations for
  cost monitoring).

**3A-3. Port the retry policy + error taxonomy — `llm/retry.py` (new).**
`llm/translation.py:79-93` has a small status-code retry. Replace its internals with a reusable,
injectable policy (keep the translator's public behavior):

- `LlmErrorCode(str, Enum)`: `EMPTY_RESPONSE | INVALID_CREDENTIAL | CONTEXT_WINDOW_EXCEEDED | QUOTA |
  ACCOUNT_QUOTA | RATE_LIMIT | SERVER | TIMEOUT | TRANSPORT | AUTH | INVALID_REQUEST |
  MALFORMED_RESPONSE | UNSUPPORTED_CONTENT`. `LlmError(WorkbenchError)` carries `code`, `status`,
  `retry_after_ms`, `request_id`. Classifiers map provider exceptions → codes, with **regex as
  last-resort only** (`error.ts:83-103`); routing uses the code, never the message.
- `RetryPolicy(WorkbenchModel)`: `max_retries=5`, `initial_delay_ms=500`, `max_delay_ms=10_000`,
  `jitter_ratio=0.1`, `mode: Literal['normal','always']`, `retryable_codes`. Backoff
  `min(initial·2^min(retry-1,1024), max_delay)` × symmetric jitter `1−r+2r·rand()`. Honor provider
  `Retry-After` when present and ≤ max_delay. **Determinism:** the `sleep` callable and a `jitter`
  callable (`retry_count -> float in [0,1]`) are both injected — tests pass a fixed sequence.
- Map `LlmError` → `WorkbenchErrorCode` for the graph (e.g. `RATE_LIMIT`/`TIMEOUT`→
  `PROVIDER_UNAVAILABLE`, `CONTEXT_WINDOW_EXCEEDED`→`TRANSLATION_FAILED`).

### 3B. File editing — `services/patches.py` (new, pure) + `graph/subgraphs/repair.py`

**3B-1. A pure, content-addressed `str_replace`.** DSH edits a live path; our artifacts are
immutable. Per the research's reconciliation: make `str_replace` a **pure function
`(base_bytes, ops) -> output_bytes`** — editing produces a *new* content-addressed artifact, and the
"version CAS" becomes the **digest itself** (DSH's mtime token would break the deterministic-clock
invariant; content-addressing is strictly better and free).

- `services/patches.py`:
  - `PatchOp(WorkbenchModel)` discriminated union: `str_replace(old: str, new: str, replace_all:
    bool = False)` and `insert(after_line: int, content: str)`. (No `create`/`view` — artifact
    creation is `artifacts.create`; viewing is `artifacts.read_reference`.)
  - Add to `WorkbenchErrorCode` (`core/errors.py`): `PATCH_NOT_FOUND`, `PATCH_AMBIGUOUS`,
    `PATCH_EMPTY_OLD`, `PATCH_STALE_BASE`, `PATCH_INSERT_OUT_OF_RANGE`, `PATCH_NO_OPS`. Raise as
    `PatchError(WorkbenchError)` — typed codes so the graph/UI branch without parsing messages
    (DSH's `FsErrorCode` discipline).
  - `apply_patch(*, base: bytes, ops: Sequence[PatchOp]) -> bytes` — **pure, no I/O**. Port DSH's
    semantics verbatim: exact literal matching (enumerate all offsets; **no whitespace
    normalization**); `old` non-empty → else `PATCH_EMPTY_OLD`; **0 matches → `PATCH_NOT_FOUND`**;
    **>1 match → `PATCH_AMBIGUOUS` listing the 1-based line number of every occurrence** (DSH
    `lineNumbersAt` — this is the actionable hint the model needs); `replace_all` must be explicit,
    default False (repair requires uniqueness for reviewability); `insert.after_line` bounds-checked
    to `[0, line_count]` → else `PATCH_INSERT_OUT_OF_RANGE`.
  - Determinism is automatic: identical `(base, ops)` ⇒ identical output ⇒ identical digest.

**3B-2. Stale-base guard (DSH version-CAS, tightened to the digest).** DSH checks the observed
version *before* literal matching so a stale read yields `FS_STALE_VERSION`, not a misleading match
failure (`fs-local/src/index.ts:264-275`). Ours: a patch proposal carries `baseDigest`; the applying
node asserts `baseDigest == current candidate artifact digest` **before** matching → else
`PATCH_STALE_BASE`. This is DSH's "edit requires reading first", mapped to immutability, and it
closes DSH's windowed-read hole (freshness ≠ completeness): **the full digest is the edit authority**.

**3B-3. Wire into the repair subgraph (backward compatible).** Extend `graph/subgraphs/repair.py`
`propose_patch` so `repairer.propose_patch(...)` may return **either**:
- (existing) `{candidateSource, hypothesis, expectedEffect}` — full replacement, unchanged; or
- (new) `{baseDigest, patch: [PatchOp, …], hypothesis, expectedEffect}` — a structured, diff-able
  patch (spec §19.2's `apply_patch` marker format, as structured data).

The node applies the patch against the current candidate bytes, creates a **new** artifact (never
mutates; lineage via `parent_artifact`), and emits `PATCH_APPLIED` (`events.py:35`) with the **ops
recorded in the event data** — reviewable and diff-able. Downstream `guard_patch` → `make_guard_node`
still runs the full static/secret/repeat-digest guard on the result; `signature_seen` /
`near_oscillation` in `reconcile` are unchanged.

### 3C. Agent running — `services/delegation.py` + `graph/delegation.py` (new)

Port DSH's subagent runtime **as a bounded compiled-subgraph call**, not a free agent tree (the
research's reconciliation for a fixed `StateGraph`).

**3C-1. Descriptor (`services/delegation.py`).**
- `DELEGATION_DESCRIPTOR_VERSION = 1` (new lineage; DSH is at 3 — do not copy the number).
- `DelegationDescriptor(WorkbenchModel)`: `descriptor_id`, `label`, `mode: Literal["one-shot"]`,
  `parent_scope_id`, `scope_id`, `tool_filter: tuple[str, …]`, `max_depth: int = 1`, `depth: int`,
  `parent_delegation_id: str | None`, `created_at`. Frozen + canonical JSON.
- `snapshot_descriptor(...)` validates **lossless canonical JSON before any work** — port DSH's
  validate-and-freeze-first (`descriptor.ts:267-303`); round-trip through `canonical_json` and assert
  equality. The descriptor is **model-hidden, log-only** (`descriptor.ts:34-38`) — it never enters
  model history.
- Add `MigrationEventType.DELEGATION_STARTED = "delegation/started"` (and `DELEGATION_SETTLED`) to
  `core/events.py`. The descriptor is appended as a hash-chained event by a node, never by the LLM.
- Add `WorkbenchErrorCode`: `DELEGATION_DEPTH_EXCEEDED`, `DELEGATION_CAP_EXCEEDED`,
  `DELEGATION_DESCRIPTOR_INVALID`; `DelegationError`, `DelegationDepthError(DelegationError)`.

**3C-2. Bounded runtime (`graph/delegation.py`).** `run_delegation(descriptor, runtime, state)`:
- **Fail-loud preflight** (`index.ts:642-658`): every tool in `tool_filter` must exist in the
  runtime's tool registry; unknown → `DELEGATION_DESCRIPTOR_INVALID`. No silent degradation.
- **One-visibility tool allowlist** (`types.ts:186-192`): filter `make_translation_tools` to
  `tool_filter` — the excluded tools are removed from the model's bound tools **and** refused if
  invoked (wrap each allowed tool; a call to a non-allowlisted name raises).
- **Fixed, narrowed scope**: build a child scope via `scopes.create_child(...)` and assert
  `policy.assert_policy_no_wider(parent, child)` (`services/policy.py:69`) — DSH's delegation-scope
  statement. Delegated tools are read-only, so approval is pinned off (DSH approval `'never'`).
- **Monotone depth**: `depth = parent_depth + 1`, rejected loudly at `max_depth` (`depth.ts:28-36`);
  `max_active_delegations` cap (default 8) validated as a non-negative safe integer.
- **Settle, don't throw**: return `DelegationResult{stop_reason: Literal['completed','error',
  'max-tokens','aborted'], output: str | None, diagnostic: str}` — model-level failure resolves with
  `stop_reason='error'`, never raises (`types.ts:294-297`); `diagnostic` is redacted
  (`redact_value`) and capped at 4096 bytes.
- **The result is a proposal, never an outcome.** It must flow proposal → guard → reconciler → event
  log. Never write `DelegationResult.output` to the log or certify it directly — that would violate
  the five-step rule and the "LLM never writes the log unguarded" invariant.
- **Resume** = LangGraph `interrupt()` + `Command(resume)` on a stored thread (the checkpointer is
  our "cold resume"), with the descriptor as the resumable composition. **No `send_message`-style
  adjacent-agent steering** — it undermines deterministic edges; drop it.
- Keep it **opt-in and depth-capped** so existing graph tests are unaffected.

---

## 4. Tests to add (`backend/tests/`, pytest, `tmp_path` + `conftest.py` fixtures)

Mirror the existing style — deterministic, no network, injected clock/sleep.

1. `test_prompt_cache_plan.py` — prefix/suffix split; **prefix is byte-identical across iterations
   when only source/reconciliation change**; `prefix_digest` deterministic; capabilities registry;
   `apply_cache_markers` emits markers only for capable providers and is a no-op otherwise; tool
   update mode appends rather than rewrites.
2. `test_token_meter_buckets.py` (extend `test_token_budget.py`) — disjoint buckets; DeepSeek field
   mapping; OpenAI-style folded `prompt_tokens` has the cached portion subtracted;
   `normalize_usage` returns `None` on contradiction / non-safe-integer; optional buckets only when
   every attempt reports them; budget enforcement unchanged.
3. `test_llm_retry.py` — backoff math with an injected `jitter` sequence; `Retry-After` honored and
   capped; code classification (`RATE_LIMIT` from a 429 stub, `TIMEOUT` from `TimeoutError`);
   `max_retries` boundary; `always` vs `normal` modes; `LlmError` → `WorkbenchErrorCode` mapping.
4. `test_patches.py` — exact match; **`PATCH_AMBIGUOUS` carries the line number of every
   occurrence**; `PATCH_EMPTY_OLD`; `PATCH_INSERT_OUT_OF_RANGE` at both bounds; `PATCH_STALE_BASE`;
   multi-op sequencing; `replace_all` opt-in; **determinism: identical `(base, ops)` ⇒ identical
   digest**; no I/O (assert purity is testable without a workbench).
5. `test_delegation.py` — descriptor canonical round-trip; depth-cap rejection raises
   `DelegationDepthError`; active-cap rejection; **one-visibility** (allowlisted tool hidden from the
   model's schema *and* refused when called); child policy `assert_policy_no_wider` holds;
   **settle-don't-throw** on a failing model (returns `stop_reason='error'`, no raise); **output is a
   proposal — no event appended for the result itself**, only the descriptor event.
6. `test_repair.py` (extend) — a repairer returning a structured `{baseDigest, patch}` produces a new
   artifact whose digest differs from the parent, emits `PATCH_APPLIED` with the ops recorded, and
   still passes `guard_patch`; a stale `baseDigest` → `PATCH_STALE_BASE` and `HUMAN_REQUIRED`.

**Do not weaken or delete existing assertions.** Reconciliation/parity/policy-fingerprint tests are
port-verbatim per AGENTS.md.

---

## 5. Verification

- AGENTS.md gate: `cd backend && PYTHONPATH=src .venv/Scripts/python.exe -m pytest tests/ -q`, plus
  `ruff` and `mypy` (strict) per `backend/pyproject.toml`. Validation order is fixed: check → test → build.
- **Known environment gotcha (do not waste time):** this `.venv` was created by `uv`, and on this
  machine Windows denies list/read on `backend/.venv/Lib/site-packages/pytest` (verified: the ACL
  grants only owner/SYSTEM/Admins and the owner implicit rights don't cover ListDirectory). The
  result is that `import pytest` falls back to an **empty namespace package**, so
  `-m pytest` fails with `No module named pytest.__main__`. This is a *filesystem permissions*
  problem, not a code problem. If you hit it: **do not edit ACLs by hand** and do not "fix" it by
  deleting the venv. Try `python -c "import pytest; raise SystemExit(pytest.main(['tests','-q']))"`
  first; if that also imports the empty package, report the blocker and continue with pure-logic
  implementation — everything in §3B-1, §3A-1, §3A-2, §3C-1 is pure and unit-testable once pytest
  reads again.
- Confirm no regression in the graph end-to-end path (a full migration run still reaches
  `certify`/`HUMAN_REQUIRED` deterministically).

---

## 6. Do not

- Do not add a client-side LLM **response** cache (DSH has none; it breaks cache accounting and risks
  stale proposals past the reconciler).
- Do not let the LLM — or a delegation result — append to the event log or certify anything.
- Do not call `time.time()`/`uuid.uuid4()` in domain code; do not hard-code sleeps (inject).
- Do not introduce in-place mutation of artifacts; patches create new content-addressed objects.
- Do not restructure `migration_graph.py` topology, rename services, or change signatures the tests
  rely on. Additive only.
- Do not use poetry/uv/conda; do not add npm/yarn lockfiles; do not commit credentials.
- Do not treat DSH's `send_message`, idle continuable agents, or free parent→child recursion as
  targets — they conflict with a deterministic graph and are explicitly out.
