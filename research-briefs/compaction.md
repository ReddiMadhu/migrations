# Context Compaction — source-level brief (DSH @ 00102833, 2026-09-22)

Scope: `packages/compaction/{compaction, compaction-basic, compaction-tool-result-pruner, compaction-image-offload, command-compact}/src`, `packages/core/session/src/surface.ts`, `packages/llm/token-meter/src/*`, `docs/subsystems/compaction.md`, `.agents/notes/**`.

**Bottom line for a replayer/auditor: compaction never deletes anything. It is an append-only log plus a positional shadow-mask ("surface") that *hides* originals from the model. Every byte an auditor needs is still in the log, addressable by seq, with a record of exactly which seqs were shadowed and why.** The one thing it does destroy is *the model's own access path* to the originals — no built-in tool lets the agent read a shadowed span back (§7). That asymmetry is the whole audit story.

---

## 0. Leading findings

1. **The model-visible "replace" is a pure projection.** `surface.ts:576` `applySurfacePlan` does `state.nodes.splice(plan.startIdx, plan.endIdx - plan.startIdx + 1, plan.seq)` — it mutates a *position list*, not event data. Originals stay byte-identical in the log. Recoverability is structural, not a promise.
2. **A summary node has a HIGHER seq than what it shadows** (appended later) yet sits at an OLDER surface position, so `shadowedRange.start` can be `> end` (`compaction/src/types.ts:107-113`). `shadowedSeqs` (surface order) is authoritative — **never treat `shadowedRange` as a numeric interval.**
3. **The log enforces that a replacement cites every node it removes.** `surface.ts:367-369` `assertSourceEventReferences`: `"surface replace: sourceEventSeqs must include every shadowed surface node; missing ..."`. Compaction's replacement `user/message` carries `sourceEventSeqs: [startEvent.seq, summaryEvent.seq, ...shadowedSeqs]` (`compaction-basic/src/region.ts:506-509`). An auditor can walk the graph backwards from any checkpoint to its exact inputs.
4. **Pruning and image-offload are NOT compaction** — sibling surface rewriters. Pruning replaces one `tool/result`'s content with a head+marker+tail slice; image-offload *replaces nothing at all*, it only marks blocks. **This is the one real data-loss channel, documented not accidental:** pruning permanently discards a middle text span from the model-visible message; the original is still in the log (`sourceEventSeqs: [seq]`, `pruner/src/index.ts:165-171`), but the model can never see it.
5. **`maxTokens` default contradicts the docs.** Both Agent Notes say `maxTokens: 8192` (`2026-06-18-compaction-capability-seam.md:70`, `2026-07-10-...md:35`). **Code says otherwise**: `compaction-basic/src/config.ts:75-76` — `headroomTokens ?? 65_536; maxTokens ?? headroomTokens`. Default is **65536**. Doc drift; flag it.
6. **The summary must provably shrink or the transaction fails** (`region.ts:418-422` throws when `framedSummaryTokenCount >= prepared.shadowedRouteTokenCount`). This is the *only* quality gate, and it is a **token-price comparison, not a fidelity check**. Nothing verifies the summary is accurate.
7. **Compaction ships an independent invariant companion** re-deriving the whole bracket from the log (`compaction/src/invariant.ts:288-352`): owner/turn nesting, id matching, shadowed-span coverage against the live surface fold, "successful `compaction/end` requires one `compaction/summary`" (`:254-256`). A ready-made audit check to reuse.

---

## 1. Trigger conditions and token accounting; target count; who decides

**Who decides:** the *backend* (`BasicCompactionEngine`), not the seam — `compaction/src/index.ts:110-118`: "Implementations own trigger policy, retention, and summarization". The seam declares three abstract verbs and owns *zero* policy; `ctx.tokenMeter` is a separate singleton owning all measurement (`docs/subsystems/compaction.md:84`: "the seam owns no pricing API").

**Automatic trigger sites** (only when `auto: true`, default `config.ts:108`): `agent/pre-step` → `compactIfNeeded(agent, 'pressure', signal)` — `index.ts:158-176`, before request derivation, failures swallowed (`:172`: "step compaction failed: …; continuing the turn" — **a pressure failure never blocks the turn**); and `agent/request-error` → `compactIfNeeded(agent, 'context-overflow', signal)` — `:190-234`, gated on `CONTEXT_WINDOW_EXCEEDED_CODE`.

**Pressure arithmetic** (`config.ts:153-217` `resolveCompactSpec`): `thresholdTokens = floor(min(contextWindow × thresholdRatio, contextWindow − reservedCompletionTokens − headroomTokens))`; `retainTokens = floor((contextWindow − reservedCompletionTokens) × retainRatio)`, or absolute. Defaults: `thresholdRatio 0.8`, `retainRatio 0.16`, `headroomTokens 65536`, `maxTokens 65536` (code) vs `8192` (docs — **divergence**), `compactionRetries 1`, `maxOverflowRetries 1`, `auto true` (`config.ts:75-109`). `reservedCompletionTokens` = routed request's effective `maxTokens`, else adapter `defaultMaxTokens`, else 0 (`index.ts:65-68`).

**The estimator is a fixed heuristic, not a tokenizer:** `token-meter/src/estimate.ts:13` `CHARS_PER_TOKEN = 4`, `+ ROLE_OVERHEAD (4)` per message, `+ BLOCK_OVERHEAD (4)` per block; README concedes it "underprices CJK text and JSON Schema documents" (`compaction-basic/README.md:258`).

**There is no target count — a threshold plus a retain budget.** `selectCompactableRange` (`region.ts:117-155`) walks the priced surface **tail→head**, accumulating `pricedNodes[index].tokens` until `>= retainTokens`, then backs the cut to a tool-pairing-balanced boundary (`:143-147`). Range is `[first non-system node, cutoff)` — **the `system/message` at surface node 0 is never shadowed** (`:131`, `:494-513` `assertSystemHeadRewrite`).

**Overflow bypasses threshold and retention entirely** (`index.ts:294-302`): prune first, then `selectCompactableRange(session, measurement, 0)` — retain budget **0**, maximal head reduction. Retry is authorized **only if** `session.surface.replaceGeneration` increased (`:229-231`) — generation count, not a result object, is the proof of progress.

**Manual:** `compactNow` (`index.ts:383-435`) runs inside `agent.runMaintenance(...)` (idle-only), selects with `retainTokens: 0`, `owner: null`, `stability: 'selected-span'`, and flushes. `/compact` takes no arguments (`command-compact/src/index.ts:63-65`).

---

## 2. Model-based vs model-free strategies; which is default

| | Model-based | Model-free |
|---|---|---|
| Package | `dsh-compaction-basic` | `dsh-compaction-tool-result-pruner` |
| Op | summarize region → one `user/message` replace | rewrite one `tool/result`'s `content` |
| Cost | 1 extra LLM call (`summarizer.ts:162`) | zero model calls |
| Events | `compaction/start`+`compaction/summary`+`user/message`+`compaction/end` | `compaction/prune`+`tool/result` |
| Selects by | tokens (route-priced) | Unicode **code points** (`index.ts:68-74`) |

**Order is fixed and deliberate** (`index.ts:321-327`): *prune first, remeasure, then decide whether a summary is needed at all* — "Pruning is optional so compaction-basic remains independently composable" (`:289-291`).

**Which is default?** Both are on by default in the shipped composition (`compaction-basic/README.md:38-44`; `examples/tui-agent/cordis.yml`), but they are *independently mountable*: `this.ctx.get('toolResultPruner')` returns `undefined` if unmounted (`index.ts:292`); `auto: false` disables both automatic listeners. **Model-free is the first line of defense and can satisfy pressure alone** — if pruning drops `totalTokens < thresholdTokens`, `compactIfNeeded` returns `null` with no LLM call (`index.ts:323-327`); the model-based summary is the fallback.

Pruner budgets: `thresholdChars 8192`, `headChars 4096`, `tailChars 1024`; marker `'\n\n[... tool result middle pruned ...]\n\n'` (`pruner/src/config.ts:7-14`). Slicing is by Unicode code point (surrogate pairs survive; **grapheme clusters may still split** — `docs/subsystems/compaction.md:227-229`).

---

## 3. What gets replaced; log representation; recoverability; the bracket lock

### 3.1 What is replaced

`compactSurfaceRegion` (`region.ts:173-276`) replaces **surface nodes only**, never event data. It appends `compaction/summary` (`compactionId, summary, rawOutput?, llmStreamCall?, shadowedRange, shadowedSeqs, shadowedTokenCount, provider, model, maxTokens?, usage?`), then the replacement `user/message` with `surfaceOp: { op: 'replace', startSeq, endSeq }` and `sourceEventSeqs: [startEvent.seq, summaryEvent.seq, ...shadowedSeqs]` (`region.ts:491-509`).

`SurfaceEventType` is closed to message-producing types, so a `compaction/*` event **cannot** carry `surfaceOp` (`2026-06-18-compaction-capability-seam.md:13`). Hence the summary rides on a `user/message` with `source: { kind: 'compact-checkpoint', compactionId, sourceCommandId? }` (`checkpoint.ts:33-42`).

### 3.2 Event sequence (order is contractual)

`compaction/start` (log-only, lock) → `[summarize]` → `compaction/summary` (log-only, raw summary + envelope) → `user/message` (**the only surface mutation**) → `compaction/end` (log-only, lock released, carries `error?`) — `region.ts:210-239`.

The `compaction/summary` → replacement adjacency is "contractual" (`types.ts:28-32`): `compaction/summary.shadowedTokenCount` *is* the replacement's shadow price, so a consumer pairs a replacement with the metering event directly before it. `compaction/prune` shares the protocol (`types.ts:78-80`: "The replacement MUST be appended synchronously right after this event").

### 3.3 Is the original history still recoverable? — YES, structurally

- **The log is append-only.** `foldSurface` (`surface.ts:600-614`) replays from seq 0 and returns both `nodes` (current surface) and `replacements` (full replacement history).
- `surface.ts:82-85` — **the key quote**: "The model-visible surface deliberately shadows replaced ranges, so it is the wrong source for a human transcript — a landed replacement would erase conversation the user already saw. Append-origin events are that transcript's durable source material; replacement copies stay model-only."
- **Real consumers already do this.** `session-query/src/tracing.ts:199-205` builds `replacedBy`/`replacedEventSeqs` from `folded.replacements`; `documents.ts:71-73` classifies every event `'current' | 'shadowed' | 'log-only'`; `api/session-controller/src/history.ts:412` filters `!isAppendSurfaceEvent(event)` for the human journal. **An auditor has off-the-shelf access to the full pre-compaction transcript.**

### 3.4 The bracket lock

`compaction/start` is appended **synchronously and first**, before any async work (`region.ts:183-210`). The lock is *log-recorded*, not a process mutex; entry state is scanned backwards over the log (`region.ts:566-599`).

Staleness rule (`region.ts:308-320`): an unmatched start **before** a newer `session/end-seed` is stale evidence from a prior lifecycle and ignored; **after** it → `ManualCompactionError('busy', ...)`.

**The lock is a time point, not an exclusive container.** Idle-injected `user/message`s may land between a manual start and end; the manual path revalidates only its selected span (`region.ts:447-468` `assertSelectedSpanStable`), so injected context survives. Automatic compaction instead demands **whole-surface** stability (`:431-440`).

---

## 4. `compaction-tool-result-pruner` and `compaction-image-offload`

### 4.1 Pruner — removes a text middle, leaves everything else

`pruneContent` (`pruner/src/index.ts:83-122`): when `totalChars > thresholdChars`, keep `[0, headChars)` + marker + `[totalChars − tailChars, end)`. **Non-text blocks pass through untouched and in order** (`:94-97`), so images and structured blocks inside the same result survive.

`pruneSession` (`:136-182`), per over-budget `tool/result` **current surface node**: appends `compaction/prune` carrying the **heuristic** price (`shadowedTokenCount: estimateMessage(original)`, so pure projection folds stay self-consistent — `types.ts:87`, `region.ts:376-382`); then appends a new `tool/result` with `{ ...event.data, message }` — **complete event data except `content`** — with `surfaceOp: replace` over that one node. `assertToolResultRewrite` (`surface.ts:462-489`) enforces "may change only content" by deep-equaling everything else.

**Leaves:** the tool `callId`, surface position, non-text blocks, and the original in the log. **Removes permanently from the model:** the middle text span. Replacements committed earlier in the pass are durable even if a later one throws (`:133-134`).

### 4.2 Image-offload — removes nothing; marks occurrences

The subtle one. `image/offload` is a **message-projection event**, not a replace: **no `surfaceOp`, no `sourceEventSeqs`** (`projection.ts:16-27`).

`offloadOldestImages` (`image-offload.ts:16-46`) walks `sourceEventSeqs` in request order, skips assistant nodes and already-`offloaded` images, and records `{ seq, imageIndexes }` — **depth-first indexes counting every image including already-offloaded ones**.

`imageOffloadProjection.project` (`projection.ts:40-73`) is a *pure function* returning a `Map<SessionSeq, Message>` overriding the derived message for those seqs; `offloadMessageImages` (`project-message.ts:13-35`) builds a **new immutable message with the same `id`** — "an immutable message with the same identity and selected images marked" — setting `offloaded: true` on chosen blocks.

**Message nodes and identities remain unchanged** (`projection.ts:22`). The model sees placeholder text `offloadedImageText(...)` (`llm/src/content.ts:108-117`): `"image omitted to fit request image limits; <identity>. <access path>"`. **The original image block is fully intact in the log**; the projection is recomputed on every replay — the cleanest of the three mechanisms for audit, *reversible in principle*, though no code ever un-marks.

**Registration is load-bearing:** unloading a used definition blocks further message derivation (`surface.ts:704-716`); a `project` plan bumps `contentGeneration` **without** touching `replaceGeneration` (`:579-582`) — **image offload does NOT count as a surface-replacement generation advance** and cannot by itself authorize an overflow retry.

**Triggers:** `agent/request-error` with `IMAGE_OFFLOAD_REQUIRED` + `offloadImages`, and `compaction/summary-error` for summary retries (`image-offload/src/index.ts:27-40`). The retry "spends no provider retry budget and logs no `llm/retry` event" (README:42). **Selections survive a later summary failure or cancellation** (README:54).

---

## 5. Failure handling and resume behavior

**The summary-error waterfall.** `summarizeCompaction` (`region.ts:387-409`) loops: on throw, if aborted → rethrow; else re-check stability, then `ctx.waterfall('compaction/summary-error', ...)` (`index.ts:442-448`). A recovery listener must **synchronously record a durable input change** and return `true`; the backend then **re-derives and re-prices** the region (`region.ts:406-407`) before retrying. `image-offload` is the shipped listener; `false` → original error thrown.

**If the model refuses / produces garbage** — there is no "refusal" detection. Concrete modes: **no text output** → `summarizer.ts:168-170` throws `'summarization produced no text summary content'`; **image output** → `summaryText` (`:216-218`) throws `LlmError(..., 'UNSUPPORTED_CONTENT')` — **fails loudly rather than silently dropping**; **truncated at maxTokens** → `finishError` (`:196-210`) throws `code: 'MAX_TOKENS'` (hidden reasoning tokens can consume the cap); **tool calls in output** → filtered out (only `type: 'text'` retained, `:219`); **doesn't shrink** → `region.ts:418-422`. All five are `summary`-class failures on the manual path (`region.ts:279-299`); on the automatic path they are swallowed with a warning, leaving the full over-budget history visible.

**Crash between start and end — the designed-for case.** Releasing the lock *last* converts a mid-summarization crash from silent corruption into a **detectable orphan**: `compaction/start` with no matching `compaction/end` (`docs/subsystems/compaction.md:19`, `2026-06-18-...md:96`). Resume/fork are not wedged — a later `session/end-seed` proves the orphan stale.

**Exactly one close attempt.** `region.ts:240-251`: any failure after start → one `session.append('compaction/end', { ...lifecycle, error: errorChain(error) })`; if *that* append throws, the orphan start **deliberately remains blocking** (`:247-249`). **The surface is untouched on summary failure** — the replacement was never appended; `changed`/`summary` "close and persist the failed attempt without a summary replacement" (`docs/subsystems/compaction.md:99`). **Manual taxonomy** (`region.ts:279-299`): `commit` at commit stage, `changed` on `SurfaceChangedError`, else `summary`; cancellation beats everything (`index.ts:416-423` → `ManualCompactionError('cancelled')`, preserving the exact abort reason).

**Overflow-resume subtlety** (`index.ts:204-227`): if pruning landed but later summary work threw, recovery **still retries** — "A model-free prune can land before later summary work fails. That durable reduction is sufficient retry proof; do not discard it just because the optional second phase threw. Cancellation still wins." (`:208-211`). **`persistence`** = bracket closed but `sessions.flush` failed (`region.ts:253-272`).

---

## 6. What the model sees after compaction; message-id/parent continuity

`deriveMessages()` yields `[checkpoint_as_user_message, ...retained]`. Checkpoint content (`summarizer.ts:187-193` `frameSummary`): CHECKPOINT_PREAMBLE text + raw summary blocks + closing tag, wrapped in `<compacted-summary>` … `</compacted-summary>` (`:22-23`). Preamble (`:70-71`): "Treat the captured context as established background and build on it without restating it. Continue the task directly from the messages that follow, without acknowledging this checkpoint."

**Summarizer instruction** (`:32-67`): a fixed 8-section Markdown template (Primary Request and Intent / Key Technical Concepts / Files and Code / Errors and Fixes / Pending Jobs / Current Work / Next Step / Critical Context). Rules: "Preserve exact file paths, commands, error strings, identifiers, numeric values, function signatures, and syntax fragments" and — for incremental merges — "If the conversation already contains a `<compacted-summary>` block, it is a PRIOR checkpoint. Do not copy it forward verbatim: preserve still-true facts, drop stale ones, and merge newer information."

**Head-anchoring:** auto-compaction always starts at the surface head, so **exactly one automatic checkpoint exists**; a new pass merges the prior checkpoint into itself (`2026-06-18-...md:64-66`). Manual mid-range compaction may leave several.

**Message identity:** the checkpoint is created by `createUserMessage` (`region.ts:410-413`) → a **fresh `MessageId`** (`llm/src/message.ts:246-253`) — a *new* message, not a renamed old one. **Parent continuity is by `compactionId`, not message id**: `isCompactCheckpointSource(source)` (`checkpoint.ts:49-51`) tests `source.kind === 'compact-checkpoint'` and exposes `compactionId` + optional `sourceCommandId`. The invariant proves the checkpoint's id matches its open `compaction/start` (`invariant.ts:98-113`); `context/session-reference/src/projection.ts:43` tags projected items `checkpoint: true`.

**KV cache:** each checkpoint "invalidates reuse from the first replaced history token; the unchanged request prefix before that range remains reusable" (`README:181`). The *summarizer* call instead reuses the warm prefix — it replays the real system prompt, tools, and shadowed messages verbatim, only the trailing instruction novel (`summarizer.ts:144-162`). The call sets `GenerateOptions.purpose: 'compaction'`.

**What the model does NOT see:** `compaction/*` events (log-only), the summarizer's own request/reasoning ("only returned text is stored", README:187), or that compaction happened at all.

---

## 7. CRITICAL: does compaction destroy auditor/replayer-needed information?

**For a log-holding auditor/replayer: NO. This is a hard, checkable structural property, not a gentleman's agreement.**

Governing guarantee — `2026-07-05-reconstructable-requests.md:17`:

> **"Model-visible ⟺ durably referenced."** Anything that reaches a model request must be reconstructable from the session log and the immutable content-addressed objects it references.

Consequence for compaction — `surface.ts:82-85`:

> "The model-visible surface deliberately shadows replaced ranges, so it is the wrong source for a human transcript … Append-origin events are that transcript's durable source material; replacement copies stay model-only."

Verified supporting facts: append-only log with `foldSurface` reproducing surface + full replacement history (`surface.ts:600-614`); replacements must cite every shadowed node (`surface.ts:367-369`), re-validated by format migrations (`session-format-v0-to-v1/src/payload-validation.ts:835-837`); `session-query` exposes `replacedBy`/`replacedEventSeqs`; the invariant companion re-derives the bracket and fails loudly (`invariant.ts:288-352`). One caveat: the note **excludes** compaction's own one-shot summarize call from the loop-request invariant ("outside the invariant because only the loop marks request ownership", `2026-07-05-...md:17`), compensating by logging `compaction/summary.{provider, model, maxTokens, usage?, rawOutput?, llmStreamCall?}` so the call is "reconstructable from log + code".

**Where information IS destroyed — three channels, all model-side, all logged:**

1. **The shadowed span's semantic content is replaced by a summary.** Originals are in the log, but the *model* can never read them — **the absence of a guarantee.** `2026-07-06-recallable-compaction.md` (status: **proposed**) states it (`:9`): "Compaction is irreversible from the model's current context. The summary the model sees carries no reference to what it shadows … Whatever the summarizer drops is unavailable to the model, even though the append-only log holds every byte." I verified it is **unimplemented**: no `dsh-tool-recall` package exists and no `history_read`/`history_search` symbol appears anywhere in `packages/compaction`.
2. **The pruner's removed middle text** is gone from the model-visible message forever, though present in the log.
3. **Image-offload marks are permanent and one-directional** — README:94: "Offloaded images never return automatically … recovery is the read-only path in the placeholder"; also README:96: "A temporary small budget offloads permanently."

**No fidelity guarantee exists.** The only quality gate is `framedSummaryTokenCount < shadowedRouteTokenCount` (`region.ts:418-422`) — a *size* check. **An auditor must not treat a checkpoint as evidence of what was said**; it is evidence that a specific model (logged) produced a specific text (logged) over a specific input (logged by `shadowedSeqs`).

**Replayer consequences:**
- **Model-visible replay ≠ log replay.** "What the model saw at step N" needs the surface fold *up to N*, including all projections (image-offload); "what happened" uses `isAppendSurfaceEvent` only.
- **Projections must be registered** or the fold throws (`surface.ts:714`, `:536`). Offline replayers must supply `imageOffloadProjection` — exported browser-safe from `compaction-image-offload/src/projection.ts` for exactly this purpose; the installed format catalog assembles it for offline readers (README:52).
- **Never use `shadowedRange` as a numeric interval** — use `shadowedSeqs`. Compaction timing is **nondeterministic** (heuristic token counts + LLM output), so byte-exact model-request replay across a boundary additionally requires the summarizer's output — which *is* logged on `compaction/summary`.

---

## Guarantees vs Conventions

| Property | Status | Evidence |
|---|---|---|
| Shadowed events remain in the log, byte-identical | **GUARANTEED** (structural) | `surface.ts:576` splice on position list only |
| Replacement cites every shadowed node | **GUARANTEED + ENFORCED** | `surface.ts:367-369`; migrations re-validate |
| Shadowed span recoverable offline | **GUARANTEED** | `foldSurface` returns `replacements`; `session-query` builds `replacedBy` |
| Lock correctness / orphan detectability | **GUARANTEED + INDEPENDENTLY CHECKED** | `region.ts:210-251`; `invariant.ts:288-352` |
| Model-visible ⟺ durably referenced | **GUARANTEED** (loop requests) | `2026-07-05-reconstructable-requests.md:17` |
| Compaction's summarize call reconstructable | **CONVENTION** (logged, not invariant-covered) | `types.ts:43-53`; excluded at `2026-07-05-...md:17` |
| Summary smaller than what it shadows | **GUARANTEED** (size only) | `region.ts:418-422` |
| Summary semantically faithful | **NOT GUARANTEED — absence is explicit** | no check exists |
| Model can read shadowed history back | **NOT PROVIDED** (proposed, unimplemented) | no `history_read`/`history_search` in tree |
| One auto checkpoint, always at head | **CONVENTION** (backend policy) | `2026-06-18-...md:64-66`; manual can leave several |
| Image-offload marks reversible | **NOT PROVIDED — permanent by design** | README:94 |
| Pruned middle text recoverable by the model | **NOT PROVIDED** | `pruner/src/index.ts:83-122` |
| `maxTokens` default 8192 | **DOC DRIFT — actual default 65536** | `config.ts:75-76` vs both Agent Notes |
| `compaction/summary` immediately precedes replacement | **CONTRACTUAL** | `types.ts:28-32` |

## Implications for a regulated SAS→PySpark migration verification platform

1. **Treat the session log as the system of record, never the model-visible surface** — the surface is a lossy projection by construction. Route audit queries through `foldSurface` + `isAppendSurfaceEvent` (or `session-query`, which already does it) for the *complete* pre-compaction transcript: every tool result, argument, and error string the model ever saw.
2. **Compaction is audit-positive, not audit-hostile.** It preserves originals *and* writes rich provenance per transaction: `compactionId`, the summarizing `provider`/`model`/`maxTokens`/`usage`, raw output, and exact shadowed seqs — you can prove which model condensed what, when.
3. **Never certify a migration-critical decision from a `<compacted-summary>` checkpoint alone** — a size-validated but semantically unvalidated LLM output. If an artifact's rationale appears only inside a checkpoint, re-derive it from `shadowedSeqs`.
4. **Build the recall path yourself if you need it.** The `2026-07-06-recallable-compaction.md` design (paginated reads over `compaction/summary.shadowedRange` + `shadowedSeqs`) is specified but unimplemented; its core insight — "the session log stores the content … and the tools read both" — is implementable today against existing logged fields, with no new storage.
5. **Replay must pin projection definitions.** Offline byte-exact replay of any image-bearing session requires `imageOffloadProjection`, else the fold throws (`surface.ts:536`, `:714`). Ship it with your replay harness.
6. **Token accounting is a 4-chars-per-token heuristic.** Do not reason about "the model was at 79.8% of context" with regulatory precision; treat thresholds as policy triggers, not measurements.
7. **Failure modes are benign and observable** — a crash mid-compaction leaves a detectable orphan and unchanged surface; a failed summary leaves full history visible. The real risk is the opposite asymmetry: a *successful* compaction permanently narrows the model's view, by design.
