# Subagents, Skills, and the Autonomy Primitives — Source-Level Brief

Target: `C:\Users\madhu\deepseek-harness` @ `00102833` (master, 2026-09-22). All claims verified against current source unless marked UNVERIFIED. File sizes differ from the assignment's stated line counts (e.g. `packages/subagent/subagent/src/index.ts` is 663 lines, not 634; `packages/skill/skill/src/index.ts` is 868, not 797) — current code is larger than the assignment assumed.

## Most important verified findings

1. **A subagent inherits the parent's permission posture, but approvals are pinned `'never'`** — the child can never ask for escalation. `captureDelegatedPolicyOverrides` sets `approvalPolicy: 'never'` *whenever the approval capability is composed*, regardless of the parent's own policy (`packages/subagent/subagent/src/child-agent.ts:246-253`). The model is told: "your permission scope was fixed when you were started and cannot be widened" (`child-agent.ts:172-176`).
2. **A background subagent's transcript, tool output, and reasoning are NOT delivered to the parent.** The child is told it must `send_message` its result: "The parent shares your workspace but does not automatically receive your transcript, tool output, or reasoning" (`packages/subagent/subagent/src/continuation-messages.ts:81-97`). Settlement is a one-line runtime-authored notice whose `MessageSource` is deliberately a distinct kind (`subagent-settled`) from an agent-authored message, so a transcript "would credit the child with words it never wrote" (`continuation-messages.ts:14-38`, `135-159`).
3. **Out-of-process subagents advertise zero start capabilities** — `agentOptions`, `outputSchema`, `maxDepth`, `toolFilter`, `persona` all `false`; the service rejects such a request *before* `start`, never accepts-then-ignores (`packages/subagent/subagent/src/out-of-process.ts:51-63`; enforced at `packages/subagent/subagent/src/index.ts:644-660`).
4. **`maxActiveSubagents` is a hard admission ceiling, default 8**, shared across a parent and *all* its continuable descendants; exceeding it throws `ACTIVATION_LIMIT_REACHED` rather than queuing (`packages/subagent/subagent/src/index.ts:194-203`; `continuation-activation.ts:41-57`, `:489`).
5. **The workflow VM is explicitly not a security boundary.** File enforcement, heap limits, and process cleanup belong to the PTC runtime; the counters are "not host-enforced security quotas" (`.agents/notes/implemented/architecture/2026-09-13-workflow-ptc-sandbox-reuse.md`; `workflow-ptc/src/index.ts:31-43`).
6. **Skills are prompt-only.** `SkillDefinition.content` is a string; no skill package registers tools or mutates policy (`packages/skill/tool-skill/src/index.ts:122`). A `/name` gesture only counts when the message source is `user` (`tool-skill/src/index.ts:177-204`).
7. **Goal tools reject subagent authority outright** — "Execution rejects non-human and subagent authority" (`packages/goal/tool-goal/src/index.ts:51-55`); `requireDirectHuman` demands a `user`-sourced message on a root agent (`packages/goal/tool-goal/src/authority.ts:100-103`).

## 1. Spawning models and the parent–child relationship

Three transports, one seam `ctx.subagents` — a named-provider registry (`packages/subagent/subagent/src/index.ts:31-77`).

**spawn (in-process, fresh)** — `SpawnInProcessProvider` builds a fresh child `Agent` on the same cordis context: own session, own system prompt, `inheritsParentContext = false`; `start` delegates to `startInProcessRun(request, {})` with no seed (`packages/subagent/subagent-spawn-in-process/src/index.ts:41-59`). Cheapest transport; reuses the agent factory's quiescent teardown.

**fork (in-process, seeded)** — `completedTurnPrefix` slices the parent's log through the last `turn/end`; the in-flight unbalanced turn is excluded, and since seq === array index the slice stays a valid seed from seq 0 (`packages/subagent/subagent-fork-in-process/src/index.ts:48-55`). The seed is captured **once at creation** for continuable children, so "a later cold resume replays that prefix instead of re-forking the parent's newer history" (`:85-91`). `inheritsParentContext = true` (`:72`).

**out-of-process / external** — `subagent-acp`, `-claude-code`, `-codex`, `-dsh-sdk` compose `NO_START_CAPABILITIES` with their own wire drivers; process machinery (spawn, env scrub, managed-range teardown) belongs to the `dsh-subprocess` seam (`out-of-process.ts:1-12`). Child cwd resolves from config override, else the parent session's workspace (`out-of-process.ts:78-86`, probing `X_OK`). Provider diagnostics are capped at 4096 UTF-8 bytes, split-safe (`out-of-process.ts:19-42`).

**Child context** — one shared home for one-shot and continuable paths: `childSessionMeta` stamps `cwd`, the parent's *live-scope* `agentPreset`, `parentSession`, `isSeeded`, `origin: 'subagent'`, and durable `delegationDepth` (`child-agent.ts:139-157`). `resolveChildDepth` is parent+1 with "the persisted parent header the monotone floor, so a resumed parent cannot delegate as if it were top-level" (`child-agent.ts:50-59`; `depth.ts:28-36`).

**Event visibility** — `subagent/start` + `subagent/end` are observe-only, paired by runId; invariant companions verify pairing and identity (`lifecycle.ts:45-75`; `invariant.ts:44-83`). The parent catalog is *parent-owned and durable*: `establishCatalogChild` appends `subagent/catalog` to the **parent's** session and the projection discards events below `inheritedEventCount`, so "fork-inherited catalog facts are excluded" (`catalog.ts:130-164`; `docs/subsystems/subagent.md:11`). Direct children are discovered from the parent catalog, descendants from the Session corpus (`index.ts:74`, `list-children.ts`). A child's own composition (persona, tool filter, delegation statement) lives in the child's scope and is "therefore invisible to its parent and siblings" (`child-agent.ts:179-219`).

## 2. What is inherited — and what is deliberately not

**Inherited (in-process, snapshot at delegation):**
- **Model route**: provider, model, reasoningEffort, maxTokens from the parent, with the *latest request header* winning over creation options (`parentAgentOptionsForDelegation`, `child-agent.ts:69-86`); per-request overrides win (`resolveChildAgentOptions`, `:99-120`). Changing route without naming an effort clears the parent's effort so the new model resolves its own default (`:117-118`).
- **Composition**: `applyChildComposition` first joins the parent's preset via `composeFrom` — "a child composed without the join … sees an empty tool registry and none of its parent's prompt sections" — then installs the delegation-context statement, a shadowing persona, and a per-child `tools.restrict()` (`child-agent.ts:200-219`).
- **Permission posture**: `permission/preset` only when the parent currently runs `auto` or `danger-full-access` (the shared bundle); the parent's *explicit* sandbox override only, "never deployment defaults or one-shot grants"; approval pinned `'never'` (`child-agent.ts:246-253`). These land as `source: 'delegation'` events **after** any fork seed, so fresh policy beats stale seed state (`appendDelegatedPolicyOverrides`, `:264-277`; `.agents/notes/implemented/feature/2026-07-25-subagent-policy-inheritance.md`).
- **Depth budget**: parent depth + 1; `maxDepth` requests throw `SubagentDepthError` (`child-agent.ts:50-59`). Runtime options may only *deepen* (`depth.ts:28-36`).

**Deliberately NOT inherited:**
- The parent's conversation — spawn children get nothing; fork children only the completed-turn prefix, never the in-flight turn (`fork-in-process/src/index.ts:48-55`).
- Deployment defaults — "defaults remain operator-owned and may change; an unswitched parent stamps nothing" (policy-inheritance note).
- Live policy changes — snapshot semantics: "a running child does not follow later parent switches" (same note).
- Approvals — pinned `'never'`; the note's original approval-override inheritance "is superseded" by `.agents/notes/implemented/feature/2026-08-10-subagent-approval-pinned-never.md`.
- Ambient credentials for out-of-process children: the subprocess seam scrubs the parent env; an explicit `env` entry (e.g. the child's own `DEEPSEEK_API_KEY`) is an opt-in merged *after* the scrub (`subagent-acp/src/index.ts:49-54`, `run.ts:44-52`, `:345-354`; `subagent-claude-code/src/run.ts:329`). UNVERIFIED: the scrub pattern list lives in `dsh-subprocess`, unread here.

## 3. Result delivery, cancellation, limits, parallelism, isolation

**Delivery (one-shot)** — `startInProcessRun` drives exactly one turn; output is `finalAssistantOutput(own)` and stop reason maps `TurnEndReason` to the seam vocabulary, with a `blocked` pre-step rejection becoming `refusal`, so "the caller must not read the run as done" (`subagent-in-process-driver/src/index.ts:50-67`, `:222-237`). Foreground tool calls always `dispose()` after collection (`tool-subagent/src/index.ts:4`, `:226-231`). Background one-shots own a `ctx.jobs` Task; `settleRun` maps the result to a `JobOutcome` — `aborted` without a diagnostic is `killed`, everything else non-completed is `failed` with no partial output (`run-settlement.ts:37-75`).

**Delivery (continuable)** — no Task, no per-message result. The manager-owned settlement notice is delivered to the parent before ownership release, carrying only the child's text closing blocks ("Parent providers receive this notice as a user message and may reject nontext assistant blocks") (`continuation-messages.ts:135-159`; `continuation-activation.ts:860-888`).

**Cancellation** — `interrupt` requires either a `user` authority matching the child's recorded `parentSession`, or an exact *live* ancestor present in the activation's `ancestry` `WeakSet`; an agent cannot interrupt itself; a closing activation is a no-op; cancel uses `keepInbox: true` so queued messages survive (`continuation-activation.ts:283-326`). The model-facing tool states only the current turn stops — "messages already queued … stay parked until a later send_message, agents it started keep running" (`tool-subagent-control/src/index.ts:76-115`). Workflows abort the PTC process and the signal shared by pending and active children, then await child disposal including a child that publishes after cancellation (`host.ts:293-303`; PTC note).

**Limits** — delegation depth (`child-agent.ts:32-59`); `maxActiveSubagents` (default 8, volatile) as an admission ceiling shared through uninterrupted parent links (`index.ts:194-203`; `continuation-activation.ts:41-57`); out-of-process diagnostics capped at 4 KiB (`out-of-process.ts:19-22`); workflow caps: `maxConcurrentAgents` auto-resolves to `min(16, max(1, cores-2))`, `maxTotalAgents` 1000, `maxItemsPerCall` 4096, 5 s sync-slice timeout, elapsed time *explicitly disabled* (`workflow-ptc/src/index.ts:31-43`). Jobs: bounded output ring — appends past the live cap drop oldest bytes and a below-window reader gets a lossy read, never an error (`jobs/src/index.ts:76-83`).

**Parallelism / isolation** — activations run concurrently up to the slot ceiling; *delivery* per child is serialized ("Run `operation` after every previously queued operation for `childId`", `continuation-activation.ts:156`), and "the Agent inbox is the only turn queue" (`continuation.ts:6-11`). Workflows expose `parallel()` (barrier; a throwing thunk → `null`) and `pipeline()` (per-item stage chains, explicitly "NO cross-stage barrier") (`workflow-ptc/src/runtime.ts:86-90`, `:284-350`). Session isolation is a stated invariant — live-resolution walking `parentSession` was rejected precisely because it "breaks the 'two sessions never see each other's state' isolation invariant" (policy-inheritance note). Lineage authorization requires the exact live agent object, not a matching id (`authorizeLineage`, `continuation-activation.ts:463-477`). Job ids are predictable, so "authorization — not secrecy — is the boundary" (`jobs/src/index.ts:59-60`).

## 4. Skills: registration, disclosure, caching, policy reach

**Registration** — providers implement `SkillProvider` and register on `ctx.skills`; `skill-filesystem` discovers directory bundles and flat Markdown with YAML frontmatter from ranked roots: `project-dsh` 100, `project-agents` 200, `custom` 300, `user-dsh` 400, `user-agents` 500 (`skill-filesystem/src/index.ts:36-40`), with `runtime` at 250 and bundled at 600 (`skill/src/index.ts:24-28`). "Lower ranks win duplicate skill names before provider registration order" (`:77-84`). Names are kebab-case (`:21`). Roots are watched (chokidar, 200 ms stability threshold, max 128 projects) so the catalog can change mid-session (`skill-filesystem/src/index.ts:41-43`, `:61-73`).

**Progressive disclosure** — each pre-step (only when this plugin's exact `skill` tool is visible to that agent: `ctx.tools.get(skillTool.name, agent) === skillTool`, `tool-skill/src/index.ts:220`) publishes a `<system-reminder>` catalog of name + truncated description (default 500 chars, `:27`, `:50-58`) that explicitly says: "This catalog contains summaries only; do not infer or follow a skill's instructions until it has been loaded" (`:266`). Full bodies load only on the `skill` tool call, or on a user `/name` gesture. Republishing is keyed to a sha256 digest over the entry list — "the surrounding `<system-reminder>` framing is written for the model and must not decide whether a republish is needed" (`:328-334`).

**Policy re-check on load** — the tool checks `isModelInvocable` on the *summary* before loading and again on the *loaded definition*, so a provider whose catalog entry and body disagree cannot smuggle in a disabled skill (`tool-skill/src/index.ts:127-147`; test `tool-skill/tests/tool-skill.spec.ts:929`). The `invocation` frontmatter policy resolves `modelInvocable` / `userInvocable` independently (`skill/src/index.ts:48-54`, `:786-792`); a `disable-model-invocation` skill is reachable only through the `/name` gesture, never via catalog or tool (`tool-skill/src/index.ts:175-176`).

**Caching** — a per-scope collect cache (default 128 entries; max 2 collection attempts) keyed by cwd + scope chain + revision (`skill/src/index.ts:22-23`, `:527-545`, `:643`). Incomplete observations are never cached so consumers retain last-good state (`:476`); any provider registration change invalidates the whole cache (`:621-630`).

**Context cost** — the catalog rides every step; a loaded body enters as a tool result (pruned above 8192 chars by the standard preset's pruner — hence the creator-skills restructure keeping `SKILL.md` under the head, `.agents/notes/implemented/architecture/2026-09-21-creator-skills-progressive-disclosure.md`). First load ≈3.9 KB, recipes 1–3 KB more.

**Policy reach — none, structurally.** Skills are markdown text; they register no tools and hold no hooks, influencing the model only through injected instructions. The nearest things to "skill-provided tools" are separate deployment-owned tool plugins (`tool-workspace-dependencies`, `tool-skill`), not skill-authored ones.

## 5. Orchestration primitives and persistence vs the session log

| Primitive | Service | Persistence |
|---|---|---|
| `subagent` one-shot / continuable | `ctx.subagents` | Child session log (own session); `subagent/catalog` on parent log; `subagent/descriptor`, `sandbox/mode`, `approval/policy`, `permission/preset` seeded in child log |
| `workflow` | `ctx.workflowEngine` | Run not in the session log; `tool-workflow` mirrors `tool-workflow/{run,agent}-{start,end}` session events (`packages/workflow/tool-workflow/src/record.ts`) |
| background jobs | `ctx.jobs` (`jobs-local`) | **Process-local only**: in-memory ring + registry; owner = session id; no session-log entries. Tie-in is `workspace/session-activity` + kill on archive (`jobs/src/archive-admission.ts:24-42`) |
| goal | `ctx.goals` | Event-sourced `goal` projection with CAS revisions; `activation` is process-local, "never persisted" (`goal/src/types.ts:97-98`) |
| plan mode | `ctx.planMode` | `plan/mode` log-only whole-value-replace event; projection folds, so resume/fork restore it (`plan-mode/src/index.ts:46-55`) |
| todo | (tool only) | `todo/write` whole-list-replace events, last-write-wins, per-agent-session (`tool-todo/src/index.ts:210`) |

Key persistence facts: continuable subagents **require** session persistence and session query services, else `PERSISTENCE_UNAVAILABLE` / `CONTINUATION_UNAVAILABLE` (`continuation.ts:528-549`). Goal rounds run under a serialized per-Agent scheduler with `queued → claimed → admitted` attempts and race fences (`goal-round-driver/src/index.ts:29-46`); teardown cancels and waits for idle (`:436-453`). Jobs deliberately left the session control stream — the roster moved to `dsh-api-job-controller` and the notice ledger to `dsh-tool-jobs` (`.agents/notes/implemented/architecture/2026-09-03-jobs-seam-consolidation.md`). Plan mode is orthogonal to enforcement: "Sandbox mode and approval policy enforce restrictions independently and do not read or write plan state" (`plan-mode/src/index.ts:6-8`). `exit_plan_mode` stays registered while inactive so the tool catalog is stable across transitions (`:15-18`, `:63-67`).

**Docs vs code:** `docs/subsystems/subagent.md` was checked and is consistent with current code (capability gating, `prepareContinuable` presence-as-capability, catalog projection). No divergence found. The 2026-07-25 policy-inheritance note is accurate *except* its original approval-override wording, which the note itself flags as superseded — code confirms `'never'` pinning (`child-agent.ts:251`).

## 6. Mapping to a SAS→PySpark migration platform — and inheritance risks

Natural fits: **translation** = fork or spawn subagent with `outputSchema` (structured capture, `subagent-in-process-driver/src/structured.ts`); **diagnosis** = workflow `pipeline()`/`parallel()` fan-out over many files with per-item null-swallowing (`workflow-ptc/src/runtime.ts:284-350`); **repair** = continuable child steered by `send_message` with `interrupt_agent` for runaway edits; **escalation** = the parent agent, the only party that can widen scope or hold goal authority.

**Inheritance risks, in order of severity:**
1. **Silent privilege carry-over.** An Auto/Full-access parent stamps its preset into the child (`child-agent.ts:246-253`). A "diagnosis" subagent inherits that identity *and* the parent's explicit sandbox override. Pin `sandboxMode` explicitly at delegation and never let a privileged parent spawn file-mutating children.
2. **Approval pinning cuts both ways.** A blocked child cannot escalate, only report — good for regulation, but human gating must happen at the parent, which must be a root/human-driven agent (`tool-goal/src/authority.ts:80-84`).
3. **Result loss is the default.** Background children do not return transcripts or tool output (`continuation-messages.ts:81-97`). The platform must mandate structured `outputSchema` or explicit `send_message` handoffs, or evidence disappears.
4. **Snapshot staleness.** A child keeps the policy it was handed; a mid-run tightening does not propagate, and cancel-and-redelegate is the only way to pick it up. Audit logs must record the delegation snapshot, not the parent's then-current policy.
5. **Workflow caps are not security controls.** `maxTotalAgents`/`maxConcurrentAgents` are backstops and the VM is not a boundary (PTC note); enforcement must come from the sandbox/subprocess layer.
6. **Out-of-process backends ignore parent policy entirely** — they "retain their own deployment policy" (policy-inheritance note) and advertise `NO_START_CAPABILITIES`. Don't delegate regulated work to `acp`/`codex`/`claude-code` children expecting inherited restrictions.
7. **Goal/todo state is agent-scoped and log-folded.** Cross-agent audit requires reading each agent's own session; the parent's projection will not show a child's todo list or goal rounds.

## Guarantees vs Conventions

| Property | Status | Evidence |
|---|---|---|
| Child approvals pinned `'never'` | **Guaranteed** (in-process) | `child-agent.ts:251` |
| Parent never auto-receives child transcript | **Guaranteed** | `continuation-messages.ts:81-97` |
| Out-of-process children inherit nothing | **Guaranteed** | `out-of-process.ts:57-63` |
| Depth budget monotone, header-authoritative | **Guaranteed** | `depth.ts:28-36` |
| `maxActiveSubagents` enforced pre-admission | **Guaranteed** | `continuation-activation.ts:45-57` |
| Job access fenced by session id | **Authorization, not secrecy** | `jobs/src/index.ts:59-60` |
| Ambient env scrubbed for subprocess children | **Guaranteed** (scrub in `dsh-subprocess`) | `subagent-acp/src/run.ts:44-52` |
| Skills cannot alter tools/policy | **Structural guarantee** (text-only content) | `tool-skill/src/index.ts:122` |
| Catalog summaries are routing only | **Convention enforced by prompt text** | `tool-skill/src/index.ts:266` |
| Workflow caps are backstops | **Convention, explicitly not a security boundary** | PTC note |
| Plan mode independent of sandbox/approval | **Guaranteed** | `plan-mode/src/index.ts:6-8` |
| Goal authority = direct human or exact round | **Guaranteed** | `authority.ts:100-117` |
| Child follows later parent policy changes | **Deliberately NOT guaranteed** | policy-inheritance note |
| Jobs survive process restart | **NOT guaranteed** (process-local) | `jobs-local/src/index.ts` |

## Implications for a regulated SAS→PySpark migration verification platform

- Require structured output (`outputSchema`) on every translation/diagnosis child so evidence is machine-checkable rather than prose-quoted.
- Treat the delegation snapshot (`source: 'delegation'` events in the child's own log) as the auditable permission record — it is reconstructable from the child log alone (`child-agent.ts:256-260`).
- Prefer in-process `spawn` for regulated work; out-of-process backends opt out of every parent-enforced restriction.
- Use `workflow` for fan-out breadth, but put file/credential containment in the sandbox layer, never in workflow caps.
- Model escalation as a parent-side, human-attested act: goal tools already demand direct-human authority, matching a regulated sign-off gate.
- Do not rely on `ctx.jobs` for durable evidence — process-local with bounded, lossy rings; persist artifacts to files or the session log explicitly.
- Todo/plan states are per-agent and log-folded; for cross-agent audit, fold each child's own session, not just the root's.

## UNVERIFIED

- Credential-scrub pattern list in `dsh-subprocess` (only callers' contracts read).
- `subagent-codex` / `subagent-dsh-sdk` wire specifics and containment limits.
- `skill-office` bundled skill bodies (manifests only).
- Whether `attachController` permits `run_in_background` without a mounted controller — explicitly deferred upstream (`jobs-seam-consolidation.md:22`).
