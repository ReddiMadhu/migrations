# Sandbox & Execution Worlds — source-level brief

HEAD `00102833dfaee1da9f48a3a8eae9d34005a75218` (master, 2026-09-22). Citations are
`path:line` against the current checkout; README and `.agents/notes` citations are marked
by their extension. `docs/subsystems/fs-sandbox.md` does **not exist** —
`docs/subsystems/filesystem.md` covers the `FS_SANDBOX_DENIED` taxonomy but the fs fence's
own design lives only in `packages/fs/fs-sandbox/README.md` and its JSDoc. No divergence
found between `docs/subsystems/sandbox.md` and code; that doc is generated-with-JSDoc and
matches verbatim.

## Headline findings (most important/surprising)

1. **The sandbox isolates one thing only: filesystem *write* effects.** No backend
   restricts network; only bwrap restricts process visibility, and even that is a backend
   invariant, not a mode promise (`packages/sandbox/sandbox/src/index.ts:27` "Network and
   process visibility are outside this vocabulary";
   `.agents/notes/implemented/bug-fix/2026-08-06-bwrap-private-pid-namespace.md`).
2. **There is no resource confinement at all** — no rlimit, cgroup, CPU, or memory limit
   is applied by `dsh-sandbox`/`sandbox-local` (repo-wide grep matches only
   `subprocess-local` cgroup scoping for *process-tree teardown* and the unrelated
   `ptc-runtime-python` `setrlimit`). Time is bounded by the *shell executor*.
3. **Windows is structurally `partial`, and the Low integrity label is a standing,
   non-revoked widening of the workspace**: "the workspace's inheritable label survives
   the session … so any other process running at Low integrity as the same user … can
   write and delete inside the workspace, where a Medium label would have denied it"
   (`packages/sandbox/sandbox-windows-acl/README.md`, "Verified boundaries").
4. **Two different mechanisms carry one policy vocabulary**: an in-process
   canonicalize-then-contain fence for `ctx.fs`, a kernel runner (Landlock/Seatbelt/bwrap/
   ACL-restricted-token) for `ctx.shell`, sharing `writableRoots()` so they cannot drift
   (`packages/sandbox/sandbox/src/roots.ts:52-54`).
5. **Fail-closed is real and load-bearing**: no usable backend → `SANDBOX_UNAVAILABLE` and
   the command never spawns (`packages/sandbox/sandbox/src/index.ts:124,131-144`;
   `native/system/packages/entry/src/main.c:233-236` "Either way: not enforceable — fail
   CLOSED, never exec unconfined").
6. **Escalation is a one-shot, approved, strictly-wider retry** carried per call — never
   persisted (`packages/fs/tool-fs/src/sandbox.ts:88-109`).

## Q1 — What the sandbox actually isolates

### Mechanism per platform (`packages/sandbox/sandbox-local/src/index.ts:160-167` `PLATFORM_CHAINS`)

| Platform | Runner | Kernel mechanism | Isolates |
|---|---|---|---|
| Linux (1st) | `bwrap` | mount namespace: `--ro-bind / /`, `--dev`, `--unshare-pid` + private `/proc`, `--die-with-parent`; `workspace-write` adds `--tmpfs /tmp` + `--bind <root> <root>` (`profiles.ts:16-23`) | fs writes; also host PID visibility (procfs magic-link escape closed) |
| Linux (2nd) | `landlock-run` (shipped C11 binary, `native/system/.../src/main.c`) | Landlock LSM allow-list, self-restrict-then-`execvp`; `--ro`/`--rw` grants; `prctl(PR_SET_NO_NEW_PRIVS)` before `restrict_self` (`main.c:230-262`) | fs accesses the negotiated kernel ABI governs only |
| macOS | `sandbox-exec` (SBPL) | Seatbelt: `(allow default) (deny file-write*)` + `subpath` allow-lists (`profiles.ts:51-57`) | fs writes only |
| Windows | ACL restricted-token runner | `CreateRestrictedToken(WRITE_RESTRICTED|DISABLE_MAX_PRIVILEGE|LUA_TOKEN)` + Low integrity + capability-SID ACEs (`sandbox-windows-acl/src/index.ts:284-303`) | write/delete only; reads/network/process visibility NOT |

Landlock grants under `workspace-write`: `readWrite = ['/dev/null', '/tmp', workspaceRoot]`,
`readOnly = ['/']` (`profiles.ts:30-36`). `main.c:185-191` scales the handled mask by ABI;
`main.c:288-292` prints the partial notice on older ABI rather than refusing. Seatbelt and
bwrap "govern every promised file effect by construction" → always `full`
(`sandbox-local/src/index.ts:178-189`).

### Full vs partial enforcement — a reported fact, not a promise

`SandboxEnforcement = 'full' | 'partial'` (`sandbox/src/index.ts:59`) rides every wrap on
`ConfinedArgv.enforcement` and is surfaced to the model in `ShellSandboxInfo.enforcement`
(`packages/shell/shell/src/types.ts:35-44`). Documented partial cases
(`docs/subsystems/sandbox.md:30`):

- **Windows ACL** — always `partial`: NTFS hard links alias one file object across paths;
  reads unconfined; a tree another AppContainer tool ACL'd with a package SID is unreadable
  by the Low-IL child (`sandbox-local/src/index.ts:182-188`). The README adds:
  `NULL-DACL` directories not identity-preserving under grant+revoke; piped-stdio
  grandchildren fail with EPERM; eager full-tree DACL propagation ("tens of seconds on
  large workspaces"); CIM unavailable (Authenticated Users dropped, closing the `C:\`-root
  tree-creation escape); PowerShell drops to ConstrainedLanguage under `read-only`.
- **Landlock older ABI** — `--probe` enforces a maximal ruleset in a short-lived child;
  exit 0 without the `partially enforced` marker is `full`
  (`native/landlock-run/packages/entry/lib/index.js:89-98`). A missing binary, wrong arch,
  or unenforcing kernel are deliberately indistinguishable → `unusable`.

### Explicitly NOT isolated

- **Network** — no backend; `sandbox/src/index.ts:27`. The Windows README: "pair this
  backend with a read-side policy or an AppContainer capability token for stronger
  confinement."
- **Reads** — Windows: `WRITE_RESTRICTED` "intersects write accesses only"; the fs fence:
  "Reads pass through untouched: every mode permits reading" (`fs-sandbox/src/index.ts:7-8`).
- **Process visibility** — Landlock/Seatbelt unchanged; bwrap private-PID is a backend
  invariant only. **Resources** — none (see Q5).

## Q2 — SandboxPolicy vs SandboxProvider: the exact split

- **`SandboxPolicyService` (`ctx.sandboxPolicy`)** is the *single policy home*. It owns the
  deployment default (`mode` default `'read-only'` — the fail-safe default,
  `sandbox-policy/src/index.ts:113`), the fallback `workspaceRoot` (default
  `process.cwd()`, `:131`), the `sandbox/mode` session-projection fold (`:133-139`), and
  the system-prompt context section `sandbox:policy` (`:141-152`). `resolve()` precedence
  (`:164-171`): **approved explicit mode > session's last `sandbox/mode` event >
  deployment default**, and **`session.header.cwd` > configured root**. It preserves
  "execution-world spelling" and does not canonicalize (`:35-39`) — enforcing providers
  resolve filesystem identity on their own host.
- **`SandboxProvider` (`ctx.sandbox`, abstract `SandboxProvider.confine`)** is the
  *mechanism*. `confine(argv, policy, signal)` returns `ConfinedArgv` or throws
  `SandboxUnavailableError` (`sandbox/src/index.ts:158-177`). The JSDoc is explicit:
  "the provider treats the policy as fully specified" (`:62-72`) — **the provider does no
  defaulting and no resolution**.

**Where policy is evaluated relative to dispatch:** at the *consumer boundary* —
`SandboxBashExecutor.resolve()` stamps `request.sandboxPolicy ??
ctx.sandboxPolicy.resolve()` onto the spec (`bash-sandbox/src/index.ts:85-87`), and
`execute()` short-circuits on `danger-full-access` **before ever calling the provider**
(`:92-97`) — a full-access run never touches `ctx.sandbox`. Only then does `confine()` wrap
`['bash','-c',command]` (`:188`); `LocalSandboxProvider.confine` canonicalizes the root and
dispatches to the platform chain (`sandbox-local/src/index.ts:319-338`). Identical shape in
`pwsh-sandbox` (`pwsh-sandbox/src/index.ts:92-94`) and the fs family
(`fs-sandbox/src/index.ts:122-124`).

## Q3 — Verifying the "policy check in TRUSTED code" claim

JSDoc, `packages/fs/fs-sandbox/src/index.ts:10-18` (verbatim, ≤25 words/fragment):

> "The fence is a policy check in TRUSTED code over a MODEL-CONTROLLED path, NOT a kernel boundary"

> "the operations are the seam's own (open, rename), and only the target path is untrusted"

> "kernel-grade isolation of untrusted CODE stays `ctx.shell`'s job"

> "The residual TOCTOU … is narrowed by re-canonicalizing immediately before delegating"

**Implemented behavior** (`checkedTarget`, `fs-sandbox/src/index.ts:122-144`):
`danger-full-access` returns the caller's target unfenced; `read-only` throws
`FS_SANDBOX_DENIED`; `workspace-write` re-realpaths **now**, requires containment under one
of `writableRoots(policy)` (workspaceRoot + `/tmp` + `os.tmpdir()`, canonicalized via
`realpathSync.native`, `roots.ts:30-54`), and returns **that fresh target** so the checked
identity is the mutated one. Containment (`containment.ts:58-75`) is a lexical fast path
with an ancestor-walk dev/ino identity fallback for Windows 8.3/casing aliases.

**What a tool must do to be unconstrained (all verified):**
1. Mount the bare backend — `dsh-fs-local` / `dsh-bash-local`. `FileSystem.sandboxMode`
   returns `undefined` (`fs/src/index.ts:119-121`) and `writeText`'s `sandboxPolicy` param
   is ignored ("the bare backend ignores it", `fs/src/index.ts:261-263`). Capability-gated:
   `FsSandboxController.escalationModes` is `[]` unless `ctx.fs.sandboxMode !== undefined`
   (`tool-fs/src/sandbox.ts:44-49`).
2. Run `mode: 'danger-full-access'` (approved escalation or deployment default).
3. Bypass the seam: call `LocalFileSystem.writeText` or any Node `fs` API directly. The
   fence is a subclass override on two methods; nothing else in the process is fenced, and
   web/todo are unfenced by design — "an argv wrapper cannot confine a function closing
   over `ctx`" (`2026-07-06-sandbox.md` § In-process tools).
4. On Windows, exploit a documented partial boundary (hard link out of the workspace; a
   file ACL'd by a foreign AppContainer package SID; the standing Low label).

The claim is **accurate for the fs seam**. Note the asymmetry: the fs fence is a pure
policy check, so its completeness is `full` by construction — it is never reported
`partial` — whereas the shell seam's completeness is a kernel fact.

## Q4 — One "execution world" shared by fs + subprocess

`ctx.fs` and `ctx.subprocess` jointly define one execution world
(`packages/subprocess/subprocess/src/index.ts:95-96`: "Executable paths belong to one
execution world shared with the mounted filesystem provider"). The seam gives the pair the
facts they must share without leaking identity: `processPath(target)`,
`processPathFromHostPath()`, `fileUrl(target)`, `contains(parent, child)`
(`fs/src/index.ts:134-173`).

**Swapping a provider moves a whole world** — verified concretely in the SSH family:
`packages/ssh/sandbox-ssh/src/index.ts:10-12` — "Resolve each confinement request on the
same host as its filesystem and subprocess providers": `SshSandboxProvider.confine` RPCs
the SSH host's own `sandbox` request and rehydrates the identical `ConfinedArgv` schema.
`packages/ssh/{fs-ssh,subprocess-ssh,sandbox-ssh}` are the coherent group; the sandbox is a
*third* capability that must be world-consistent, not a wrapper around subprocess.
`docs/subsystems/sandbox.md:5` agrees: containers and microVMs "replace the surrounding
capability seam instead; this service shares the host kernel and filesystem." The
`2026-07-06-sandbox.md` note is blunter: an agent whose bash runs in a container while its
fs tools write the host "lives in two split worlds." This is why `sandbox-policy` preserves
absolute execution-world spelling rather than host-normalizing: the same root string must be
meaningful to whatever provider pair is mounted (`sandbox-policy/src/index.ts:35-39`).

## Q5 — Resource limits, timeouts, env, credentials

**Resource limits: none in the sandbox.** `sandbox-local` Config has only `runnerCommand`,
`runnerFailureSignatures`, `probeTimeoutMs` (`sandbox-local/src/index.ts:254-258`). No
memory/CPU/fd/nproc cap is applied to confined children. `subprocess-local` uses Linux
systemd scopes and Windows Jobs for **process-tree lifetime/teardown**, not quotas. The one
place rlimits appear is `experimental/ptc-runtime-python`, which calls
`setrlimit(RLIMIT_CPU/RLIMIT_AS)` *inside its own child* — a runtime feature, not the harness
sandbox.

**Timeouts belong to the shell executor**, are per-call, and apply regardless of sandbox:
`ShellExecRequest.timeoutMs`/`onExpiry: 'kill'|'none'` (`shell/src/types.ts:53,65-68`);
`LocalBashExecutor` defaults `timeoutMs: 120_000`, `maxTimeoutMs: 600_000`,
`maxOutputBytes: 64_000`, `maxSpillBytes: 64 MiB`, `graceMs: 3_000` (SIGTERM→SIGKILL)
(`bash-local/src/index.ts:100-107`). `timedOut` vs `aborted` is a first-cause fused-deadline
classification (`shell/src/types.ts:150-158`); confinement preparation is inside the same
deadline. **No timeouts on file IO** (`docs/subsystems/filesystem.md`).

**Env var handling.** Every child starts from `scrubbedParentEnv()`: ambient parent env
minus credential-shaped names (`SENSITIVE_ENV_PATTERN = /KEY|PASSWORD|SECRET|TOKEN/i`)
minus all `DSH_*` (case-insensitive, for Windows), plus a proxy overlay
(`subprocess/src/index.ts:47-80`). Merge order: scrubbed ambient → `ENV_OVERRIDES`
(`NO_COLOR`, `TERM=dumb`, `PAGER`, `GIT_PAGER`) → request `env` → managed `dshEnv` last,
so a caller entry can never displace a managed one (`shell/src/types.ts:87-101`); managed
`DSH_*` values come from `ctx.shellEnv` (`shell-env/src/index.ts:156-185`). The Windows ACL
runner rewrites `TMP`/`TEMP` to the private temp dir before spawn (`runner.ts:173-180`)
because an explicit env block trips `ERROR_INVALID_PARAMETER` in `CreateProcessAsUserW`
(`runner.ts:36-38`). The Landlock package: "Deliberately no environment-variable overrides
anywhere in this module: which binary confines a process must never be decidable by the
ambient environment" (`native/landlock-run/packages/entry/lib/index.js:12-14`).

**Credential injection into sandboxes: there is none, by design.** Credentials are a
separate seam (`ctx.credentials`, `ctx.authorization`) holding *references*, resolved once
per operation, never cached (`docs/subsystems/credentials.md`). The sandbox boundary is
the *scrub*: credential-shaped names are stripped from the child env, and a deliberately
supplied entry survives because explicit `env` merges after the scrub
(`subprocess/src/index.ts:41-46`). Nothing in `packages/sandbox` imports `dsh-credentials`;
a confined process that needs a secret must receive it through the tool's explicit `env`.

## Q6 — Known escaping risks and postmortems

**Postmortem 0004** (`docs/postmortem/0004-landlock-partial-notice-misclassified-child-failures.md`)
is the only sandbox-attribution postmortem. The harness matched one `landlock-run: `
substring plus any nonzero exit, so ripgrep's no-match exit 1 (and even a child-chosen 125)
surfaced as `SANDBOX_UNAVAILABLE`; the then-bash-backed filesystem search hid the structured
error behind `SEARCH_FAILED`. **It did not weaken confinement** — "Its security effect was
availability and diagnostic integrity." Fix: `RunnerFailureRule` with exit-code gates, exact
informational-line exclusions, and per-line fatal signatures (`sandbox/src/index.ts:81-116`);
fs search moved to packaged ripgrep over `ctx.subprocess`. Residual risk: "Stderr remains an
in-band attribution channel. A confined child can deliberately reproduce a runner's gated
fatal line and exit status" — not a bypass (the child is already confined). The other three
postmortems are unrelated.

**Documented escape/enforcement gaps (all current, all cited above):** fs-fence TOCTOU
(accepted); NTFS hard links; Windows unconfined reads; AppContainer-package-SID unreadable
trees; the standing Low label widening the workspace for *other* Low-IL processes; `one
write allowlist per workspace` (reusing an instance across two workspaces widens both);
`NULL-DACL` round trip; FAT-class targets unverified; macOS depends on deprecated
`sandbox-exec`; Landlock partial ABI; `runnerCommand` skips probing by design. Also:
in-band stderr attribution; the model may over-ask escalation (the human prompt is the
actual gate); a granted escalation is not a working sandbox — granted `danger-full-access`
never touches the provider, "there the grant, not the probe, is the authority"
(`2026-07-06-sandbox.md` Consequences).

**One doc-vs-code note:** `docs/subsystems/sandbox.md:11` says the POSIX runners
"additionally grant the `/dev/null` sink their shells require" — code grants
`/dev/null` in `landlockProfileArgs` and `seatbeltProfileArgs` only (`profiles.ts:31,
52`); `bwrapProfileArgs` supplies `--dev /dev` instead. Consistent in effect, different in
mechanism; not a contradiction.

## Q7 — What would need to change for a regulated SAS→PySpark verification platform

1. **A network axis in the policy.** A confined process opens sockets freely on every
   backend, and SAS/Spark workloads fetch packages, reach metastores, and call object
   storage. The seam leaves this open ("whether network restriction arrives as a separate
   `network_mode`", `2026-07-06-sandbox.md` § The seam). Add it to the per-call policy and
   enforce with a backend that can (bwrap `--unshare-net`, Landlock TCP ABI v4; on Windows,
   not `WRITE_RESTRICTED`).
2. **Read confinement and resource quotas.** The modes are *write* modes: the Windows
   backend cannot express read restriction without wholesale DACL grants and the fs fence
   deliberately passes reads through — expect to pay the identity-route cost the design
   rejected, or run in a container world. Add CPU/memory budgets (rlimits via the runner,
   cgroup v2, Job objects); today a runaway job is bounded only by `timeoutMs`/
   `maxTimeoutMs` and the grace ladder.
3. **Make `partial` a hard reject.** `enforcement: 'partial'` is reported and then *consumed
   by the model*; "callers requiring the absolute boundary must not treat it as `full`" is a
   convention (`sandbox/src/index.ts:54-58`) — no consumer is observed rejecting it.
4. **Replace or pin the Windows rung.** The standing Low label durably widens the workspace
   for unrelated Low-IL processes and is never revoked by design. Either accept Linux-only
   enforcement or revisit mxc/AppContainer (rejected for OS-floor and arbitrary-read reasons
   a curated-image platform could absorb).
5. **Out-of-band attribution and durable grants.** Denial/runner-failure classification is
   inferred from **stderr text** — an in-band, unauthenticated channel a confined child can
   forge (postmortem 0004 names out-of-band attribution "separate hardening"). Escalation
   grants are one-shot and unpersisted; `allow_always` is explicitly left open.
6. **World-local sandbox per tenant.** Already supported: mount a coherent
   `fs`+`subprocess`+`sandbox` triple per world (container, microVM, SSH host) and
   bash/pwsh/terminal/LSP/fs-tools move with it. But confinement must resolve in the world
   (`sandbox-ssh` RPCs the *remote* host), and Windows per-machine standing workspace ACEs
   make a shared-host multi-tenant deployment unsafe.

## Guarantees vs Conventions

| Property | Status | Evidence |
|---|---|---|
| No usable backend → command never runs | **Guarantee** | `sandbox/src/index.ts:124,131-144`; `main.c:233-236`; `sandbox-windows-acl/src/index.ts:34` "a child is NEVER spawned unrestricted" |
| Process + descendants stay file-write-confined | **Guarantee (per backend completeness)** | Landlock ruleset inherited across `execve` (`main.c:9-11`); bwrap `--die-with-parent`; ACL token per spawn |
| Policy per call; concurrent calls independent | **Guarantee** | `sandbox/src/index.ts:62-72`; per-process facts map (`bash-sandbox/src/index.ts:59-66`) |
| fs and shell confine to the same writable roots | **Guarantee** | one `writableRoots()` home (`roots.ts:52-54`) |
| Escalation strictly wider, one call, never persisted | **Guarantee** | `tool-fs/src/sandbox.ts:88-109` |
| `enforcement: 'partial'` not treated as `full` | **Convention** | `sandbox/src/index.ts:54-58`; no consumer observed rejecting it |
| Network / reads / process visibility / resources confined | **Absent (not claimed)** | `sandbox/src/index.ts:27`; no rlimit/cgroup in `packages/sandbox` |
| Denial vs runner-failure attribution | **Convention (in-band stderr)** | `docs/postmortem/0004` "does not authenticate the writer" |
| Windows workspace writable only by the confined child | **Violated by design** | standing inheritable Low label (`sandbox-windows-acl/README.md`) |
| Session log reconstructs policy history | **Guarantee** | `sandbox-policy/src/index.ts:8-11` |

## Implications for a regulated SAS→PySpark migration verification platform

- **Attest the platform, not the mode.** Pin Linux + a known Landlock ABI (≥3 for
  truncate) or bwrap as the attested target; treat any `partial` verdict as a validation
  failure and reject `danger-full-access` runs from evidence-producing calls.
- **Put heavy runtimes in the subprocess world.** A SAS interpreter, Spark driver, or MCP
  server writing through Node `fs` in-process is entirely outside the fs fence; only a
  kernel runner covers it.
- **Scope grants to datasets.** One-shot, model-requested, human-approved escalation
  expresses no dataset/path-scoped authority, and `approval/policy: 'never'` fails every
  escalation closed — the correct unattended posture, which makes a pre-provisioned
  scope-bounded grant store required, not optional.
- **Budget egress and quotas as new mechanism.** Neither is expressible in the current
  vocabulary; both are mandatory for a migration proof touching package repos, object
  storage, metastores, and runaway jobs.
- **Prefer one world per tenant.** The provider triple is the right isolation unit, and
  the evidence spine it carries (per-call policy, immutable session cwd, projected mode
  events, runtime-context snapshots) is a credible audit base once it records enforcement
  facts and runner identity per artifact.
