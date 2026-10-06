# Bounded model context

`ModelContextService` turns one durable `MigrationWorkUnit` into a deterministic context object for a future translation, diagnosis, or repair provider. It does not call a model and it does not execute code.

The service follows the work-unit boundary:

- only nodes already attached to the work unit are eligible
- only source spans inside the work unit's dependency closure are eligible
- only small SAS source artifacts are read
- only reconciliation/execution summaries are included
- report artifacts remain references; report contents are not copied into context
- the Databricks reference output remains a logical reference, never raw data

## API

```ts
const context = await runtime.ctx.modelContext.assemble({
  session,
  workUnitId: 'wu-claims-01',
  maxNodes: 96,
  maxExcerptBytes: 48 * 1024,
  maxEvidence: 16,
})
```

The result is a frozen `BoundedModelContext` with:

```ts
{
  contextVersion: 'bounded-model-context-v1',
  contextDigest,
  workUnitId,
  sasModule,
  dependencyClosure,
  irSnapshotId,
  nodes,
  evidence,
  omittedNodeIds,
  omittedEvidenceCount,
  totalExcerptBytes,
  macro,
  limits,
}
```

Each node contains:

- stable IR node ID
- IR kind and name
- source span
- bounded IR attributes
- source artifact digest/reference
- bounded source excerpt
- excerpt byte count and truncation flag

If the work unit has a macro-resolution snapshot, `macro` also contains bounded resolver counts, unresolved call names/spans, and an omitted-unresolved count. It never contains expanded macro bodies.

Each evidence entry contains only a durable event summary, such as:

- reconciliation verdicts
- difference count
- certifiability
- reason code
- execution outcome
- report artifact reference

The context does not include rows from reference or candidate datasets.

## Determinism

`contextDigest` is a SHA-256 digest of canonical JSON containing:

- work-unit identity
- selected IR nodes
- exact excerpts
- evidence summaries
- limits
- omitted-node information

The same session state and the same requested limits produce the same digest. The digest is a reproducibility fingerprint; it is not a model or provider policy decision.

## Limits

The initial hard bounds are:

- 128 IR nodes
- 64 KiB total source excerpt bytes
- 32 evidence entries
- 2 MiB per source artifact

If the excerpt budget is exhausted, remaining node IDs are reported in `omittedNodeIds`. If the evidence limit is exceeded, the latest bounded event summaries are retained and `omittedEvidenceCount` records the remainder. The service never silently expands the work-unit closure.

## Current limitations

The service does not yet:

- resolve macros or autocall libraries
- retrieve report contents for deeper diagnosis
- render provider-specific chat messages
- call an LLM
- apply repair policy
- execute PySpark or SAS
- query Databricks tables

Those responsibilities belong in later deterministic providers. The context contract is intentionally transport-agnostic so a prompt assembler, repair coordinator, or operator tool can consume the same bounded evidence.
