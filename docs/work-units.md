# MigrationWorkUnit

`MigrationWorkUnit` is the bounded semantic context passed to diagnosis and repair components.

It is deliberately smaller than a migration session. A session owns the complete evidence history; a work unit identifies the failing SAS module, its dependency closure, the relevant semantic IR, the current candidate, and the bounded verification evidence needed for the next repair decision.

## Shape

```json
{
  "workUnitId": "wu-claims-04",
  "migrationId": "migration-2026-04",
  "scopeId": "tenant/acme/migration/migration-2026-04/attempt/1/candidate/7",
  "sasModule": "lib/claims.sas",
  "dependencyClosure": [
    "lib/common.sas",
    "lib/dates.sas",
    "lib/formats.sas"
  ],
  "irSnapshotId": "ir-claims-04",
  "macroResolutionId": "macro-claims-04",
  "irNodeIds": ["ir-14", "ir-15", "ir-22"],
  "inputSnapshotId": "snapshot-819",
  "referenceOutput": {
    "backend": "databricks",
    "uri": "databricks://migration_prod/reconciliation/sas_reference_delta",
    "format": "delta",
    "version": "18",
    "contentDigest": "sha256:..."
  },
  "candidateId": "candidate-7",
  "reconciliationId": "recon-8392",
  "failureSignature": "duplicate-business-key",
  "requiredSkills": ["sas-merge", "sas-first-last"],
  "status": "REPAIRING",
  "createdAt": "2026-01-01T00:00:00.000Z",
  "updatedAt": "2026-01-01T00:01:00.000Z"
}
```

The dependency closure, IR snapshot ID, macro-resolution ID, and IR node list are immutable after creation. Later lifecycle events can attach a candidate, reconciliation, failure signature, or terminal reason, but cannot widen the original context.

`ctx.workspace.createWorkUnit()` can derive this closure from a durable SAS include graph. `ctx.semanticIr.createWorkUnit()` can additionally attach a bounded structural IR snapshot and selected source-span node IDs. `ctx.macroResolution.createWorkUnit()` can attach static macro-resolution evidence to the same immutable closure. All paths reject a root with unresolved or cyclic dependencies, so a work unit cannot silently omit a required source dependency.

## Durable lifecycle

The service exposes `ctx.workUnits` and records these durable events in the migration session:

```text
work-unit/created
work-unit/updated
work-unit/closed
```

The event payload contains a complete frozen work-unit snapshot. This makes replay independent of mutable service memory and keeps the bounded context in the evidence chain.

Execution and reconciliation requests may carry the work-unit ID. The runtime verifies that the work unit owns the requested candidate/execution scope before it can execute or compare anything, and copies that ID into durable execution/reconciliation events.

The migration projection exposes a read-only map of work units:

```ts
session.projection.workUnits.get('wu-claims-04')
```

## Bounds

The first implementation rejects work units when they exceed:

- 256 dependency-closure entries
- 512 IR node IDs
- 64 required skills
- 512 characters for a SAS module label
- 2,048 characters for a logical data reference URI

Dependency closure entries are logical module identifiers, not local file paths. They may be used by a future workspace graph and are never used to construct filesystem paths.

## Databricks references

A `databricks` reference is a logical manifest, not a data transfer request. The control plane stores the URI, format, version, and content digest. The table contents remain in Databricks and are read by Databricks/Spark jobs.

The current local `FileSystemArtifactService` remains suitable for small control-plane evidence such as reports and IR snapshots. It is not used as the 10GB data transport.

## Model boundary

`ctx.modelContext.assemble()` exposes only:

- the work unit's SAS module
- its dependency closure
- relevant IR node IDs and spans
- current candidate identity
- bounded reconciliation summaries and report references
- required skill versions
- previous hypotheses and outcomes

The work unit must never grant the model access to unrestricted SQL, credentials, raw large datasets, arbitrary filesystem paths, or authority to mark a reconciliation as passed.
