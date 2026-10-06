# ADR-015: Cordis workers over distributed durable state

**Status:** Accepted

## Problem
Cordis is an excellent local composition runtime, but its in-process registry and live-object assumptions cannot coordinate a multi-tenant batch fleet.

## Options
One long-lived process/context; embed Cordis only; Cordis inside workers backed by PostgreSQL/object storage; replace Cordis with a distributed framework.

## Decision
Use short-lived or tenant-local Cordis worker contexts over a PostgreSQL durable queue/event store and object storage. Store locks, job mappings, and projections in PostgreSQL. Local JSONL/filesystem leases and process-local registries remain test adapters, not distributed authority; pin and attest the composition at worker boot.

## Why
It preserves Cordis’s lifecycle/DI/event strengths while placing authority where distributed workers can coordinate safely.

## Tradeoffs
Context recreation overhead, serializable event writes, lease/heartbeat complexity, provider configuration per worker.

## Failure modes
Split brain, stale lease, event-store outage, duplicate job submission, long-lived local state drift.

## Reversal cost High; the durable protocol remains portable, but orchestration and operations are substantial.
