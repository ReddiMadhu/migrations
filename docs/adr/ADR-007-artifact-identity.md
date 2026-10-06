# ADR-007: Immutable content-addressed artifacts

**Status:** Accepted

## Problem
Names such as `candidate-001.py` are mutable aliases and cannot prove replay or approval identity.

## Options
Paths only; database blobs only; content-addressed objects plus append-only metadata/events.

## Decision
Use SHA-256 exact-byte identity, immutable storage locations, parent lineage, scope ownership, media type, creator, and timestamps. Events reference artifact ID and digest.

## Why
It supports replay, audit, stale-approval detection, certificate integrity, and safe deduplication.

## Tradeoffs
Object-store metadata and garbage collection are required; hash collisions are theoretically possible but negligible.

## Failure modes
Incorrect digest, orphan object, lost metadata, deletion of referenced evidence.

## Reversal cost
High; nearly every evidence-bearing feature depends on stable artifact identity.
