# ADR-011: Reference CSV before live SAS execution

**Status:** Accepted

## Problem
SAS runtime infrastructure may not be available, but the verification workbench still needs a pinned reference path.

## Options
Block on SAS; silently model expected SAS output; treat supplied `sas_output.csv` as a reference artifact under a bounded profile.

## Decision
Implement `executor.sas.reference-csv` first. Mark provenance and non-repeatability explicitly. Reserve separate Viya, Grid, and container providers later.

## Why
It allows independent reconciliation and repair development without pretending the CSV is a full SAS runtime.

## Tradeoffs
Reference authenticity depends on intake provenance and snapshot pinning. Certificates state the reference identity and limitation.

## Failure modes
Stale/mismatched CSV, missing locale/timezone, source/input mismatch, false claim of live SAS equivalence.

## Reversal cost
Low: the provider contract remains and live providers become additional compositions.
