#!/usr/bin/env python3
"""
run_pipeline.py — Standalone executable that drives the full SAS → PySpark pipeline.

Usage:
    cd backend
    PYTHONPATH=src .venv/Scripts/python.exe scripts/run_pipeline.py

What it does:
    1. Reads the 3 SAS files, parses and expands macros
    2. Reads the 3 input CSVs
    3. Executes the ETL logic in pure Python (simulating PySpark transforms)
    4. Reconciles actual outputs against expected outputs
    5. Produces a hash-chained evidence manifest
    6. Prints a human-readable summary with PASS / FAIL verdicts
"""

from __future__ import annotations

import csv
import io
import json
import sys
import time

# Force UTF-8 output on Windows (cp1252 cannot encode Unicode symbols)
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from pathlib import Path
from typing import Any

# ── Ensure importable ───────────────────────────────────────────────────────
_BACKEND = Path(__file__).resolve().parent.parent
_SRC = _BACKEND / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from sas2pyspark.core.hash import sha256_bytes, sha256_text, sha256_json, GENESIS_HASH
from sas2pyspark.core.json import canonical_json
from sas2pyspark.sas.parser import parse_sas
from sas2pyspark.sas.macro_expander import expand_macros
from sas2pyspark.services.reconciliation import (
    ReconciliationService,
    DEFAULT_RECONCILIATION_POLICY,
    _summarize_dataset,
    _resolve_policy,
)


# ── Paths ───────────────────────────────────────────────────────────────────
_PIPELINE = _BACKEND / "tests" / "pipeline"
_SAS_DIR = _PIPELINE / "sas_sources"
_INPUT_DIR = _PIPELINE / "input"
_EXPECTED_DIR = _PIPELINE / "expected_output"
_OUTPUT_DIR = _PIPELINE / "actual_output"


# ── Utilities ───────────────────────────────────────────────────────────────

def read_csv(path: Path) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as f:
        return [dict(row) for row in csv.DictReader(f)]


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def to_decimal(s: str) -> Decimal:
    return Decimal(s.strip())


def to_float(s: str) -> float:
    return float(Decimal(s.strip()))


def rows_to_recon(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    """Convert string-valued rows to typed rows for reconciliation."""
    out: list[dict[str, Any]] = []
    for r in rows:
        typed: dict[str, Any] = {}
        for k, v in r.items():
            k = k.strip()
            v = v.strip()
            try:
                typed[k] = float(Decimal(v))
            except (InvalidOperation, ValueError):
                typed[k] = v
        out.append(typed)
    return out


# ── ETL Logic (Python simulation of SAS pipeline) ──────────────────────────

def standardize_name(name: str) -> str:
    """Simulate SAS %standardize_name: strip, compress, propcase."""
    return " ".join(name.split()).title()


def validate_date(date_str: str, ref_date: str = "2024-12-31") -> str:
    """Simulate SAS %validate_dates."""
    if not date_str or date_str.strip() == "":
        return "MISSING"
    if date_str > ref_date:
        return "FUTURE"
    if date_str < "1900-01-01":
        return "TOO_OLD"
    return "VALID"


def compute_kpis(order_total: Decimal, total_cost: Decimal) -> dict[str, str]:
    """Simulate SAS %compute_kpis."""
    if order_total > 0:
        margin_pct = ((order_total - total_cost) / order_total * 100).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
    else:
        margin_pct = Decimal("0")

    if order_total >= 1000:
        value_tier = "PREMIUM"
    elif order_total >= 500:
        value_tier = "STANDARD"
    else:
        value_tier = "ECONOMY"

    if margin_pct >= 30:
        profit_flag = "HIGH_MARGIN"
    elif margin_pct >= 10:
        profit_flag = "MID_MARGIN"
    elif margin_pct >= 0:
        profit_flag = "LOW_MARGIN"
    else:
        profit_flag = "LOSS"

    return {
        "margin_pct": str(margin_pct),
        "value_tier": value_tier,
        "profit_flag": profit_flag,
    }


def flag_outlier(value: Decimal, q1: Decimal, q3: Decimal, multiplier: Decimal = Decimal("1.5")) -> str:
    """Simulate SAS %flag_outliers using IQR method."""
    iqr = q3 - q1
    lower = q1 - multiplier * iqr
    upper = q3 + multiplier * iqr
    if value < lower:
        return "LOW"
    elif value > upper:
        return "HIGH"
    return "NORMAL"


def compute_quartiles(values: list[Decimal]) -> tuple[Decimal, Decimal]:
    """Compute Q1 and Q3 from a sorted list of values."""
    s = sorted(values)
    n = len(s)
    q1_idx = (n - 1) * 0.25
    q3_idx = (n - 1) * 0.75
    q1 = _interpolate(s, q1_idx)
    q3 = _interpolate(s, q3_idx)
    return q1, q3


def _interpolate(s: list[Decimal], idx: float) -> Decimal:
    low = int(idx)
    high = min(low + 1, len(s) - 1)
    frac = Decimal(str(idx - low))
    return s[low] + frac * (s[high] - s[low])


# ── Main pipeline ──────────────────────────────────────────────────────────

def run_pipeline() -> dict[str, Any]:
    """Execute the full ETL pipeline and return a results manifest."""
    print("=" * 78)
    print("  SAS → PySpark Migration Pipeline — Production Run")
    print("=" * 78)
    t0 = time.perf_counter()

    # ── Step 0: Parse SAS sources ───────────────────────────────────────
    print("\n[STEP 0] Parsing SAS sources...")
    sas_files = {
        "macros_data_quality.sas": _SAS_DIR / "macros_data_quality.sas",
        "macros_aggregation.sas": _SAS_DIR / "macros_aggregation.sas",
        "main_etl_pipeline.sas": _SAS_DIR / "main_etl_pipeline.sas",
    }
    parse_results: dict[str, Any] = {}
    for name, path in sas_files.items():
        source = path.read_text(encoding="utf-8")
        ir = parse_sas(source, path=name)
        parse_results[name] = {
            "nodes": len(ir.nodes),
            "diagnostics": len(ir.diagnostics),
            "hash": sha256_bytes(path.read_bytes()),
        }
        print(f"  ✓ {name}: {len(ir.nodes)} nodes, {len(ir.diagnostics)} diagnostics")

    # ── Step 0b: Expand macros ──────────────────────────────────────────
    print("\n[STEP 0b] Expanding macros...")
    combined_source = "\n".join(p.read_text(encoding="utf-8") for p in sas_files.values())
    expansion = expand_macros(combined_source)
    print(f"  ✓ {len(expansion.definitions)} macros defined: {list(expansion.definitions)}")
    if expansion.issues:
        for issue in expansion.issues:
            print(f"  ⚠ {issue.code}: {issue.message}")

    # ── Step 1: Read inputs ─────────────────────────────────────────────
    print("\n[STEP 1] Reading input data...")
    transactions = read_csv(_INPUT_DIR / "transactions.csv")
    customers = read_csv(_INPUT_DIR / "customers.csv")
    products = read_csv(_INPUT_DIR / "products.csv")
    print(f"  ✓ transactions: {len(transactions)} rows")
    print(f"  ✓ customers: {len(customers)} rows")
    print(f"  ✓ products: {len(products)} rows")

    # Build lookup maps
    cust_map = {r["customer_id"].strip(): r for r in customers}
    prod_map = {r["product_id"].strip(): r for r in products}

    # ── Step 2: Standardize customer names ──────────────────────────────
    print("\n[STEP 2] Standardizing customer names...")
    for c in customers:
        c["customer_name"] = standardize_name(c["customer_name"])
    cust_map = {r["customer_id"].strip(): r for r in customers}  # refresh
    print(f"  ✓ {len(customers)} names standardized")

    # ── Step 3: Validate dates ──────────────────────────────────────────
    print("\n[STEP 3] Validating transaction dates...")
    for t in transactions:
        t["date_flag"] = validate_date(t["transaction_date"])
    flags = {}
    for t in transactions:
        flags[t["date_flag"]] = flags.get(t["date_flag"], 0) + 1
    print(f"  ✓ Date flags: {flags}")

    # ── Step 4: Join + compute ──────────────────────────────────────────
    print("\n[STEP 4] Joining transactions ↔ customers ↔ products...")
    enriched: list[dict[str, str]] = []
    for t in transactions:
        cid = t["customer_id"].strip()
        pid = t["product_id"].strip()
        c = cust_map[cid]
        p = prod_map[pid]
        qty = to_decimal(t["quantity"])
        unit_price = to_decimal(t["unit_price"])
        unit_cost = to_decimal(p["unit_cost"])
        order_total = qty * unit_price
        total_cost = qty * unit_cost

        kpis = compute_kpis(order_total, total_cost)

        enriched.append({
            "transaction_id": t["transaction_id"].strip(),
            "transaction_date": t["transaction_date"].strip(),
            "date_flag": t["date_flag"],
            "customer_id": cid,
            "customer_name": c["customer_name"].strip(),
            "region": c["region"].strip(),
            "segment": c["segment"].strip(),
            "product_id": pid,
            "product_name": p["product_name"].strip(),
            "category": p["category"].strip(),
            "unit_cost": str(unit_cost),
            "quantity": str(qty),
            "unit_price": str(unit_price),
            "order_total": str(order_total),
            "total_cost": str(total_cost),
            "margin_pct": kpis["margin_pct"],
            "value_tier": kpis["value_tier"],
            "profit_flag": kpis["profit_flag"],
        })
    print(f"  ✓ {len(enriched)} joined rows")

    # ── Step 5: Outlier detection ───────────────────────────────────────
    print("\n[STEP 5] Flagging outliers (IQR method)...")
    order_totals = [to_decimal(r["order_total"]) for r in enriched]
    q1, q3 = compute_quartiles(order_totals)
    print(f"  Q1={q1}, Q3={q3}, IQR={q3-q1}")
    for r in enriched:
        r["outlier_flag"] = flag_outlier(to_decimal(r["order_total"]), q1, q3)
    outlier_counts = {}
    for r in enriched:
        outlier_counts[r["outlier_flag"]] = outlier_counts.get(r["outlier_flag"], 0) + 1
    print(f"  ✓ Outlier distribution: {outlier_counts}")

    # ── Step 6: Aggregate by customer ───────────────────────────────────
    print("\n[STEP 6] Aggregating by customer + region...")
    agg: dict[str, dict[str, Any]] = {}
    for r in enriched:
        key = (r["customer_id"], r["region"])
        if key not in agg:
            agg[key] = {"total": Decimal("0"), "count": 0, "values": []}
        agg[key]["total"] += to_decimal(r["order_total"])
        agg[key]["count"] += 1
        agg[key]["values"].append(to_decimal(r["order_total"]))

    customer_summary: list[dict[str, str]] = []
    for (cid, region), data in sorted(agg.items()):
        avg = (data["total"] / data["count"]).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        customer_summary.append({
            "customer_id": cid,
            "region": region,
            "total_amount": str(data["total"]),
            "avg_amount": str(avg),
            "txn_count": str(data["count"]),
        })
    print(f"  ✓ {len(customer_summary)} customer groups")

    # ── Step 7: Rank products by region ─────────────────────────────────
    print("\n[STEP 7] Ranking products within each region...")
    by_region: dict[str, list[dict[str, str]]] = {}
    for r in enriched:
        by_region.setdefault(r["region"], []).append(dict(r))

    product_performance: list[dict[str, str]] = []
    for region in sorted(by_region):
        rows_in_region = sorted(by_region[region], key=lambda x: -to_decimal(x["order_total"]))
        for rank_idx, r in enumerate(rows_in_region, 1):
            r["product_rank"] = str(rank_idx)
            # Remove outlier_flag (not in product_performance output)
            r.pop("outlier_flag", None)
            product_performance.append(r)
    print(f"  ✓ {len(product_performance)} ranked rows across {len(by_region)} regions")

    # ── Step 8: Write actual outputs ────────────────────────────────────
    print("\n[STEP 8] Writing actual output files...")
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    enriched_fields = [
        "transaction_id", "transaction_date", "date_flag", "customer_id",
        "customer_name", "region", "segment", "product_id", "product_name",
        "category", "unit_cost", "quantity", "unit_price", "order_total",
        "total_cost", "margin_pct", "value_tier", "profit_flag", "outlier_flag",
    ]
    write_csv(_OUTPUT_DIR / "enriched_transactions.csv", enriched, enriched_fields)

    summary_fields = ["customer_id", "region", "total_amount", "avg_amount", "txn_count"]
    write_csv(_OUTPUT_DIR / "customer_summary.csv", customer_summary, summary_fields)

    perf_fields = [
        "transaction_id", "transaction_date", "date_flag", "customer_id",
        "customer_name", "region", "segment", "product_id", "product_name",
        "category", "unit_cost", "quantity", "unit_price", "order_total",
        "total_cost", "margin_pct", "value_tier", "profit_flag", "product_rank",
    ]
    write_csv(_OUTPUT_DIR / "product_performance.csv", product_performance, perf_fields)
    print(f"  ✓ Written to {_OUTPUT_DIR}")

    # ── Step 9: Reconciliation ──────────────────────────────────────────
    print("\n[STEP 9] Running deterministic reconciliation...")
    service = ReconciliationService.__new__(ReconciliationService)
    policy = DEFAULT_RECONCILIATION_POLICY
    verdicts: dict[str, dict[str, str]] = {}

    pairs = [
        ("enriched_transactions", _EXPECTED_DIR / "enriched_transactions.csv", _OUTPUT_DIR / "enriched_transactions.csv"),
        ("customer_summary", _EXPECTED_DIR / "customer_summary.csv", _OUTPUT_DIR / "customer_summary.csv"),
        ("product_performance", _EXPECTED_DIR / "product_performance.csv", _OUTPUT_DIR / "product_performance.csv"),
    ]

    all_pass = True
    for name, expected_path, actual_path in pairs:
        expected_rows = rows_to_recon(read_csv(expected_path))
        actual_rows = rows_to_recon(read_csv(actual_path))
        ref = _summarize_dataset(expected_rows, policy)
        cand = _summarize_dataset(actual_rows, policy)
        summary = service.compare(ref, cand, policy)
        verdicts[name] = {
            "schema": summary.schema_verdict,
            "row": summary.row_verdict,
            "aggregate": summary.aggregate_verdict,
            "statistical": summary.statistical_verdict,
            "certifiable": str(summary.certifiable),
        }
        status = "✓ PASS" if summary.certifiable else "✗ FAIL"
        print(f"  {status}  {name}")
        print(f"       schema={summary.schema_verdict}  rows={summary.row_verdict}  "
              f"aggregate={summary.aggregate_verdict}  statistical={summary.statistical_verdict}")
        if not summary.certifiable:
            all_pass = False

    # ── Step 10: Build evidence manifest ────────────────────────────────
    print("\n[STEP 10] Building hash-chained evidence manifest...")
    manifest: dict[str, Any] = {
        "version": "pipeline-evidence-v1",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "genesis_hash": GENESIS_HASH,
        "sas_sources": {},
        "inputs": {},
        "expected_outputs": {},
        "actual_outputs": {},
        "parse_results": parse_results,
        "macro_definitions": list(expansion.definitions),
        "reconciliation_verdicts": verdicts,
        "policy_fingerprint": str(
            canonical_json(policy.model_dump(by_alias=True))
        ),
    }

    # Hash all files
    for name, d in [("sas_sources", _SAS_DIR), ("inputs", _INPUT_DIR),
                    ("expected_outputs", _EXPECTED_DIR), ("actual_outputs", _OUTPUT_DIR)]:
        for f in sorted(d.iterdir()):
            if f.is_file():
                manifest[name][f.name] = sha256_bytes(f.read_bytes())

    # Chain hash
    chain_hash = GENESIS_HASH
    for event_key in ["sas_sources", "inputs", "expected_outputs", "actual_outputs"]:
        event_data = canonical_json(manifest[event_key])
        chain_hash = sha256_text(chain_hash + event_data)
    manifest["chain_hash"] = chain_hash

    manifest_path = _OUTPUT_DIR / "evidence_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(f"  ✓ Evidence manifest: {manifest_path}")
    print(f"  ✓ Chain hash: {chain_hash}")

    elapsed = time.perf_counter() - t0
    print("\n" + "=" * 78)
    if all_pass:
        print(f"  ✅  ALL RECONCILIATION CHECKS PASSED  ({elapsed:.2f}s)")
    else:
        print(f"  ❌  RECONCILIATION FAILURES DETECTED  ({elapsed:.2f}s)")
    print("=" * 78)

    return manifest


if __name__ == "__main__":
    manifest = run_pipeline()
    sys.exit(0 if all(
        v["certifiable"] == "True"
        for v in manifest["reconciliation_verdicts"].values()
    ) else 1)
