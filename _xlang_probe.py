"""Throwaway cross-language digest probe — deleted after the audit."""
import json
import sys

sys.path.insert(0, "backend/src")

from sas2pyspark.core.json import canonical_json, snapshot_json
from sas2pyspark.core.hash import sha256_json

corpus = [
    ("key-order", {"z": 1, "a": 2, "m": 3}),
    ("non-ascii", {"naïve": "café", "日本": "語"}),
    ("whole-float", {"amount": 1.0, "count": 2.0}),
    ("int", {"amount": 1, "count": 2}),
    ("negative-zero", {"z": -0.0}),
    ("frac", {"amount": 1.5}),
    ("nested-array", {"rows": [[1, 2], [3, 4]], "meta": {"b": 1, "a": 2}}),
    ("bool-null", {"ok": True, "none": None, "no": False}),
]

out = {}
for name, value in corpus:
    snap = snapshot_json(value)
    out[name] = {"canonical": canonical_json(snap), "sha256": sha256_json(snap)}

print(json.dumps(out, indent=2, ensure_ascii=False))