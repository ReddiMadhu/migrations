"""SHA-256 hashing primitives — content-addressed digests and the genesis hash."""

from __future__ import annotations

import hashlib
from typing import Any

from sas2pyspark.core.json import canonical_json

GENESIS_HASH = "sha256:" + "0" * 64


def sha256_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def sha256_json(value: Any) -> str:
    return sha256_text(canonical_json(value))
