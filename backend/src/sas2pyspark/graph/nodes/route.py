"""route_after_reconcile — conditional router after reconciliation.

certifiable → certify → END
mismatch → repair → propose loop
failed → HUMAN_REQUIRED → interrupt → END
"""

from __future__ import annotations

from typing import Any

from sas2pyspark.graph.state import MigrationGraphState


def route_after_reconcile(state: MigrationGraphState) -> str:
    recon = state.get("last_reconciliation_result")
    if recon is None:
        return "human_required"
    certifiable = recon.get("certifiable", False)
    if certifiable is True:
        return "certify"
    return "repair"
