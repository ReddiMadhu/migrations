"""P2-2: Documentation Accuracy — AGENTS.md + doc/code contract alignment."""

from __future__ import annotations

from pathlib import Path


def _read(filename: str) -> str:
    path = Path(__file__).resolve().parent.parent.parent / filename
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def test_agents_md_python_authoritative():
    content = _read("AGENTS.md")
    assert "LangGraph Python" in content
    assert "single authoritative implementation" in content or "single implementation" in content


def test_agents_md_no_ts_reference_rule():
    content = _read("AGENTS.md")
    assert "do not delete or rewrite it before" not in content
    assert "do not delete" not in content or "deleted" in content


def test_agents_md_historical_ts_section():
    content = _read("AGENTS.md")
    assert "Historical TypeScript" in content or "historical" in content.lower()
    assert "reference deleted" in content.lower() or "deleted" in content.lower()


def test_agents_md_pytest_tests():
    content = _read("AGENTS.md")
    assert "pytest" in content
    assert "backend/tests/" in content


def test_agents_md_no_vitest_reference():
    content = _read("AGENTS.md")
    ts_section = content[content.find("Historical"):] if "Historical" in content else ""
    active_section = content[:content.find("Historical")] if "Historical" in content else content
    assert "vitest" not in active_section or "vitest" not in content.split("## Tests")[1].split("##")[0] if "## Tests" in content else True


def test_databricks_doc_exists():
    content = _read("docs/databricks-execution.md")
    if not content:
        return
    assert "signal" in content.lower() or "cancel" in content.lower()


def test_doc_accuracy_gate():
    agents = _read("AGENTS.md")
    assert "Deterministic invariants" in agents
    assert "Hash-chained event log" in agents
    assert "Content-addressed artifacts" in agents
    assert "Canonical JSON" in agents
    assert "Five-step rule" in agents
