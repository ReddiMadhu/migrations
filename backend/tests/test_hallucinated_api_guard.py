from __future__ import annotations

from sas2pyspark.graph.nodes.guard import _hallucinated_api_violations


def test_nonexistent_method_flagged():
    source = "df = spark.range(3)\nresult = df.nonexistent()\n"
    flagged, message = _hallucinated_api_violations(source)
    assert flagged is True
    assert "nonexistent" in (message or "")


def test_valid_pyspark_method_passes():
    source = "df = spark.range(3)\nresult = df.filter('id > 1')\n"
    flagged, message = _hallucinated_api_violations(source)
    assert flagged is False
    assert message is None


def test_inconclusive_routes_human_required():
    source = "value = some_unknown_api.unknown_method()\n"
    flagged, message = _hallucinated_api_violations(source)
    assert flagged is False
    assert message == "validation inconclusive"
