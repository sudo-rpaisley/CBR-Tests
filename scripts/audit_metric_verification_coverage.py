#!/usr/bin/env python3
"""Audit verification coverage for the original 61 canonical metric leaves.

This script is intentionally conservative. It proves structural facts from the
repository (canonical metric -> production implementation -> pytest references)
and identifies tests that *look* like numerical/exact or boundary oracles. It
does not promote a heuristic match into a scientifically approved oracle; rows
without an explicitly named hand-calculated/oracle test remain review targets.
"""

from __future__ import annotations

import ast
import inspect
import json
import re
from dataclasses import dataclass
from pathlib import Path

from runner import dispatch

ROOT = Path(__file__).resolve().parents[1]
TAXONOMY = ROOT / "taxonomy" / "master_taxonomy.json"
TEST_ROOT = ROOT / "tests"
REPORT = ROOT / "docs" / "metric_verification_coverage.md"
JSON_REPORT = ROOT / "docs" / "metric_verification_coverage.json"
EXPECTED_CANONICAL_COUNT = 61


@dataclass(frozen=True)
class TestCase:
    path: str
    name: str
    source: str
    has_assert: bool
    has_numeric_expected: bool
    has_exact_expected: bool
    boundary_signal: bool
    explicit_oracle_signal: bool


def _collect_canonical_metrics(node, path=()):
    rows: list[dict] = []
    if isinstance(node, dict):
        metrics = node.get("_metrics")
        if isinstance(metrics, list):
            # The original expert-reviewed catalogue is 61 leaves. Supporting
            # diagnostics and explicitly labelled post-review additions are kept
            # executable but audited separately from this canonical set.
            if not node.get("_role") and not node.get("_review_status"):
                for entry in metrics:
                    metric_id = entry if isinstance(entry, str) else entry.get("metric_id")
                    if metric_id:
                        rows.append({"metric_id": metric_id, "taxonomy_path": "/".join(path)})
            return rows
        for key, value in node.items():
            if not key.startswith("_"):
                rows.extend(_collect_canonical_metrics(value, (*path, key)))
    return rows


def canonical_metrics() -> list[dict]:
    payload = json.loads(TAXONOMY.read_text(encoding="utf-8"))
    rows = _collect_canonical_metrics(payload)
    rows.sort(key=lambda row: (row["taxonomy_path"], row["metric_id"]))
    if len(rows) != EXPECTED_CANONICAL_COUNT:
        raise SystemExit(
            f"Expected {EXPECTED_CANONICAL_COUNT} canonical metrics after excluding "
            f"supporting/post-review additions; found {len(rows)}"
        )
    return rows


def _identifiers_in_source(function) -> set[str]:
    try:
        source = inspect.getsource(function)
    except (OSError, TypeError):
        return {getattr(function, "__name__", "")}
    identifiers = set(re.findall(r"\b(?:run|compute|validate)_[A-Za-z0-9_]+\b", source))
    identifiers.add(getattr(function, "__name__", ""))
    return {item for item in identifiers if item}


def implementation_symbols(metric_id: str) -> set[str]:
    symbols: set[str] = set()
    compute_fn = dispatch.TABULAR_COMPUTE_METRICS.get(metric_id)
    if compute_fn is not None:
        symbols.add(compute_fn.__name__)

    registered = dispatch.METRIC_REGISTRY.get(metric_id)
    if registered is not None:
        symbols.update(_identifiers_in_source(registered))

    special = {
        "missing_value_ratio": {"run_missing_value_metric", "compute_missing_value_ratio"},
        "duplicate_row_ratio": {"run_duplicate_row_metric", "compute_duplicate_row_ratio"},
    }
    symbols.update(special.get(metric_id, set()))
    return symbols


def _assert_has_numeric_expected(node: ast.Assert) -> bool:
    text = ast.unparse(node.test)
    if "pytest.approx" in text or "math.isclose" in text or "np.isclose" in text:
        return True
    for child in ast.walk(node.test):
        if isinstance(child, ast.Constant) and isinstance(child.value, (int, float)) and not isinstance(child.value, bool):
            return True
    return False


def _assert_has_exact_expected(node: ast.Assert) -> bool:
    if not isinstance(node.test, ast.Compare):
        return False
    return any(isinstance(op, (ast.Eq, ast.Is, ast.IsNot)) for op in node.test.ops)


def test_cases() -> list[TestCase]:
    cases: list[TestCase] = []
    boundary_re = re.compile(
        r"missing|empty|zero|undefined|not_applicable|invalid|reject|without|absent|"
        r"boundary|denominator|non_positive|no_|requires|exclude",
        re.I,
    )
    oracle_re = re.compile(r"oracle|hand[_ -]?calculated|known[_ -]?value|closed[_ -]?form", re.I)

    for path in sorted(TEST_ROOT.glob("test_*.py")):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        lines = source.splitlines()
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or not node.name.startswith("test_"):
                continue
            end = getattr(node, "end_lineno", node.lineno)
            function_source = "\n".join(lines[node.lineno - 1 : end])
            assertions = [child for child in ast.walk(node) if isinstance(child, ast.Assert)]
            cases.append(
                TestCase(
                    path=path.relative_to(ROOT).as_posix(),
                    name=node.name,
                    source=function_source,
                    has_assert=bool(assertions),
                    has_numeric_expected=any(_assert_has_numeric_expected(a) for a in assertions),
                    has_exact_expected=any(_assert_has_exact_expected(a) for a in assertions),
                    boundary_signal=bool(boundary_re.search(node.name + "\n" + function_source)),
                    explicit_oracle_signal=bool(oracle_re.search(node.name + "\n" + function_source)),
                )
            )
    return cases


def _matches(case: TestCase, metric_id: str, symbols: set[str]) -> bool:
    if metric_id in case.source:
        return True
    return any(symbol in case.source for symbol in symbols)


def build_rows() -> list[dict]:
    cases = test_cases()
    rows: list[dict] = []
    for metric in canonical_metrics():
        metric_id = metric["metric_id"]
        symbols = implementation_symbols(metric_id)
        matched = [case for case in cases if _matches(case, metric_id, symbols)]
        explicit = [case for case in matched if case.explicit_oracle_signal and case.has_assert]
        numeric = [case for case in matched if case.has_numeric_expected]
        exact = [case for case in matched if case.has_exact_expected]
        boundary = [case for case in matched if case.boundary_signal and case.has_assert]

        if explicit:
            oracle_status = "explicit_oracle_test"
        elif numeric:
            oracle_status = "numerical_assertion_needs_manual_oracle_review"
        elif exact:
            oracle_status = "exact_assertion_needs_manual_oracle_review"
        elif matched:
            oracle_status = "covered_but_no_expected_value_detected"
        else:
            oracle_status = "no_pytest_reference_detected"

        rows.append(
            {
                **metric,
                "implementation_symbols": sorted(symbols),
                "test_count": len(matched),
                "tests": [f"{case.path}::{case.name}" for case in matched],
                "explicit_oracle_tests": [f"{case.path}::{case.name}" for case in explicit],
                "numerical_oracle_candidates": [f"{case.path}::{case.name}" for case in numeric],
                "boundary_candidates": [f"{case.path}::{case.name}" for case in boundary],
                "oracle_status": oracle_status,
            }
        )
    return rows


def _short_tests(values: list[str], limit: int = 2) -> str:
    if not values:
        return "—"
    shown = [f"`{value}`" for value in values[:limit]]
    if len(values) > limit:
        shown.append(f"+{len(values) - limit} more")
    return "; ".join(shown)


def write_reports(rows: list[dict]) -> None:
    JSON_REPORT.write_text(json.dumps({"canonical_metric_count": len(rows), "metrics": rows}, indent=2) + "\n", encoding="utf-8")

    counts: dict[str, int] = {}
    for row in rows:
        counts[row["oracle_status"]] = counts.get(row["oracle_status"], 0) + 1

    out = [
        "# Canonical metric verification coverage audit",
        "",
        "This report is generated by `scripts/audit_metric_verification_coverage.py` for the original 61 canonical leaves. It is deliberately conservative: source-level pytest matches prove test coverage, while only tests explicitly named as an oracle/hand-calculated check are promoted automatically to `explicit_oracle_test`. Numerical assertions without that explicit intent remain manual-review candidates.",
        "",
        "## Summary",
        "",
        f"- Canonical leaves audited: **{len(rows)}**",
    ]
    for status in sorted(counts):
        out.append(f"- `{status}`: **{counts[status]}**")
    out.extend(
        [
            "",
            "## Coverage table",
            "",
            "| Metric | Production symbol(s) | Tests | Oracle status | Best oracle candidate | Boundary/undefined candidate |",
            "| --- | --- | ---: | --- | --- | --- |",
        ]
    )
    for row in rows:
        best = row["explicit_oracle_tests"] or row["numerical_oracle_candidates"]
        out.append(
            "| "
            + f"`{row['metric_id']}` | "
            + (", ".join(f"`{s}`" for s in row["implementation_symbols"]) or "—")
            + f" | {row['test_count']} | `{row['oracle_status']}` | "
            + _short_tests(best, 1)
            + " | "
            + _short_tests(row["boundary_candidates"], 1)
            + " |"
        )

    out.extend(
        [
            "",
            "## Interpretation",
            "",
            "A green software test suite is necessary but is not sufficient evidence that every scientific equation has an independent oracle. Rows marked `numerical_assertion_needs_manual_oracle_review`, `exact_assertion_needs_manual_oracle_review`, `covered_but_no_expected_value_detected`, or `no_pytest_reference_detected` must be reviewed against the corresponding paper `verification.md`. Where the existing test does not implement that paper oracle directly, a dedicated conformance test should be added before the final experiment tag.",
            "",
        ]
    )
    REPORT.write_text("\n".join(out), encoding="utf-8")


def main() -> int:
    rows = build_rows()
    write_reports(rows)
    print(f"Audited {len(rows)} canonical metrics.")
    for status in sorted({row['oracle_status'] for row in rows}):
        print(f"{status}: {sum(row['oracle_status'] == status for row in rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
