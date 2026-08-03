from __future__ import annotations

import argparse
import ast
import re
from dataclasses import dataclass
from pathlib import Path

EXCLUDED = {".git", ".pytest_cache", ".venv", "venv", "__pycache__"}
MODULE_SUMMARIES = {
    "run_plan.py": "Top-level command workflow from arguments to atomic outcome JSON.",
    "export_outcomes_for_graphs.py": "Flattens selected outcome fields into CSV tables for graphing.",
    "runner/dispatch.py": "Metric registry, wrappers, field translation, and handler construction.",
    "runner/execution.py": "Live status rendering and bounded parallel metric execution.",
    "runner/run_plan_serial.py": "Serial metric execution and interruption handling.",
    "runner/run_context.py": "Input resolution, validation, ordering, signals, display, and telemetry setup.",
    "runner/run_plan_helpers.py": "CLI parsing, headers, signal handlers, outcome construction, and atomic writes.",
    "runner/schema.py": "Plan JSON structural validation.",
    "runner/io.py": "Case/plan loading and path resolution.",
    "runner/tabular.py": "CSV, TSV, XLSX, and XLS loading.",
    "runner/dataset_loading.py": "Shared dataframe loading with progress presentation.",
    "runner/parallel_results.py": "Parallel record normalization and result aggregation.",
    "runner/parallel_progress.py": "Parallel progress callbacks and telemetry updates.",
    "runner/telemetry.py": "Run, metric, and event state models.",
    "runner/taxonomy.py": "Plan and result taxonomy tree construction.",
    "runner/progress.py": "Terminal colour, progress bars, and live output.",
    "runner/live_rendering.py": "ANSI interactive dashboard rendering.",
    "runner/order.py": "Taxonomy-order loading and deterministic metric ordering.",
    "runner/run_display.py": "Display-mode configuration and phase/title presentation.",
    "runner/field_translation.py": "Public field-translation facade and compatibility exports.",
    "runner/field_translation_schema.py": "Translation payload validation and mapping normalization.",
    "runner/field_translation_sidecar.py": "Sidecar detection, creation, and extension.",
    "runner/field_translation_reports.py": "Translation report construction and formatting.",
    "runner/field_translation_workflow.py": "Translation preflight and requested report workflow.",
    "cbr_tests/metrics/column_quality.py": "Column completeness, numeric usability, and variation metrics.",
    "cbr_tests/metrics/data_quality.py": "Missing-value and duplicate-row metrics.",
    "cbr_tests/metrics/pearson.py": "Numeric validation and Pearson correlation profiles.",
    "cbr_tests/metrics/spearman.py": "Spearman rank-correlation profiles.",
    "cbr_tests/metrics/statistical.py": "Internal distribution drift and distance-correlation calculations.",
    "cbr_tests/metrics/temporal.py": "Timestamp, duration, timing-drift, hourly, and periodicity calculations.",
    "cbr_tests/metrics/task_validation.py": "Accuracy and binary precision/recall/F1 calculations.",
    "cbr_tests/metrics/timestamp_coherence.py": "Raw packet timestamp coherence scanning.",
    "tests/label_fidelity_profile.py": "Label-integrity metric implementations awaiting package migration.",
    "tests/slice_representation_profile.py": "Slice-representation metric implementations awaiting package migration.",
    "tests/reference_model_comparison_profile.py": "Reference-comparison metric implementations awaiting package migration.",
    "scripts/build_documentation_inventory.py": "Repository inventory generator used for documentation audits.",
    "scripts/build_reference_documentation.py": "Generator for this function and test reference.",
}
TEST_MODULE_SUMMARIES = {
    "test_correctness_reproducibility.py": "Parallel correctness, timestamps, atomic writes, and schema regression coverage.",
    "test_field_translation.py": "Translation loading, detection, sidecars, reports, suggestions, and formatting.",
    "test_run_plan_field_translation.py": "Command-level translation dry-run and sidecar behaviour.",
    "test_runner_execution_invariants.py": "Execution and live-rendering invariants.",
    "test_runner_progress_invariants.py": "Progress and terminal-output invariants.",
    "test_runner_schema_taxonomy.py": "Plan schema, registry, and taxonomy behaviour.",
    "test_metric_package_layout.py": "Production-package and compatibility import boundaries.",
}
EXACT = {
    "write_outcome": "Writes JSON through a flushed and fsynced temporary file, then atomically replaces the destination.",
    "validate_plan_schema": "Validates plan metadata, execution policy, metric IDs, taxonomy paths, requirements, calculation blocks, and retention blocks.",
    "run_metrics_parallel": "Runs metrics with bounded thread submission, timing, pause/cancel controls, and explicit fail-fast not-run records.",
    "run_serial_metrics": "Runs metrics one at a time, updates display/telemetry, and builds an outcome.",
    "build_metric_handlers": "Builds the metric-ID-to-callable mapping for a run.",
    "prepare_run_context": "Resolves and validates all inputs and creates the initial run state.",
    "collect_parallel_metric_results": "Restores plan order and aggregates parallel execution records, test results, and validations.",
    "translate_metric_fields": "Returns a metric copy with canonical field references resolved to dataset columns.",
    "_ks_statistic": "Computes the largest empirical-CDF difference between two samples.",
    "_wasserstein_distance": "Computes one-dimensional Wasserstein distance from empirical CDFs.",
    "_energy_distance": "Computes the sample energy-distance expression from cross and within-sample distances.",
    "_rbf_mmd": "Computes squared RBF-kernel MMD with a median-distance bandwidth when gamma is omitted.",
    "_distance_correlation": "Computes distance correlation from double-centred pairwise distance matrices.",
    "_positive_label": "Uses an explicit positive class or the lexicographically last class when exactly two labels are observed.",
}

@dataclass(frozen=True)
class Symbol:
    file: str
    name: str
    kind: str
    signature: str
    line: int
    docstring: str | None
    calls: tuple[str, ...]


def _signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    return ast.unparse(node).split(":", 1)[0].removeprefix("def ").removeprefix("async def ").removeprefix(node.name)


def _calls(node: ast.AST) -> tuple[str, ...]:
    values: list[str] = []
    for child in ast.walk(node):
        if isinstance(child, ast.Call):
            try:
                value = ast.unparse(child.func)
            except Exception:
                continue
            if value not in values:
                values.append(value)
    return tuple(values)


def _walk(file: Path, root: Path, body: list[ast.stmt], prefix: str = "", parent: str | None = None) -> list[Symbol]:
    result: list[Symbol] = []
    for node in body:
        if isinstance(node, ast.ClassDef):
            name = prefix + node.name
            result.append(Symbol(file.relative_to(root).as_posix(), name, "class", "", node.lineno, ast.get_docstring(node, clean=True), ()))
            result.extend(_walk(file, root, node.body, name + ".", "class"))
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            name = prefix + node.name
            kind = "method" if parent == "class" else "nested function" if prefix else "function"
            result.append(Symbol(file.relative_to(root).as_posix(), name, kind, _signature(node), node.lineno, ast.get_docstring(node, clean=True), _calls(node)))
            result.extend(_walk(file, root, node.body, name + ".", "function"))
    return result


def collect(root: Path) -> list[Symbol]:
    result: list[Symbol] = []
    for file in sorted(root.rglob("*.py")):
        if any(part in EXCLUDED for part in file.parts):
            continue
        try:
            tree = ast.parse(file.read_text(encoding="utf-8"), filename=str(file))
        except (OSError, UnicodeDecodeError, SyntaxError):
            continue
        result.extend(_walk(file, root, tree.body))
    return result


def _words(name: str) -> str:
    value = name.rsplit(".", 1)[-1].lstrip("_").replace("_", " ")
    value = re.sub(r"\s+", " ", value).strip()
    replacements = {"df": "dataframe", "ip": "IP", "pcap": "PCAP", "tcp": "TCP", "json": "JSON", "mmd": "MMD", "ks": "KS", "cli": "CLI"}
    return " ".join(replacements.get(word, word) for word in value.split())


def describe(symbol: Symbol) -> str:
    leaf = symbol.name.rsplit(".", 1)[-1]
    if symbol.docstring:
        text = " ".join(symbol.docstring.split())
        return text if text.endswith(".") else text + "."
    if leaf in EXACT:
        return EXACT[leaf]
    words = _words(leaf)
    prefixes = {
        "test_": "Verifies that {}.", "compute_": "Computes {} and returns a structured result.",
        "run_": "Runs {}.", "build_": "Builds {}.", "render_": "Renders {} for terminal output.",
        "format_": "Formats {} for human-readable output.", "load_": "Loads {}.", "write_": "Writes {}.",
        "validate_": "Validates {}.", "detect_": "Detects {}.", "collect_": "Collects {}.",
        "parse_": "Parses {}.", "prepare_": "Prepares {}.", "configure_": "Configures {}.",
        "resolve_": "Resolves {}.", "classify_": "Classifies {}.", "normalise_": "Normalizes {}.",
        "normalize_": "Normalizes {}.", "merge_": "Merges {}.", "update_": "Updates {}.",
    }
    for prefix, template in prefixes.items():
        if leaf.startswith(prefix):
            return template.format(_words(leaf[len(prefix):]))
    return ("Data model for " if symbol.kind == "class" else "Implementation helper for ") + words + "."


def module_summary(path: str) -> str:
    if path in MODULE_SUMMARIES:
        return MODULE_SUMMARIES[path]
    if path.startswith("tests/metrics/"):
        return "Network/protocol realism implementation module awaiting migration from the test package."
    if path.startswith("tests/"):
        return "Compatibility or not-yet-migrated metric implementation module."
    return f"Python symbols defined by `{path}`."


def function_reference(symbols: list[Symbol]) -> str:
    grouped: dict[str, list[Symbol]] = {}
    for symbol in symbols:
        if not symbol.file.startswith("tests/test_"):
            grouped.setdefault(symbol.file, []).append(symbol)
    lines = ["# Function and class reference", "", "Exhaustive AST-generated reference for runtime code, metric implementations, compatibility modules, and scripts. Nested helpers are included. Actual pytest cases are documented in [Test suite reference](test_reference.md).", "", "**Public** only means the leaf name lacks a leading underscore; it is not an API stability promise. Nested functions are always internal.", ""]
    for file, items in grouped.items():
        lines += [f"## `{file}`", "", module_summary(file), "", "| Symbol | Kind | Visibility | Purpose |", "| --- | --- | --- | --- |"]
        for symbol in items:
            leaf = symbol.name.rsplit(".", 1)[-1]
            visibility = "Internal" if symbol.kind == "nested function" or leaf.startswith("_") else "Public"
            lines.append(f"| `{symbol.name}{symbol.signature}` (L{symbol.line}) | {symbol.kind} | {visibility} | {describe(symbol).replace('|', '\\|')} |")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def relevant_calls(symbol: Symbol) -> str:
    ignored = ("assert", "pd.", "json.", "Path", "len", "str", "int", "float", "list", "dict", "set", "tuple", "sorted", "round", "sum", "min", "max", "range", "enumerate", "zip", "isinstance", "open")
    values = [value for value in symbol.calls if not value.startswith(ignored)][:8]
    return ", ".join(f"`{value}`" for value in values) or "Assertions and fixtures in the module"


def test_reference(symbols: list[Symbol]) -> str:
    grouped: dict[str, list[Symbol]] = {}
    for symbol in symbols:
        if symbol.file.startswith("tests/test_"):
            grouped.setdefault(symbol.file, []).append(symbol)
    count = sum(1 for items in grouped.values() for symbol in items if symbol.name.rsplit(".", 1)[-1].startswith("test_"))
    lines = ["# Test suite reference", "", f"The suite contains **{count} pytest test functions**. Every test and helper in `tests/test_*.py` is listed below.", "", "```bash", "python -m pip install -r requirements-dev.txt", "python -m pytest -q", "```", "", "Run one test with `python -m pytest -q path/to/test.py::test_name`.", ""]
    for file, items in grouped.items():
        lines += [f"## `{file}`", "", TEST_MODULE_SUMMARIES.get(Path(file).name, "Tests and local helpers in this module."), ""]
        tests = [s for s in items if s.name.rsplit(".", 1)[-1].startswith("test_")]
        helpers = [s for s in items if s not in tests]
        if tests:
            lines += ["### Pytest cases", "", "| Test | What it verifies | Primary code exercised |", "| --- | --- | --- |"]
            for symbol in tests:
                lines.append(f"| `{symbol.name}{symbol.signature}` (L{symbol.line}) | {describe(symbol).replace('|', '\\|')} | {relevant_calls(symbol)} |")
            lines.append("")
        if helpers:
            lines += ["### Test helpers", "", "| Helper | Purpose |", "| --- | --- |"]
            for symbol in helpers:
                lines.append(f"| `{symbol.name}{symbol.signature}` (L{symbol.line}) | {describe(symbol).replace('|', '\\|')} |")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_or_check(path: Path, content: str, check: bool) -> bool:
    if check:
        return path.exists() and path.read_text(encoding="utf-8") == content
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate exhaustive function and pytest references.")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    symbols = collect(root)
    outputs = {root / "docs/function_reference.md": function_reference(symbols), root / "docs/test_reference.md": test_reference(symbols)}
    stale = [path for path, content in outputs.items() if not write_or_check(path, content, args.check)]
    for path in stale:
        print(f"Generated documentation is stale: {path.relative_to(root)}")
    runtime = sum(1 for symbol in symbols if not symbol.file.startswith("tests/test_"))
    tests = sum(1 for symbol in symbols if symbol.file.startswith("tests/test_") and symbol.name.rsplit(".", 1)[-1].startswith("test_"))
    print(f"Documented {runtime} runtime/script symbols and {tests} tests.")
    return 1 if stale else 0


if __name__ == "__main__":
    raise SystemExit(main())
