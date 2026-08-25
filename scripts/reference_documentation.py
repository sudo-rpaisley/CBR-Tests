from __future__ import annotations

import argparse
import ast
import re
from dataclasses import dataclass
from pathlib import Path

EXCLUDED_PARTS = {".git", ".pytest_cache", ".venv", "venv", "__pycache__"}

MODULE_SUMMARIES = {
    "run_plan.py": "Top-level command workflow from parsed arguments to the atomic outcome JSON.",
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
    "runner/field_translation_workflow.py": "Translation preflight and requested-report workflow.",
    "cbr_tests/metrics/column_quality.py": "Column completeness, numeric usability, and variation metrics.",
    "cbr_tests/metrics/data_quality.py": "Missing-value and duplicate-row metrics.",
    "cbr_tests/metrics/pearson.py": "Numeric validation and Pearson correlation profiles.",
    "cbr_tests/metrics/spearman.py": "Spearman rank-correlation profiles.",
    "cbr_tests/metrics/statistical.py": "Internal distribution drift and distance-correlation calculations.",
    "cbr_tests/metrics/temporal.py": "Timestamp, duration, timing-drift, hourly, and periodicity calculations.",
    "cbr_tests/metrics/task_validation.py": "Accuracy and binary precision, recall, and F1 calculations.",
    "cbr_tests/metrics/timestamp_coherence.py": "Raw packet timestamp coherence scanning.",
    "tests/label_fidelity_profile.py": "Label-integrity metric implementations awaiting package migration.",
    "tests/slice_representation_profile.py": "Slice-representation metric implementations awaiting package migration.",
    "tests/reference_model_comparison_profile.py": "Reference-comparison metric implementations awaiting package migration.",
    "scripts/build_documentation_inventory.py": "Repository inventory generator used for documentation audits.",
    "scripts/build_reference_documentation.py": "Stable command wrapper for generated references.",
    "scripts/reference_documentation.py": "AST engine that generates the exhaustive function and test references.",
}

TEST_MODULE_SUMMARIES = {
    "test_correctness_reproducibility.py": "Parallel correctness, timestamps, atomic writes, and schema regression coverage.",
    "test_field_translation.py": "Translation loading, detection, sidecars, reports, suggestions, and formatting.",
    "test_run_plan_field_translation.py": "Command-level translation dry-run and sidecar behavior.",
    "test_runner_execution_invariants.py": "Execution and live-rendering invariants.",
    "test_runner_progress_invariants.py": "Progress and terminal-output invariants.",
    "test_runner_schema_taxonomy.py": "Plan schema, registry, and taxonomy behavior.",
    "test_metric_package_layout.py": "Production-package and compatibility-import boundaries.",
}

EXACT_DESCRIPTIONS = {
    "write_outcome": "Writes JSON through a flushed and fsynced temporary file, then atomically replaces the destination.",
    "validate_plan_schema": "Validates plan metadata, execution policy, metric IDs, taxonomy paths, requirements, calculation blocks, and retention blocks.",
    "run_metrics_parallel": "Runs metrics with bounded thread submission, timing, pause/cancel controls, and explicit fail-fast not-run records.",
    "run_serial_metrics": "Runs metrics one at a time, updates display and telemetry, and builds an outcome.",
    "build_metric_handlers": "Builds the metric-ID-to-callable mapping for a run.",
    "prepare_run_context": "Resolves and validates all inputs and creates the initial run state.",
    "collect_parallel_metric_results": "Restores plan order and aggregates parallel records, test results, and validations.",
    "translate_metric_fields": "Returns a metric copy with canonical field references resolved to dataset columns.",
    "_ks_statistic": "Computes the largest empirical-CDF difference between two samples.",
    "_wasserstein_distance": "Computes one-dimensional Wasserstein distance from empirical CDFs.",
    "_energy_distance": "Computes the sample energy-distance expression from cross- and within-sample distances.",
    "_rbf_mmd": "Computes squared RBF-kernel MMD with a median-distance bandwidth when gamma is omitted.",
    "_distance_correlation": "Computes distance correlation from double-centered pairwise distance matrices.",
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


def _annotation(node: ast.expr | None) -> str | None:
    return ast.unparse(node) if node is not None else None


def _argument(argument: ast.arg, default: ast.expr | None = None) -> str:
    text = argument.arg
    annotation = _annotation(argument.annotation)
    if annotation:
        text += f": {annotation}"
    if default is not None:
        text += f" = {ast.unparse(default)}"
    return text


def function_signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    args = node.args
    parts: list[str] = []
    positional = [*args.posonlyargs, *args.args]
    defaults: list[ast.expr | None] = [None] * (len(positional) - len(args.defaults)) + list(args.defaults)

    for index, (argument, default) in enumerate(zip(positional, defaults)):
        parts.append(_argument(argument, default))
        if args.posonlyargs and index + 1 == len(args.posonlyargs):
            parts.append("/")

    if args.vararg is not None:
        vararg = _argument(args.vararg)
        parts.append(f"*{vararg}")
    elif args.kwonlyargs:
        parts.append("*")

    for argument, default in zip(args.kwonlyargs, args.kw_defaults):
        parts.append(_argument(argument, default))

    if args.kwarg is not None:
        kwarg = _argument(args.kwarg)
        parts.append(f"**{kwarg}")

    signature = f"({', '.join(parts)})"
    returns = _annotation(node.returns)
    if returns:
        signature += f" -> {returns}"
    return signature


def called_names(node: ast.AST) -> tuple[str, ...]:
    values: list[str] = []
    for child in ast.walk(node):
        if not isinstance(child, ast.Call):
            continue
        try:
            value = ast.unparse(child.func)
        except (ValueError, TypeError):
            continue
        if value not in values:
            values.append(value)
    return tuple(values)


def walk_symbols(
    file: Path,
    root: Path,
    body: list[ast.stmt],
    prefix: str = "",
    parent_kind: str | None = None,
) -> list[Symbol]:
    symbols: list[Symbol] = []
    relative = file.relative_to(root).as_posix()
    for node in body:
        if isinstance(node, ast.ClassDef):
            name = prefix + node.name
            symbols.append(Symbol(relative, name, "class", "", node.lineno, ast.get_docstring(node, clean=True), ()))
            symbols.extend(walk_symbols(file, root, node.body, name + ".", "class"))
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            name = prefix + node.name
            if parent_kind == "class":
                kind = "method"
            elif prefix:
                kind = "nested function"
            else:
                kind = "function"
            symbols.append(
                Symbol(
                    relative,
                    name,
                    kind,
                    function_signature(node),
                    node.lineno,
                    ast.get_docstring(node, clean=True),
                    called_names(node),
                )
            )
            symbols.extend(walk_symbols(file, root, node.body, name + ".", "function"))
    return symbols


def collect_symbols(root: Path) -> list[Symbol]:
    symbols: list[Symbol] = []
    for file in sorted(root.rglob("*.py")):
        if any(part in EXCLUDED_PARTS for part in file.parts):
            continue
        try:
            tree = ast.parse(file.read_text(encoding="utf-8"), filename=str(file))
        except (OSError, UnicodeDecodeError, SyntaxError):
            continue
        symbols.extend(walk_symbols(file, root, tree.body))
    return symbols


def words(name: str) -> str:
    value = name.rsplit(".", 1)[-1].lstrip("_").replace("_", " ")
    value = re.sub(r"\s+", " ", value).strip()
    replacements = {
        "df": "dataframe",
        "ip": "IP",
        "pcap": "PCAP",
        "tcp": "TCP",
        "json": "JSON",
        "mmd": "MMD",
        "ks": "KS",
        "cli": "CLI",
    }
    return " ".join(replacements.get(word, word) for word in value.split())


def describe_symbol(symbol: Symbol) -> str:
    leaf = symbol.name.rsplit(".", 1)[-1]
    if symbol.docstring:
        text = " ".join(symbol.docstring.split())
        return text if text.endswith(".") else text + "."
    if leaf in EXACT_DESCRIPTIONS:
        return EXACT_DESCRIPTIONS[leaf]

    prefix_templates = {
        "test_": "Verifies that {}.",
        "compute_": "Computes {} and returns a structured result.",
        "run_": "Runs {}.",
        "build_": "Builds {}.",
        "render_": "Renders {} for terminal output.",
        "format_": "Formats {} for human-readable output.",
        "load_": "Loads {}.",
        "write_": "Writes {}.",
        "validate_": "Validates {}.",
        "detect_": "Detects {}.",
        "collect_": "Collects {}.",
        "parse_": "Parses {}.",
        "prepare_": "Prepares {}.",
        "configure_": "Configures {}.",
        "resolve_": "Resolves {}.",
        "classify_": "Classifies {}.",
        "normalise_": "Normalizes {}.",
        "normalize_": "Normalizes {}.",
        "merge_": "Merges {}.",
        "update_": "Updates {}.",
    }
    for prefix, template in prefix_templates.items():
        if leaf.startswith(prefix):
            return template.format(words(leaf[len(prefix) :]))

    subject = words(leaf)
    return f"Data model for {subject}." if symbol.kind == "class" else f"Implementation helper for {subject}."


def module_summary(path: str) -> str:
    if path in MODULE_SUMMARIES:
        return MODULE_SUMMARIES[path]
    if path.startswith("tests/metrics/"):
        return "Network/protocol realism implementation module awaiting migration from the test package."
    if path.startswith("tests/"):
        return "Compatibility or not-yet-migrated metric implementation module."
    return f"Python symbols defined by `{path}`."


def escape_table_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


def render_function_reference(symbols: list[Symbol]) -> str:
    grouped: dict[str, list[Symbol]] = {}
    for symbol in symbols:
        if not symbol.file.startswith("tests/test_"):
            grouped.setdefault(symbol.file, []).append(symbol)

    lines = [
        "# Function and class reference",
        "",
        "Exhaustive AST-generated reference for runtime code, metric implementations, compatibility modules, and scripts. Nested helpers are included. Actual pytest cases are documented in [Test suite reference](test_reference.md).",
        "",
        "**Public** only means the leaf name lacks a leading underscore; it is not an API-stability promise. Nested functions are always internal.",
        "",
    ]
    for file, items in grouped.items():
        lines.extend(
            [
                f"## `{file}`",
                "",
                module_summary(file),
                "",
                "| Symbol | Kind | Visibility | Purpose |",
                "| --- | --- | --- | --- |",
            ]
        )
        for symbol in items:
            leaf = symbol.name.rsplit(".", 1)[-1]
            visibility = "Internal" if symbol.kind == "nested function" or leaf.startswith("_") else "Public"
            purpose = escape_table_cell(describe_symbol(symbol))
            lines.append(
                f"| `{symbol.name}{symbol.signature}` (L{symbol.line}) | {symbol.kind} | {visibility} | {purpose} |"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def relevant_calls(symbol: Symbol) -> str:
    ignored_prefixes = (
        "assert",
        "pd.",
        "json.",
        "Path",
        "len",
        "str",
        "int",
        "float",
        "list",
        "dict",
        "set",
        "tuple",
        "sorted",
        "round",
        "sum",
        "min",
        "max",
        "range",
        "enumerate",
        "zip",
        "isinstance",
        "open",
    )
    values = [value for value in symbol.calls if not value.startswith(ignored_prefixes)][:8]
    return ", ".join(f"`{value}`" for value in values) or "Assertions and fixtures in the module"


def render_test_reference(symbols: list[Symbol]) -> str:
    grouped: dict[str, list[Symbol]] = {}
    for symbol in symbols:
        if symbol.file.startswith("tests/test_"):
            grouped.setdefault(symbol.file, []).append(symbol)

    test_count = sum(
        1
        for items in grouped.values()
        for symbol in items
        if symbol.name.rsplit(".", 1)[-1].startswith("test_")
    )
    lines = [
        "# Test suite reference",
        "",
        f"The suite contains **{test_count} pytest test functions**. Every test and helper in `tests/test_*.py` is listed below.",
        "",
        "```bash",
        "python -m pip install -r requirements-dev.txt",
        "python -m pytest -q",
        "```",
        "",
        "Run one test with `python -m pytest -q path/to/test.py::test_name`.",
        "",
    ]

    for file, items in grouped.items():
        lines.extend(
            [
                f"## `{file}`",
                "",
                TEST_MODULE_SUMMARIES.get(Path(file).name, "Tests and local helpers in this module."),
                "",
            ]
        )
        tests = [item for item in items if item.name.rsplit(".", 1)[-1].startswith("test_")]
        helpers = [item for item in items if item not in tests]

        if tests:
            lines.extend(
                [
                    "### Pytest cases",
                    "",
                    "| Test | What it verifies | Primary code exercised |",
                    "| --- | --- | --- |",
                ]
            )
            for symbol in tests:
                purpose = escape_table_cell(describe_symbol(symbol))
                calls = escape_table_cell(relevant_calls(symbol))
                lines.append(
                    f"| `{symbol.name}{symbol.signature}` (L{symbol.line}) | {purpose} | {calls} |"
                )
            lines.append("")

        if helpers:
            lines.extend(["### Test helpers", "", "| Helper | Purpose |", "| --- | --- |"])
            for symbol in helpers:
                purpose = escape_table_cell(describe_symbol(symbol))
                lines.append(f"| `{symbol.name}{symbol.signature}` (L{symbol.line}) | {purpose} |")
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def write_or_check(path: Path, content: str, check: bool) -> bool:
    if check:
        return path.exists() and path.read_text(encoding="utf-8") == content
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate exhaustive function and pytest references.")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--check", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.root.resolve()
    symbols = collect_symbols(root)
    outputs = {
        root / "docs/function_reference.md": render_function_reference(symbols),
        root / "docs/test_reference.md": render_test_reference(symbols),
    }
    stale = [path for path, content in outputs.items() if not write_or_check(path, content, args.check)]
    for path in stale:
        print(f"Generated documentation is stale: {path.relative_to(root)}")

    runtime_count = sum(1 for symbol in symbols if not symbol.file.startswith("tests/test_"))
    test_count = sum(
        1
        for symbol in symbols
        if symbol.file.startswith("tests/test_")
        and symbol.name.rsplit(".", 1)[-1].startswith("test_")
    )
    print(f"Documented {runtime_count} runtime/script symbols and {test_count} tests.")
    return 1 if stale else 0
