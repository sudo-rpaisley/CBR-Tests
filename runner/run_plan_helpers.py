import argparse
import json
import os
import signal
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from runner.human_summary import default_human_summary_path, format_human_summary
from runner.progress import set_live_header
from runner.taxonomy import build_plan_taxonomy, build_result_taxonomy, build_test_results_taxonomy
from runner.tui import launch_tui, validate_required_run_args

SOURCE_FIELD_CANDIDATES = ("Source IP", "Src IP", "source_ip", "src_ip")
DESTINATION_FIELD_CANDIDATES = ("Destination IP", "Dst IP", "destination_ip", "dst_ip")


def build_outcome(
    status: str,
    case_id: str,
    plan_id: str,
    metrics: list[dict],
    dataset_path: Path,
    metric_results: list[dict],
    test_results: dict,
    run_started_at: datetime,
    run_start_perf: float,
    column_validations: dict,
    skipped_metrics: list[dict] | None = None,
    all_metrics: list[dict] | None = None,
    provenance: dict | None = None,
) -> dict:
    taxonomy_metrics = all_metrics or metrics
    outcome = {
        "schema_version": 2,
        "status": status,
        "case_id": case_id,
        "plan_id": plan_id,
        "metric_ids": [m["metric_id"] for m in taxonomy_metrics],
        "dataset_path": str(dataset_path),
        "plan_taxonomy": build_plan_taxonomy(taxonomy_metrics),
        "metric_results": list(metric_results),
        "test_results": test_results,
        "test_results_taxonomy": build_test_results_taxonomy(taxonomy_metrics, test_results),
        "result_taxonomy": build_result_taxonomy(taxonomy_metrics, metric_results, test_results),
        "run_started_at": run_started_at.isoformat(),
        "run_finished_at": datetime.now(timezone.utc).isoformat(),
        "run_elapsed_seconds": round(time.perf_counter() - run_start_perf, 6),
    }
    if provenance:
        outcome["run_id"] = provenance.get("run_id")
        outcome["provenance"] = provenance
    if column_validations:
        outcome["column_validations"] = column_validations
    if skipped_metrics:
        outcome["skipped_metrics"] = skipped_metrics
        outcome["metric_results"].extend(skipped_metrics)
    return outcome


def _open_atomic_text_file(destination: Path) -> tuple[int, Path]:
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=destination.parent,
        text=True,
    )
    return fd, Path(temporary_name)


def write_outcome(output_path: Path, outcome: dict) -> Path:
    """Write the authoritative JSON outcome and its human-readable Markdown companion.

    Both files are fully written and fsynced before either destination is replaced.
    The summary is published first and the JSON outcome second, so publication of a
    new JSON outcome implies that its companion summary was already published.

    Returns the companion summary path.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path = default_human_summary_path(output_path)
    summary_text = format_human_summary(outcome, outcome_path=output_path)

    json_fd, temporary_json_path = _open_atomic_text_file(output_path)
    summary_fd, temporary_summary_path = _open_atomic_text_file(summary_path)
    try:
        with os.fdopen(json_fd, "w", encoding="utf-8") as handle:
            json.dump(outcome, handle, indent=2, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())

        with os.fdopen(summary_fd, "w", encoding="utf-8") as handle:
            handle.write(summary_text)
            if not summary_text.endswith("\n"):
                handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary_summary_path, summary_path)
        os.replace(temporary_json_path, output_path)
    except Exception:
        temporary_json_path.unlink(missing_ok=True)
        temporary_summary_path.unlink(missing_ok=True)
        raise

    return summary_path


def detect_ip_fields(tabular_df) -> tuple[str, str]:
    source_field = next((c for c in SOURCE_FIELD_CANDIDATES if c in tabular_df.columns), "n/a")
    destination_field = next((c for c in DESTINATION_FIELD_CANDIDATES if c in tabular_df.columns), "n/a")
    return source_field, destination_field


def build_title_box_lines(lines: list[str], status_lines: list[str] | None = None, width: int = 108) -> list[str]:
    framed = ["=" * width]
    for line in lines:
        framed.append(f"| {line[:width-4].ljust(width-4)} |")
    if status_lines:
        framed.append(f"| {'-' * (width-4)} |")
        for status_line in status_lines:
            framed.append(f"| {status_line[:width-4].ljust(width-4)} |")
    framed.append("=" * width)
    return framed


def build_base_header_lines(
    plan: dict,
    case_id: str,
    dataset_path: Path,
    output_path: Path,
    include_dataset_size: bool = False,
) -> list[str]:
    lines = [
        f"Run Title: {plan['plan_meta']['name']} ({plan['plan_meta']['plan_id']})",
        f"Case ID: {case_id}",
    ]
    if include_dataset_size:
        dataset_name = dataset_path.name
        dataset_size = dataset_path.stat().st_size if dataset_path.exists() else 0
        dataset_size_mb = round(dataset_size / (1024 * 1024), 2)
        lines.append(f"Source Dataset: {dataset_name} ({dataset_size_mb} MB)")
    lines.extend([
        f"Source Path: {dataset_path}",
        f"Destination Output: {output_path}",
    ])
    return lines


def configure_signal_handlers(control_state: dict, shutdown_requested: dict) -> None:
    def _handle_sigint(_signum, _frame):
        control_state["cancel_requested"] = True
        shutdown_requested["requested"] = True
        shutdown_requested["confirm_before"] = time.time()
        print("\nStop requested. Cancelling current task and pending tasks...")

    def _handle_sigusr1(_signum, _frame):
        control_state["pause_requested"] = True
        print("\nPause requested (SIGUSR1). Send SIGUSR2 to resume.")

    def _handle_sigusr2(_signum, _frame):
        control_state["pause_requested"] = False
        print("\nResume requested (SIGUSR2).")

    signal.signal(signal.SIGINT, _handle_sigint)
    if hasattr(signal, "SIGUSR1"):
        signal.signal(signal.SIGUSR1, _handle_sigusr1)
    if hasattr(signal, "SIGUSR2"):
        signal.signal(signal.SIGUSR2, _handle_sigusr2)


def parse_run_plan_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a test plan from a case JSON or use --tui to configure a run interactively.")
    parser.add_argument(
        "--tui",
        action="store_true",
        help="Open the curses terminal UI to select a case/plan, dataset and run options",
    )
    parser.add_argument("--case", help="Path to case JSON or plan JSON file; required unless selected in --tui")
    parser.add_argument("--dataset", help="Dataset path (required when --case points to a plan JSON)")
    parser.add_argument("--output", help="Output path (required when --case points to a plan JSON)")
    parser.add_argument("--case-id", default="ad_hoc_case", help="Case ID used when running a plan JSON directly")
    parser.add_argument("--force-output", action="store_true", help="Allow replacement of an existing output file")
    parser.add_argument("--field-translation", help="Optional JSON file mapping dataset column names to test field names")
    parser.add_argument(
        "--no-update-field-translation",
        action="store_true",
        help="Run without creating or updating dataset sidecar field translation templates",
    )
    parser.add_argument(
        "--field-translation-dry-run",
        action="store_true",
        help="Validate/generate field translations and report skipped metrics without running metrics",
    )
    parser.add_argument(
        "--field-translation-report",
        help="Optional JSON report path for field translation validation and skipped metric details",
    )
    parser.add_argument(
        "--yes-field-translation-sidecar",
        action="store_true",
        help="Create or update sidecar field translation templates without prompting",
    )
    parser.add_argument(
        "--field-translation-text-report",
        help="Optional text report path for a human-readable field translation summary",
    )
    parser.add_argument(
        "--field-translation-markdown-report",
        help="Optional Markdown report path for field translation validation details",
    )
    parser.add_argument("--taxonomy-file", help="Optional taxonomy JSON used to order metrics")
    parser.add_argument(
        "--taxonomy-strict",
        action="store_true",
        help="Fail if enabled plan metrics are missing from taxonomy order",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Optional worker count override. Use 1 to force serial execution.",
    )
    parser.add_argument(
        "--display",
        choices=("compact", "full", "quiet", "interactive"),
        default="compact",
        help=(
            "Live display mode: compact fits tmux panes, full shows every metric, "
            "quiet suppresses live taxonomy updates, and interactive shows the ANSI dashboard."
        ),
    )
    args = parser.parse_args()
    args.tui_session = bool(args.tui)
    if args.tui:
        if "--display" not in sys.argv:
            args.display = "interactive"
        if not sys.stdin.isatty() or not sys.stdout.isatty():
            raise SystemExit("error: --tui requires an interactive terminal")
        args = launch_tui(args)
        args.tui_session = True
    validate_required_run_args(args)
    return args


def update_live_header(
    lines: list[str],
    status_lines: list[str] | None = None,
    width: int = 108,
) -> None:
    set_live_header(build_title_box_lines(lines, status_lines, width=width))
