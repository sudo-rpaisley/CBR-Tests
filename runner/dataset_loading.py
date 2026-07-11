from __future__ import annotations

import time
from pathlib import Path

from runner.live_rendering import render_live_taxonomy
from runner.progress import render_overall_progress_line, print_live_status
from runner.run_display import compact_overall_progress_line, print_phase_status
from runner.run_plan_helpers import build_base_header_lines, detect_ip_fields, update_live_header
from runner.tabular import load_tabular_dataset

TABULAR_SUFFIXES = {".csv", ".tsv", ".xlsx", ".xls"}


def is_tabular_dataset(dataset_path: Path) -> bool:
    """Return true when the dataset extension is handled by the tabular loader."""
    return dataset_path.suffix.lower() in TABULAR_SUFFIXES


def load_shared_tabular_dataset(
    *,
    dataset_path: Path,
    plan: dict,
    case_id: str,
    output_path: Path,
    metrics: list[dict],
    field_translation: dict[str, str],
    default_metric_predictions: dict[str, float],
    display_mode: str,
    display_max_lines: int | None,
    run_state,
):
    """Load a tabular dataset once while updating the live loading display."""
    print_phase_status("Dataset", "Loading tabular dataset")
    started = time.perf_counter()

    def _chunk_progress(chunk_idx: int, total_rows: int) -> None:
        elapsed = time.perf_counter() - started
        overall_header = render_overall_progress_line(0, len(metrics), 0.0, 0.0)
        compact_overall_header = compact_overall_progress_line(overall_header)
        base_lines = build_base_header_lines(plan, case_id, dataset_path, output_path)
        update_live_header(base_lines, [
            "Status: Loading dataset",
            f"Elapsed: {elapsed:0.1f}s | Chunk: {chunk_idx} | Rows Loaded: {total_rows:,}",
            f"Overall Progress: 0/{len(metrics)} metrics completed",
            compact_overall_header,
        ])
        print_live_status(
            render_live_taxonomy(
                metrics,
                "dataset_loading",
                {},
                {},
                default_metric_predictions,
                max(20.0, float(len(metrics))),
                elapsed=0.0,
                completed=False,
                display_mode=display_mode,
                max_lines=display_max_lines,
                run_state=run_state,
            ),
            "",
            None,
        )

    shared_tabular_df = load_tabular_dataset(
        dataset_path,
        progress_callback=_chunk_progress,
        field_translation=field_translation,
    )
    _update_loaded_dataset_header(plan, case_id, dataset_path, output_path, shared_tabular_df, len(metrics))
    return shared_tabular_df


def _update_loaded_dataset_header(
    plan: dict,
    case_id: str,
    dataset_path: Path,
    output_path: Path,
    shared_tabular_df,
    metric_count: int,
) -> None:
    source_field, destination_field = detect_ip_fields(shared_tabular_df)
    update_live_header([
        f"Run Title: {plan['plan_meta']['name']} ({plan['plan_meta']['plan_id']})",
        f"Case ID: {case_id}",
        f"Rows: {len(shared_tabular_df):,} | Columns: {shared_tabular_df.shape[1]}",
        f"Source Field: {source_field}",
        f"Destination Field: {destination_field}",
        f"Source Path: {dataset_path}",
        f"Destination Output: {output_path}",
    ], [
        "Status: Dataset loaded",
        f"Overall Progress: 0/{metric_count} metrics completed",
    ])
