from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from runner.io import load_case_or_plan
from runner.order import load_taxonomy_order, order_metrics_by_taxonomy
from runner.run_display import configure_display
from runner.run_plan_helpers import configure_signal_handlers
from runner.schema import validate_plan_schema
from runner.telemetry import RunState


@dataclass
class PreparedRunContext:
    """Resolved run inputs and display/control state needed by the runner."""

    shutdown_requested: dict
    control_state: dict
    live_render_enabled: bool
    display_mode: str
    display_max_lines: int | None
    default_metric_predictions: dict[str, float]
    plan: dict
    dataset_path: Path
    output_path: Path
    case_id: str
    translation_path: Path | None
    metrics: list[dict]
    all_enabled_metrics: list[dict]
    run_state: RunState


def prepare_run_context(args, default_metric_predictions: dict[str, float]) -> PreparedRunContext:
    """Resolve CLI inputs, validate the plan, and prepare run state."""
    shutdown_requested = {"requested": False, "confirm_before": 0.0}
    control_state = {"pause_requested": False, "cancel_requested": False}
    live_render_enabled, display_mode, display_max_lines = configure_display(args)
    metric_predictions = dict(default_metric_predictions)

    configure_signal_handlers(control_state, shutdown_requested)

    case_file = Path(args.case).expanduser().resolve()
    if not case_file.exists():
        raise FileNotFoundError(f"Case or plan file does not exist: {case_file}")

    plan, dataset_path, output_path, case_id, translation_path = load_case_or_plan(
        case_file,
        args.dataset,
        args.output,
        args.case_id,
        args.field_translation,
    )
    validate_plan_schema(plan)

    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset does not exist: {dataset_path}")
    if dataset_path.is_dir():
        raise IsADirectoryError(f"Dataset path must be a file: {dataset_path}")

    metrics = [metric for metric in plan["metrics"] if metric.get("enabled", True)]
    all_enabled_metrics = list(metrics)
    if args.taxonomy_file:
        taxonomy_file = Path(args.taxonomy_file).expanduser().resolve()
        if not taxonomy_file.exists():
            raise FileNotFoundError(f"Taxonomy file does not exist: {taxonomy_file}")
        taxonomy_ranks = load_taxonomy_order(taxonomy_file)
        metrics = order_metrics_by_taxonomy(metrics, taxonomy_ranks, strict=args.taxonomy_strict)
        all_enabled_metrics = list(metrics)
    if not metrics:
        raise ValueError("The plan does not contain any enabled metrics.")

    run_state = RunState.from_plan(
        case_id=case_id,
        plan=plan,
        metrics=all_enabled_metrics,
        dataset_path=dataset_path,
        output_path=output_path,
        started_at=datetime.now(timezone.utc),
    )

    return PreparedRunContext(
        shutdown_requested=shutdown_requested,
        control_state=control_state,
        live_render_enabled=live_render_enabled,
        display_mode=display_mode,
        display_max_lines=display_max_lines,
        default_metric_predictions=metric_predictions,
        plan=plan,
        dataset_path=dataset_path,
        output_path=output_path,
        case_id=case_id,
        translation_path=translation_path,
        metrics=metrics,
        all_enabled_metrics=all_enabled_metrics,
        run_state=run_state,
    )
