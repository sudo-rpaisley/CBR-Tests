from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from runner.schema import validate_plan_schema


REFERENCE_PATH_KEY = "reference_dataset_path"
PCAP_SUFFIXES = {".pcap", ".pcapng"}


def reference_bound_metric_ids(plan: dict) -> list[str]:
    """Return enabled metric IDs whose requirements contain a reference dataset path."""

    metric_ids: list[str] = []
    for metric in plan.get("metrics", []):
        if not isinstance(metric, dict) or not metric.get("enabled", True):
            continue
        requirements = metric.get("input_requirements")
        if isinstance(requirements, dict) and REFERENCE_PATH_KEY in requirements:
            metric_ids.append(str(metric.get("metric_id") or ""))
    return [metric_id for metric_id in metric_ids if metric_id]


def bind_runtime_reference(plan: dict, reference_dataset_path: Path | str) -> dict:
    """Bind one PCAP/PCAPNG reference path into an existing reference-enabled plan.

    The scientific plan structure is preserved. Only the reference dataset path in
    already-configured reference metrics is replaced, allowing one validated PCAP
    plan to be reused across a matrix of candidate/reference dataset bindings.
    """

    reference_path = Path(reference_dataset_path).expanduser().resolve()
    if not reference_path.is_file():
        raise FileNotFoundError(f"Reference dataset does not exist: {reference_path}")
    if reference_path.suffix.lower() not in PCAP_SUFFIXES:
        raise ValueError("Runtime reference binding currently supports only PCAP/PCAPNG references.")

    bound = deepcopy(plan)
    bound_ids = reference_bound_metric_ids(bound)
    if not bound_ids:
        raise ValueError(
            "The selected plan has no reference-enabled metrics. Build the PCAP plan once with "
            "an independent PCAP reference so the reference metric definitions are present, then "
            "reuse that plan for matrix dataset mapping."
        )

    for metric in bound.get("metrics", []):
        if not isinstance(metric, dict):
            continue
        requirements = metric.get("input_requirements")
        if isinstance(requirements, dict) and REFERENCE_PATH_KEY in requirements:
            requirements[REFERENCE_PATH_KEY] = str(reference_path)

    creation = bound.get("plan_creation")
    if isinstance(creation, dict):
        creation["reference_dataset"] = str(reference_path)
        creation["reference_dataset_format"] = reference_path.suffix.lower().lstrip(".")
        creation["runtime_reference_binding"] = True

    validate_plan_schema(bound)
    return bound
