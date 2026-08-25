from __future__ import annotations

from pathlib import Path


TABULAR_FORMATS = {"csv", "tsv", "xlsx", "xls"}


def dataset_format(dataset_path: Path) -> str:
    """Return the normalized dataset format derived from its filename suffix."""
    return dataset_path.suffix.lower().lstrip(".")


def validate_dataset_format_applicability(plan: dict, dataset_path: Path) -> None:
    """Reject a dataset whose format is incompatible with the plan declaration."""
    applicability = plan.get("applicability", {})
    allowed = applicability.get("dataset_formats")
    if not allowed:
        return

    allowed_formats = {str(value).strip().lower().lstrip(".") for value in allowed}
    actual = dataset_format(dataset_path)
    if actual not in allowed_formats:
        raise ValueError(
            f"Dataset format '{actual or 'unknown'}' is not permitted by this plan; "
            f"allowed formats: {', '.join(sorted(allowed_formats))}."
        )


def validate_loaded_dataset_applicability(plan: dict, dataframe) -> None:
    """Validate applicability rules that require access to loaded tabular data."""
    applicability = plan.get("applicability", {})
    if not applicability.get("requires_numeric_fields", False):
        return

    minimum = int(applicability.get("minimum_numeric_fields", 1))
    numeric_columns = list(dataframe.select_dtypes(include="number").columns)
    if len(numeric_columns) < minimum:
        raise ValueError(
            "Dataset does not satisfy the plan's numeric-field applicability requirement: "
            f"found {len(numeric_columns)} numeric columns, requires at least {minimum}."
        )


def enforce_skip_policy(plan: dict, skipped_metrics: dict[str, list[str]], *, dry_run: bool = False) -> None:
    """Enforce execution_policy.allow_skips once field preflight is complete."""
    if not skipped_metrics or dry_run:
        return
    if plan.get("execution_policy", {}).get("allow_skips", False):
        return

    detail = "; ".join(
        f"{metric_id}: {', '.join(fields)}"
        for metric_id, fields in sorted(skipped_metrics.items())
    )
    raise ValueError(
        "Plan execution_policy.allow_skips is false, but required field mappings are missing for: "
        + detail
    )


def collect_reference_paths(plan: dict, *, base_dir: Path | None = None) -> list[Path]:
    """Collect configured reference dataset paths for safety/provenance checks."""
    paths: list[Path] = []
    seen: set[Path] = set()
    for metric in plan.get("metrics", []):
        requirements = metric.get("input_requirements", {})
        parameters = metric.get("calculation", {}).get("parameters", {})
        raw = requirements.get("reference_dataset_path") or parameters.get("reference_dataset_path")
        if not isinstance(raw, str) or not raw.strip():
            continue
        path = Path(raw).expanduser()
        if not path.is_absolute() and base_dir is not None:
            path = base_dir / path
        resolved = path.resolve()
        if resolved not in seen:
            seen.add(resolved)
            paths.append(resolved)
    return paths


def validate_output_path_safety(
    output_path: Path,
    *,
    protected_paths: list[Path],
    allow_overwrite: bool = False,
) -> None:
    """Prevent results from overwriting experiment inputs or existing output accidentally."""
    output = output_path.expanduser().resolve()
    protected = {path.expanduser().resolve() for path in protected_paths if path is not None}
    if output in protected:
        raise ValueError(f"Output path collides with a protected experiment input: {output}")
    if output.exists() and not allow_overwrite:
        raise FileExistsError(
            f"Output already exists: {output}. Re-run with --force-output to replace it."
        )
