from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile

from runner.plan_builder import write_plan
from runner.reference_binding import PCAP_SUFFIXES, bind_runtime_reference, reference_bound_metric_ids
from runner.schema import validate_plan_schema


BATCH_SCHEMA_VERSION = 1


def _slug(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9]+", "-", str(value).strip()).strip("-").lower()
    return value or "pcap-matrix"


def _portable_path(path: Path, repo_root: Path) -> str:
    resolved = path.expanduser().resolve()
    try:
        return str(resolved.relative_to(repo_root.resolve()))
    except ValueError:
        return str(resolved)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json_atomic(path: Path, payload: dict, *, overwrite: bool) -> Path:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        raise FileExistsError(f"File already exists: {path}. Enable replacement to overwrite it.")
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent, text=True)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise
    return path


def _load_plan(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Plan JSON must contain an object.")
    validate_plan_schema(payload)
    formats = {str(value).lower() for value in payload.get("applicability", {}).get("dataset_formats", [])}
    if not formats.intersection({"pcap", "pcapng"}):
        raise ValueError("The selected plan is not applicable to PCAP/PCAPNG datasets.")
    return payload


def _normalise_pcaps(values: list[Path | str], *, label: str) -> list[Path]:
    output: list[Path] = []
    seen: set[Path] = set()
    for value in values:
        path = Path(value).expanduser().resolve()
        if path in seen:
            continue
        if not path.is_file():
            raise FileNotFoundError(f"{label} dataset does not exist: {path}")
        if path.suffix.lower() not in PCAP_SUFFIXES:
            raise ValueError(f"{label} dataset is not PCAP/PCAPNG: {path}")
        seen.add(path)
        output.append(path)
    if not output and label == "Candidate":
        raise ValueError("Select at least one candidate PCAP/PCAPNG dataset.")
    return output


def build_fixed_plan_matrix(
    *,
    name: str,
    plan_path: Path | str,
    candidate_paths: list[Path | str],
    reference_paths: list[Path | str] | None = None,
    output_path: Path | str,
    overwrite: bool = False,
    repo_root: Path | None = None,
) -> Path:
    """Map PCAP datasets onto one existing scientific plan and write a batch manifest.

    The source plan is never regenerated from individual datasets. Candidate-only
    jobs use the source plan directly. For reference matrices, one deterministic
    binding copy is written per reference dataset; each copy changes only the
    already-configured reference dataset path and records the source plan hash.
    """

    root = (repo_root or Path.cwd()).expanduser().resolve()
    source_plan_path = Path(plan_path).expanduser()
    if not source_plan_path.is_absolute():
        source_plan_path = root / source_plan_path
    source_plan_path = source_plan_path.resolve()
    if not source_plan_path.is_file():
        raise FileNotFoundError(f"Plan does not exist: {source_plan_path}")
    source_plan = _load_plan(source_plan_path)
    candidates = _normalise_pcaps(list(candidate_paths), label="Candidate")
    references = _normalise_pcaps(list(reference_paths or []), label="Reference")

    clean_name = str(name).strip()
    if not clean_name:
        raise ValueError("Matrix name is required.")
    batch_id = _slug(clean_name)
    output = Path(output_path).expanduser()
    if not output.is_absolute():
        output = root / output
    output = output.resolve()

    reference_metric_ids = reference_bound_metric_ids(source_plan)
    if references and not reference_metric_ids:
        raise ValueError(
            "References were selected but the PCAP plan has no reference-enabled metrics. "
            "Build the plan once with any independent PCAP reference, then reuse it here."
        )

    combinations: list[tuple[Path, Path | None]] = []
    if references:
        for candidate in candidates:
            for reference in references:
                if candidate == reference:
                    continue
                combinations.append((candidate, reference))
    else:
        combinations = [(candidate, None) for candidate in candidates]
    if not combinations:
        raise ValueError("No jobs remain after excluding candidate/reference self-comparisons.")

    source_hash = _file_sha256(source_plan_path)
    bindings_dir = output.parent / f"{batch_id}_reference_bindings"
    reference_plan_paths: dict[Path, Path] = {}
    if references:
        for index, reference in enumerate(references, start=1):
            bound = bind_runtime_reference(source_plan, reference)
            creation = bound.setdefault("plan_creation", {})
            creation["fixed_plan_matrix_binding"] = {
                "source_plan": _portable_path(source_plan_path, root),
                "source_plan_sha256": source_hash,
                "reference_dataset": str(reference),
                "binding_only": True,
            }
            reference_slug = _slug(reference.stem)
            bound_path = bindings_dir / f"{index:02d}_{reference_slug}_plan.json"
            write_plan(bound_path, bound, overwrite=overwrite)
            reference_plan_paths[reference] = bound_path

    jobs: list[dict] = []
    used_ids: set[str] = set()
    metric_ids = [
        str(metric.get("metric_id"))
        for metric in source_plan.get("metrics", [])
        if isinstance(metric, dict) and metric.get("enabled", True) and metric.get("metric_id")
    ]
    for index, (candidate, reference) in enumerate(combinations, start=1):
        candidate_slug = _slug(candidate.stem)
        job_slug = candidate_slug if reference is None else f"{candidate_slug}-vs-{_slug(reference.stem)}"
        job_id = f"{batch_id}-{index:03d}-{job_slug}"
        while job_id in used_ids:
            job_id += "-x"
        used_ids.add(job_id)
        selected_plan = source_plan_path if reference is None else reference_plan_paths[reference]
        jobs.append(
            {
                "job_id": job_id,
                "dataset_path": str(candidate),
                "reference_dataset_path": str(reference) if reference is not None else None,
                "plan_path": _portable_path(selected_plan, root),
                "runnable_metric_count": len(metric_ids),
                "metric_ids": metric_ids,
            }
        )

    payload = {
        "schema_version": BATCH_SCHEMA_VERSION,
        "batch_meta": {
            "batch_id": batch_id,
            "name": clean_name,
            "description": "Fixed PCAP plan with dataset/reference bindings only.",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "execution_mode": "sequential",
            "dataset_count": len(candidates),
            "reference_dataset_count": len(references),
            "job_count": len(jobs),
            "comparison_mode": "candidate_reference_matrix" if references else "dataset_batch",
            "metric_policy": "fixed_plan",
            "plan_binding_mode": "fixed_pcap_plan",
            "source_plan": _portable_path(source_plan_path, root),
            "source_plan_sha256": source_hash,
        },
        "output_directory": str(Path("outcomes") / batch_id),
        "common_metric_ids": metric_ids,
        "reference_datasets": [str(path) for path in references],
        "reference_dataset": str(references[0]) if len(references) == 1 else None,
        "jobs": jobs,
    }
    return _write_json_atomic(output, payload, overwrite=overwrite)
