from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Iterable


CAMPAIGN_SCHEMA_VERSION = 1
CAMPAIGN_STATE_SCHEMA_VERSION = 1


def slug(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9]+", "-", value).strip("-").lower()
    return value or "comparison-campaign"


def resolve_repo_path(repo_root: Path, value: str | Path) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (repo_root / path).resolve()


def portable_path(path: Path, repo_root: Path) -> str:
    resolved = path.expanduser().resolve()
    try:
        return str(resolved.relative_to(repo_root.expanduser().resolve()))
    except ValueError:
        return str(resolved)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON document must be an object: {path}")
    return payload


def validate_batch_manifest(path: Path) -> dict[str, Any]:
    payload = _read_json(path)
    if payload.get("schema_version") != 1:
        raise ValueError(f"Not a supported CBR-Tests batch manifest: {path}")
    meta = payload.get("batch_meta")
    jobs = payload.get("jobs")
    if not isinstance(meta, dict) or not str(meta.get("batch_id") or "").strip():
        raise ValueError(f"Batch manifest is missing batch_meta.batch_id: {path}")
    if not isinstance(jobs, list) or not jobs:
        raise ValueError(f"Batch manifest has no jobs: {path}")
    for index, job in enumerate(jobs):
        if not isinstance(job, dict):
            raise ValueError(f"Batch manifest jobs[{index}] is not an object: {path}")
        for field in ("job_id", "dataset_path", "plan_path"):
            if not str(job.get(field) or "").strip():
                raise ValueError(f"Batch manifest jobs[{index}].{field} is required: {path}")
    return payload


def build_campaign(
    *,
    name: str,
    batch_paths: Iterable[Path],
    repo_root: Path,
    description: str = "",
) -> dict[str, Any]:
    name = name.strip()
    if not name:
        raise ValueError("Campaign name is required.")

    root = repo_root.expanduser().resolve()
    resolved_batches = [Path(path).expanduser().resolve() for path in batch_paths]
    if not resolved_batches:
        raise ValueError("A comparison campaign must contain at least one batch matrix.")

    campaign_id = slug(name)
    matrices: list[dict[str, Any]] = []
    total_jobs = 0
    for index, batch_path in enumerate(resolved_batches, start=1):
        batch = validate_batch_manifest(batch_path)
        meta = batch["batch_meta"]
        batch_id = str(meta["batch_id"])
        job_count = len(batch["jobs"])
        total_jobs += job_count
        matrices.append(
            {
                "queue_id": f"matrix-{index:03d}-{slug(batch_id)}",
                "position": index,
                "batch_id": batch_id,
                "batch_name": str(meta.get("name") or batch_id),
                "batch_path": portable_path(batch_path, root),
                "job_count": job_count,
                "comparison_mode": meta.get("comparison_mode"),
                "metric_policy": meta.get("metric_policy"),
            }
        )

    return {
        "schema_version": CAMPAIGN_SCHEMA_VERSION,
        "campaign_meta": {
            "campaign_id": campaign_id,
            "name": name,
            "description": description.strip(),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "execution_mode": "sequential_matrices",
            "matrix_count": len(matrices),
            "job_count": total_jobs,
        },
        "output_directory": str(Path("outcomes") / campaign_id),
        "matrices": matrices,
    }


def load_campaign(path: Path) -> dict[str, Any]:
    payload = _read_json(path)
    if payload.get("schema_version") != CAMPAIGN_SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported campaign schema version: {payload.get('schema_version')!r}; "
            f"expected {CAMPAIGN_SCHEMA_VERSION}."
        )
    meta = payload.get("campaign_meta")
    if not isinstance(meta, dict) or not str(meta.get("campaign_id") or "").strip():
        raise ValueError("Campaign manifest must include campaign_meta.campaign_id.")
    matrices = payload.get("matrices")
    if not isinstance(matrices, list) or not matrices:
        raise ValueError("Campaign manifest must include a non-empty matrices queue.")

    queue_ids: set[str] = set()
    for index, matrix in enumerate(matrices):
        if not isinstance(matrix, dict):
            raise ValueError(f"matrices[{index}] must be an object.")
        for field in ("queue_id", "batch_id", "batch_path"):
            if not str(matrix.get(field) or "").strip():
                raise ValueError(f"matrices[{index}].{field} is required.")
        queue_id = str(matrix["queue_id"])
        if queue_id in queue_ids:
            raise ValueError(f"Duplicate campaign queue_id: {queue_id}")
        queue_ids.add(queue_id)
    return payload


def _write_json_atomic(path: Path, payload: dict[str, Any], *, overwrite: bool = True) -> Path:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        raise FileExistsError(f"File already exists: {path}")
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


def write_campaign(path: Path, payload: dict[str, Any], *, overwrite: bool = False) -> Path:
    load_campaign_payload(payload)
    return _write_json_atomic(path, payload, overwrite=overwrite)


def load_campaign_payload(payload: dict[str, Any]) -> None:
    if payload.get("schema_version") != CAMPAIGN_SCHEMA_VERSION:
        raise ValueError("Invalid comparison campaign schema version.")
    meta = payload.get("campaign_meta")
    matrices = payload.get("matrices")
    if not isinstance(meta, dict) or not str(meta.get("campaign_id") or "").strip():
        raise ValueError("Campaign metadata is incomplete.")
    if not isinstance(matrices, list) or not matrices:
        raise ValueError("Campaign matrices queue is empty.")


def campaign_state_path(output_dir: Path) -> Path:
    return output_dir.expanduser().resolve() / "campaign_state.json"


def write_campaign_state(path: Path, payload: dict[str, Any]) -> Path:
    return _write_json_atomic(path, payload, overwrite=True)


def load_campaign_state(path: Path) -> dict[str, Any]:
    payload = _read_json(path)
    if payload.get("schema_version") != CAMPAIGN_STATE_SCHEMA_VERSION:
        raise ValueError("Unsupported campaign checkpoint schema version.")
    return payload
