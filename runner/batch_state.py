from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile


BATCH_STATE_SCHEMA_VERSION = 1
DEFAULT_STATE_FILENAME = "batch_state.json"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def batch_manifest_fingerprint(batch: dict) -> str:
    """Return a stable SHA-256 fingerprint of the parsed batch manifest."""

    payload = json.dumps(batch, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def default_batch_state_path(output_dir: Path) -> Path:
    return Path(output_dir) / DEFAULT_STATE_FILENAME


def build_initial_batch_state(
    *,
    batch_path: Path,
    batch: dict,
    output_dir: Path,
    run_timestamp: str,
    started_at: str | None = None,
) -> dict:
    meta = batch["batch_meta"]
    jobs = batch["jobs"]
    started = started_at or _utc_now_iso()
    return {
        "schema_version": BATCH_STATE_SCHEMA_VERSION,
        "batch_id": meta["batch_id"],
        "batch_name": meta.get("name"),
        "batch_manifest": str(Path(batch_path).expanduser().resolve()),
        "batch_manifest_sha256": batch_manifest_fingerprint(batch),
        "output_directory": str(Path(output_dir).expanduser().resolve()),
        "run_timestamp": run_timestamp,
        "status": "running",
        "started_at": started,
        "updated_at": started,
        "requested_job_count": len(jobs),
        "current_job": None,
        "results": [],
    }


def load_batch_state(path: Path) -> dict:
    state_path = Path(path)
    with open(state_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("Batch state must be a JSON object.")
    if payload.get("schema_version") != BATCH_STATE_SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported batch state schema version: {payload.get('schema_version')!r}; "
            f"expected {BATCH_STATE_SCHEMA_VERSION}."
        )
    if not str(payload.get("batch_id", "")).strip():
        raise ValueError("Batch state is missing batch_id.")
    if not str(payload.get("run_timestamp", "")).strip():
        raise ValueError("Batch state is missing run_timestamp.")
    results = payload.get("results")
    if not isinstance(results, list):
        raise ValueError("Batch state results must be a list.")
    return payload


def validate_resume_state(state: dict, *, batch_path: Path, batch: dict) -> None:
    expected_batch_id = str(batch["batch_meta"]["batch_id"])
    if str(state.get("batch_id")) != expected_batch_id:
        raise ValueError(
            f"Batch state belongs to '{state.get('batch_id')}', not '{expected_batch_id}'."
        )
    expected_fingerprint = batch_manifest_fingerprint(batch)
    if str(state.get("batch_manifest_sha256")) != expected_fingerprint:
        raise ValueError(
            "Batch manifest changed since this run was checkpointed. "
            "Create a new batch run instead of resuming with different plans/jobs."
        )
    if int(state.get("requested_job_count", -1)) != len(batch["jobs"]):
        raise ValueError("Batch state job count does not match the current manifest.")
    recorded_manifest = Path(str(state.get("batch_manifest", ""))).expanduser().resolve()
    requested_manifest = Path(batch_path).expanduser().resolve()
    if recorded_manifest != requested_manifest:
        raise ValueError(
            "Batch state points at a different manifest path. Use the original manifest to resume."
        )


def write_batch_state(path: Path, payload: dict) -> Path:
    """Atomically write a batch checkpoint so interruption cannot leave partial JSON."""

    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    serialisable = dict(payload)
    serialisable["updated_at"] = _utc_now_iso()
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{state_path.name}.",
        suffix=".tmp",
        dir=state_path.parent,
        text=True,
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(serialisable, handle, indent=2, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, state_path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise
    payload.clear()
    payload.update(serialisable)
    return state_path


def result_map(state: dict) -> dict[str, dict]:
    return {
        str(result.get("job_id")): result
        for result in state.get("results", [])
        if isinstance(result, dict) and str(result.get("job_id", "")).strip()
    }


def replace_result(state: dict, result: dict) -> None:
    job_id = str(result["job_id"])
    results = [
        item
        for item in state.get("results", [])
        if not isinstance(item, dict) or str(item.get("job_id")) != job_id
    ]
    results.append(result)
    state["results"] = results
