from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
import uuid
from importlib import metadata
from pathlib import Path
from typing import Any

from runner.contract import collect_reference_paths


RUNTIME_DISTRIBUTIONS = ("pandas", "scapy", "openpyxl")


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def file_manifest(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    manifest: dict[str, Any] = {
        "path": str(resolved),
        "exists": resolved.exists(),
    }
    if not resolved.exists() or not resolved.is_file():
        return manifest
    stat = resolved.stat()
    manifest.update(
        {
            "size_bytes": int(stat.st_size),
            "sha256": sha256_file(resolved),
        }
    )
    return manifest


def _git_metadata(repo_root: Path) -> dict[str, Any]:
    env_revision = os.environ.get("CBR_TESTS_REVISION") or os.environ.get("GITHUB_SHA")
    revision = env_revision
    dirty: bool | None = None
    try:
        if revision is None:
            revision = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=repo_root,
                check=True,
                capture_output=True,
                text=True,
                timeout=2,
            ).stdout.strip() or None
        status = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        ).stdout
        dirty = bool(status.strip())
    except (OSError, subprocess.SubprocessError):
        pass
    return {"revision": revision, "dirty": dirty}


def _dependency_versions() -> dict[str, str | None]:
    versions: dict[str, str | None] = {}
    for distribution in RUNTIME_DISTRIBUTIONS:
        try:
            versions[distribution] = metadata.version(distribution)
        except metadata.PackageNotFoundError:
            versions[distribution] = None
    return versions


def software_manifest() -> dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[1]
    return {
        "python": {
            "version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "executable": sys.executable,
        },
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        },
        "dependencies": _dependency_versions(),
        "code": _git_metadata(repo_root),
    }


def resolve_plan_source_path(case_file: Path) -> Path:
    """Resolve the plan file used by a case, or return the direct plan file itself."""
    resolved_case = case_file.expanduser().resolve()
    try:
        payload = json.loads(resolved_case.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return resolved_case

    plan_spec = payload.get("test_plan") if isinstance(payload, dict) else None
    if isinstance(plan_spec, dict) and isinstance(plan_spec.get("path"), str):
        path = Path(plan_spec["path"]).expanduser()
        if not path.is_absolute():
            path = resolved_case.parent / path
        return path.resolve()
    return resolved_case


def build_provenance_manifest(
    *,
    plan: dict,
    dataset_path: Path,
    case_file: Path,
    plan_source_path: Path,
    field_translation: dict[str, str],
    translation_path: Path | None,
    taxonomy_path: Path | None,
    cli_arguments: dict[str, Any],
) -> dict[str, Any]:
    """Build the immutable experiment-identification metadata stored with an outcome."""
    run_id = str(uuid.uuid4())
    plan_snapshot = json.loads(json.dumps(plan))
    translation_snapshot = dict(sorted(field_translation.items()))
    reference_paths = collect_reference_paths(plan, base_dir=plan_source_path.parent)

    return {
        "schema_version": 1,
        "run_id": run_id,
        "dataset": file_manifest(dataset_path),
        "case_source": file_manifest(case_file),
        "plan": {
            "source": file_manifest(plan_source_path),
            "plan_id": plan.get("plan_meta", {}).get("plan_id"),
            "version": plan.get("plan_meta", {}).get("version"),
            "sha256": sha256_json(plan_snapshot),
            "snapshot": plan_snapshot,
        },
        "field_translation": {
            "source": file_manifest(translation_path) if translation_path is not None else None,
            "effective_mapping_sha256": sha256_json(translation_snapshot),
            "effective_mapping": translation_snapshot,
        },
        "taxonomy_source": file_manifest(taxonomy_path) if taxonomy_path is not None else None,
        "reference_datasets": [file_manifest(path) for path in reference_paths],
        "software": software_manifest(),
        "cli_arguments": dict(sorted(cli_arguments.items())),
    }
