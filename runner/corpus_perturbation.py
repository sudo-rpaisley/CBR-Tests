"""Recursive leave-one-out CTGAN experiments with lossless metric export.

Each *physical directory* is one comparison group. At least two PCAP/PCAPNG
files must be directly inside that directory, regardless of how deeply nested
the directory is under the selected root. No training data crosses groups.
Original files, complete outcome JSONs and failed attempts are never deleted.
"""
from __future__ import annotations

import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any

from runner.perturbation_experiment import (
    DEFAULT_PERCENTAGES,
    file_sha256,
    run_experiment,
)
from runner.batch_reports import extract_primary_metric_value

CAPTURE_SUFFIXES = frozenset({".pcap", ".pcapng"})
EXPORT_FIELDS = (
    "campaign_id", "run_date_utc", "group", "held_out", "held_out_sha256",
    "training_files", "attempt", "attempt_status", "seed", "perturbation_pct", "actual_packet_pct",
    "metric_id", "execution_status", "domain_status", "primary_value",
    "outcome_json", "outcome_sha256",
)
MEASUREMENT_FIELDS = EXPORT_FIELDS + (
    "json_pointer", "value_type", "value_json", "numeric_value",
)
JOB_FIELDS = (
    "campaign_id", "run_date_utc", "group", "held_out", "attempt", "attempt_status", "seed",
    "perturbation_pct", "dataset_path", "reference_path", "outcome_json",
    "outcome_sha256", "job_status", "metrics_in_outcome",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path: Path, value: Any) -> None:
    """Replace a checkpoint atomically, rather than truncating one on crash."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, ensure_ascii=False, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def _slug(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "-", value).strip("-").lower()[:48] or "root"


def discover_groups(root: Path) -> tuple[list[dict], list[dict]]:
    """Find per-directory cohorts recursively; skip solitary PCAP directories."""
    root = root.expanduser().resolve()
    if not root.is_dir():
        raise NotADirectoryError(f"Dataset root does not exist: {root}")
    groups = []
    skipped = []
    # One group per directory containing PCAPs; do not combine parent and child.
    paths = [root] + sorted(
        (p for p in root.rglob("*") if p.is_dir() and not p.is_symlink()),
        key=lambda x: str(x.relative_to(root)).casefold(),
    )
    for directory in paths:
        captures = sorted(
            (p for p in directory.iterdir()
             if p.is_file() and not p.is_symlink()
             and p.suffix.lower() in CAPTURE_SUFFIXES),
            key=lambda p: (p.name.casefold(), p.name),
        )
        if not captures:
            continue
        rel = directory.relative_to(root).as_posix()
        if len(captures) < 2:
            skipped.append({
                "folder": rel, "reason": "fewer_than_two_pcaps",
                "files": [p.name for p in captures],
            })
            continue
        groups.append({
            "folder": rel,
            "captures": captures,
        })
    return groups, skipped


def inventory(root: Path, groups: list[dict]) -> list[dict]:
    """Freeze identities before experiments so a resumed run is reproducible."""
    result = []
    for group_index, group in enumerate(groups, start=1):
        cohort = []
        for index, capture in enumerate(group["captures"], start=1):
            print(f"Indexing {group['folder']}: {capture.name}", flush=True)
            cohort.append({
                "path": str(capture.resolve()),
                "name": capture.name,
                "position": index,
                "sha256": file_sha256(capture),
                "file_bytes": capture.stat().st_size,
            })
        group_key = f"{group_index:04d}-{_slug(group['folder'])}-{hashlib.sha256(group['folder'].encode()).hexdigest()[:8]}"
        for capture in cohort:
            case_id = f"{group_key}/holdout_{capture['position']:04d}-{_slug(capture['name'])}"
            result.append({
                "case_id": case_id,
                "folder": group["folder"],
                "group_id": group_key,
                "held_out": capture["path"],
                "held_out_name": capture["name"],
                "held_out_sha256": capture["sha256"],
                "training": [
                    other for other in cohort if other["path"] != capture["path"]
                ],
            })
    return result


def _fingerprint(config: dict) -> str:
    return hashlib.sha256(
        json.dumps(config, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _pointer_segment(s: str) -> str:
    return s.replace("~", "~0").replace("/", "~1")


def leaves(value: Any, path: str = ""):
    """Yield every scalar leaf, including entries in arrays and empty containers."""
    if isinstance(value, dict) and value:
        for key, member in value.items():
            yield from leaves(member, path + "/" + _pointer_segment(str(key)))
    elif isinstance(value, list) and value:
        for index, member in enumerate(value):
            yield from leaves(member, path + "/" + str(index))
    else:
        yield path or "/", value


def _load_json(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return payload


def _case_completed(case: dict) -> bool:
    if case.get("status") != "completed" or not case.get("output_dir"):
        return False
    folder = Path(case["output_dir"])
    try:
        experiment = _load_json(folder / "experiment.json")
    except (OSError, ValueError, json.JSONDecodeError):
        return False
    if experiment.get("status") != "completed":
        return False
    for run in experiment.get("runs", []):
        state = Path(run["batch_output_dir"]) / "batch_state.json"
        try:
            saved = _load_json(state)
            if saved.get("status") != "completed" or not saved.get("results"):
                return False
            if saved.get("requested_job_count") is not None and (
                len(saved["results"]) != saved["requested_job_count"]
            ):
                return False
            if any(not Path(str(job.get("output_path", ""))).is_file()
                   for job in saved["results"]):
                return False
        except (OSError, KeyError, ValueError, json.JSONDecodeError):
            return False
    return bool(experiment.get("runs"))


def _source_index(cases: list[dict]) -> dict[str, dict]:
    return {case["case_id"]: case for case in cases}


def export_results(campaign: dict, output: Path) -> dict:
    """Rebuild outputs from every readable attempt (including failed/partial jobs).

    Every metric execution record and full test_result is written to JSONL.
    Each nested test-result leaf (including arrays) is also written as a row
    with its JSON pointer to support querying and date-to-date comparison.
    """
    exports = output / "exports"
    exports.mkdir(parents=True, exist_ok=True)
    rows = 0
    jobs = 0
    measurements = 0
    with (
        (exports / "metric_results.csv").open("w", encoding="utf-8", newline="") as compact,
        (exports / "measurements_long.csv").open("w", encoding="utf-8", newline="") as detailed,
        (exports / "jobs.csv").open("w", encoding="utf-8", newline="") as jobs_file,
        (exports / "metric_payloads.jsonl").open("w", encoding="utf-8") as jsonl,
    ):
        compact_writer = csv.DictWriter(compact, EXPORT_FIELDS)
        detail_writer = csv.DictWriter(detailed, MEASUREMENT_FIELDS)
        jobs_writer = csv.DictWriter(jobs_file, JOB_FIELDS)
        compact_writer.writeheader()
        detail_writer.writeheader()
        jobs_writer.writeheader()

        for case in campaign["cases"]:
            attempts = case.get("attempts") or [{
                "path": case.get("output_dir", ""), "status": case.get("status", "unknown")
            }]
            for attempt_index, attempt in enumerate(attempts, start=1):
                experiment_path = Path(attempt["path"]) / "experiment.json"
                if not experiment_path.is_file():
                    continue
                experiment = _load_json(experiment_path)
                for run in experiment.get("runs", []):
                    state_path = Path(run["batch_output_dir"]) / "batch_state.json"
                    if not state_path.is_file():
                        continue
                    state = _load_json(state_path)
                    captures = {
                        str(Path(cap["dataset_path"]).resolve()): cap
                        for cap in run["captures"]
                    }
                    for job in state.get("results", []):
                        candidate = str(Path(job.get("dataset_path", "")).resolve())
                        capture = captures.get(candidate)
                        if capture is None:
                            raise ValueError(f"Batch job does not match a generated capture: {candidate}")
                        outcome_path = Path(str(job.get("output_path", ""))).expanduser().resolve()
                        if not outcome_path.is_file():
                            # Failed jobs without a complete outcome still remain
                            # documented in their raw batch_state.json.
                            continue
                        outcome = _load_json(outcome_path)
                        digest = file_sha256(outcome_path)
                        pct = capture["requested_pct"]
                        seed = run["seed"]
                        common = {
                            "campaign_id": campaign["campaign_id"],
                            "run_date_utc": campaign["started_at"],
                            "group": case["folder"],
                            "held_out": case["held_out"],
                            "held_out_sha256": case["held_out_sha256"],
                            "training_files": json.dumps([p["path"] for p in case["training"]]),
                            "attempt": attempt_index,
                            "attempt_status": attempt.get("status", "unknown"),
                            "seed": seed,
                            "perturbation_pct": pct,
                            "actual_packet_pct": capture["actual_packet_pct"],
                            "outcome_json": str(outcome_path),
                            "outcome_sha256": digest,
                        }
                        result_map = {
                            str(record["metric_id"]): record
                            for record in outcome.get("metric_results", [])
                            if isinstance(record, dict) and record.get("metric_id")
                        }
                        payloads = outcome.get("test_results", {})
                        if not isinstance(payloads, dict):
                            payloads = {}
                        metric_ids = sorted(set(result_map) | set(payloads))
                        jobs_writer.writerow({
                            "campaign_id": common["campaign_id"],
                            "run_date_utc": common["run_date_utc"],
                            "group": common["group"],
                            "held_out": common["held_out"],
                            "attempt": attempt_index,
                            "attempt_status": attempt.get("status", "unknown"),
                            "seed": seed,
                            "perturbation_pct": pct,
                            "dataset_path": candidate,
                            "reference_path": case["held_out"],
                            "outcome_json": str(outcome_path),
                            "outcome_sha256": digest,
                            "job_status": job.get("status", ""),
                            "metrics_in_outcome": len(metric_ids),
                        })
                        jobs += 1
                        for metric_id in metric_ids:
                            execution = result_map.get(metric_id, {})
                            payload = payloads.get(metric_id)
                            primary, _, _ = extract_primary_metric_value(metric_id, payload)
                            row = dict(common)
                            row.update({
                                "metric_id": metric_id,
                                "execution_status": execution.get("status", ""),
                                "domain_status": execution.get("result_status", ""),
                                "primary_value": primary if primary is not None else "",
                            })
                            compact_writer.writerow(row)
                            # Lossless: full execution record, raw metric result and
                            # taxonomy-ready original outcome retained on disk.
                            jsonl.write(json.dumps({
                                **common, "metric_id": metric_id,
                                "execution_record": execution,
                                "test_result": payload,
                                "raw_outcome": str(outcome_path),
                            }, ensure_ascii=False, allow_nan=False) + "\n")
                            rows += 1
                            for pointer, value in leaves(payload):
                                detail = dict(row)
                                detail.update({
                                    "json_pointer": pointer,
                                    "value_type": (
                                        "null" if value is None else
                                        "boolean" if isinstance(value, bool) else
                                        "number" if isinstance(value, (float, int)) else
                                        "string" if isinstance(value, str) else
                                        "object" if isinstance(value, dict) else "array"
                                    ),
                                    "value_json": json.dumps(value, ensure_ascii=False, allow_nan=False),
                                    "numeric_value": value if isinstance(value, (int, float)) and not isinstance(value, bool) else "",
                                })
                                detail_writer.writerow(detail)
                                measurements += 1
    summary = {
        "complete_cases": sum(_case_completed(x) for x in campaign["cases"]),
        "total_cases": len(campaign["cases"]),
        "jobs_exported": jobs,
        "metrics_exported": rows,
        "measurement_leaves_exported": measurements,
        "metrics_csv": str(exports / "metric_results.csv"),
        "measurements_csv": str(exports / "measurements_long.csv"),
        "metric_payloads_jsonl": str(exports / "metric_payloads.jsonl"),
        "jobs_csv": str(exports / "jobs.csv"),
    }
    atomic_json(exports / "export_manifest.json", summary)
    return summary


def run_corpus(
    *,
    dataset_root: Path,
    output_dir: Path,
    generator_script: Path,
    percentages: tuple[int, ...] = DEFAULT_PERCENTAGES,
    seeds: tuple[int, ...] = (42,),
    epochs: int = 100,
    batch_size: int = 100,
    device: str = "cpu",
    workers: int | None = None,
    size_tolerance_pct: float = 5.0,
    prepare_only: bool = False,
    resume: bool = False,
    preview_only: bool = False,
) -> dict:
    """Execute every held-out file in every recursive PCAP directory.

    Errors are checkpointed per held-out capture. If a case fails, subsequent
    cases continue; --resume skips verified completed cases and retains failed
    attempts rather than overwriting results.
    """
    root = dataset_root.expanduser().resolve()
    generator_script = generator_script.expanduser().resolve()
    output = output_dir.expanduser().resolve()
    if not generator_script.is_file():
        raise FileNotFoundError(f"GAN generator script not found: {generator_script}")
    if not percentages or 0 not in percentages or any(p < 0 or p > 100 for p in percentages):
        raise ValueError("Percentages must include 0 and lie between 0 and 100.")
    if not seeds or len(set(seeds)) != len(seeds) or any(s < 0 for s in seeds):
        raise ValueError("Seeds must be distinct nonnegative integers.")
    if epochs < 1 or batch_size < 10 or batch_size % 10 or device not in {"cpu", "cuda"}:
        raise ValueError("Invalid CTGAN training parameters.")
    if workers is not None and workers < 1:
        raise ValueError("Worker count must be positive.")

    groups, skipped = discover_groups(root)
    if not groups:
        raise ValueError(
            "No eligible group found. Each directory must contain at least two "
            "PCAP/PCAPNG files directly inside it. Singletons are skipped."
        )
    # Preview must remain cheap even for multi-terabyte trees: count paths
    # without hashing every packet capture or creating directories.
    if preview_only:
        return {
            "eligible_groups": len(groups),
            "eligible_cases": sum(len(g["captures"]) for g in groups),
            "estimated_capture_bytes": (
                len(percentages) * len(seeds)
                * sum(p.stat().st_size for g in groups for p in g["captures"])
            ),
            "skipped_folders": skipped,
            "groups": [{
                "folder": group["folder"],
                "files": [p.name for p in group["captures"]],
            } for group in groups],
        }
    cases = inventory(root, groups)
    snapshot = {
        "dataset_root": str(root),
        "inventory": [{
            "case_id": c["case_id"], "folder": c["folder"],
            "held_out": c["held_out"], "sha256": c["held_out_sha256"],
            "training": [(x["path"], x["sha256"]) for x in c["training"]],
        } for c in cases],
        "generator_path": str(generator_script),
        "generator_sha256": file_sha256(generator_script),
        "percentages": list(percentages),
        "seeds": list(seeds),
        "epochs": epochs,
        "batch_size": batch_size,
        "device": device,
        "workers": workers,
        "size_tolerance_pct": size_tolerance_pct,
        "prepare_only": prepare_only,
    }
    checksum = _fingerprint(snapshot)
    manifest_path = output / "campaign.json"
    if resume:
        campaign = _load_json(manifest_path)
        if campaign.get("config_sha256") != checksum:
            raise ValueError("Dataset files, generator or experiment settings changed; cannot resume this campaign.")
    else:
        if output.exists() and any(output.iterdir()):
            raise FileExistsError(f"Choose a new empty output directory: {output}")
        campaign = {
            "schema_version": 1,
            "method": "recursive_leave_one_out_ctgan_suffix",
            "campaign_id": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + checksum[:10],
            "started_at": utc_now(),
            "status": "running",
            "config_sha256": checksum,
            "settings": snapshot,
            "skipped_folders": skipped,
            "cases": [
                {**case, "status": "pending", "attempts": []} for case in cases
            ],
        }
        atomic_json(manifest_path, campaign)

    for index, case in enumerate(campaign["cases"], start=1):
        if _case_completed(case):
            print(f"[{index}/{len(campaign['cases'])}] Completed, skipping: {case['case_id']}", flush=True)
            continue
        n = len(case.get("attempts", [])) + 1
        attempt_dir = output / "cases" / case["case_id"] / f"attempt_{n:03d}"
        attempt_dir.parent.mkdir(parents=True, exist_ok=True)
        print(f"[{index}/{len(campaign['cases'])}] Hold out {case['held_out_name']} from {case['folder']}", flush=True)
        print(f"  Training on {len(case['training'])} other files; {len(percentages)} levels × {len(seeds)} seeds.", flush=True)
        case["status"] = "running"
        case["output_dir"] = str(attempt_dir)
        case.setdefault("attempts", []).append({
            "path": str(attempt_dir), "started_at": utc_now(), "status": "running"
        })
        atomic_json(manifest_path, campaign)
        try:
            run_experiment(
                base=Path(case["held_out"]),
                comparison=Path(case["held_out"]),
                plan=None,
                auto_plan=True,
                allow_self_reference=True,
                plan_template_reference=Path(case["training"][0]["path"]),
                model=None,
                training_pcaps=tuple(Path(v["path"]) for v in case["training"]),
                generator_script=generator_script,
                output_dir=attempt_dir,
                percentages=percentages,
                seeds=seeds,
                training_epochs=epochs,
                training_batch_size=batch_size,
                training_device=device,
                workers=workers,
                size_tolerance_pct=size_tolerance_pct,
                prepare_only=prepare_only,
            )
            case["status"] = "prepared" if prepare_only else "completed"
        except KeyboardInterrupt:
            case["status"] = "interrupted"
            case["attempts"][-1]["status"] = "interrupted"
            campaign["status"] = "interrupted"
            atomic_json(manifest_path, campaign)
            raise
        except Exception as exc:
            case["status"] = "failed"
            case["error"] = f"{type(exc).__name__}: {exc}"
            print(f"  Failed: {case['error']}", flush=True)
        case["attempts"][-1]["status"] = case["status"]
        case["attempts"][-1]["finished_at"] = utc_now()
        atomic_json(manifest_path, campaign)
        # Intermediate exports make long unattended runs useful before completion.
        if not prepare_only:
            campaign["exports"] = export_results(campaign, output)
            atomic_json(manifest_path, campaign)
    successes = sum(x["status"] in {"completed", "prepared"} for x in campaign["cases"])
    failures = sum(x["status"] == "failed" for x in campaign["cases"])
    campaign["status"] = "completed" if not failures else "needs_attention"
    campaign["finished_at"] = utc_now()
    campaign["completed_cases"] = successes
    campaign["failed_cases"] = failures
    if not prepare_only:
        campaign["exports"] = export_results(campaign, output)
    atomic_json(manifest_path, campaign)
    return campaign
