"""GAN trailing-suffix experiment orchestration for the existing CBR-Tests batch runner.

The generator is intentionally an external, separately installed program.  This
module never imports CTGAN and never changes the default metric runner.
"""
from __future__ import annotations

import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys

from runner.fixed_plan_matrix import _load_plan, build_fixed_plan_matrix
from runner.reference_binding import reference_bound_metric_ids
from runner.batch_reports import extract_primary_metric_value


DEFAULT_PERCENTAGES = tuple(range(0, 101, 10))
RESULT_COLUMNS = (
    "seed", "requested_pct", "actual_packet_pct", "base_dataset_path",
    "comparison_dataset_path", "metric_id",
    "execution_status", "domain_status", "primary_value",
    "dataset_path", "outcome_path",
)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_integer_set(raw: str, *, low: int, high: int) -> tuple[int, ...]:
    try:
        values = tuple(int(piece.strip()) for piece in raw.split(","))
    except ValueError as exc:
        raise ValueError("Expected comma-separated integer values.") from exc
    if not values or any(value < low or value > high for value in values):
        raise ValueError(f"All values must be integers between {low} and {high}.")
    if len(set(values)) != len(values):
        raise ValueError("Repeated percentages/seeds are not allowed.")
    return tuple(sorted(values))


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _file_path(path: Path, label: str, suffixes: set[str] | None = None) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"{label} not found: {resolved}")
    if suffixes is not None and resolved.suffix.lower() not in suffixes:
        raise ValueError(f"{label} must be one of {sorted(suffixes)}")
    return resolved


def packet_count(path: Path) -> int:
    """Independent full-capture count; bounded memory via Scapy's reader."""
    from scapy.utils import PcapReader
    with PcapReader(str(path)) as reader:
        return sum(1 for _ in reader)


def validate_generation(
    *,
    seed_dir: Path,
    source_path: Path,
    model_path: Path,
    percentages: tuple[int, ...],
    seed: int,
    size_tolerance_pct: float,
    verify_packets: bool = True,
) -> list[dict]:
    """Reject malformed/missing outputs before any costly CBR metric jobs start."""
    manifest = seed_dir / "manifest.csv"
    if not manifest.is_file():
        raise ValueError(f"Generator did not write {manifest}")
    with manifest.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    expected = set(percentages)
    if len(rows) != len(expected) or {int(row["requested_pct"]) for row in rows} != expected:
        raise ValueError("Generator manifest percentages do not match the requested sweep.")
    source_hash = file_sha256(source_path)
    model_hash = file_sha256(model_path)
    source_size = source_path.stat().st_size
    reference_count = packet_count(source_path) if verify_packets else None
    verified: list[dict] = []
    for row in sorted(rows, key=lambda record: int(record["requested_pct"])):
        pct = int(row["requested_pct"])
        # Do not allow a generator-controlled CSV path to escape the seed directory.
        expected_name = f"gan_{pct:03d}{source_path.suffix.lower()}"
        if row.get("output") != expected_name:
            raise ValueError(f"Unexpected generated filename at {pct}%: {row.get('output')}")
        candidate = seed_dir / expected_name
        if not candidate.is_file():
            raise FileNotFoundError(candidate)
        if row["input_sha256"] != source_hash or row["model_sha256"] != model_hash:
            raise ValueError(f"Source/model hash mismatch at {pct}%.")
        if int(row["seed"]) != seed or int(row["source_file_bytes"]) != source_size:
            raise ValueError(f"Source size or seed mismatch at {pct}%.")
        total = int(row["total_packets"])
        generated = int(row["synthetic_packets"])
        real = int(row["real_packets"])
        if total <= 0 or real + generated != total:
            raise ValueError(f"Invalid packet accounting at {pct}%.")
        if abs((100.0 * generated / total) - pct) > (100.0 / total + 1e-6):
            raise ValueError(f"Actual packet percentage mismatch at {pct}%.")
        if pct == 0 and (generated != 0 or file_sha256(candidate) != source_hash):
            raise ValueError("The 0% baseline must be byte-for-byte identical to the input.")
        if row["output_sha256"] != file_sha256(candidate):
            raise ValueError(f"Output SHA-256 mismatch at {pct}%.")
        if int(row["output_file_bytes"]) != candidate.stat().st_size:
            raise ValueError(f"Incorrect generated file size at {pct}%.")
        if abs(100.0 * (candidate.stat().st_size - source_size) / source_size) > size_tolerance_pct:
            raise ValueError(f"Output {pct}% exceeds file-size tolerance ({size_tolerance_pct}%).")
        if verify_packets:
            actual = packet_count(candidate)
            if actual != total or actual != reference_count:
                raise ValueError(f"Independent packet-count check failed at {pct}%: {actual}.")
        for key in ("packet_labels", "flow_labels"):
            if row.get(key) != f"gan_{pct:03d}." + ("packet_labels.csv" if key == "packet_labels" else "flow_labels.csv"):
                raise ValueError(f"Unexpected provenance filename at {pct}%.")
            if not (seed_dir / row[key]).is_file():
                raise FileNotFoundError(seed_dir / row[key])
        verified.append({
            "seed": seed,
            "requested_pct": pct,
            "actual_packet_pct": float(row["actual_packet_pct"]),
            "dataset_path": str(candidate.resolve()),
            "sha256": row["output_sha256"],
            "file_size_error_pct": float(row["file_size_error_pct"]),
            "total_packets": total,
            "synthetic_packets": generated,
            "real_packets": real,
            "manifest_path": str((seed_dir / f"gan_{pct:03d}.json").resolve()),
        })
    return verified


def collect_metric_rows(batch_state_path: Path, captures: list[dict]) -> list[dict]:
    """Join CBR-Tests' authoritative per-job outcomes with perturbation metadata."""
    if not batch_state_path.is_file():
        return []
    state = json.loads(batch_state_path.read_text(encoding="utf-8"))
    lookup = {str(Path(row["dataset_path"]).resolve()): row for row in captures}
    records: list[dict] = []
    for job in state.get("results", []):
        capture = lookup.get(str(Path(str(job["dataset_path"])).resolve()))
        if capture is None:
            continue
        outcome_path = Path(str(job.get("output_path", "")))
        if not outcome_path.is_file():
            continue
        outcome = json.loads(outcome_path.read_text(encoding="utf-8"))
        test_results = outcome.get("test_results", {})
        if not isinstance(test_results, dict):
            test_results = {}
        for metric in outcome.get("metric_results", []):
            if not isinstance(metric, dict) or not metric.get("metric_id"):
                continue
            metric_id = str(metric["metric_id"])
            value, _, _ = extract_primary_metric_value(metric_id, test_results.get(metric_id))
            records.append({
                "seed": capture["seed"],
                "requested_pct": capture["requested_pct"],
                "actual_packet_pct": capture["actual_packet_pct"],
                "base_dataset_path": capture.get("base_dataset_path", ""),
                "comparison_dataset_path": capture.get("comparison_dataset_path", ""),
                "metric_id": metric_id,
                "execution_status": metric.get("status", ""),
                "domain_status": metric.get("result_status", ""),
                "primary_value": value if value is not None else "",
                "dataset_path": capture["dataset_path"],
                "outcome_path": str(outcome_path.resolve()),
            })
    return records


def run_experiment(
    *,
    base: Path,
    comparison: Path,
    plan: Path,
    model: Path | None,
    generator_script: Path,
    output_dir: Path,
    percentages: tuple[int, ...] = DEFAULT_PERCENTAGES,
    seeds: tuple[int, ...] = (42,),
    basis: str = "packets",
    workers: int | None = None,
    display: str = "quiet",
    size_tolerance_pct: float = 5.0,
    prepare_only: bool = False,
    experiment_mode: bool = False,
    training_pcaps: tuple[Path, ...] = (),
    training_epochs: int = 100,
    training_batch_size: int = 100,
    training_device: str = "cpu",
) -> dict:
    """Compare fixed Dataset 1 with Dataset 2 using an existing or newly trained CTGAN."""
    base = _file_path(base, "Dataset 1 (base reference)", {".pcap", ".pcapng"})
    comparison = _file_path(comparison, "Dataset 2 (comparison)", {".pcap", ".pcapng"})
    if base == comparison:
        raise ValueError("Choose separate Dataset 1 (base) and Dataset 2 (comparison) captures.")
    # The GAN replaces packets only from Dataset 2. Dataset 1 never enters the generator.
    source = comparison
    plan = _file_path(plan, "Fixed test plan", {".json"})
    if bool(model) == bool(training_pcaps):
        raise ValueError("Choose either an existing CTGAN model or independent training captures.")
    training = tuple(
        _file_path(candidate, "Training capture", {".pcap", ".pcapng"})
        for candidate in training_pcaps
    )
    training_hashes = [file_sha256(candidate) for candidate in training]
    if len(set(training)) != len(training) or len(set(training_hashes)) != len(training):
        raise ValueError("Each selected training capture must be distinct.")
    if training and (
        {base, comparison}.intersection(training)
        or {file_sha256(base), file_sha256(comparison)}.intersection(training_hashes)
    ):
        raise ValueError("GAN training PCAPs must be independent of both Dataset 1 and Dataset 2.")
    if training_epochs < 1 or training_batch_size < 10 or training_batch_size % 10:
        raise ValueError("Training epochs must be positive and batch size a multiple of ten (>=10).")
    if training_device not in {"cpu", "cuda"}:
        raise ValueError("Training device must be cpu or cuda.")
    model = _file_path(model, "CTGAN model", {".pkl"}) if model is not None else None
    generator_script = _file_path(generator_script, "Trusted GAN generator", {".py"})
    if source.stat().st_size == 0:
        raise ValueError("Source capture must not be empty.")
    if basis not in ("packets", "packet-bytes"):
        raise ValueError("Unsupported percentage basis.")
    if not percentages or 0 not in percentages or any(p < 0 or p > 100 for p in percentages):
        raise ValueError("Include the 0% baseline and valid percentages from 0 to 100.")
    if not seeds or any(s < 0 for s in seeds) or len(set(seeds)) != len(seeds):
        raise ValueError("Seeds must be distinct nonnegative integers.")
    if size_tolerance_pct < 0:
        raise ValueError("File-size tolerance cannot be negative.")
    source_plan = _load_plan(plan)
    if not reference_bound_metric_ids(source_plan):
        raise ValueError(
            "The fixed PCAP plan contains no reference-comparison metrics. "
            "Create a plan with an independent reference PCAP first; the runner "
            "will then bind those metrics to Dataset 1 for every comparison."
        )
    if experiment_mode:
        from runner.experiment_contract import validate_final_experiment_plan
        validate_final_experiment_plan(source_plan)
    root = Path(__file__).resolve().parents[1]
    destination = output_dir.expanduser().resolve()
    if destination.exists() and any(destination.iterdir()):
        raise FileExistsError(f"Output directory must be empty to protect previous experiments: {destination}")
    destination.mkdir(parents=True, exist_ok=True)
    metadata = {
        "schema_version": 1,
        "name": "ctgan_trailing_suffix",
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "base_dataset_path": str(base),
        "base_dataset_sha256": file_sha256(base),
        "comparison_dataset_path": str(comparison),
        "comparison_dataset_sha256": file_sha256(comparison),
        "source_path": str(comparison),
        "source_sha256": file_sha256(comparison),
        "model_source": "train_from_selected_pcaps" if training else "existing_checkpoint",
        "training_captures": [
            {"path": str(path), "sha256": checksum}
            for path, checksum in zip(training, training_hashes)
        ],
        "training_parameters": (
            {"epochs": training_epochs, "batch_size": training_batch_size,
             "device": training_device, "seed": seeds[0]} if training else None
        ),
        "model_path": str(model) if model is not None else None,
        "model_sha256": file_sha256(model) if model is not None else None,
        "generator_script": str(generator_script),
        "generator_sha256": file_sha256(generator_script),
        "plan_path": str(plan),
        "plan_sha256": file_sha256(plan),
        "percentages": list(percentages),
        "seeds": list(seeds),
        "basis": basis,
        "size_tolerance_pct": size_tolerance_pct,
        "runs": [],
    }
    experiment_manifest = destination / "experiment.json"
    _write_json(experiment_manifest, metadata)
    all_metric_rows: list[dict] = []
    try:
        if training:
            # Import only when requested; ordinary CBR runner never depends on CTGAN.
            from runner.gan_training import train_from_captures
            print(
                f"Training CTGAN on {len(training)} independent capture(s) "
                f"({training_device}, {training_epochs} epochs).", flush=True
            )
            model, training_record = train_from_captures(
                captures=training, generator_script=generator_script,
                output_dir=destination / "training",
                epochs=training_epochs, batch_size=training_batch_size,
                device=training_device, seed=seeds[0],
            )
            metadata["training"] = training_record
            metadata["model_path"] = str(model)
            metadata["model_sha256"] = file_sha256(model)
            _write_json(experiment_manifest, metadata)
        assert model is not None
        for seed in seeds:
            seed_root = destination / f"seed_{seed}"
            capture_dir = seed_root / "captures"
            command = [
                sys.executable, str(generator_script), "sweep",
                "--input", str(source), "--model", str(model),
                "--output", str(capture_dir), "--percentages",
                ",".join(map(str, percentages)), "--basis", basis, "--seed", str(seed),
            ]
            print(f"Generating trailing-suffix captures for seed {seed}", flush=True)
            subprocess.run(command, cwd=root, check=True)
            captures = validate_generation(
                seed_dir=capture_dir, source_path=source, model_path=model,
                percentages=percentages, seed=seed,
                size_tolerance_pct=size_tolerance_pct,
            )
            for capture in captures:
                capture["base_dataset_path"] = str(base)
                capture["comparison_dataset_path"] = str(comparison)
            matrix_path = seed_root / "fixed_plan_batch.json"
            build_fixed_plan_matrix(
                name=f"gan-trailing-suffix-seed-{seed}",
                plan_path=plan,
                candidate_paths=[Path(row["dataset_path"]) for row in captures],
                reference_paths=[base],
                output_path=matrix_path,
                overwrite=False,
                repo_root=root,
            )
            run = {
                "seed": seed,
                "base_dataset_path": str(base),
                "comparison_dataset_path": str(comparison),
                "matrix": str(matrix_path),
                "captures": captures,
                "batch_output_dir": str(seed_root / "evaluation"),
                "status": "prepared",
            }
            metadata["runs"].append(run)
            _write_json(experiment_manifest, metadata)
            if prepare_only:
                continue
            batch_command = [
                sys.executable, str(root / "run_batch.py"),
                "--batch", str(matrix_path),
                "--output-dir", str(seed_root / "evaluation"),
                "--display", display, "--no-update-field-translation", "--fail-fast",
            ]
            if workers is not None:
                batch_command.extend(["--workers", str(workers)])
            if experiment_mode:
                batch_command.append("--experiment-mode")
            completed = subprocess.run(batch_command, cwd=root, check=False)
            state_path = seed_root / "evaluation" / "batch_state.json"
            all_metric_rows.extend(collect_metric_rows(state_path, captures))
            run["return_code"] = completed.returncode
            run["status"] = (
                json.loads(state_path.read_text(encoding="utf-8")).get("status", "unknown")
                if state_path.exists() else "not_written"
            )
            _write_json(experiment_manifest, metadata)
            if completed.returncode != 0 or run["status"] != "completed":
                raise RuntimeError(f"CBR batch for seed {seed} needs attention; see {state_path}.")
        results_csv = destination / "perturbation_metrics.csv"
        with results_csv.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=RESULT_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(all_metric_rows)
        metadata["status"] = "prepared" if prepare_only else "completed"
        metadata["finished_at"] = datetime.now(timezone.utc).isoformat()
        metadata["metrics_csv"] = str(results_csv)
        _write_json(experiment_manifest, metadata)
    except Exception as exc:
        metadata["status"] = "needs_attention"
        metadata["error"] = str(exc)
        _write_json(experiment_manifest, metadata)
        raise
    return metadata
