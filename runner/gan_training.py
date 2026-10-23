"""Train a CTGAN checkpoint from selected, independent PCAP/PCAPNG captures.

Keeps the training data/model outside Git and records hashes and extraction
counts for reproducible CBR perturbation experiments. The upstream generator's
extract/train commands remain the source of truth for feature semantics.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
import subprocess
import sys
from typing import Callable

from runner.perturbation_experiment import file_sha256


def train_from_captures(
    *,
    captures: tuple[Path, ...],
    generator_script: Path,
    output_dir: Path,
    epochs: int = 100,
    batch_size: int = 100,
    device: str = "cpu",
    seed: int = 42,
    progress_callback: Callable[[str, dict], None] | None = None,
) -> tuple[Path, dict]:
    """Extract each capture, concatenate flow rows and train one checkpoint."""
    if not captures:
        raise ValueError("Select at least one training PCAP/PCAPNG.")
    if device not in {"cpu", "cuda"}:
        raise ValueError("Training device must be cpu or cuda.")
    if epochs < 1 or batch_size < 10 or batch_size % 10:
        raise ValueError("Epochs must be positive and batch size a multiple of ten (>=10).")
    directory = output_dir.expanduser().resolve()
    if directory.exists() and any(directory.iterdir()):
        raise FileExistsError(f"Training output directory is not empty: {directory}")
    directory.mkdir(parents=True, exist_ok=True)
    features_dir = directory / "individual_features"
    features_dir.mkdir()
    sources = []
    combined = directory / "training_flows.csv"
    columns: list[str] | None = None
    total_flows = 0
    with combined.open("w", newline="", encoding="utf-8") as output:
        writer = None
        for index, candidate in enumerate(captures, start=1):
            if progress_callback:
                progress_callback("extracting", {
                    "capture_index": index, "capture_total": len(captures),
                    "capture_name": candidate.name,
                })
            path = candidate.expanduser().resolve()
            if not path.is_file() or path.suffix.lower() not in {".pcap", ".pcapng"}:
                raise ValueError(f"Invalid training capture: {path}")
            features_path = features_dir / f"{index:03d}_flows.csv"
            subprocess.run([
                sys.executable, str(generator_script), "extract",
                "--input", str(path), "--features", str(features_path),
            ], check=True)
            info_path = features_path.with_suffix(".metadata.json")
            info = json.loads(info_path.read_text(encoding="utf-8"))
            with features_path.open(newline="", encoding="utf-8") as stream:
                reader = csv.DictReader(stream)
                if not reader.fieldnames:
                    raise ValueError(f"No training feature columns: {features_path}")
                if columns is None:
                    columns = list(reader.fieldnames)
                    writer = csv.DictWriter(output, fieldnames=columns)
                    writer.writeheader()
                elif list(reader.fieldnames) != columns:
                    raise ValueError(f"Inconsistent CTGAN feature columns: {features_path}")
                count = 0
                for row in reader:
                    writer.writerow(row)
                    count += 1
            if count != int(info["flow_count"]):
                raise ValueError(f"Extraction row count mismatch: {features_path}")
            total_flows += count
            sources.append({
                "path": str(path),
                "sha256": file_sha256(path),
                "feature_rows": count,
                "features_sha256": file_sha256(features_path),
                "metadata": str(info_path),
            })
    if total_flows < 50:
        raise ValueError(
            f"Only {total_flows} training flows across {len(captures)} captures; "
            "CTGAN requires at least 50 (preferably thousands)."
        )
    model = directory / "ctgan.pkl"
    if progress_callback:
        progress_callback("training", {"epochs": epochs, "flows": total_flows})
    command = [
        sys.executable, str(generator_script), "train",
        "--features", str(combined), "--model", str(model),
        "--epochs", str(epochs), "--batch-size", str(batch_size),
        "--device", device, "--seed", str(seed),
    ]
    subprocess.run(command, check=True)
    if not model.is_file() or model.stat().st_size == 0:
        raise ValueError(f"Training did not produce a checkpoint: {model}")
    record = {
        "method": "ctgan_flow_features",
        "training_captures": sources,
        "training_flow_count": total_flows,
        "features_csv": str(combined),
        "features_sha256": file_sha256(combined),
        "model_path": str(model),
        "model_sha256": file_sha256(model),
        "epochs": epochs,
        "batch_size": batch_size,
        "device": device,
        "seed": seed,
    }
    (directory / "training_manifest.json").write_text(
        json.dumps(record, indent=2) + "\n", encoding="utf-8"
    )
    return model, record
