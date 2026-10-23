"""Storage checks for large, recursive GAN PCAP experiments.

A selected output parent is an existing directory on any writable filesystem.
Results are written to its own new timestamped subfolder, never to the source
dataset tree. Free-space checks use the destination filesystem, not the source.
"""
from __future__ import annotations

import os
from pathlib import Path
import shutil


def validate_storage_parent(parent: Path, dataset_root: Path) -> Path:
    """Ensure the destination is an accessible directory outside source PCAPs."""
    path = Path(parent).expanduser().resolve()
    source = Path(dataset_root).expanduser().resolve()
    if not path.is_dir():
        raise NotADirectoryError(f"Output storage folder does not exist: {path}")
    if path == source or source in path.parents:
        raise ValueError(
            "Output storage must be outside the selected dataset tree. "
            "Otherwise generated PCAPs could be picked up as input in later runs."
        )
    if not os.access(path, os.W_OK | os.X_OK):
        raise PermissionError(f"No write access to output storage folder: {path}")
    return path


def check_campaign_output_path(output: Path, dataset_root: Path) -> None:
    """Protect CLI users from writing generated traffic into the source tree."""
    destination = Path(output).expanduser().resolve()
    source = Path(dataset_root).expanduser().resolve()
    if destination == source or source in destination.parents:
        raise ValueError(
            f"Campaign output {destination} is inside dataset root {source}. "
            "Choose a separate output storage folder."
        )


def storage_estimate(
    parent: Path, dataset_root: Path, generated_bytes: int, *,
    reserve_fraction: float = 0.10,
) -> dict:
    """Report logical PCAP estimate and actual free bytes at selected output FS.

    This is an estimate, not a quota guarantee: files can differ in size by
    the allowed tolerance, and metrics/checkpoints/models add further usage.
    """
    selected = validate_storage_parent(parent, dataset_root)
    if generated_bytes < 0 or reserve_fraction < 0:
        raise ValueError("Storage estimates and reserve fraction must be nonnegative.")
    free = shutil.disk_usage(selected).free
    required = int(generated_bytes * (1 + reserve_fraction))
    return {
        "output_parent": str(selected),
        "estimated_capture_bytes": generated_bytes,
        "estimated_minimum_with_reserve_bytes": required,
        "available_bytes": free,
        "reserve_fraction": reserve_fraction,
        "appears_sufficient": free >= required,
    }


def gib(amount: int) -> str:
    return f"{amount / (1024 ** 3):,.2f} GiB"
