"""Smoke tests for the pinned CTGAN source bundle and CLI launcher."""
from __future__ import annotations

import hashlib
from pathlib import Path
import subprocess
import sys
import tarfile


ROOT = Path(__file__).resolve().parents[1]
ARCHIVE = ROOT / "gan_generator" / "ctgan-pcap-same-count-source.tar.xz"
REQUIRED = {
    "ctgan-pcap-same-count/traffic_gan.py",
    "ctgan-pcap-same-count/flow_selection.py",
    "ctgan-pcap-same-count/tail_replacement.py",
    "ctgan-pcap-same-count/variable_replacement.py",
    "ctgan-pcap-same-count/requirements.txt",
    "ctgan-pcap-same-count/tests/test_variable_replacement.py",
    "ctgan-pcap-same-count/tests/test_tail_replacement.py",
    "ctgan-pcap-same-count/tests/test_flow_selection.py",
}


def test_generator_bundle_is_complete_and_pinned():
    """Protect the embedded, peer-reviewable source snapshot from drift."""
    actual = hashlib.sha256(ARCHIVE.read_bytes()).hexdigest()
    assert actual == "05b977017302d54a2c3d29c529d973ade54379c8946a4c7aea7d3a27aee9f115"
    with tarfile.open(ARCHIVE, "r:xz") as archive:
        assert REQUIRED.issubset(set(archive.getnames()))


def test_generator_launcher_exposes_extract_train_and_sweep():
    """Exercise the same launcher and CLI that Spark users will invoke."""
    completed = subprocess.run(
        [sys.executable, str(ROOT / "traffic_gan.py"), "--help"],
        cwd=ROOT, capture_output=True, text=True, check=True,
    )
    assert "extract" in completed.stdout
    assert "train" in completed.stdout
    assert "sweep" in completed.stdout
