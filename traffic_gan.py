#!/usr/bin/env python3
"""Run the bundled CTGAN PCAP generator without downloading a separate ZIP.

The complete tested research-prototype sources (including unit tests, README,
and license-independent study notes) are stored in gan_generator/ as a pinned
source archive. Only the four runtime Python modules are extracted temporarily.
"""
from __future__ import annotations

from pathlib import Path
import runpy
import sys
import tarfile
import tempfile


_SOURCE_NAMES = (
    "traffic_gan.py",
    "flow_selection.py",
    "tail_replacement.py",
    "variable_replacement.py",
)
_ARCHIVE = (
    Path(__file__).resolve().parent
    / "gan_generator"
    / "ctgan-pcap-same-count-source.tar.xz"
)


def main() -> None:
    """Execute the CTGAN CLI using the source version committed to Git."""
    if not _ARCHIVE.is_file():
        raise SystemExit(
            f"Bundled generator archive missing: {_ARCHIVE}. Pull the feature branch again."
        )
    with tempfile.TemporaryDirectory(prefix="cbr-tests-gan-") as working:
        source = Path(working)
        with tarfile.open(_ARCHIVE, mode="r:xz") as bundle:
            for name in _SOURCE_NAMES:
                member = bundle.getmember(f"ctgan-pcap-same-count/{name}")
                stream = bundle.extractfile(member)
                if stream is None or not member.isfile():
                    raise RuntimeError(f"Generator source missing: {name}")
                (source / name).write_bytes(stream.read())
        sys.path.insert(0, str(source))
        # Keep the archived CTGAN training implementation unchanged, but
        # stream capture extraction and suffix rewriting in bounded memory.
        if len(sys.argv) > 1 and sys.argv[1] in ("extract", "sweep"):
            runpy.run_path(str(Path(__file__).resolve().parent /
                              "gan_generator" / "streaming_sweep.py"), run_name="__main__")
        else:
            runpy.run_path(str(source / "traffic_gan.py"), run_name="__main__")


if __name__ == "__main__":
    main()
