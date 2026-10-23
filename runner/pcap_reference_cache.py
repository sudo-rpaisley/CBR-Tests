"""Optional, content-verified cross-process cache for decoded reference PCAP features.

Set CBR_PCAP_REFERENCE_CACHE_DIR to a dedicated writable directory. Every lookup
hashes the complete input capture, so changing the source can never silently
reuse data from an older capture. NumPy archives contain only non-object arrays:
they are opened with allow_pickle=False and replaced atomically after creation.

Only the reference dataframe is cached; the canonical dataset under evaluation
and its complete metrics are never sampled or modified.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import scapy

CACHE_SCHEMA_VERSION = 1
CACHE_ENV = "CBR_PCAP_REFERENCE_CACHE_DIR"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(4 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _decoder_identity() -> str:
    """Invalidate old cache entries whenever packet feature semantics change."""
    from runner import pcap_adapter, pcap_compact
    digest = hashlib.sha256()
    digest.update(str(CACHE_SCHEMA_VERSION).encode("ascii"))
    digest.update(str(scapy.__version__).encode("ascii"))
    for module in (pcap_adapter, pcap_compact):
        digest.update(Path(module.__file__).read_bytes())
    return digest.hexdigest()


def _archive_path(source: Path, cache_dir: Path) -> tuple[Path, str, str]:
    source_sha = _sha256_file(source)
    decoder_id = _decoder_identity()
    return cache_dir / f"ref-{source_sha}-{decoder_id[:16]}.npz", source_sha, decoder_id


def _serialize(frame: pd.DataFrame, source_sha: str, decoder_id: str) -> dict:
    meta = {
        "version": CACHE_SCHEMA_VERSION,
        "sha256": source_sha,
        "decoder": decoder_id,
        "columns": list(frame.columns),
        "rows": len(frame),
        "categories": {},
    }
    arrays = {}
    for index, column in enumerate(frame.columns):
        series = frame[column]
        if isinstance(series.dtype, pd.CategoricalDtype):
            meta["categories"][column] = {
                "values": [str(category) for category in series.cat.categories],
                "ordered": bool(series.cat.ordered),
            }
            arrays[f"column_{index}"] = series.cat.codes.to_numpy(copy=True)
        else:
            data = series.to_numpy(copy=True)
            if data.dtype.hasobject:
                raise ValueError(f"Reference cache cannot serialize object column: {column}")
            arrays[f"column_{index}"] = data
    arrays["metadata"] = np.frombuffer(
        json.dumps(meta, sort_keys=True).encode("utf-8"), dtype=np.uint8
    )
    return arrays


def _deserialize(archive: Path, source_sha: str, decoder_id: str) -> pd.DataFrame:
    with np.load(archive, allow_pickle=False) as data:
        meta = json.loads(data["metadata"].tobytes().decode("utf-8"))
        if (
            meta.get("version") != CACHE_SCHEMA_VERSION
            or meta.get("sha256") != source_sha
            or meta.get("decoder") != decoder_id
        ):
            raise ValueError("Stale reference cache metadata")
        columns = meta["columns"]
        if not isinstance(columns, list) or len(set(columns)) != len(columns):
            raise ValueError("Invalid cached columns")
        items = {}
        for index, column in enumerate(columns):
            array = data[f"column_{index}"]
            if array.ndim != 1 or len(array) != meta["rows"]:
                raise ValueError("Corrupted cached column shape")
            if column in meta["categories"]:
                spec = meta["categories"][column]
                items[column] = pd.Categorical.from_codes(
                    np.asarray(array, dtype=np.int64),
                    categories=spec["values"],
                    ordered=bool(spec["ordered"]),
                )
            elif array.dtype.hasobject:
                raise ValueError("Object arrays are forbidden in reference cache")
            else:
                items[column] = np.asarray(array).copy()
    frame = pd.DataFrame(items, columns=columns)
    if len(frame) != meta["rows"]:
        raise ValueError("Cached reference dataframe row count mismatch")
    return frame


def load_reference_pcap_with_cache(source: Path, decode) -> pd.DataFrame:
    """Decode reference PCAP, optionally reusing an immutable hashed snapshot."""
    directory = os.environ.get(CACHE_ENV, "").strip()
    if not directory:
        return decode(source)
    source = Path(source).expanduser().resolve()
    cache_dir = Path(directory).expanduser().resolve()
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        archive, source_sha, decoder_id = _archive_path(source, cache_dir)
    except OSError as exc:
        warnings.warn(f"Reference cache unavailable, decoding normally: {exc}", stacklevel=2)
        return decode(source)

    if archive.exists():
        try:
            frame = _deserialize(archive, source_sha, decoder_id)
            print(f"Reference PCAP feature cache hit: {archive}", flush=True)
            return frame
        except (OSError, ValueError, KeyError, TypeError, IndexError, json.JSONDecodeError) as exc:
            warnings.warn(f"Invalid reference cache, decoding again: {exc}", stacklevel=2)

    frame = decode(source)
    try:
        temporary_path = None
        with tempfile.NamedTemporaryFile(
            dir=cache_dir, prefix=".reference-", suffix=".npz", delete=False
        ) as stream:
            temporary_path = Path(stream.name)
            np.savez(stream, **_serialize(frame, source_sha, decoder_id))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, archive)
        print(f"Reference PCAP feature cache created: {archive}", flush=True)
    except (OSError, ValueError, TypeError) as exc:
        warnings.warn(f"Reference cache write skipped: {exc}", stacklevel=2)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    return frame
