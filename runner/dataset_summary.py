from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from runner.dataset_loading import is_tabular_dataset
from runner.pcap_adapter import is_packet_capture


DATASET_SUMMARY_SCHEMA_VERSION = 1
_METADATA_PREFIX = "<!-- cbr-tests-dataset-summary "
_METADATA_SUFFIX = " -->"

_TIMESTAMP_FIELD_NAMES = {
    "timestamp",
    "time",
    "datetime",
    "date time",
    "start timestamp",
    "start time",
    "flow start",
    "flow start time",
    "flow start timestamp",
}

_SOURCE_IP_FIELDS = ("Source IP", "Src IP", "source_ip", "src_ip")
_DESTINATION_IP_FIELDS = ("Destination IP", "Dst IP", "destination_ip", "dst_ip")
_SOURCE_PORT_FIELDS = ("Source Port", "Src Port", "source_port", "src_port")
_DESTINATION_PORT_FIELDS = ("Destination Port", "Dst Port", "destination_port", "dst_port")
_PROTOCOL_FIELDS = ("Protocol", "protocol")
_PACKET_LENGTH_FIELDS = ("Packet Length", "packet_length", "Frame Length", "frame_length")
_IAT_FIELDS = ("Inter Arrival Time", "inter_arrival_time", "IAT", "iat")


def default_dataset_summary_path(dataset_path: Path) -> Path:
    """Return the human-readable summary sidecar path beside the dataset."""
    path = Path(dataset_path).expanduser().resolve()
    return path.with_name(f"{path.name}.summary.md")


def _read_summary_metadata(summary_path: Path) -> dict[str, Any] | None:
    if not summary_path.exists() or not summary_path.is_file():
        return None
    try:
        with summary_path.open("r", encoding="utf-8") as handle:
            for _ in range(8):
                line = handle.readline()
                if not line:
                    break
                stripped = line.strip()
                if stripped.startswith(_METADATA_PREFIX) and stripped.endswith(_METADATA_SUFFIX):
                    payload = stripped[len(_METADATA_PREFIX) : -len(_METADATA_SUFFIX)]
                    value = json.loads(payload)
                    return value if isinstance(value, dict) else None
    except (OSError, json.JSONDecodeError):
        return None
    return None


def _metadata_line(dataset_sha256: str) -> str:
    payload = {
        "schema_version": DATASET_SUMMARY_SCHEMA_VERSION,
        "dataset_sha256": dataset_sha256,
    }
    return f"{_METADATA_PREFIX}{json.dumps(payload, sort_keys=True, separators=(',', ':'))}{_METADATA_SUFFIX}"


def _find_column(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    by_lower = {str(column).strip().lower(): str(column) for column in df.columns}
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
        match = by_lower.get(candidate.lower())
        if match is not None:
            return match
    return None


def _format_bytes(size_bytes: int) -> str:
    value = float(size_bytes)
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    unit = units[0]
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            break
        value /= 1024.0
    return f"{value:.2f} {unit}" if unit != "B" else f"{int(value)} B"


def _format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "Not determined"
    seconds = max(0.0, float(seconds))
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)
    if days >= 1:
        return f"{int(days)}d {int(hours):02d}h {int(minutes):02d}m {secs:06.3f}s"
    if hours >= 1:
        return f"{int(hours)}h {int(minutes):02d}m {secs:06.3f}s"
    if minutes >= 1:
        return f"{int(minutes)}m {secs:06.3f}s"
    return f"{secs:.6f}s"


def _iso_timestamp(value: pd.Timestamp) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _time_coverage(df: pd.DataFrame, *, packet_capture: bool) -> dict[str, Any]:
    field: str | None = None
    parsed: pd.Series | None = None
    interpretation = None

    if packet_capture and "Timestamp" in df.columns:
        field = "Timestamp"
        parsed = pd.to_datetime(df[field], errors="coerce", utc=True, unit="s")
        interpretation = "Unix epoch seconds from the packet capture"
    else:
        for column in df.columns:
            normalised = str(column).strip().lower().replace("_", " ")
            if normalised not in _TIMESTAMP_FIELD_NAMES:
                continue
            series = df[column]
            if pd.api.types.is_numeric_dtype(series):
                # A numeric timestamp needs an explicit unit. Do not guess one in a
                # descriptive sidecar because that could create false time coverage.
                continue
            candidate = pd.to_datetime(series, errors="coerce", utc=True)
            if int(candidate.notna().sum()) >= 2:
                field = str(column)
                parsed = candidate
                interpretation = "Parsed datetime values; no timezone assumptions beyond parser/embedded offsets"
                break

    if field is None or parsed is None:
        return {
            "field": None,
            "parseable_count": 0,
            "first": None,
            "last": None,
            "duration_seconds": None,
            "observed_utc_dates": None,
            "interpretation": "No timestamp field could be interpreted safely without additional unit/configuration information",
        }

    valid = parsed.dropna().sort_values()
    if valid.empty:
        return {
            "field": field,
            "parseable_count": 0,
            "first": None,
            "last": None,
            "duration_seconds": None,
            "observed_utc_dates": 0,
            "interpretation": interpretation,
        }

    first = valid.iloc[0]
    last = valid.iloc[-1]
    return {
        "field": field,
        "parseable_count": int(valid.shape[0]),
        "first": _iso_timestamp(first),
        "last": _iso_timestamp(last),
        "duration_seconds": float((last - first).total_seconds()),
        "observed_utc_dates": int(valid.dt.date.nunique()),
        "interpretation": interpretation,
    }


def _numeric_stats(series: pd.Series) -> dict[str, float] | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None
    return {
        "min": float(numeric.min()),
        "median": float(numeric.median()),
        "mean": float(numeric.mean()),
        "max": float(numeric.max()),
    }


def _network_profile(df: pd.DataFrame) -> dict[str, Any]:
    source_ip = _find_column(df, _SOURCE_IP_FIELDS)
    destination_ip = _find_column(df, _DESTINATION_IP_FIELDS)
    source_port = _find_column(df, _SOURCE_PORT_FIELDS)
    destination_port = _find_column(df, _DESTINATION_PORT_FIELDS)
    protocol = _find_column(df, _PROTOCOL_FIELDS)
    packet_length = _find_column(df, _PACKET_LENGTH_FIELDS)
    iat = _find_column(df, _IAT_FIELDS)

    profile: dict[str, Any] = {}
    if source_ip:
        profile["source_ip_field"] = source_ip
        profile["unique_source_ips"] = int(df[source_ip].dropna().astype(str).nunique())
    if destination_ip:
        profile["destination_ip_field"] = destination_ip
        profile["unique_destination_ips"] = int(df[destination_ip].dropna().astype(str).nunique())
    if source_ip or destination_ip:
        endpoints: set[str] = set()
        if source_ip:
            endpoints.update(df[source_ip].dropna().astype(str).unique().tolist())
        if destination_ip:
            endpoints.update(df[destination_ip].dropna().astype(str).unique().tolist())
        profile["unique_ip_endpoints"] = len(endpoints)

    if source_port:
        profile["source_port_field"] = source_port
        profile["unique_source_ports"] = int(pd.to_numeric(df[source_port], errors="coerce").dropna().nunique())
    if destination_port:
        profile["destination_port_field"] = destination_port
        profile["unique_destination_ports"] = int(pd.to_numeric(df[destination_port], errors="coerce").dropna().nunique())
    if source_port or destination_port:
        ports: set[int] = set()
        for field in (source_port, destination_port):
            if field:
                values = pd.to_numeric(df[field], errors="coerce").dropna().astype(int).unique().tolist()
                ports.update(int(value) for value in values)
        profile["unique_ports"] = len(ports)

    if protocol:
        counts = df[protocol].dropna().astype(str).value_counts().head(12)
        profile["protocol_field"] = protocol
        profile["protocol_counts"] = {str(key): int(value) for key, value in counts.items()}

    if packet_length:
        stats = _numeric_stats(df[packet_length])
        if stats is not None:
            profile["packet_length_field"] = packet_length
            profile["packet_length"] = stats
    if iat:
        stats = _numeric_stats(df[iat])
        if stats is not None:
            profile["inter_arrival_field"] = iat
            profile["inter_arrival"] = stats

    return profile


def _build_summary_data(
    dataset_path: Path,
    dataset_sha256: str,
    dataframe: pd.DataFrame,
    *,
    packet_capture: bool,
) -> dict[str, Any]:
    stat = dataset_path.stat()
    row_count = int(len(dataframe))
    column_count = int(dataframe.shape[1])
    missing_cells = int(dataframe.isna().sum().sum()) if not packet_capture else None
    total_cells = row_count * column_count
    duplicate_rows = int(dataframe.duplicated().sum()) if not packet_capture and row_count else None

    return {
        "schema_version": DATASET_SUMMARY_SCHEMA_VERSION,
        "dataset_sha256": dataset_sha256,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "path": str(dataset_path),
        "filename": dataset_path.name,
        "format": dataset_path.suffix.lower().lstrip(".") or "unknown",
        "size_bytes": int(stat.st_size),
        "modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
        "record_type": "decoded IPv4/IPv6 packets" if packet_capture else "rows",
        "record_count": row_count,
        "column_count": column_count,
        "columns": [str(column) for column in dataframe.columns],
        "numeric_column_count": int(len(dataframe.select_dtypes(include="number").columns)),
        "missing_cell_count": missing_cells,
        "missing_cell_ratio": (
            float(missing_cells / total_cells) if missing_cells is not None and total_cells else None
        ),
        "duplicate_row_count": duplicate_rows,
        "duplicate_row_ratio": (
            float(duplicate_rows / row_count) if duplicate_rows is not None and row_count else None
        ),
        "time_coverage": _time_coverage(dataframe, packet_capture=packet_capture),
        "network": _network_profile(dataframe),
    }


def _fmt_number(value: Any, *, digits: int = 6) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return f"{value:,}" if isinstance(value, int) else str(value)


def format_dataset_summary(data: dict[str, Any]) -> str:
    time_coverage = data["time_coverage"]
    network = data["network"]
    lines = [
        "# Dataset Summary",
        "",
        _metadata_line(data["dataset_sha256"]),
        "",
        "> Generated by CBR-Tests as a descriptive dataset sidecar. This report does not itself make a realism judgement.",
        "",
        "## File identity",
        "",
        f"- **Dataset:** `{data['filename']}`",
        f"- **Path:** `{data['path']}`",
        f"- **Format:** `{data['format']}`",
        f"- **Size:** {_format_bytes(data['size_bytes'])} ({data['size_bytes']:,} bytes)",
        f"- **SHA-256:** `{data['dataset_sha256']}`",
        f"- **File modified (UTC):** {data['modified_at']}",
        f"- **Summary generated (UTC):** {data['generated_at']}",
        "",
        "## Structure",
        "",
        f"- **Records:** {data['record_count']:,} {data['record_type']}",
        f"- **Fields/columns:** {data['column_count']:,}",
        f"- **Numeric fields:** {data['numeric_column_count']:,}",
    ]

    if data["missing_cell_count"] is not None:
        missing_ratio = (
            f"{data['missing_cell_ratio']:.4%}"
            if data["missing_cell_ratio"] is not None
            else "n/a"
        )
        duplicate_ratio = (
            f"{data['duplicate_row_ratio']:.4%}"
            if data["duplicate_row_ratio"] is not None
            else "n/a"
        )
        lines.extend(
            [
                f"- **Missing cells:** {data['missing_cell_count']:,} ({missing_ratio})",
                f"- **Exact duplicate rows:** {data['duplicate_row_count']:,} ({duplicate_ratio})",
            ]
        )
    elif data["record_type"].startswith("decoded"):
        lines.append("- **PCAP note:** record count covers decoded IPv4/IPv6 packets in the canonical packet view; non-IP frames are not represented here.")

    lines.extend(["", "### Fields", ""])
    lines.extend(f"- `{column}`" for column in data["columns"])

    lines.extend(
        [
            "",
            "## Time coverage",
            "",
            f"- **Timestamp field:** `{time_coverage['field']}`" if time_coverage["field"] else "- **Timestamp field:** not determined safely",
            f"- **Parseable timestamps:** {time_coverage['parseable_count']:,}",
            f"- **First timestamp:** {time_coverage['first'] or 'n/a'}",
            f"- **Last timestamp:** {time_coverage['last'] or 'n/a'}",
            f"- **Covered duration:** {_format_duration(time_coverage['duration_seconds'])}",
            f"- **Observed UTC calendar dates:** {_fmt_number(time_coverage['observed_utc_dates'])}",
            f"- **Interpretation:** {time_coverage['interpretation']}",
        ]
    )

    if network:
        lines.extend(["", "## Network characteristics", ""])
        labels = (
            ("unique_ip_endpoints", "Unique IP endpoints"),
            ("unique_source_ips", "Unique source IPs"),
            ("unique_destination_ips", "Unique destination IPs"),
            ("unique_ports", "Unique source/destination ports"),
            ("unique_source_ports", "Unique source ports"),
            ("unique_destination_ports", "Unique destination ports"),
        )
        for key, label in labels:
            if key in network:
                lines.append(f"- **{label}:** {_fmt_number(network[key])}")

        if network.get("protocol_counts"):
            lines.extend(["", "### Protocol values", "", "| Protocol value | Records |", "| --- | ---: |"])
            for protocol, count in network["protocol_counts"].items():
                lines.append(f"| `{protocol}` | {count:,} |")

        for stats_key, title, unit in (
            ("packet_length", "Packet length", "bytes"),
            ("inter_arrival", "Inter-arrival time", "seconds / source units when tabular"),
        ):
            stats = network.get(stats_key)
            if stats:
                lines.extend(
                    [
                        "",
                        f"### {title}",
                        "",
                        f"- **Minimum:** {_fmt_number(stats['min'])} {unit}",
                        f"- **Median:** {_fmt_number(stats['median'])} {unit}",
                        f"- **Mean:** {_fmt_number(stats['mean'])} {unit}",
                        f"- **Maximum:** {_fmt_number(stats['max'])} {unit}",
                    ]
                )

    lines.extend(
        [
            "",
            "## Cache behaviour",
            "",
            "This sidecar is reused only while both the dataset SHA-256 and dataset-summary schema version match. If either changes, CBR-Tests regenerates the summary on the next run.",
            "",
        ]
    )
    return "\n".join(lines)


def _load_dataframe_for_summary(dataset_path: Path) -> pd.DataFrame:
    if is_packet_capture(dataset_path):
        from runner.pcap_adapter import build_pcap_packet_dataframe

        return build_pcap_packet_dataframe(dataset_path)
    if is_tabular_dataset(dataset_path):
        from runner.tabular import load_tabular_dataset

        return load_tabular_dataset(dataset_path)
    raise ValueError(f"Dataset summary is not supported for: {dataset_path}")


def _write_atomic(path: Path, text: str) -> None:
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
        text=True,
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            if not text.endswith("\n"):
                handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def ensure_dataset_summary(
    dataset_path: Path,
    *,
    dataset_sha256: str,
    dataframe: pd.DataFrame | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Create, refresh, or reuse the dataset summary sidecar.

    The expensive dataset scan is skipped when the existing sidecar metadata has
    both the current SHA-256 and the current summary schema version.
    """
    resolved = Path(dataset_path).expanduser().resolve()
    summary_path = default_dataset_summary_path(resolved)
    metadata = _read_summary_metadata(summary_path)
    cache_valid = bool(
        metadata
        and metadata.get("schema_version") == DATASET_SUMMARY_SCHEMA_VERSION
        and metadata.get("dataset_sha256") == dataset_sha256
    )
    if cache_valid and not force:
        return {
            "path": str(summary_path),
            "status": "reused",
            "dataset_sha256": dataset_sha256,
            "schema_version": DATASET_SUMMARY_SCHEMA_VERSION,
        }

    existed = summary_path.exists()
    frame = dataframe if dataframe is not None else _load_dataframe_for_summary(resolved)
    data = _build_summary_data(
        resolved,
        dataset_sha256,
        frame,
        packet_capture=is_packet_capture(resolved),
    )
    _write_atomic(summary_path, format_dataset_summary(data))
    return {
        "path": str(summary_path),
        "status": "refreshed" if existed else "created",
        "dataset_sha256": dataset_sha256,
        "schema_version": DATASET_SUMMARY_SCHEMA_VERSION,
    }
