"""Persistent DuckDB catalogue for complete CBR-Tests GAN outcome archives.

Source JSON files remain authoritative, immutable inputs. The catalogue can be
rebuilt at any time and exports compressed Parquet snapshots for analysis.
DuckDB is optional for the ordinary CBR runner; only results workflows import it.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Iterator

from runner.batch_reports import extract_primary_metric_value
from runner.corpus_perturbation import leaves


SCHEMA_VERSION = 1
TABLES = ("campaigns", "cases", "jobs", "metrics", "measurements")


def _duckdb():
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError(
            "DuckDB is not installed. Run: python -m pip install -r requirements-results.txt"
        ) from exc
    return duckdb


def _key(*parts: Any) -> str:
    return hashlib.sha256(
        json.dumps(parts, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True)


def _numeric(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    converted = float(value)
    return converted if math.isfinite(converted) else None


def _value_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "object"
    return "array"


def _hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def open_catalogue(path: Path, *, read_only: bool = False):
    """Open DuckDB and initialize the schema when writing."""
    target = Path(path).expanduser().resolve()
    if read_only and not target.is_file():
        raise FileNotFoundError(f"Catalogue does not exist: {target}")
    if not read_only:
        target.parent.mkdir(parents=True, exist_ok=True)
    conn = _duckdb().connect(str(target), read_only=read_only)
    if not read_only:
        _create_schema(conn)
    return conn


def _create_schema(conn) -> None:
    """Minimal relational keys; no compressed PCAP or original JSON is deleted."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_info (version INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS campaigns (
            campaign_key VARCHAR PRIMARY KEY,
            campaign_id VARCHAR,
            started_at VARCHAR,
            indexed_at VARCHAR,
            status VARCHAR,
            source_path VARCHAR,
            source_kind VARCHAR,
            source_signature VARCHAR,
            config_sha256 VARCHAR,
            total_cases INTEGER,
            completed_cases INTEGER,
            failed_cases INTEGER,
            data_root VARCHAR
        );
        CREATE TABLE IF NOT EXISTS cases (
            case_key VARCHAR PRIMARY KEY, campaign_key VARCHAR,
            case_id VARCHAR, group_path VARCHAR,
            held_out_path VARCHAR, held_out_sha256 VARCHAR,
            reference_path VARCHAR, training_json VARCHAR,
            status VARCHAR, attempt_count INTEGER
        );
        CREATE TABLE IF NOT EXISTS jobs (
            job_key VARCHAR PRIMARY KEY, campaign_key VARCHAR, case_key VARCHAR,
            attempt INTEGER, attempt_status VARCHAR, seed INTEGER,
            perturbation_pct INTEGER, actual_packet_pct DOUBLE,
            dataset_path VARCHAR, reference_path VARCHAR,
            job_status VARCHAR, outcome_path VARCHAR, outcome_sha256 VARCHAR,
            model_sha256 VARCHAR, plan_sha256 VARCHAR
        );
        CREATE TABLE IF NOT EXISTS metrics (
            metric_key VARCHAR PRIMARY KEY, campaign_key VARCHAR,
            case_key VARCHAR, job_key VARCHAR,
            metric_id VARCHAR, execution_status VARCHAR,
            domain_status VARCHAR, primary_value DOUBLE,
            primary_value_json VARCHAR, execution_json VARCHAR,
            payload_json VARCHAR
        );
        CREATE TABLE IF NOT EXISTS measurements (
            measurement_key VARCHAR PRIMARY KEY, campaign_key VARCHAR,
            job_key VARCHAR, metric_key VARCHAR,
            json_pointer VARCHAR, value_type VARCHAR,
            numeric_value DOUBLE, value_json VARCHAR
        );
        CREATE VIEW IF NOT EXISTS metric_observations AS
            SELECT ca.campaign_id, ca.started_at, ca.status AS campaign_status,
                cs.group_path, cs.held_out_path, cs.held_out_sha256,
                j.attempt, j.attempt_status, j.seed,
                j.perturbation_pct, j.actual_packet_pct,
                j.outcome_path, j.outcome_sha256,
                m.metric_id, m.execution_status, m.domain_status,
                m.primary_value, m.primary_value_json
            FROM metrics m
            JOIN jobs j ON m.job_key = j.job_key
            JOIN cases cs ON j.case_key = cs.case_key
            JOIN campaigns ca ON j.campaign_key = ca.campaign_key;
    """)
    values = conn.execute("SELECT version FROM schema_info").fetchall()
    if not values:
        conn.execute("INSERT INTO schema_info VALUES (?)", [SCHEMA_VERSION])
    elif values != [(SCHEMA_VERSION,)]:
        raise ValueError(f"Unsupported catalogue schema version {values!r}")


def _source_folders(search_root: Path) -> list[tuple[Path, str]]:
    """Discover manifests recursively without double-importing nested experiments."""
    root = Path(search_root).expanduser().resolve()
    if not root.is_dir():
        raise NotADirectoryError(root)
    campaign_files = sorted(root.rglob("campaign.json"))
    campaign_dirs = [p.parent.resolve() for p in campaign_files]
    sources = [(p, "corpus") for p in campaign_files]
    for path in sorted(root.rglob("experiment.json")):
        resolved = path.resolve()
        if any(folder in resolved.parents for folder in campaign_dirs):
            continue
        sources.append((path, "single"))
    return sorted(sources, key=lambda x: str(x[0]))


def _normalise(path: Path, kind: str) -> dict:
    """Represent both corpus campaigns and historical individual experiments."""
    source = path.expanduser().resolve()
    data = _read(source)
    if kind == "corpus":
        return data
    if kind != "single":
        raise ValueError(f"Unknown source kind {kind}")
    case = {
        "case_id": source.parent.name or "single",
        "folder": source.parent.name,
        "held_out": data.get("comparison_dataset_path") or data.get("source_path", ""),
        "held_out_sha256": data.get("comparison_dataset_sha256") or data.get("source_sha256", ""),
        "reference_path": data.get("base_dataset_path") or "",
        "training": data.get("training_captures") or [],
        "status": data.get("status", "unknown"),
        "attempts": [{"path": str(source.parent), "status": data.get("status", "unknown")}],
        "output_dir": str(source.parent),
    }
    return {
        "campaign_id": f"single-{source.parent.name}-{_key(str(source))[:12]}",
        "started_at": data.get("started_at", ""),
        "status": data.get("status", "unknown"),
        "config_sha256": data.get("plan_sha256"),
        "cases": [case],
        "settings": {"dataset_root": str(source.parent)},
    }


def _iter_attempts(case: dict):
    attempts = case.get("attempts") or [{
        "path": case.get("output_dir", ""), "status": case.get("status", "unknown")
    }]
    for index, attempt in enumerate(attempts, start=1):
        yield index, attempt


def _source_signature(source: Path, campaign: dict) -> str:
    """Cheap but change-sensitive fingerprint, including raw outcome file stats."""
    files: set[Path] = {source.resolve()}
    for case in campaign.get("cases", []):
        for _, attempt in _iter_attempts(case):
            base = Path(str(attempt.get("path", "")))
            experiment = base / "experiment.json"
            if not experiment.is_file():
                continue
            files.add(experiment.resolve())
            data = _read(experiment)
            for run in data.get("runs", []):
                state = Path(run.get("batch_output_dir", "")) / "batch_state.json"
                if not state.is_file():
                    continue
                files.add(state.resolve())
                for job in _read(state).get("results", []):
                    candidate = Path(str(job.get("output_path", "")))
                    if candidate.is_file():
                        files.add(candidate.resolve())
    entries = []
    for p in sorted(files):
        stat = p.stat()
        entries.append((str(p), stat.st_size, stat.st_mtime_ns))
    return _key(entries)


def _insert_batches(conn, table: str, columns: tuple[str, ...], rows: Iterator[tuple], batch_size: int = 500) -> int:
    """Insert incrementally; never materialize every measurement in Python RAM."""
    placeholders = ",".join("?" for _ in columns)
    statement = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
    count = 0
    pending = []
    for row in rows:
        pending.append(row)
        if len(pending) >= batch_size:
            conn.executemany(statement, pending)
            count += len(pending)
            pending = []
    if pending:
        conn.executemany(statement, pending)
        count += len(pending)
    return count


def _purge_campaign(conn, key: str):
    """Delete only catalogue copies of this campaign, never original files."""
    for table in ("measurements", "metrics", "jobs", "cases", "campaigns"):
        conn.execute(f"DELETE FROM {table} WHERE campaign_key = ?", [key])


def _metric_data(outcome: dict):
    records = {
        str(v["metric_id"]): v
        for v in outcome.get("metric_results", [])
        if isinstance(v, dict) and v.get("metric_id")
    }
    payloads = outcome.get("test_results", {})
    if not isinstance(payloads, dict):
        payloads = {}
    for metric in sorted(set(records) | set(payloads)):
        yield metric, records.get(metric, {}), payloads.get(metric)


def index_source(conn, source: Path, kind: str, *, force: bool = False) -> dict:
    """Import one campaign atomically; repeated indexing never duplicates rows."""
    source = Path(source).expanduser().resolve()
    campaign = _normalise(source, kind)
    key = _key(str(source))
    signature = _source_signature(source, campaign)
    previous = conn.execute(
        "SELECT source_signature FROM campaigns WHERE campaign_key = ?", [key]
    ).fetchone()
    if previous and previous[0] == signature and not force:
        return {"path": str(source), "status": "unchanged", "jobs": 0, "metrics": 0, "measurements": 0}

    counters = {"jobs": 0, "metrics": 0, "measurements": 0}
    conn.execute("BEGIN TRANSACTION")
    try:
        _purge_campaign(conn, key)
        cases = campaign.get("cases", [])
        data_root = campaign.get("settings", {}).get("dataset_root", "")
        conn.execute(
            "INSERT INTO campaigns VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                key, campaign.get("campaign_id", ""), campaign.get("started_at", ""),
                _now(), campaign.get("status", ""), str(source), kind, signature,
                campaign.get("config_sha256"), len(cases),
                sum(c.get("status") == "completed" for c in cases),
                sum(c.get("status") == "failed" for c in cases),
                str(data_root),
            ],
        )
        for case in cases:
            case_id = str(case.get("case_id", ""))
            case_key = _key(key, case_id)
            conn.execute(
                "INSERT INTO cases VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    case_key, key, case_id, str(case.get("folder", "")),
                    str(case.get("held_out", "")), str(case.get("held_out_sha256", "")),
                    str(case.get("reference_path") or case.get("held_out", "")),
                    _json(case.get("training", [])), str(case.get("status", "")),
                    len(list(_iter_attempts(case))),
                ],
            )
            for attempt, attempt_meta in _iter_attempts(case):
                experiment_path = Path(str(attempt_meta.get("path", ""))) / "experiment.json"
                if not experiment_path.is_file():
                    continue
                experiment = _read(experiment_path)
                for run in experiment.get("runs", []):
                    state_path = Path(str(run.get("batch_output_dir", ""))) / "batch_state.json"
                    if not state_path.is_file():
                        continue
                    state = _read(state_path)
                    captures = {
                        str(Path(p["dataset_path"]).resolve()): p
                        for p in run.get("captures", []) if p.get("dataset_path")
                    }
                    for job_index, job in enumerate(state.get("results", [])):
                        dataset = str(Path(str(job.get("dataset_path", ""))).resolve())
                        capture = captures.get(dataset)
                        pct = int(capture["requested_pct"]) if capture else None
                        actual_pct = float(capture["actual_packet_pct"]) if capture else None
                        outcome_path = Path(str(job.get("output_path", ""))).expanduser().resolve()
                        exists = outcome_path.is_file()
                        outcome_hash = _hash_file(outcome_path) if exists else None
                        job_key = _key(case_key, attempt, run.get("seed"), pct, dataset, job_index)
                        reference = case.get("reference_path") or case.get("held_out")
                        conn.execute(
                            "INSERT INTO jobs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            [
                                job_key, key, case_key, attempt, attempt_meta.get("status", ""),
                                int(run["seed"]), pct, actual_pct, dataset, str(reference),
                                job.get("status", ""), str(outcome_path) if exists else None,
                                outcome_hash, experiment.get("model_sha256"),
                                experiment.get("plan_sha256"),
                            ],
                        )
                        counters["jobs"] += 1
                        if not exists:
                            continue
                        outcome = _read(outcome_path)
                        for metric_id, execution, payload in _metric_data(outcome):
                            primary, _, _ = extract_primary_metric_value(metric_id, payload)
                            metric_key = _key(job_key, metric_id)
                            conn.execute(
                                "INSERT INTO metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                [
                                    metric_key, key, case_key, job_key, metric_id,
                                    execution.get("status", ""), execution.get("result_status", ""),
                                    _numeric(primary), _json(primary),
                                    _json(execution), _json(payload),
                                ],
                            )
                            counters["metrics"] += 1
                            def measurements():
                                for pointer, value in leaves(payload):
                                    yield (
                                        _key(metric_key, pointer), key, job_key, metric_key,
                                        pointer, _value_type(value), _numeric(value), _json(value)
                                    )
                            counters["measurements"] += _insert_batches(
                                conn, "measurements",
                                ("measurement_key", "campaign_key", "job_key", "metric_key",
                                 "json_pointer", "value_type", "numeric_value", "value_json"),
                                measurements(),
                            )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return {"path": str(source), "status": "indexed", **counters}


def index_roots(
    conn,
    roots: list[Path],
    *,
    force: bool = False,
) -> dict:
    """Index every corpus or single experiment under one or more output roots."""
    files = {}
    for root in roots:
        for path, kind in _source_folders(root):
            files[str(path.resolve())] = (path, kind)
    reports = []
    for path, kind in sorted(files.values(), key=lambda item: str(item[0])):
        try:
            reports.append(index_source(conn, path, kind, force=force))
        except (OSError, ValueError, KeyError, TypeError, _duckdb().Error) as exc:
            reports.append({"path": str(path), "status": "failed", "error": str(exc)})
    return {
        "sources_found": len(files),
        "indexed": sum(x["status"] == "indexed" for x in reports),
        "unchanged": sum(x["status"] == "unchanged" for x in reports),
        "failed": sum(x["status"] == "failed" for x in reports),
        "reports": reports,
    }


def export_parquet(conn, folder: Path) -> dict:
    """Produce compressed, replaceable analytical snapshots from indexed tables."""
    out = Path(folder).expanduser().resolve()
    out.mkdir(parents=True, exist_ok=True)
    counts = {}
    for table in TABLES:
        final = out / f"{table}.parquet"
        staged = out / f".{table}.new.parquet"
        try:
            # SQL identifier comes only from fixed TABLES constant.
            conn.execute(
                f"COPY (SELECT * FROM {table}) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
                [str(staged)],
            )
            os.replace(staged, final)
        finally:
            staged.unlink(missing_ok=True)
        counts[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    (out / "catalogue_manifest.json").write_text(
        _json({"schema_version": SCHEMA_VERSION, "exported_at": _now(), "rows": counts}) + "\n",
        encoding="utf-8",
    )
    return {"folder": str(out), "rows": counts}


def summary(conn) -> dict:
    return {
        table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in TABLES
    }


def compare_metric(
    conn, metric_id: str, *,
    held_out: str = "",
    date_from: str = "",
    date_to: str = "",
    limit: int = 1000,
    offset: int = 0,
) -> list[dict]:
    """Query a metric across campaigns/dates with repeatable filters."""
    if limit < 1 or limit > 10000 or offset < 0:
        raise ValueError("Limit must be 1–10000 and offset nonnegative.")
    sql = """
        SELECT campaign_id, started_at, group_path, held_out_path,
               seed, perturbation_pct, attempt, attempt_status,
               execution_status, domain_status, primary_value,
               outcome_path
        FROM metric_observations WHERE metric_id = ?
    """
    args: list[Any] = [metric_id]
    if held_out:
        sql += " AND lower(held_out_path) LIKE ?"
        args.append(f"%{held_out.lower()}%")
    if date_from:
        sql += " AND started_at >= ?"
        args.append(date_from)
    if date_to:
        sql += " AND started_at < ?"
        args.append(date_to)
    sql += " ORDER BY started_at DESC, held_out_path, perturbation_pct, seed, attempt LIMIT ? OFFSET ?"
    args.extend([limit, offset])
    cursor = conn.execute(sql, args)
    names = [c[0] for c in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]
