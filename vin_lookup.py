#!/usr/bin/env python3
"""Import and query an offline VIN-prefix database, with an optional web page."""

from __future__ import annotations

import argparse
import base64
import csv
import json
import sqlite3
import sys
import tempfile
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable

from vin_readers import VinReaderError, read_elm327_diagnostics, read_elm327_vin
from vehicle_records import create_report, export_report, list_diagnostic_scans, list_vehicles, save_diagnostic_scan, save_translation, save_vehicle
from code_database_import import download_source, import_code_database


DEFAULT_DATABASE = Path("data/vin.sqlite3")
PAGE_PATH = Path(__file__).with_name("vin_lookup_page.html")
_TRANSLITERATION = {
    **{str(number): number for number in range(10)},
    **dict(zip("ABCDEFGH", (1, 2, 3, 4, 5, 6, 7, 8))),
    **dict(zip("JKLMNPR", (1, 2, 3, 4, 5, 7, 9))),
    **dict(zip("STUVWXYZ", (2, 3, 4, 5, 6, 7, 8, 9))),
}
_CHECKSUM_WEIGHTS = (8, 7, 6, 5, 4, 3, 2, 10, 0, 9, 8, 7, 6, 5, 4, 3, 2)
_FIELDS = ("make", "manufacturer", "model", "model_year", "vehicle_type", "body_class")
_FIELD_ALIASES = {
    "make": ("make", "make_name"),
    "manufacturer": ("manufacturer", "manufacturer_name", "mfr_name"),
    "model": ("model", "model_name"),
    "model_year": ("model_year", "modelyear"),
    "vehicle_type": ("vehicle_type", "vehicle_type_name", "vehicletypename"),
    "body_class": ("body_class", "body_class_name", "bodyclassname"),
}


class VinLookupError(RuntimeError):
    """Raised when a VIN or local database operation is invalid."""


def normalize_vin(value: str) -> str:
    vin = "".join(value.split()).upper()
    if len(vin) != 17:
        raise VinLookupError("A VIN must contain exactly 17 characters.")
    invalid = sorted({character for character in vin if character not in _TRANSLITERATION})
    if invalid:
        raise VinLookupError(f"VIN contains invalid character(s): {', '.join(invalid)}.")
    return vin


def vin_checksum(vin: str) -> dict[str, str | bool]:
    normalized = normalize_vin(vin)
    remainder = sum(
        _TRANSLITERATION[character] * weight
        for character, weight in zip(normalized, _CHECKSUM_WEIGHTS)
    ) % 11
    expected = "X" if remainder == 10 else str(remainder)
    actual = normalized[8]
    return {"actual": actual, "expected": expected, "valid": actual == expected}


def _connect(database: Path) -> sqlite3.Connection:
    if not database.is_file():
        raise VinLookupError(
            f"VIN database not found: {database}. Import a downloaded CSV or JSON database first."
        )
    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    return connection


def lookup_vin(vin: str, *, database: Path | str = DEFAULT_DATABASE) -> dict[str, Any]:
    """Look up a VIN using the longest matching prefix in a local SQLite database."""
    normalized = normalize_vin(vin)
    with _connect(Path(database)) as connection:
        try:
            row = connection.execute(
                "SELECT * FROM vin_records WHERE ? LIKE prefix || '%' "
                "ORDER BY length(prefix) DESC LIMIT 1",
                (normalized,),
            ).fetchone()
        except sqlite3.DatabaseError as exc:
            raise VinLookupError(f"Invalid VIN database: {exc}") from exc
    if row is None:
        raise VinLookupError("No matching VIN prefix was found in the local database.")
    vehicle = {field: row[field] for field in _FIELDS if row[field] not in (None, "")}
    return {
        "vin": normalized,
        "matched_prefix": row["prefix"],
        "checksum": vin_checksum(normalized),
        "vehicle": vehicle,
    }


def _records(source: Path) -> Iterable[dict[str, Any]]:
    if source.suffix.lower() == ".json":
        value = json.loads(source.read_text(encoding="utf-8"))
        records = value.get("records") if isinstance(value, dict) else value
        if not isinstance(records, list):
            raise VinLookupError("JSON database must be a list or contain a 'records' list.")
        yield from records
        return
    with source.open(newline="", encoding="utf-8-sig") as handle:
        yield from csv.DictReader(handle)


def import_database(source: Path | str, database: Path | str = DEFAULT_DATABASE) -> int:
    """Replace the local database with records from a downloaded CSV or JSON file."""
    source_path, database_path = Path(source), Path(database)
    if not source_path.is_file():
        raise VinLookupError(f"Import file not found: {source_path}")
    database_path.parent.mkdir(parents=True, exist_ok=True)
    normalized_records = []
    for number, raw in enumerate(_records(source_path), start=1):
        if not isinstance(raw, dict):
            raise VinLookupError(f"Record {number} is not an object.")
        record = {str(key).strip().lower(): value for key, value in raw.items()}
        prefix = str(record.get("prefix") or record.get("wmi") or "").strip().upper()
        if not prefix or len(prefix) > 17 or any(char not in _TRANSLITERATION for char in prefix):
            raise VinLookupError(f"Record {number} has an invalid prefix/WMI.")
        values = (
            next((record[alias] for alias in aliases if record.get(alias) not in (None, "")), "")
            for aliases in _FIELD_ALIASES.values()
        )
        normalized_records.append((prefix, *values))
    if not normalized_records:
        raise VinLookupError("The import file contains no VIN records.")

    with sqlite3.connect(database_path) as connection:
        connection.execute("DROP TABLE IF EXISTS vin_records")
        connection.execute(
            "CREATE TABLE vin_records (prefix TEXT PRIMARY KEY, make TEXT, manufacturer TEXT, "
            "model TEXT, model_year TEXT, vehicle_type TEXT, body_class TEXT)"
        )
        connection.executemany("INSERT INTO vin_records VALUES (?, ?, ?, ?, ?, ?, ?)", normalized_records)
    return len(normalized_records)


def make_handler(database: Path, page: bytes | None = None):
    """Create a request handler bound to one offline database (also useful in tests)."""
    page_bytes = page if page is not None else PAGE_PATH.read_bytes()

    class VinHandler(BaseHTTPRequestHandler):
        def _send(self, status: HTTPStatus, content: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            if self.path == "/":
                self._send(HTTPStatus.OK, page_bytes, "text/html; charset=utf-8")
            elif self.path == "/api/vehicles":
                body = json.dumps(list_vehicles(database)).encode()
                self._send(HTTPStatus.OK, body, "application/json; charset=utf-8")
            else:
                self._send(HTTPStatus.NOT_FOUND, b"Not found", "text/plain; charset=utf-8")

        def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            if self.path not in ("/api/vin", "/api/vehicles", "/api/translations", "/api/reports", "/api/code-databases", "/api/scans"):
                self._send(HTTPStatus.NOT_FOUND, b"Not found", "text/plain; charset=utf-8")
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length > 67_000_000:
                    raise VinLookupError("Request is too large.")
                request = json.loads(self.rfile.read(length))
                if self.path == "/api/vin":
                    result = lookup_vin(str(request.get("vin", "")), database=database)
                elif self.path == "/api/vehicles":
                    lookup = lookup_vin(str(request.get("vin", "")), database=database)
                    result = save_vehicle(database, lookup["vin"], str(request.get("name", "")), lookup["vehicle"])
                elif self.path == "/api/translations":
                    result = save_translation(database, str(request.get("code", "")), str(request.get("description", "")), str(request.get("make", "")), str(request.get("model", "")))
                elif self.path == "/api/reports":
                    result = create_report(database, int(request.get("vehicle_id", 0)), str(request.get("type", "full")), str(request.get("title", "")), notes=str(request.get("notes", "")), odometer=str(request.get("odometer", "")), codes=request.get("codes", []), work=request.get("work", []))
                elif self.path == "/api/code-databases":
                    suffix = Path(str(request.get("filename", "codes.csv"))).suffix or ".csv"
                    with tempfile.TemporaryDirectory() as temporary:
                        source = Path(temporary) / f"codes{suffix}"
                        if request.get("url"):
                            download_source(str(request["url"]), source)
                        else:
                            content = base64.b64decode(str(request.get("content", "")), validate=True)
                            if len(content) > 50_000_000:
                                raise ValueError("Code database upload is larger than 50 MB.")
                            source.write_bytes(content)
                        count = import_code_database(database, source, make=str(request.get("make", "")), model=str(request.get("model", "")))
                    result = {"imported": count}
                else:
                    vehicle_id = int(request.get("vehicle_id", 0))
                    device = str(request.get("device", ""))
                    diagnostics = read_elm327_diagnostics(device, baud=int(request.get("baud", 38400)), timeout=float(request.get("timeout", 5)))
                    result = {"scan": save_diagnostic_scan(database, vehicle_id, diagnostics, device), "history": list_diagnostic_scans(database, vehicle_id)}
                status = HTTPStatus.OK
            except (VinLookupError, VinReaderError, ValueError, AttributeError) as exc:
                result, status = {"error": str(exc)}, HTTPStatus.BAD_REQUEST
            body = json.dumps(result).encode()
            self._send(status, body, "application/json; charset=utf-8")

        def log_message(self, _format: str, *_args: Any) -> None:
            return

    return VinHandler


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Query a downloaded VIN database without internet access.")
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    commands = parser.add_subparsers(dest="command", required=True)
    lookup = commands.add_parser("lookup", help="look up one VIN")
    lookup.add_argument("vin")
    importer = commands.add_parser("import", help="import a downloaded CSV or JSON database")
    importer.add_argument("source", type=Path)
    serve = commands.add_parser("serve", help="run the local lookup page")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    scan = commands.add_parser("scan", help="read a VIN from an ELM327 OBD-II adapter and look it up")
    scan.add_argument("device", help="serial device, for example /dev/ttyUSB0 or /dev/rfcomm0")
    scan.add_argument("--baud", type=int, default=38400)
    scan.add_argument("--timeout", type=float, default=5)
    saved = commands.add_parser("save-vehicle", help="look up and save a vehicle")
    saved.add_argument("vin")
    saved.add_argument("--name", default="")
    translation = commands.add_parser("add-code", help="save a diagnostic-code translation")
    translation.add_argument("code")
    translation.add_argument("description")
    translation.add_argument("--make", default="")
    translation.add_argument("--model", default="")
    export = commands.add_parser("export-report", help="export a saved report as JSON")
    export.add_argument("report_id", type=int)
    export.add_argument("destination", type=Path)
    report = commands.add_parser("create-report", help="save a diagnostic, work, or full report")
    report.add_argument("vehicle_id", type=int)
    report.add_argument("title")
    report.add_argument("--type", choices=("diagnostic", "work", "full"), default="full")
    report.add_argument("--code", action="append", default=[])
    report.add_argument("--notes", default="")
    report.add_argument("--odometer", default="")
    codes = commands.add_parser("import-codes", help="import DTC translations from CSV, JSON, PDF, or URL")
    codes.add_argument("source")
    codes.add_argument("--make", default="")
    codes.add_argument("--model", default="")
    diagnostic = commands.add_parser("scan-codes", help="read and save stored, pending, and permanent DTCs")
    diagnostic.add_argument("vehicle_id", type=int)
    diagnostic.add_argument("device")
    diagnostic.add_argument("--baud", type=int, default=38400)
    diagnostic.add_argument("--timeout", type=float, default=5)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "import":
            count = import_database(args.source, args.database)
            print(f"Imported {count} VIN prefix record(s) into {args.database}.")
        elif args.command == "lookup":
            print(json.dumps(lookup_vin(args.vin, database=args.database), indent=2, sort_keys=True))
        elif args.command == "serve":
            server = ThreadingHTTPServer((args.host, args.port), make_handler(args.database))
            print(f"VIN lookup page: http://{args.host}:{server.server_port}")
            server.serve_forever()
        elif args.command == "scan":
            vin = read_elm327_vin(args.device, baud=args.baud, timeout=args.timeout)
            print(json.dumps(lookup_vin(vin, database=args.database), indent=2, sort_keys=True))
        elif args.command == "save-vehicle":
            result = lookup_vin(args.vin, database=args.database)
            print(json.dumps(save_vehicle(args.database, result["vin"], args.name, result["vehicle"]), indent=2))
        elif args.command == "add-code":
            print(json.dumps(save_translation(args.database, args.code, args.description, args.make, args.model), indent=2))
        elif args.command == "export-report":
            print(export_report(args.database, args.report_id, args.destination))
        elif args.command == "create-report":
            report = create_report(args.database, args.vehicle_id, args.type, args.title, notes=args.notes, odometer=args.odometer, codes=args.code, work=[{"description": args.notes}] if args.notes else [])
            print(json.dumps(report, indent=2))
        elif args.command == "import-codes":
            source = Path(args.source)
            temporary = None
            if args.source.startswith(("http://", "https://")):
                suffix = Path(args.source.split("?", 1)[0]).suffix or ".pdf"
                temporary = tempfile.TemporaryDirectory(); source = Path(temporary.name) / f"codes{suffix}"
                download_source(args.source, source)
            try: print(f"Imported {import_code_database(args.database, source, make=args.make, model=args.model)} diagnostic code(s).")
            finally:
                if temporary: temporary.cleanup()
        else:
            diagnostics = read_elm327_diagnostics(args.device, baud=args.baud, timeout=args.timeout)
            print(json.dumps(save_diagnostic_scan(args.database, args.vehicle_id, diagnostics, args.device), indent=2))
    except (VinLookupError, VinReaderError, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
