"""Import diagnostic trouble-code databases from files or an explicit URL."""
from __future__ import annotations

import csv
import io
import json
import re
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from vehicle_records import save_translation

DTC_RE = re.compile(r"^\s*([PBCU][0-9A-F]{4})\s*[-:–—]?\s*(.+?)\s*$", re.IGNORECASE)

def download_source(url: str, destination: Path, timeout: float = 30) -> Path:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError("Code database URL must use http or https.")
    request = Request(url, headers={"User-Agent": "CBR-Tests VIN tool/1.0"})
    try:
        with urlopen(request, timeout=timeout) as response:
            if int(response.headers.get("Content-Length", "0") or 0) > 50_000_000:
                raise ValueError("Code database download is larger than 50 MB.")
            content = response.read(50_000_001)
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        raise ValueError(f"Unable to download code database: {exc}") from exc
    if len(content) > 50_000_000:
        raise ValueError("Code database download is larger than 50 MB.")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(content)
    return destination

def parse_pdf(path: Path) -> list[dict]:
    from pypdf import PdfReader

    records = []
    for page in PdfReader(path).pages:
        for line in (page.extract_text() or "").splitlines():
            match = DTC_RE.match(line)
            if match:
                records.append({"code": match.group(1).upper(), "description": match.group(2).strip()})
    return records

def parse_database(path: Path) -> list[dict]:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        return parse_pdf(path)
    if suffix == ".json":
        value = json.loads(path.read_text(encoding="utf-8"))
        records = value.get("records", value) if isinstance(value, dict) else value
        if not isinstance(records, list): raise ValueError("JSON must be a list or contain a records list.")
        return records
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))

def import_code_database(database, source, *, make="", model="") -> int:
    records = parse_database(Path(source)); count = 0
    for record in records:
        normalized = {str(k).strip().lower(): v for k, v in record.items()}
        code = normalized.get("code") or normalized.get("dtc")
        description = normalized.get("description") or normalized.get("meaning") or normalized.get("definition")
        if code and description:
            save_translation(database, str(code), str(description), str(normalized.get("make") or make), str(normalized.get("model") or model)); count += 1
    if not count: raise ValueError("No diagnostic codes could be parsed from the source.")
    return count
