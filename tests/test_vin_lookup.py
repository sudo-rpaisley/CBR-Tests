import json
import threading
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer

import pytest

from vin_lookup import VinLookupError, import_database, lookup_vin, make_handler, normalize_vin, vin_checksum


def sample_database(tmp_path):
    source = tmp_path / "download.json"
    source.write_text(json.dumps({"records": [
        {"prefix": "1M8", "manufacturer": "Example Motors", "vehicle_type": "Bus"},
        {"prefix": "1M8GDM", "make": "Example", "model": "Roadster", "model_year": "1989"},
    ]}))
    database = tmp_path / "vin.sqlite3"
    assert import_database(source, database) == 2
    return database


def test_normalize_and_checksum():
    assert normalize_vin(" 1m8gdm9axkp042788 ") == "1M8GDM9AXKP042788"
    assert vin_checksum("1M8GDM9AXKP042788")["valid"] is True


@pytest.mark.parametrize("vin", ["SHORT", "1M8GDM9AIKP042788", "1M8GDM9AQKP042788"])
def test_normalize_rejects_invalid_values(vin):
    with pytest.raises(VinLookupError):
        normalize_vin(vin)


def test_import_and_lookup_use_longest_local_prefix(tmp_path):
    result = lookup_vin("1M8GDM9AXKP042788", database=sample_database(tmp_path))
    assert result["matched_prefix"] == "1M8GDM"
    assert result["vehicle"] == {"make": "Example", "model": "Roadster", "model_year": "1989"}


def test_csv_import_accepts_wmi_column(tmp_path):
    source = tmp_path / "download.csv"
    source.write_text("WMI,Make_Name,Mfr_Name,VehicleTypeName\n1M8,Example,Example Motors,Bus\n")
    database = tmp_path / "vin.sqlite3"
    assert import_database(source, database) == 1
    result = lookup_vin("1M8GDM9AXKP042788", database=database)
    assert result["matched_prefix"] == "1M8"
    assert result["vehicle"] == {
        "make": "Example",
        "manufacturer": "Example Motors",
        "vehicle_type": "Bus",
    }


def test_missing_database_is_actionable(tmp_path):
    with pytest.raises(VinLookupError, match="Import a downloaded"):
        lookup_vin("1M8GDM9AXKP042788", database=tmp_path / "missing.sqlite3")


def test_lookup_page_and_api_are_served_locally(tmp_path):
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(sample_database(tmp_path), b"lookup page"))
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        connection = HTTPConnection("127.0.0.1", server.server_port)
        connection.request("GET", "/")
        response = connection.getresponse()
        assert response.status == 200
        assert response.read() == b"lookup page"

        connection.request("POST", "/api/vin", json.dumps({"vin": "1M8GDM9AXKP042788"}), {"Content-Type": "application/json"})
        response = connection.getresponse()
        assert response.status == 200
        assert json.loads(response.read())["matched_prefix"] == "1M8GDM"
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
