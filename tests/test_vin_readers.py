import pytest

from vin_readers import VinReaderError, parse_dtc_response, parse_obd_vin, read_elm327_diagnostics, read_elm327_vin


def test_parse_plain_scanner_vin():
    assert parse_obd_vin("VIN: 1M8GDM9AXKP042788\r\n") == "1M8GDM9AXKP042788"


def test_parse_multiframe_elm327_response():
    response = """SEARCHING...
10 14 49 02 01 31 4D 38
21 47 44 4D 39 41 58 4B
22 50 30 34 32 37 38 38
>"""
    assert parse_obd_vin(response) == "1M8GDM9AXKP042788"


def test_parse_response_without_vin_fails():
    with pytest.raises(VinReaderError, match="did not contain"):
        parse_obd_vin("NO DATA\r>")


def test_reader_initializes_adapter_and_requests_vin():
    commands = []

    def transport(device, baud, command, timeout):
        commands.append((device, baud, command, timeout))
        return "1M8GDM9AXKP042788" if command == "0902" else "OK>"

    assert read_elm327_vin("/dev/fake", transport=transport) == "1M8GDM9AXKP042788"
    assert [call[2] for call in commands] == ["ATZ", "ATE0", "ATL0", "ATS1", "ATH0", "ATSP0", "0902"]

def test_parse_and_read_diagnostic_code_groups():
    assert parse_dtc_response("43 01 00 03 00 00 00>",0x43)==["P0100","P0300"]
    def transport(_device,_baud,command,_timeout):
        return {"03":"43 01 00>","07":"47 03 00>","0A":"NO DATA>"}.get(command,"OK>")
    assert read_elm327_diagnostics("/dev/fake",transport=transport)=={"stored":["P0100"],"pending":["P0300"],"permanent":[]}
