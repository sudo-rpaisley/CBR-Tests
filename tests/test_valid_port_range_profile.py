from tests.valid_port_range_profile import parse_port, classify_port_range


def test_parse_port_statuses():
    assert parse_port(None)[0] == "missing"
    assert parse_port(" ")[0] == "missing"
    assert parse_port("abc")[0] == "non_integer"
    assert parse_port("80.5")[0] == "non_integer"
    assert parse_port("-1")[0] == "out_of_range"
    assert parse_port("70000")[0] == "out_of_range"
    assert parse_port("53") == ("valid", 53)


def test_classify_port_range():
    assert classify_port_range(0) == "well_known"
    assert classify_port_range(8080) == "registered"
    assert classify_port_range(55000) == "dynamic_private"
