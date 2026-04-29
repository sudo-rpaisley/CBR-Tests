from tests.service_port_consistency_profile import parse_port


def test_parse_port_categories():
    assert parse_port(None)[0] == "missing"
    assert parse_port("")[0] == "missing"
    assert parse_port("abc")[0] == "non_integer"
    assert parse_port("80.1")[0] == "non_integer"
    assert parse_port("70000")[0] == "out_of_range"
    assert parse_port("53") == ("valid", 53)
