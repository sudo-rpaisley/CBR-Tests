from tests.protocol_validity_profile import classify_ip_value


def test_ipv4_values_are_classified():
    assert classify_ip_value("0.0.0.0") == "ipv4"
    assert classify_ip_value("255.255.255.255") == "ipv4"


def test_ipv6_values_are_classified():
    assert classify_ip_value("2001:db8::1") == "ipv6"
    assert classify_ip_value("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff") == "ipv6"


def test_invalid_values_are_separate_from_missing():
    assert classify_ip_value("256.1.1.1") == "invalid"
    assert classify_ip_value("gggg::1") == "invalid"
    assert classify_ip_value("") == "missing"
    assert classify_ip_value("   ") == "missing"
    assert classify_ip_value(None) == "missing"
