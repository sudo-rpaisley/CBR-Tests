from pathlib import Path


def fix_prompt_escapes() -> None:
    path = Path("create_plan.py")
    text = path.read_text(encoding="utf-8")
    text = text.replace(
        'answer = input("\nAdd an independent reference PCAP',
        'answer = input("\\nAdd an independent reference PCAP',
    )
    text = text.replace(
        'answer = input("\nIs the entire capture',
        'answer = input("\\nIs the entire capture',
    )
    path.write_text(text, encoding="utf-8")


def isolate_pcap_service_configuration() -> None:
    path = Path("runner/plan_builder.py")
    text = path.read_text(encoding="utf-8")
    old = '''            elif metric_id == "service_port_consistency_profile" and service_port_configuration:
                spec = dict(spec)
                spec["template"] = pcap_service_port_template(
                    service_port_configuration.get("service_name", ""),
                    service_port_configuration.get("expected_ports", []),
                )
'''
    new = '''            elif metric_id == "service_port_consistency_profile":
                # Never inherit a service definition from another discovered plan.
                # Raw-PCAP service identity must be asserted for this capture.
                spec = dict(spec)
                spec["template"] = None
                if service_port_configuration:
                    spec["template"] = pcap_service_port_template(
                        service_port_configuration.get("service_name", ""),
                        service_port_configuration.get("expected_ports", []),
                    )
'''
    if old not in text:
        raise RuntimeError("Expected PCAP service-template block not found")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


def preserve_automatic_support_set_semantics() -> None:
    path = Path("runner/pcap_adapter.py")
    text = path.read_text(encoding="utf-8")
    old = "PCAP_SUPPORTED_METRICS = PCAP_DIRECT_METRICS | PCAP_PACKET_BACKED_METRICS\n"
    new = '''# Keep this name reserved for the configuration-free automatic set. Optional
# reference/service metrics are packet-backed but are not automatically runnable.
PCAP_SUPPORTED_METRICS = PCAP_DIRECT_METRICS | PCAP_PACKET_METRICS
'''
    if old not in text:
        raise RuntimeError("Expected PCAP_SUPPORTED_METRICS definition not found")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


def update_existing_pcap_contract_tests() -> None:
    path = Path("tests/test_pcap_all_runnable_metrics.py")
    text = path.read_text(encoding="utf-8")
    text = text.replace("assert len(PCAP_DIRECT_METRICS) == 2", "assert len(PCAP_DIRECT_METRICS) == 3", 1)
    text = text.replace("assert len(PCAP_SUPPORTED_METRICS) == 20", "assert len(PCAP_SUPPORTED_METRICS) == 21", 1)
    text = text.replace(
        "def test_automatic_pcap_plan_contains_all_twenty_currently_runnable_metrics(tmp_path):",
        "def test_automatic_pcap_plan_contains_all_twenty_one_currently_runnable_metrics(tmp_path):",
        1,
    )
    text = text.replace('assert report["runnable_metric_count"] == 20', 'assert report["runnable_metric_count"] == 21', 1)
    old = '''    assert report["metrics"]["handshake_plausibility_profile"]["status"] == "needs_configuration"
    assert report["metrics"]["handshake_plausibility_profile"]["reason"] == "capture_boundary_policy_required"
'''
    new = '''    assert report["metrics"]["handshake_plausibility_profile"]["status"] == "ready"
    assert report["metrics"]["handshake_plausibility_profile"]["reason"] is None
'''
    if old not in text:
        raise RuntimeError("Expected old handshake plan assertions not found")
    text = text.replace(old, new, 1)
    old = '''def test_context_configuration_reasons_are_not_silent_exclusions():
    assert PCAP_CONTEXT_CONFIGURATION_REASONS == {
        "handshake_plausibility_profile": "capture_boundary_policy_required"
    }
'''
    new = '''def test_context_configuration_reasons_are_not_silent_exclusions():
    assert PCAP_CONTEXT_CONFIGURATION_REASONS == {}
'''
    if old not in text:
        raise RuntimeError("Expected old context-configuration assertion not found")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


if __name__ == "__main__":
    fix_prompt_escapes()
    isolate_pcap_service_configuration()
    preserve_automatic_support_set_semantics()
    update_existing_pcap_contract_tests()
