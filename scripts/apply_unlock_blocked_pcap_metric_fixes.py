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


if __name__ == "__main__":
    fix_prompt_escapes()
    isolate_pcap_service_configuration()
