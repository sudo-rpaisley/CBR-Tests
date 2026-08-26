from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text(encoding="utf-8")
    if old not in text:
        raise SystemExit(f"Expected text not found in {path}: {old[:80]!r}")
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


# Wire central advice into plan-builder reports.
replace_once(
    "runner/plan_builder.py",
    "from runner.schema import validate_plan_schema\n",
    "from runner.preflight_advice import advice_for_exclusion, build_unlock_actions\nfrom runner.schema import validate_plan_schema\n",
)
replace_once(
    "runner/plan_builder.py",
    '''        status_by_metric[metric_id] = {\n            "status": state,\n            "included": included,\n            **({"reason": reason} if reason else {}),\n            **({"missing_fields": missing} if missing else {}),\n        }\n\n        if not included:\n''',
    '''        details = {\n            "status": state,\n            "included": included,\n            **({"reason": reason} if reason else {}),\n            **({"missing_fields": missing} if missing else {}),\n        }\n        if not included:\n            details["advice"] = advice_for_exclusion(\n                reason,\n                status=state,\n                dataset_format=dataset["format"],\n                missing_fields=missing,\n            )\n        status_by_metric[metric_id] = details\n\n        if not included:\n''',
)
replace_once(
    "runner/plan_builder.py",
    '''        "metrics": status_by_metric,\n        "dataset": str(dataset["path"]),\n''',
    '''        "metrics": status_by_metric,\n        "unlock_actions": build_unlock_actions(status_by_metric, dataset_format=fmt),\n        "dataset": str(dataset["path"]),\n''',
)

# Make the interactive/CLI report concise per metric, then show grouped remedies.
replace_once(
    "create_plan.py",
    "import sys\n",
    "import sys\nimport textwrap\n",
)
start = Path("create_plan.py").read_text(encoding="utf-8")
left = start.index("def _print_report(report: dict) -> None:\n")
right = start.index("\ndef _list_tests() -> None:\n", left)
new_report = '''def _print_wrapped(prefix: str, text: str, *, width: int = 108) -> None:\n    available = max(30, width - len(prefix))\n    lines = textwrap.wrap(str(text), width=available) or [""]\n    print(prefix + lines[0])\n    continuation = " " * len(prefix)\n    for line in lines[1:]:\n        print(continuation + line)\n\n\ndef _compact_metric_ids(metric_ids: list[str], *, limit: int = 8) -> str:\n    metric_ids = sorted(metric_ids)\n    if len(metric_ids) <= limit:\n        return ", ".join(metric_ids)\n    return ", ".join(metric_ids[:limit]) + f", +{len(metric_ids) - limit} more"\n\n\ndef _print_report(report: dict) -> None:\n    print("\\nPlan preflight summary")\n    print(f"  Available tests:       {report['available_metric_count']}")\n    print(f"  Tests considered:      {report['candidate_metric_count']}")\n    print(f"  Runnable / in plan:    {report['runnable_metric_count']}")\n    print(f"  Excluded as unrunnable:{report['excluded_metric_count']:>4}")\n    for status, count in sorted(report["configuration_status_counts"].items()):\n        print(f"  {status.replace('_', ' ').title():<22} {count}")\n\n    excluded = [\n        (metric_id, details)\n        for metric_id, details in report["metrics"].items()\n        if not details["included"]\n    ]\n    if excluded:\n        print("\\nTests excluded from the plan")\n        for metric_id, details in excluded:\n            reason = details.get("reason", details["status"])\n            print(f"  - {metric_id}: {details['status']} ({reason})")\n            missing = details.get("missing_fields", [])\n            if missing:\n                print("      missing: " + ", ".join(missing))\n            advice = details.get("advice", {})\n            if advice.get("title"):\n                print("      needed: " + str(advice["title"]))\n\n    actions = report.get("unlock_actions", [])\n    if actions:\n        print("\\nHow to unlock more tests")\n        for action in actions:\n            count = int(action.get("metric_count", 0))\n            suffix = "" if action.get("actionable", True) else " [requires different input/support]"\n            print(f"  - {action['title']} ({count} test{'s' if count != 1 else ''}){suffix}")\n            _print_wrapped("      ", str(action.get("advice", "")))\n            missing = action.get("missing_fields", [])\n            if missing:\n                _print_wrapped("      Missing fields: ", ", ".join(missing))\n            if action.get("example"):\n                _print_wrapped("      Example: ", str(action["example"]))\n            metric_ids = action.get("metric_ids", [])\n            if metric_ids:\n                _print_wrapped("      Affects: ", _compact_metric_ids(metric_ids))\n'''
Path("create_plan.py").write_text(start[:left] + new_report + start[right:], encoding="utf-8")

# Regression coverage for actionable, grouped and non-actionable advice.
replace_once(
    "tests/test_plan_builder.py",
    "from create_plan import _slug\n",
    "from create_plan import _print_report, _slug\n",
)
with Path("tests/test_plan_builder.py").open("a", encoding="utf-8") as handle:
    handle.write('''\n\ndef test_pcap_preflight_exclusions_include_unlock_advice(tmp_path):\n    dataset = tmp_path / "capture.pcap"\n    dataset.write_bytes(b"pcap-placeholder")\n\n    _plan, report = build_plan(plan_id="advice", name="Advice", dataset_path=dataset)\n\n    service = report["metrics"]["service_port_consistency_profile"]\n    assert service["advice"]["action_key"] == "single_service_evidence"\n    assert "--single-service" in service["advice"]["example"]\n\n    reference = report["metrics"][next(iter(PCAP_REFERENCE_METRICS))]\n    assert reference["advice"]["action_key"] == "independent_reference_pcap"\n    assert "--reference-dataset" in reference["advice"]["example"]\n\n    self_derived = report["metrics"][next(iter(PCAP_SELF_DERIVED_METRICS))]\n    assert self_derived["advice"]["action_key"] == "independent_flow_export"\n    assert self_derived["advice"]["actionable"] is False\n\n    actions = {action["action_key"]: action for action in report["unlock_actions"]}\n    expected_reference_count = len(PCAP_REFERENCE_METRICS) + len(PCAP_REFERENCE_UNSUPPORTED_REASONS)\n    assert actions["independent_reference_pcap"]["metric_count"] == expected_reference_count\n    assert actions["single_service_evidence"]["metric_count"] == 1\n\n\ndef test_tabular_missing_fields_advise_mapping_without_fabrication(tmp_path):\n    dataset = tmp_path / "minimal.csv"\n    pd.DataFrame({"Source Port": [12345, 443], "Destination Port": [53, 443]}).to_csv(dataset, index=False)\n\n    _plan, report = build_plan(plan_id="mapping-advice", name="Mapping Advice", dataset_path=dataset)\n    blocked = [\n        details for details in report["metrics"].values()\n        if details.get("reason") == "required_fields_not_resolved" and details.get("missing_fields")\n    ]\n    assert blocked\n    advice = blocked[0]["advice"]\n    assert advice["action_key"] == "field_mapping"\n    assert "Do not fabricate absent fields" in advice["advice"]\n    assert "--field-translation" in advice["example"]\n\n\ndef test_print_report_shows_grouped_unlock_guidance(tmp_path, capsys):\n    dataset = tmp_path / "capture.pcap"\n    dataset.write_bytes(b"pcap-placeholder")\n    _plan, report = build_plan(plan_id="printed-advice", name="Printed Advice", dataset_path=dataset)\n\n    _print_report(report)\n    output = capsys.readouterr().out\n\n    assert "How to unlock more tests" in output\n    assert "--reference-dataset" in output\n    assert "--single-service" in output\n    assert "Use independently exported flow fields" in output\n    assert "needed:" in output\n''')

# Document the new output so reason codes and remedies are part of the user-facing contract.
replace_once(
    "docs/plan_creation.md",
    '''Only `ready` metrics are written into the plan. The other states are shown in the preflight report so users can see why tests were excluded.\n''',
    '''Only `ready` metrics are written into the plan. The other states are shown in the preflight report so users can see why tests were excluded. Each excluded metric also carries research-safe advice describing what evidence, mapping, configuration, input representation or implementation support is needed before that test can run.\n\nThe CLI groups repeated remedies under **How to unlock more tests**. For example, one independent reference PCAP may unlock many reference-comparison metrics at once, while slice, attack-window, split or benchmark tests explain the specific experiment ground truth they still require. Advice never recommends fabricating absent fields or bypassing preflight merely to increase the test count.\n''',
)
replace_once(
    "docs/plan_creation.md",
    '''- the reason for each exclusion;\n- unresolved required fields where applicable.\n''',
    '''- the reason for each exclusion;\n- unresolved required fields where applicable;\n- the evidence/configuration needed to unlock each excluded test;\n- grouped actions showing how many tests each remedy could unlock.\n''',
)
replace_once(
    "README.md",
    '''This writes `plans/my-plan_plan.json`. The creator discovers metrics from the live dispatcher, applies field translations, excludes tests that need unresolved fields/configuration or an incompatible input type, and writes only runnable metrics. See [Creating plans](docs/plan_creation.md) for the full workflow.\n''',
    '''This writes `plans/my-plan_plan.json`. The creator discovers metrics from the live dispatcher, applies field translations, excludes tests that need unresolved fields/configuration or an incompatible input type, and writes only runnable metrics. For every excluded test the preflight now explains what is needed to unlock it, and groups shared remedies (reference data, field mapping, slice metadata, attack windows, split configuration, service evidence, and so on) so you can see which next input would enable the most additional tests. See [Creating plans](docs/plan_creation.md) for the full workflow.\n''',
)
