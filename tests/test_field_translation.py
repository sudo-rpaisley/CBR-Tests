import json
from pathlib import Path

import pytest

from runner.field_translation import FieldTranslationError, load_field_translation
from runner.io import load_case_or_plan
from runner.tabular import load_tabular_dataset


def test_load_field_translation_accepts_standard_test_to_dataset_object(tmp_path):
    translation_path = tmp_path / "deepsecure_fields.json"
    translation_path.write_text(
        json.dumps({"test_to_dataset_fields": {"Source IP": "Src IP", "Destination IP": "Dst IP"}}),
        encoding="utf-8",
    )

    assert load_field_translation(translation_path) == {"Src IP": "Source IP", "Dst IP": "Destination IP"}


def test_load_field_translation_rejects_duplicate_targets(tmp_path):
    translation_path = tmp_path / "bad_fields.json"
    translation_path.write_text(json.dumps({"test_to_dataset_fields": {"IP": "Src IP", "Another IP": "Src IP"}}), encoding="utf-8")

    with pytest.raises(FieldTranslationError, match="mapped more than once"):
        load_field_translation(translation_path)


def test_load_tabular_dataset_preserves_supplied_columns(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    dataset_path.write_text("Src IP,Dst IP\n10.0.0.1,10.0.0.2\n", encoding="utf-8")

    df = load_tabular_dataset(dataset_path, field_translation={"Src IP": "Source IP", "Dst IP": "Destination IP"})

    assert list(df.columns) == ["Src IP", "Dst IP"]


def test_case_can_reference_field_translation_file(tmp_path):
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps({"plan_meta": {"plan_id": "p1"}, "metrics": [{"metric_id": "m1", "taxonomy_path": ["x"]}]}),
        encoding="utf-8",
    )
    translation_path = tmp_path / "fields.json"
    translation_path.write_text(json.dumps({"test_to_dataset_fields": {"b": "a"}}), encoding="utf-8")
    case_path = tmp_path / "case.json"
    case_path.write_text(
        json.dumps(
            {
                "case_id": "c1",
                "dataset": {"path": "dataset.csv", "field_translation": {"path": "fields.json"}},
                "test_plan": {"path": "plan.json"},
                "output": {"path": "out.json"},
            }
        ),
        encoding="utf-8",
    )

    _, _, _, _, resolved_translation_path = load_case_or_plan(case_path, None, None, "ignored")

    assert resolved_translation_path == translation_path.resolve()


def test_detect_standard_pcap_field_translation_maps_tshark_headings():
    from runner.field_translation import detect_standard_pcap_field_translation

    mapping = detect_standard_pcap_field_translation([
        "frame.time_epoch",
        "ip.src",
        "ip.dst",
        "tcp.srcport",
        "tcp.dstport",
        "ip.proto",
        "frame.len",
        "tcp.flags.syn",
    ])

    assert mapping == {
        "frame.time_epoch": "timestamp",
        "ip.src": "Source IP",
        "ip.dst": "Destination IP",
        "tcp.srcport": "Source Port",
        "tcp.dstport": "Destination Port",
        "ip.proto": "Protocol",
        "frame.len": "Packet Length",
        "tcp.flags.syn": "syn_flag_count",
    }


def test_detect_standard_pcap_field_translation_chooses_one_source_per_test_field():
    from runner.field_translation import detect_standard_pcap_field_translation

    mapping = detect_standard_pcap_field_translation(["tcp.srcport", "udp.srcport", "tcp.dstport", "udp.dstport"])

    assert mapping == {"tcp.srcport": "Source Port", "tcp.dstport": "Destination Port"}


def test_merge_field_translations_allows_explicit_mapping_to_override_auto_target():
    from runner.field_translation import merge_field_translations

    merged = merge_field_translations(
        {"ip.src": "Source IP", "tcp.srcport": "Source Port"},
        {"custom_src": "Source IP"},
    )

    assert merged == {"tcp.srcport": "Source Port", "custom_src": "Source IP"}


def test_translate_metric_fields_resolves_standard_pcap_csv_headings():
    from runner.field_translation import detect_standard_pcap_field_translation, translate_metric_fields

    columns = ["frame.time_epoch", "ip.src", "ip.dst", "tcp.srcport", "tcp.dstport", "ip.proto"]
    translation = detect_standard_pcap_field_translation(columns)
    metric = {
        "metric_id": "m1",
        "input_requirements": {
            "timestamp_field": "timestamp",
            "field_map": {"source": "Source IP", "destination": "Destination IP"},
            "port_fields": ["Source Port", "Destination Port"],
        },
    }

    translated = translate_metric_fields(metric, translation, columns)

    assert translated["input_requirements"] == {
        "timestamp_field": "frame.time_epoch",
        "field_map": {"source": "ip.src", "destination": "ip.dst"},
        "port_fields": ["tcp.srcport", "tcp.dstport"],
    }


def test_collect_required_test_fields_from_plan():
    from runner.field_translation import collect_required_test_fields

    plan = {
        "metrics": [
            {
                "metric_id": "m1",
                "input_requirements": {
                    "candidate_fields": ["bytes", "packets"],
                    "timestamp_field": "timestamp",
                    "field_map": {"source": "Source IP", "destination": "Destination IP"},
                },
            },
            {"metric_id": "disabled", "enabled": False, "input_requirements": {"label_field": "label"}},
        ]
    }

    assert collect_required_test_fields(plan) == ["Destination IP", "Source IP", "bytes", "packets", "timestamp"]


def test_ensure_field_translation_file_creates_template_with_detected_fields(tmp_path):
    from runner.field_translation import ensure_field_translation_file

    dataset_path = tmp_path / "packets.csv"
    dataset_path.write_text("frame.time_epoch,ip.src\n1710000000,10.0.0.1\n", encoding="utf-8")
    plan = {
        "metrics": [
            {
                "metric_id": "m1",
                "input_requirements": {
                    "timestamp_field": "timestamp",
                    "field_map": {"source": "Source IP", "destination": "Destination IP"},
                },
            }
        ]
    }

    translation_path = ensure_field_translation_file(
        dataset_path=dataset_path,
        plan=plan,
        detected_dataset_to_test={"frame.time_epoch": "timestamp", "ip.src": "Source IP"},
    )

    payload = json.loads(translation_path.read_text(encoding="utf-8"))
    assert payload["test_to_dataset_fields"] == {
        "Destination IP": "",
        "Source IP": "ip.src",
        "timestamp": "frame.time_epoch",
    }


def test_ensure_field_translation_file_updates_template_with_new_required_fields(tmp_path):
    from runner.field_translation import ensure_field_translation_file

    dataset_path = tmp_path / "packets.csv"
    dataset_path.write_text("ip.src\n10.0.0.1\n", encoding="utf-8")
    translation_path = tmp_path / "packets.field_translation.json"
    translation_path.write_text(
        json.dumps({"test_to_dataset_fields": {"Source IP": "ip.src"}}),
        encoding="utf-8",
    )
    plan = {
        "metrics": [
            {
                "metric_id": "m1",
                "input_requirements": {
                    "field_map": {"source": "Source IP", "destination": "Destination IP"},
                },
            }
        ]
    }

    ensure_field_translation_file(dataset_path=dataset_path, plan=plan)

    payload = json.loads(translation_path.read_text(encoding="utf-8"))
    assert payload["test_to_dataset_fields"] == {"Destination IP": "", "Source IP": "ip.src"}


def test_detect_standard_pcap_field_translation_for_dataset_reads_csv_header(tmp_path):
    from runner.field_translation import detect_standard_pcap_field_translation_for_dataset

    dataset_path = tmp_path / "packets.csv"
    dataset_path.write_text("frame.time_epoch,ip.src,ip.dst\n1710000000,10.0.0.1,10.0.0.2\n", encoding="utf-8")

    assert detect_standard_pcap_field_translation_for_dataset(dataset_path) == {
        "frame.time_epoch": "timestamp",
        "ip.src": "Source IP",
        "ip.dst": "Destination IP",
    }


def test_existing_sidecar_is_not_rewritten_when_no_fields_are_missing(tmp_path):
    from runner.field_translation import ensure_field_translation_file

    dataset_path = tmp_path / "packets.csv"
    dataset_path.write_text("ip.src\n10.0.0.1\n", encoding="utf-8")
    translation_path = tmp_path / "packets.field_translation.json"
    original_text = json.dumps(
        {
            "custom_note": "preserve me",
            "test_to_dataset_fields": {"Source IP": "ip.src"},
        },
        indent=2,
    ) + "\n"
    translation_path.write_text(original_text, encoding="utf-8")
    plan = {"metrics": [{"metric_id": "m1", "input_requirements": {"field_map": {"source": "Source IP"}}}]}

    ensure_field_translation_file(dataset_path=dataset_path, plan=plan)

    assert translation_path.read_text(encoding="utf-8") == original_text


def test_metrics_missing_required_fields_reports_metrics_to_skip():
    from runner.field_translation import available_translated_fields, metrics_missing_required_fields

    columns = ["ip.src", "ip.dst"]
    field_translation = {"ip.src": "Source IP"}
    metrics = [
        {"metric_id": "has_source", "input_requirements": {"field_map": {"source": "Source IP"}}},
        {
            "metric_id": "missing_destination",
            "input_requirements": {"field_map": {"source": "Source IP", "destination": "Destination IP"}},
        },
    ]

    available = available_translated_fields(columns, field_translation)

    assert metrics_missing_required_fields(metrics, available) == {"missing_destination": ["Destination IP"]}


def test_collect_field_requirements_uses_explicit_required_and_optional_fields():
    from runner.field_translation import collect_field_requirements

    plan = {
        "metrics": [
            {
                "metric_id": "m1",
                "field_requirements": {"required": ["Source IP"], "optional": ["tcp_flags"]},
                "input_requirements": {"field_map": {"ignored_when_explicit": "Destination IP"}},
            }
        ]
    }

    assert collect_field_requirements(plan) == {
        "Source IP": {"required_by": ["m1"], "optional_for": []},
        "tcp_flags": {"required_by": [], "optional_for": ["m1"]},
    }


def test_translate_metric_fields_resolves_without_renaming_dataframe_columns():
    from runner.field_translation import translate_metric_fields

    metric = {
        "metric_id": "m1",
        "input_requirements": {
            "field_map": {"source": "Source IP", "destination": "Destination IP"},
            "candidate_fields": ["Source Port", "Destination Port"],
        },
    }
    translation = {
        "Src IP": "Source IP",
        "Dst IP": "Destination IP",
        "Src Port": "Source Port",
        "Dst Port": "Destination Port",
    }

    translated = translate_metric_fields(metric, translation, ["Src IP", "Dst IP", "Src Port", "Dst Port"])

    assert translated["input_requirements"] == {
        "field_map": {"source": "Src IP", "destination": "Dst IP"},
        "candidate_fields": ["Src Port", "Dst Port"],
    }


def test_build_field_translation_report_tracks_skipped_metrics(tmp_path):
    from runner.field_translation import build_field_translation_report

    dataset_path = tmp_path / "dataset.csv"
    translation_path = tmp_path / "dataset.field_translation.json"
    plan = {"metrics": [{"metric_id": "m1", "field_requirements": {"required": ["Source IP"]}}]}
    metrics = [{"metric_id": "m1", "field_requirements": {"required": ["Source IP"]}}]

    report = build_field_translation_report(
        dataset_path=dataset_path,
        translation_path=translation_path,
        plan=plan,
        metrics=metrics,
        available_fields=set(),
        skipped_metrics={"m1": ["Source IP"]},
    )

    assert report["metrics"]["m1"] == {"status": "skipped", "missing_fields": ["Source IP"], "missing_optional_fields": []}
    assert report["skipped_metrics"] == [
        {"metric_id": "m1", "reason": "missing_field_mappings", "missing_fields": ["Source IP"]}
    ]


def test_example_field_translations_use_standard_shape():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "examples" / "field_translations"
    assert list(root.glob("*.json"))
    for path in root.glob("*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["schema_version"] == 1
        assert isinstance(payload["test_to_dataset_fields"], dict)
        assert "fields" not in payload
        assert load_field_translation(path)


def test_validate_field_translation_payload_rejects_missing_standard_mapping():
    from runner.field_translation import validate_field_translation_payload

    with pytest.raises(FieldTranslationError, match="test_to_dataset_fields"):
        validate_field_translation_payload({"schema_version": 1})


def test_validate_field_translation_payload_rejects_unknown_schema_version():
    from runner.field_translation import validate_field_translation_payload

    with pytest.raises(FieldTranslationError, match="Unsupported field translation schema_version"):
        validate_field_translation_payload({"schema_version": 999, "test_to_dataset_fields": {}})


def test_validate_field_translation_payload_rejects_metadata_for_unknown_fields():
    from runner.field_translation import validate_field_translation_payload

    with pytest.raises(FieldTranslationError, match="field_metadata contains fields"):
        validate_field_translation_payload({
            "schema_version": 1,
            "test_to_dataset_fields": {"Source IP": "Src IP"},
            "field_metadata": {"Destination IP": {}},
        })


def test_format_column_section_uses_terminal_width(monkeypatch):
    from os import terminal_size

    from runner.field_translation_reports import format_column_section

    items = [f"long dataset col {index}" for index in range(10)]

    monkeypatch.setattr(
        "runner.field_translation_reports.shutil.get_terminal_size",
        lambda fallback: terminal_size((120, 24)),
    )

    wide_lines = format_column_section("Unused dataset columns", items)
    fixed_lines = format_column_section("Unused dataset columns", items, max_width=80)

    assert len(wide_lines) < len(fixed_lines)
    assert wide_lines[1].count("long dataset col") > fixed_lines[1].count("long dataset col")


def test_format_column_section_wraps_long_names():
    from runner.field_translation_reports import _display_width, format_column_section

    long_name = "This is a very long dataset column name that should wrap cleanly"

    lines = format_column_section("Unused dataset columns", [long_name, "short"], max_width=50)

    assert "This is a very long dataset column name that" not in "\n".join(lines)
    assert any("should wrap cleanly" in line for line in lines)
    assert all(_display_width(line) <= 50 for line in lines[1:])


def test_format_column_section_uses_display_width_for_unicode_padding():
    from runner.field_translation_reports import _display_ljust, _display_width

    padded = _display_ljust("列名", 6)

    assert _display_width("列名") == 4
    assert padded == "列名  "
    assert _display_width(padded) == 6


def test_format_column_section_uses_terminal_width_fallback(monkeypatch):
    from os import terminal_size

    from runner.field_translation_reports import format_column_section

    fallback_values = []

    def fake_get_terminal_size(fallback):
        fallback_values.append(fallback)
        return terminal_size(fallback)

    monkeypatch.setattr("runner.field_translation_reports.shutil.get_terminal_size", fake_get_terminal_size)

    lines = format_column_section("Unused dataset columns", ["alpha", "beta", "gamma"])

    assert fallback_values == [(100, 24)]
    assert lines == ["Unused dataset columns (3):", "  alpha             beta              gamma"]


def test_field_translation_report_includes_suggestions_and_markdown():
    from runner.field_translation import (
        build_field_translation_report,
        format_field_translation_markdown_report,
        format_field_translation_report,
    )

    metric = {"metric_id": "m1", "field_requirements": {"required": ["Source IP"], "optional": ["Destination IP"]}}
    report = build_field_translation_report(
        dataset_path=Path("dataset.csv"),
        translation_path=None,
        plan={"metrics": [metric]},
        metrics=[metric],
        available_fields=set(),
        skipped_metrics={"m1": ["Source IP"]},
        dataset_columns=["Source IP", "src_ip", "dst_ip", "bytes"],
        missing_optional_fields={"m1": ["Destination IP"]},
        sidecar_status="suppressed",
    )

    markdown = format_field_translation_markdown_report(report)
    text = format_field_translation_report(report)

    assert report["sidecar_status"] == "suppressed"
    assert report["unused_dataset_columns"] == ["bytes", "dst_ip", "src_ip"]
    assert report["metrics"]["m1"]["missing_optional_fields"] == ["Destination IP"]
    assert "Unused dataset columns (3):" in text
    assert "bytes" in text
    assert "dst_ip" in text
    assert "src_ip" in text
    assert "Source IP" not in report["unused_dataset_columns"]
    assert "Unused dataset columns: bytes, dst_ip, src_ip" not in text
    assert "| `m1` | skipped | Source IP | Destination IP |" in markdown
