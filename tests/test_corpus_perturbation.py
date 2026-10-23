"""Recursive corpus grouping, hold-out roles, provenance, full metrics and resume."""
from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from runner import corpus_perturbation as corpus


def _pcap(path: Path, body: bytes = b"pcap") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return path


def test_recursive_grouping_does_not_mix_directories_and_skips_singletons(tmp_path):
    root = tmp_path / "data"
    _pcap(root / "Alpha" / "one.pcap", b"1")
    _pcap(root / "Alpha" / "two.PCAPNG", b"2")
    _pcap(root / "Alpha" / "deeper" / "three.pcap", b"3")
    _pcap(root / "Beta" / "three.pcap", b"4")
    _pcap(root / "Beta" / "four.pcap", b"5")
    _pcap(root / "singleton" / "only.pcap", b"6")
    (root / "Alpha" / "note.txt").write_text("ignored")

    groups, skipped = corpus.discover_groups(root)
    assert [g["folder"] for g in groups] == ["Alpha", "Beta"]
    assert [len(g["captures"]) for g in groups] == [2, 2]
    assert sorted(x["folder"] for x in skipped) == ["Alpha/deeper", "singleton"]
    cases = corpus.inventory(root, groups)
    assert len(cases) == 4
    for case in cases:
        assert len(case["training"]) == 1
        assert case["training"][0]["path"] != case["held_out"]
        assert Path(case["training"][0]["path"]).parent == Path(case["held_out"]).parent


def test_skip_entire_corpus_without_any_eligible_pair(tmp_path):
    root = tmp_path / "dataset"
    _pcap(root / "deep" / "one.pcap")
    _pcap(root / "other" / "two.pcap")
    generator = tmp_path / "traffic_gan.py"
    generator.write_text("test")
    output = tmp_path / "result"
    with pytest.raises(ValueError, match="No eligible group"):
        corpus.run_corpus(dataset_root=root, output_dir=output,
                          generator_script=generator, preview_only=True)
    assert not output.exists()


def test_preview_is_nondestructive_and_does_not_hash_pcaps(tmp_path, monkeypatch):
    root = tmp_path / "dataset"
    _pcap(root / "nested" / "a.pcap")
    _pcap(root / "nested" / "b.pcapng")
    generator = tmp_path / "traffic_gan.py"
    generator.write_text("test")
    monkeypatch.setattr(corpus, "file_sha256", lambda _: (_ for _ in ()).throw(
        AssertionError("preview hashed a large file")
    ))
    preview = corpus.run_corpus(
        dataset_root=root, output_dir=tmp_path / "output",
        generator_script=generator, preview_only=True,
    )
    assert preview["eligible_cases"] == 2
    assert preview["eligible_groups"] == 1
    assert not (tmp_path / "output").exists()


def _fake_one_seed_case(output: Path, heldout: Path, group: str = "Group"):
    """Create a representative completed CBR batch, including nested results."""
    output.mkdir(parents=True, exist_ok=True)
    capture = output / "seed_42" / "captures" / "gan_010.pcap"
    capture.parent.mkdir(parents=True)
    capture.write_bytes(b"generated")
    result = output / "seed_42" / "evaluation" / "raw_outcome.json"
    result.parent.mkdir(parents=True)
    result.write_text(json.dumps({
        "schema_version": 1,
        "metric_results": [{
            "metric_id": "feature_wise_ks_statistic_from_reference",
            "status": "success", "result_status": "warn",
            "diagnostic_notes": {"warnings": []},
        }],
        "test_results": {
            "feature_wise_ks_statistic_from_reference": {
                "summary": {"mean_feature_wise_ks_statistic_from_reference": 0.2},
                "per_field": {"ttl": {"quantiles": [0.1, 0.2, 0.3]}},
                "empty": [],
                "status": "warn",
            },
            "another_metric": {"nested": {"other": 5}},
        },
        "test_results_taxonomy": {"detailed": {"raw": [1, 2]}},
    }))
    batch_dir = result.parent
    (batch_dir / "batch_state.json").write_text(json.dumps({
        "status": "completed",
        "results": [{
            "dataset_path": str(capture),
            "output_path": str(result),
            "status": "completed",
        }],
    }))
    (output / "experiment.json").write_text(json.dumps({
        "status": "completed",
        "runs": [{
            "seed": 42,
            "captures": [{
                "dataset_path": str(capture),
                "requested_pct": 10,
                "actual_packet_pct": 10.0,
            }],
            "batch_output_dir": str(batch_dir),
        }],
    }))
    return {
        "case_id": "001/holdout_0001",
        "folder": group,
        "held_out": str(heldout),
        "held_out_sha256": corpus.file_sha256(heldout),
        "training": [{"path": str(heldout.parent / "training.pcap")}],
        "status": "completed",
        "output_dir": str(output),
    }


def test_full_metric_export_keeps_nested_arrays_and_native_outcome(tmp_path):
    heldout = _pcap(tmp_path / "heldout.pcap", b"reference")
    case = _fake_one_seed_case(tmp_path / "run", heldout)
    campaign = {
        "campaign_id": "cohort-001", "started_at": "2026-10-22T09:00:00+00:00",
        "cases": [case],
    }
    output = tmp_path / "reports"
    summary = corpus.export_results(campaign, output)
    assert summary["jobs_exported"] == 1
    assert summary["metrics_exported"] == 2
    with (output / "exports" / "measurements_long.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    ks = [r for r in rows if r["metric_id"] == "feature_wise_ks_statistic_from_reference"]
    assert {r["json_pointer"] for r in ks} >= {
        "/summary/mean_feature_wise_ks_statistic_from_reference",
        "/per_field/ttl/quantiles/0",
        "/per_field/ttl/quantiles/1",
        "/per_field/ttl/quantiles/2",
        "/empty",
    }
    assert next(r for r in ks if r["json_pointer"].endswith("/2"))["numeric_value"] == "0.3"
    assert next(r for r in ks if r["json_pointer"] == "/empty")["value_json"] == "[]"
    records = [
        json.loads(line)
        for line in (output / "exports" / "metric_payloads.jsonl").read_text().splitlines()
    ]
    assert records[0]["execution_record"] or records[1]["execution_record"]
    assert any(r["test_result"]["per_field"]["ttl"]["quantiles"] == [0.1, 0.2, 0.3]
               for r in records if r["metric_id"] == "feature_wise_ks_statistic_from_reference")
    assert (tmp_path / "run" / "seed_42" / "evaluation" / "raw_outcome.json").is_file()


def test_corpus_executes_all_heldouts_and_resume_skips_verified_completions(tmp_path, monkeypatch):
    folder = tmp_path / "dataset" / "deep" / "group"
    for i in range(4):
        _pcap(folder / f"file_{i + 1}.pcap", bytes([i + 1]))
    script = tmp_path / "traffic_gan.py"
    script.write_text("placeholder")
    output = tmp_path / "results"
    seen = []

    def fake_run(**kwargs):
        base = kwargs["base"]
        assert base == kwargs["comparison"]
        assert kwargs["auto_plan"] and kwargs["allow_self_reference"]
        assert kwargs["plan_template_reference"] in kwargs["training_pcaps"]
        assert len(kwargs["training_pcaps"]) == 3
        assert base not in kwargs["training_pcaps"]
        seen.append(base.name)
        _fake_one_seed_case(kwargs["output_dir"], base)
        return {"status": "completed"}

    monkeypatch.setattr(corpus, "run_experiment", fake_run)
    campaign = corpus.run_corpus(
        dataset_root=folder.parent.parent,
        output_dir=output, generator_script=script,
        percentages=(0, 10), seeds=(42,),
    )
    assert seen == [f"file_{i}.pcap" for i in range(1, 5)]
    assert campaign["status"] == "completed"
    assert len(campaign["cases"]) == 4
    assert campaign["exports"]["jobs_exported"] == 4

    resumed = corpus.run_corpus(
        dataset_root=folder.parent.parent, output_dir=output,
        generator_script=script, percentages=(0, 10), seeds=(42,), resume=True,
    )
    assert seen == [f"file_{i}.pcap" for i in range(1, 5)]
    assert resumed["status"] == "completed"
    assert resumed["exports"]["jobs_exported"] == 4


def test_resume_refuses_modified_inputs_and_preserves_archive(tmp_path, monkeypatch):
    dataset = tmp_path / "dataset"
    x = _pcap(dataset / "a.pcap", b"one")
    _pcap(dataset / "b.pcap", b"two")
    script = tmp_path / "traffic_gan.py"
    script.write_text("dummy")
    output = tmp_path / "result"

    def stub(**kwargs):
        _fake_one_seed_case(kwargs["output_dir"], kwargs["base"])
        return {"status": "completed"}

    monkeypatch.setattr(corpus, "run_experiment", stub)
    corpus.run_corpus(
        dataset_root=dataset, output_dir=output,
        generator_script=script, percentages=(0, 10),
    )
    x.write_bytes(b"modified")
    with pytest.raises(ValueError, match="settings changed"):
        corpus.run_corpus(
            dataset_root=dataset, output_dir=output,
            generator_script=script, percentages=(0, 10), resume=True,
        )
    assert (output / "campaign.json").is_file()


def test_metric_leaf_json_pointer_escapes_keys():
    data = {"x/y~z": [3, {"q": True}]}
    assert list(corpus.leaves(data)) == [
        ("/x~1y~0z/0", 3), ("/x~1y~0z/1/q", True)
    ]

def test_exports_partial_and_retry_attempts_with_distinct_attempt_labels(tmp_path):
    heldout = _pcap(tmp_path / "heldout.pcap", b"source")
    first = _fake_one_seed_case(tmp_path / "attempt1", heldout)
    second = _fake_one_seed_case(tmp_path / "attempt2", heldout)
    first["status"] = "failed"
    first["output_dir"] = second["output_dir"]
    first["attempts"] = [
        {"path": str(tmp_path / "attempt1"), "status": "failed"},
        {"path": str(tmp_path / "attempt2"), "status": "completed"},
    ]
    campaign = {
        "campaign_id": "attempt-test",
        "started_at": "2026-10-22T09:00:00+00:00",
        "cases": [first],
    }
    result = corpus.export_results(campaign, tmp_path / "exports_root")
    assert result["metrics_exported"] == 4
    assert result["jobs_exported"] == 2
    with (tmp_path / "exports_root" / "exports" / "metric_results.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        rows = list(csv.DictReader(handle))
    assert {row["attempt"] for row in rows} == {"1", "2"}
    assert {row["attempt_status"] for row in rows} == {"failed", "completed"}
    assert {row["outcome_json"] for row in rows} == {
        str(tmp_path / "attempt1" / "seed_42" / "evaluation" / "raw_outcome.json"),
        str(tmp_path / "attempt2" / "seed_42" / "evaluation" / "raw_outcome.json"),
    }


def test_campaign_halts_after_three_consecutive_failed_cases(tmp_path, monkeypatch):
    folder = tmp_path / "dataset" / "group"
    for number in range(5):
        _pcap(folder / f"case_{number}.pcap", bytes([number + 1]))
    script = tmp_path / "traffic_gan.py"
    script.write_text("placeholder")
    attempts = []

    def failing_case(**kwargs):
        attempts.append(kwargs["base"].name)
        raise RuntimeError("reproducible failure")

    monkeypatch.setattr(corpus, "run_experiment", failing_case)
    output = tmp_path / "results"
    campaign = corpus.run_corpus(
        dataset_root=folder.parent, output_dir=output,
        generator_script=script, percentages=(0, 10), seeds=(42,),
        max_consecutive_failures=3,
    )
    assert len(attempts) == 3
    assert [c["status"] for c in campaign["cases"]] == [
        "failed", "failed", "failed", "pending", "pending"
    ]
    assert campaign["status"] == "needs_attention"
    assert "three" not in campaign["abort_reason"]
    assert "3 consecutive" in campaign["abort_reason"]
    saved = json.loads((output / "campaign.json").read_text())
    assert saved["abort_reason"] == campaign["abort_reason"]
