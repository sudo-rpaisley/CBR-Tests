"""Contract tests for the stand-alone CTGAN experiment adapter (no GAN dependency)."""
from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from runner.perturbation_experiment import (
    collect_metric_rows,
    file_sha256,
    parse_integer_set,
    validate_generation,
)
from run_perturbation import parse_args


def _fixture(tmp_path: Path):
    source = tmp_path / "original.pcap"
    model = tmp_path / "model.pkl"
    out = tmp_path / "captures"
    out.mkdir()
    source.write_bytes(b"example-source")
    model.write_bytes(b"model-checkpoint")
    rows = []
    for pct in (0, 10):
        output = out / f"gan_{pct:03d}.pcap"
        output.write_bytes(source.read_bytes() if pct == 0 else b"example-sourcE")
        for suffix in ("packet_labels.csv", "flow_labels.csv"):
            (out / f"gan_{pct:03d}.{suffix}").write_text("header\n", encoding="utf-8")
        (out / f"gan_{pct:03d}.json").write_text("{}", encoding="utf-8")
        rows.append({
            "requested_pct": pct,
            "actual_packet_pct": float(pct),
            "output": output.name,
            "input_sha256": file_sha256(source),
            "model_sha256": file_sha256(model),
            "seed": 42,
            "source_file_bytes": source.stat().st_size,
            "output_file_bytes": output.stat().st_size,
            "output_sha256": file_sha256(output),
            "total_packets": 10,
            "synthetic_packets": pct // 10,
            "real_packets": 10 - pct // 10,
            "packet_labels": f"gan_{pct:03d}.packet_labels.csv",
            "flow_labels": f"gan_{pct:03d}.flow_labels.csv",
            "file_size_error_pct": 0,
        })
    with (out / "manifest.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return source, model, out


def _validate(source, model, out, **kwargs):
    return validate_generation(
        seed_dir=out, source_path=source, model_path=model,
        percentages=(0, 10), seed=42, size_tolerance_pct=5,
        verify_packets=False, **kwargs,
    )


def test_integer_list_rejects_duplicates_and_bad_values():
    assert parse_integer_set("30,0,10", low=0, high=100) == (0, 10, 30)
    with pytest.raises(ValueError, match="Repeated"):
        parse_integer_set("10,10", low=0, high=100)
    with pytest.raises(ValueError):
        parse_integer_set("110", low=0, high=100)


def test_cli_requires_explicit_zero_baseline():
    with pytest.raises(SystemExit):
        parse_args([
            "--base", "reference.pcap", "--comparison", "test.pcap", "--plan", "test.json",
            "--model", "test.pkl", "--generator-script", "traffic_gan.py",
            "--output-dir", "out", "--percentages", "10,20",
        ])


def test_manifest_sweep_validates_and_orders(tmp_path):
    source, model, out = _fixture(tmp_path)
    validated = _validate(source, model, out)
    assert [row["requested_pct"] for row in validated] == [0, 10]
    assert [row["synthetic_packets"] for row in validated] == [0, 1]


def test_manifest_rejects_corrupted_hash(tmp_path):
    source, model, out = _fixture(tmp_path)
    changed = out / "gan_010.pcap"
    changed.write_bytes(b"modified-source")
    with pytest.raises(ValueError, match="SHA-256"):
        _validate(source, model, out)


def test_manifest_rejects_missing_provenance(tmp_path):
    source, model, out = _fixture(tmp_path)
    (out / "gan_010.packet_labels.csv").unlink()
    with pytest.raises(FileNotFoundError):
        _validate(source, model, out)


def test_independent_packet_count_must_match(tmp_path, monkeypatch):
    source, model, out = _fixture(tmp_path)
    import runner.perturbation_experiment as adapter
    monkeypatch.setattr(adapter, "packet_count", lambda p: 9 if p.name == "gan_010.pcap" else 10)
    with pytest.raises(ValueError, match="Independent packet-count"):
        adapter.validate_generation(
            seed_dir=out, source_path=source, model_path=model,
            percentages=(0, 10), seed=42, size_tolerance_pct=5,
            verify_packets=True,
        )


def test_outcome_metric_values_are_joined_to_percentages(tmp_path):
    source, model, out = _fixture(tmp_path)
    captures = _validate(source, model, out)
    result_path = tmp_path / "run.json"
    result_path.write_text(json.dumps({
        "metric_results": [{"metric_id": "foo", "status": "success", "result_status": "warn"}],
        "test_results": {"foo": 0.7},
    }), encoding="utf-8")
    state = tmp_path / "state.json"
    state.write_text(json.dumps({
        "results": [{"dataset_path": captures[1]["dataset_path"], "output_path": str(result_path)}],
    }), encoding="utf-8")
    rows = collect_metric_rows(state, captures)
    assert len(rows) == 1
    assert rows[0]["requested_pct"] == 10
    assert rows[0]["primary_value"] == 0.7
    assert rows[0]["domain_status"] == "warn"

def test_gan_picker_filters_file_types_without_changing_dataset_picker(tmp_path):
    from runner.friendly_tui import _dataset_browser_entries
    for filename in ("one.pcap", "two.pcapng", "plan.json", "model.pkl", "traffic_gan.py"):
        (tmp_path / filename).write_text("contents", encoding="utf-8")
    (tmp_path / "nested").mkdir()
    all_entries = _dataset_browser_entries(tmp_path, tmp_path)
    assert "one.pcap" in [item.path.name for item in all_entries]
    assert "plan.json" not in [item.path.name for item in all_entries]
    capture_entries = _dataset_browser_entries(tmp_path, tmp_path, frozenset({".pcap", ".pcapng"}))
    assert {item.path.name for item in capture_entries} == {"one.pcap", "two.pcapng", "nested"}
    assert {item.path.name for item in _dataset_browser_entries(
        tmp_path, tmp_path, frozenset({".pkl"})
    )} == {"model.pkl", "nested"}


def test_gan_setup_validates_selected_files_and_builds_command(tmp_path):
    from runner.perturbation_tui import build_command, initial_settings, validate_settings
    for filename in ("reference.pcapng", "source.pcapng", "plan.json", "ctgan.pkl", "traffic_gan.py"):
        (tmp_path / filename).write_text("fixture", encoding="utf-8")
    values = initial_settings()
    values.update({
        "base": "reference.pcapng",
        "comparison": "source.pcapng",
        "plan": "plan.json",
        "model_source": "Use existing CTGAN checkpoint",
        "model": "ctgan.pkl",
        "generator_script": "traffic_gan.py",
        "output_dir": "generated/new-experiment",
    })
    assert validate_settings(values, tmp_path) == []
    command = build_command(values, tmp_path)
    assert command[command.index("--base") + 1] == str(tmp_path / "reference.pcapng")
    assert command[command.index("--comparison") + 1] == str(tmp_path / "source.pcapng")
    assert "--generator-script" in command
    assert command[command.index("--generator-script") + 1] == str(tmp_path / "traffic_gan.py")
    assert "--prepare-only" not in command
    values["prepare_only"] = True
    assert build_command(values, tmp_path)[-1] == "--prepare-only"


def test_gan_setup_rejects_missing_files_and_existing_output(tmp_path):
    from runner.perturbation_tui import initial_settings, validate_settings
    values = initial_settings()
    problems = validate_settings(values, tmp_path)
    assert any("dataset 1" in issue.lower() for issue in problems)
    assert any("dataset 2" in issue.lower() for issue in problems)
    values["output_dir"] = "prior-run"
    (tmp_path / "prior-run").mkdir()
    (tmp_path / "prior-run" / "experiment.json").write_text("{}")
    problems = validate_settings(values, tmp_path)
    assert any("must be new or empty" in issue for issue in problems)

def test_gan_setup_prevents_using_same_capture_twice(tmp_path):
    from runner.perturbation_tui import initial_settings, validate_settings
    (tmp_path / "shared.pcap").write_bytes(b"capture")
    values = initial_settings()
    values.update({"base": "shared.pcap", "comparison": "shared.pcap"})
    assert any("must be different" in issue for issue in validate_settings(values, tmp_path))


def test_cli_requires_both_datasets():
    with pytest.raises(SystemExit):
        parse_args([
            "--comparison", "dataset2.pcap", "--plan", "test.json",
            "--model", "test.pkl", "--generator-script", "traffic_gan.py",
            "--output-dir", "out",
        ])
    parsed = parse_args([
        "--base", "dataset1.pcap", "--comparison", "dataset2.pcap",
        "--plan", "test.json", "--model", "test.pkl",
        "--generator-script", "traffic_gan.py", "--output-dir", "out",
    ])
    assert parsed.base == Path("dataset1.pcap")
    assert parsed.comparison == Path("dataset2.pcap")


def test_experiment_uses_dataset2_for_generation_and_dataset1_for_every_reference(
    tmp_path, monkeypatch
):
    from runner import perturbation_experiment as experiment

    base = tmp_path / "dataset1.pcap"
    comparison = tmp_path / "dataset2.pcap"
    plan = tmp_path / "plan.json"
    model = tmp_path / "gan.pkl"
    script = tmp_path / "traffic_gan.py"
    for filename in (base, comparison, plan, model, script):
        filename.write_bytes(filename.name.encode())
    fixed_plan = {"metrics": [{
        "metric_id": "test_from_reference", "enabled": True,
        "input_requirements": {"reference_dataset_path": str(base)}
    }]}
    monkeypatch.setattr(experiment, "_load_plan", lambda _: fixed_plan)
    generated_command = []
    reference_paths = []

    def fake_run(command, **kwargs):
        generated_command.append(command)
        Path(command[command.index("--output") + 1]).mkdir(parents=True, exist_ok=True)
        return type("Completed", (), {"returncode": 0})()

    def fake_validate(**kwargs):
        assert kwargs["source_path"] == comparison.resolve()
        assert kwargs["percentages"] == (0, 10)
        return [{
            "seed": kwargs["seed"],
            "requested_pct": pct,
            "actual_packet_pct": pct,
            "dataset_path": str(kwargs["seed_dir"] / f"gan_{pct:03d}.pcap"),
        } for pct in kwargs["percentages"]]

    def fake_matrix(**kwargs):
        reference_paths.extend(kwargs["reference_paths"])
        assert len(kwargs["candidate_paths"]) == 2
        assert all(str(p).endswith(".pcap") for p in kwargs["candidate_paths"])
        kwargs["output_path"].write_text("{}", encoding="utf-8")
        return kwargs["output_path"]

    monkeypatch.setattr(experiment.subprocess, "run", fake_run)
    monkeypatch.setattr(experiment, "validate_generation", fake_validate)
    monkeypatch.setattr(experiment, "build_fixed_plan_matrix", fake_matrix)
    result = experiment.run_experiment(
        base=base, comparison=comparison, plan=plan, model=model,
        generator_script=script, output_dir=tmp_path / "results",
        percentages=(0, 10), seeds=(7,), prepare_only=True,
    )
    assert result["status"] == "prepared"
    assert reference_paths == [base.resolve()]
    assert generated_command[0][generated_command[0].index("--input") + 1] == str(comparison.resolve())
    assert str(base.resolve()) not in generated_command[0]
    assert result["base_dataset_path"] == str(base.resolve())
    assert result["comparison_dataset_path"] == str(comparison.resolve())
    assert [x["requested_pct"] for x in result["runs"][0]["captures"]] == [0, 10]


def test_experiment_rejects_plan_with_no_comparison_metrics_before_output(tmp_path, monkeypatch):
    from runner import perturbation_experiment as experiment
    names = ("a.pcap", "b.pcap", "plan.json", "gan.pkl", "traffic_gan.py")
    for name in names:
        (tmp_path / name).write_text(name, encoding="utf-8")
    monkeypatch.setattr(experiment, "_load_plan", lambda _: {"metrics": []})
    output = tmp_path / "output"
    with pytest.raises(ValueError, match="reference-comparison metrics"):
        experiment.run_experiment(
            base=tmp_path / names[0], comparison=tmp_path / names[1],
            plan=tmp_path / names[2], model=tmp_path / names[3],
            generator_script=tmp_path / names[4], output_dir=output,
        )
    assert not output.exists()

def test_training_picker_selects_many_pcaps_without_exposing_tabular_data(tmp_path):
    from runner.tui_batch import _browser_entries
    for name in ("a.pcap", "b.pcapng", "not-training.csv"):
        (tmp_path / name).write_text("placeholder", encoding="utf-8")
    selected = {item.path.name for item in _browser_entries(
        tmp_path, tmp_path, frozenset({".pcap", ".pcapng"})
    )}
    assert selected == {"a.pcap", "b.pcapng"}


def test_training_mode_builds_multiple_pcap_arguments(tmp_path):
    from runner.perturbation_tui import build_command, initial_settings, validate_settings
    names = ("base.pcap", "comparison.pcapng", "train1.pcap", "train2.pcapng",
             "plan.json", "traffic_gan.py")
    for name in names:
        (tmp_path / name).write_text(name, encoding="utf-8")
    values = initial_settings()
    values.update({
        "base": "base.pcap", "comparison": "comparison.pcapng",
        "plan": "plan.json", "training_pcaps": ["train1.pcap", "train2.pcapng"],
        "generator_script": "traffic_gan.py", "training_epochs": "30",
        "training_device": "cpu",
    })
    assert validate_settings(values, tmp_path) == []
    command = build_command(values, tmp_path)
    assert command.count("--train-pcap") == 2
    assert "--model" not in command
    assert command[command.index("--training-epochs") + 1] == "30"


def test_training_mode_rejects_selected_evaluation_capture(tmp_path):
    from runner.perturbation_tui import initial_settings, validate_settings
    for name in ("base.pcap", "comparison.pcap", "plan.json", "traffic_gan.py"):
        (tmp_path / name).write_text(name, encoding="utf-8")
    values = initial_settings()
    values.update({
        "base": "base.pcap", "comparison": "comparison.pcap",
        "plan": "plan.json", "training_pcaps": ["comparison.pcap"],
        "generator_script": "traffic_gan.py",
    })
    assert any("independent" in issue for issue in validate_settings(values, tmp_path))


def test_training_cli_rejects_checkpoint_and_training_inputs_together():
    with pytest.raises(SystemExit):
        parse_args([
            "--base", "a.pcap", "--comparison", "b.pcap", "--plan", "plan.json",
            "--model", "model.pkl", "--train-pcap", "train.pcap", "--output-dir", "out",
        ])
    args = parse_args([
        "--base", "a.pcap", "--comparison", "b.pcap", "--plan", "plan.json",
        "--train-pcap", "train1.pcap", "--train-pcap", "train2.pcap",
        "--output-dir", "out",
    ])
    assert args.training_pcaps == [Path("train1.pcap"), Path("train2.pcap")]


def test_training_capture_duplicate_content_rejected_before_output(tmp_path, monkeypatch):
    from runner import perturbation_experiment as experiment
    paths = {name: tmp_path / name for name in
             ("base.pcap", "comparison.pcap", "training.pcap", "plan.json", "traffic_gan.py")}
    for name, path in paths.items():
        path.write_text(name, encoding="utf-8")
    paths["training.pcap"].write_bytes(paths["base.pcap"].read_bytes())
    output = tmp_path / "outputs"
    with pytest.raises(ValueError, match="independent"):
        experiment.run_experiment(
            base=paths["base.pcap"], comparison=paths["comparison.pcap"],
            plan=paths["plan.json"], model=None,
            generator_script=paths["traffic_gan.py"], output_dir=output,
            training_pcaps=(paths["training.pcap"],),
        )
    assert not output.exists()


def test_multiple_training_pcaps_extract_merge_and_train(tmp_path, monkeypatch):
    import csv
    from runner import gan_training
    paths = [tmp_path / "train_1.pcap", tmp_path / "train_2.pcapng"]
    for index, path in enumerate(paths):
        path.write_bytes(bytes([index + 1]))
    script = tmp_path / "traffic_gan.py"
    script.write_text("stub", encoding="utf-8")
    events = []

    def fake_run(command, **kwargs):
        events.append(command)
        if command[2] == "extract":
            destination = Path(command[command.index("--features") + 1])
            destination.parent.mkdir(parents=True, exist_ok=True)
            with destination.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.writer(stream)
                writer.writerow(("protocol", "service_port", "packet_count_log",
                                 "duration_ms_log", "mean_ip_length", "forward_fraction",
                                 "mean_ttl"))
                for _ in range(30):
                    writer.writerow(("TCP", "80", "3", "3", "100", "1", "64"))
            destination.with_suffix(".metadata.json").write_text(
                json.dumps({"flow_count": 30}), encoding="utf-8"
            )
        elif command[2] == "train":
            Path(command[command.index("--model") + 1]).write_bytes(b"local-checkpoint")
        return type("Completed", (), {"returncode": 0})()

    monkeypatch.setattr(gan_training.subprocess, "run", fake_run)
    model, provenance = gan_training.train_from_captures(
        captures=tuple(paths), generator_script=script,
        output_dir=tmp_path / "training", epochs=15, batch_size=100,
    )
    assert len(events) == 3
    assert model.is_file()
    assert provenance["training_flow_count"] == 60
    assert len(provenance["training_captures"]) == 2
    with (tmp_path / "training" / "training_flows.csv").open(newline="", encoding="utf-8") as stream:
        assert len(list(csv.DictReader(stream))) == 60

def test_auto_plan_default_needs_no_selected_plan(tmp_path):
    from runner.perturbation_tui import build_command, initial_settings, validate_settings
    for name in ("reference.pcap", "comparison.pcap", "training.pcap", "traffic_gan.py"):
        (tmp_path / name).write_text(name, encoding="utf-8")
    values = initial_settings()
    values.update({
        "base": "reference.pcap", "comparison": "comparison.pcap",
        "training_pcaps": ["training.pcap"], "generator_script": "traffic_gan.py",
    })
    assert validate_settings(values, tmp_path) == []
    command = build_command(values, tmp_path)
    assert "--auto-plan" in command
    assert "--plan" not in command
    assert "--train-pcap" in command


def test_existing_plan_preflight_detects_missing_comparison_metrics(tmp_path):
    from runner.plan_builder import build_plan, write_plan
    from runner.perturbation_tui import (
        EXISTING_PLAN, initial_settings, validate_settings,
    )
    for name in ("reference.pcap", "comparison.pcap", "training.pcap", "traffic_gan.py"):
        (tmp_path / name).write_text(name, encoding="utf-8")
    plan, _ = build_plan(
        plan_id="no-reference", name="No reference",
        dataset_path=tmp_path / "comparison.pcap",
    )
    write_plan(tmp_path / "no_ref.json", plan)
    values = initial_settings()
    values.update({
        "base": "reference.pcap", "comparison": "comparison.pcap",
        "training_pcaps": ["training.pcap"], "generator_script": "traffic_gan.py",
        "plan_source": EXISTING_PLAN, "plan": "no_ref.json",
    })
    issues = validate_settings(values, tmp_path)
    assert any("Auto-create comparison plan" in issue for issue in issues)


def test_cli_auto_plan_and_explicit_plan_are_mutually_exclusive():
    options = [
        "--base", "dataset1.pcap", "--comparison", "dataset2.pcap",
        "--model", "trusted.pkl", "--output-dir", "out",
    ]
    parsed = parse_args(options + ["--auto-plan"])
    assert parsed.auto_plan and parsed.plan is None
    with pytest.raises(SystemExit):
        parse_args(options + ["--auto-plan", "--plan", "plan.json"])


def test_auto_plan_uses_comparison_as_candidate_and_base_as_reference(tmp_path, monkeypatch):
    from runner import perturbation_experiment as experiment
    from runner.reference_binding import reference_bound_metric_ids

    paths = {
        name: tmp_path / name
        for name in ("dataset1.pcap", "dataset2.pcap", "model.pkl", "traffic_gan.py")
    }
    for name, path in paths.items():
        path.write_text(name, encoding="utf-8")
    generator_commands = []
    matrices = []

    def fake_run(command, **kwargs):
        generator_commands.append(command)
        if "--output" in command:
            Path(command[command.index("--output") + 1]).mkdir(parents=True, exist_ok=True)
        return type("Completed", (), {"returncode": 0})()

    def fake_validate(**kwargs):
        return [
            {
                "seed": kwargs["seed"], "requested_pct": pct,
                "actual_packet_pct": float(pct),
                "dataset_path": str(kwargs["seed_dir"] / f"gan_{pct:03d}.pcap"),
            }
            for pct in kwargs["percentages"]
        ]

    def fake_matrix(**kwargs):
        matrices.append(kwargs)
        kwargs["output_path"].write_text("{}", encoding="utf-8")
        return kwargs["output_path"]

    monkeypatch.setattr(experiment.subprocess, "run", fake_run)
    monkeypatch.setattr(experiment, "validate_generation", fake_validate)
    monkeypatch.setattr(experiment, "build_fixed_plan_matrix", fake_matrix)
    output = tmp_path / "auto-experiment"
    outcome = experiment.run_experiment(
        base=paths["dataset1.pcap"], comparison=paths["dataset2.pcap"],
        plan=None, auto_plan=True, model=paths["model.pkl"],
        generator_script=paths["traffic_gan.py"], output_dir=output,
        percentages=(0, 10), seeds=(42,), prepare_only=True,
    )
    assert outcome["status"] == "prepared"
    assert outcome["plan_source"] == "auto_generated_from_dataset1_dataset2"
    plan = json.loads((output / "auto_comparison_plan.json").read_text(encoding="utf-8"))
    assert reference_bound_metric_ids(plan)
    assert plan["plan_creation"]["dataset"] == str(paths["dataset2.pcap"].resolve())
    assert plan["plan_creation"]["reference_dataset"] == str(paths["dataset1.pcap"].resolve())
    assert (output / "auto_plan_report.json").is_file()
    assert matrices[0]["plan_path"] == output / "auto_comparison_plan.json"
    assert matrices[0]["reference_paths"] == [paths["dataset1.pcap"].resolve()]
    assert generator_commands[0][generator_commands[0].index("--input") + 1] == str(
        paths["dataset2.pcap"].resolve()
    )
    assert outcome["plan_sha256"] == experiment.file_sha256(output / "auto_comparison_plan.json")
