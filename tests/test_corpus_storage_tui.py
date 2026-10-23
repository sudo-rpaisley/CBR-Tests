"""Storage destination and two-folder GAN Toolbox selection tests."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from runner import corpus_perturbation_tui as tui
from runner.corpus_storage import (
    check_campaign_output_path, gib, storage_estimate, validate_storage_parent,
)


def _inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    root = tmp_path / "repo"
    dataset = root / "datasets" / "Group"
    dataset.mkdir(parents=True)
    (dataset / "a.pcap").write_bytes(b"1000")
    (dataset / "b.pcap").write_bytes(b"2000")
    external = tmp_path / "external-disk" / "research-output"
    external.mkdir(parents=True)
    return root, dataset, external


def test_external_storage_uses_destination_disk_not_dataset_disk(tmp_path, monkeypatch):
    root, dataset, external = _inputs(tmp_path)
    queried = []
    monkeypatch.setattr("runner.corpus_storage.shutil.disk_usage", lambda path: (
        queried.append(Path(path)) or SimpleNamespace(total=9000, used=100, free=8000)
    ))
    report = storage_estimate(external, dataset, 6000)
    assert queried == [external.resolve()]
    assert report["available_bytes"] == 8000
    assert report["estimated_capture_bytes"] == 6000
    assert report["estimated_minimum_with_reserve_bytes"] == 6600
    assert report["appears_sufficient"]
    assert gib(1024**3) == "1.00 GiB"


def test_storage_preflight_warns_when_available_space_is_too_small(tmp_path, monkeypatch):
    _, dataset, external = _inputs(tmp_path)
    monkeypatch.setattr("runner.corpus_storage.shutil.disk_usage", lambda _: (
        SimpleNamespace(total=2000, used=1000, free=1000)
    ))
    report = storage_estimate(external, dataset, 6000)
    assert not report["appears_sufficient"]


def test_refuse_outputs_inside_dataset_tree_but_allow_sibling_storage(tmp_path):
    root, dataset, external = _inputs(tmp_path)
    with pytest.raises(ValueError, match="outside the selected dataset tree"):
        validate_storage_parent(dataset, dataset)
    child = dataset / "output"
    child.mkdir()
    with pytest.raises(ValueError, match="outside the selected dataset tree"):
        validate_storage_parent(child, dataset)
    with pytest.raises(ValueError, match="inside dataset root"):
        check_campaign_output_path(dataset / "gan-2026", dataset)
    assert validate_storage_parent(external, dataset) == external.resolve()
    check_campaign_output_path(external / "a-new-run", dataset)
    # Symlinks are resolved to avoid bypassing source tree protection.
    alias = tmp_path / "dataset-alias"
    alias.symlink_to(dataset, target_is_directory=True)
    with pytest.raises(ValueError, match="outside the selected dataset tree"):
        validate_storage_parent(alias, dataset)


def test_toolbox_writes_to_chosen_external_parent_and_shows_space(tmp_path, monkeypatch, capsys):
    root, dataset, external = _inputs(tmp_path)
    calls = []
    def fake_picker(fn, *args, **kwargs):
        calls.append((fn, args, kwargs))
        return dataset if len(calls) == 1 else external
    monkeypatch.setattr(tui.curses, "wrapper", fake_picker)
    monkeypatch.setattr(tui, "storage_estimate", lambda parent, source, expected: {
        "output_parent": str(parent),
        "available_bytes": 10 * 1024**3,
        "estimated_capture_bytes": expected,
        "estimated_minimum_with_reserve_bytes": expected * 2,
        "appears_sufficient": True,
    })
    monkeypatch.setattr("builtins.input", lambda prompt: "RUN")
    class FakeProcess:
        returncode = None
        def poll(self):
            return None
    launched = []
    monkeypatch.setattr(tui.subprocess, "Popen", lambda command, **kwargs: (
        launched.append((command, kwargs)) or FakeProcess()
    ))
    import runner.corpus_monitor_tui as monitor
    watched = []
    monkeypatch.setattr(monitor, "launch_corpus_monitor", lambda output, **kwargs: (
        watched.append(Path(output))
    ))
    status = tui.run_corpus_tool_action(root)
    assert status == 0
    assert len(calls) == 2
    assert calls[1][2]["start_dir"] == root
    assert "SAVE GAN experiment outputs" in calls[1][2]["title"]
    command, opts = launched[0]
    assert command[command.index("--dataset-root") + 1] == str(dataset)
    output = Path(command[command.index("--output-dir") + 1])
    assert output.parent == external
    assert watched == [output]
    assert opts["start_new_session"]
    assert (external / (output.name + ".log")).exists()
    displayed = capsys.readouterr().out
    assert "Output storage:" in displayed
    assert "Available on destination filesystem:" in displayed


def test_undersized_storage_requires_explicit_override_before_launch(tmp_path, monkeypatch):
    root, dataset, external = _inputs(tmp_path)
    selections = iter([dataset, external])
    monkeypatch.setattr(tui.curses, "wrapper", lambda *a, **k: next(selections))
    monkeypatch.setattr(tui, "storage_estimate", lambda *a: {
        "available_bytes": 10, "estimated_capture_bytes": 1000,
        "estimated_minimum_with_reserve_bytes": 1100,
        "appears_sufficient": False,
    })
    prompt_text = []
    monkeypatch.setattr("builtins.input", lambda message: (
        prompt_text.append(message) or "RUN"
    ))
    monkeypatch.setattr(tui.subprocess, "Popen", lambda *a, **k: (
        pytest.fail("Experiment must not launch from accidental RUN on insufficient storage")
    ))
    assert tui.run_corpus_tool_action(root) == 0
    assert "RUN ANYWAY" in prompt_text[0]
    assert not list(external.iterdir())


def test_cancelling_output_location_does_not_start_experiment(tmp_path, monkeypatch):
    root, dataset, _ = _inputs(tmp_path)
    picker = iter([dataset, None])
    monkeypatch.setattr(tui.curses, "wrapper", lambda *a, **k: next(picker))
    monkeypatch.setattr(tui.subprocess, "Popen", lambda *a, **k: (
        pytest.fail("Cancelled selection cannot launch a worker")
    ))
    assert tui.run_corpus_tool_action(root) == 0
