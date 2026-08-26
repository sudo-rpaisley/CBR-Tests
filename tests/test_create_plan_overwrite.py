from argparse import Namespace

import pytest

import create_plan


class _TTYInput:
    def isatty(self):
        return True


class _NonTTYInput:
    def isatty(self):
        return False


def _args(dataset, output, *, force=False):
    return Namespace(
        list_tests=False,
        check=None,
        name="Existing Plan",
        plan_id=None,
        description=None,
        dataset=str(dataset),
        field_translation=None,
        output=str(output),
        include=None,
        exclude=None,
        force=force,
    )


def _dataset(path):
    path.write_text(
        "Source Port,Destination Port,Protocol\n12345,443,6\n53,53000,17\n",
        encoding="utf-8",
    )


def test_interactive_builder_can_confirm_overwrite(monkeypatch, tmp_path):
    dataset = tmp_path / "capture.csv"
    output = tmp_path / "existing_plan.json"
    _dataset(dataset)
    output.write_text("old-plan", encoding="utf-8")

    answers = iter(["y", "y"])
    monkeypatch.setattr(create_plan, "parse_args", lambda: _args(dataset, output))
    monkeypatch.setattr(create_plan.sys, "stdin", _TTYInput())
    monkeypatch.setattr("builtins.input", lambda _prompt: next(answers))

    assert create_plan.main() == 0
    assert output.read_text(encoding="utf-8") != "old-plan"
    assert '"plan_id": "existing-plan"' in output.read_text(encoding="utf-8")


def test_interactive_builder_can_decline_overwrite(monkeypatch, tmp_path):
    dataset = tmp_path / "capture.csv"
    output = tmp_path / "existing_plan.json"
    _dataset(dataset)
    output.write_text("old-plan", encoding="utf-8")

    answers = iter(["y", "n"])
    monkeypatch.setattr(create_plan, "parse_args", lambda: _args(dataset, output))
    monkeypatch.setattr(create_plan.sys, "stdin", _TTYInput())
    monkeypatch.setattr("builtins.input", lambda _prompt: next(answers))

    assert create_plan.main() == 0
    assert output.read_text(encoding="utf-8") == "old-plan"


def test_noninteractive_builder_still_requires_force_to_replace(monkeypatch, tmp_path):
    dataset = tmp_path / "capture.csv"
    output = tmp_path / "existing_plan.json"
    _dataset(dataset)
    output.write_text("old-plan", encoding="utf-8")

    monkeypatch.setattr(create_plan, "parse_args", lambda: _args(dataset, output))
    monkeypatch.setattr(create_plan.sys, "stdin", _NonTTYInput())

    with pytest.raises(FileExistsError, match="Use --force to replace it"):
        create_plan.main()

    assert output.read_text(encoding="utf-8") == "old-plan"
