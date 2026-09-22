from __future__ import annotations

import curses
import json
from pathlib import Path

from runner.campaign_tui import _batch_picker, _discover_batch_manifests


class _FakeScreen:
    def __init__(self, keys: list[int]):
        self.keys = list(keys)

    def erase(self):
        return None

    def getmaxyx(self):
        return (30, 180)

    def getch(self):
        if not self.keys:
            raise AssertionError("Picker requested more keys than the test supplied")
        return self.keys.pop(0)

    def addstr(self, *args, **kwargs):
        return None


def _write_batch(path: Path, *, batch_id: str, job_count: int = 2) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "batch_meta": {
            "batch_id": batch_id,
            "name": batch_id.replace("-", " ").title(),
            "metric_policy": "common_across_all_datasets",
            "comparison_mode": "candidate_reference_matrix",
        },
        "jobs": [
            {
                "job_id": f"{batch_id}-{index:02d}",
                "dataset_path": f"datasets/{batch_id}-{index}.pcapng",
                "reference_dataset_path": f"datasets/reference-{index}.pcapng",
                "plan_path": f"plans/{batch_id}_batch_plans/{index:02d}_plan.json",
            }
            for index in range(1, job_count + 1)
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_discovers_saved_batch_manifests_under_plans(tmp_path):
    amazon = _write_batch(tmp_path / "plans" / "amazon_batch.json", batch_id="amazon", job_count=56)
    bucket = _write_batch(tmp_path / "plans" / "bucket_batch.json", batch_id="bucket", job_count=12)
    discovered = _discover_batch_manifests(tmp_path)
    assert [path for path, _ in discovered] == [amazon.resolve(), bucket.resolve()]
    assert [len(batch["jobs"]) for _, batch in discovered] == [56, 12]


def test_discovery_ignores_normal_plan_json_files(tmp_path):
    _write_batch(tmp_path / "plans" / "amazon_batch.json", batch_id="amazon")
    (tmp_path / "plans" / "single_plan.json").write_text("{}", encoding="utf-8")
    discovered = _discover_batch_manifests(tmp_path)
    assert [batch["batch_meta"]["batch_id"] for _, batch in discovered] == ["amazon"]


def test_discovery_does_not_descend_into_generated_batch_plan_folders(tmp_path):
    _write_batch(tmp_path / "plans" / "amazon_batch.json", batch_id="amazon")
    generated = tmp_path / "plans" / "amazon_batch_plans"
    _write_batch(generated / "accidental_batch.json", batch_id="should-not-be-listed")
    discovered = _discover_batch_manifests(tmp_path)
    assert [batch["batch_meta"]["batch_id"] for _, batch in discovered] == ["amazon"]


def test_discovery_can_find_batches_in_nested_organisation_folders(tmp_path):
    first = _write_batch(tmp_path / "plans" / "pcap" / "amazon_batch.json", batch_id="amazon")
    second = _write_batch(tmp_path / "plans" / "pcap" / "bucket_batch.json", batch_id="bucket")
    discovered = _discover_batch_manifests(tmp_path)
    assert {path for path, _ in discovered} == {first.resolve(), second.resolve()}


def test_batch_picker_can_queue_multiple_saved_matrices(tmp_path):
    _write_batch(tmp_path / "plans" / "amazon_batch.json", batch_id="amazon")
    _write_batch(tmp_path / "plans" / "bucket_batch.json", batch_id="bucket")
    screen = _FakeScreen([ord(" "), curses.KEY_DOWN, ord(" "), 10])
    selected = _batch_picker(screen, tmp_path)
    assert selected == ["plans/amazon_batch.json", "plans/bucket_batch.json"]


def test_batch_picker_select_all_queues_every_discovered_batch(tmp_path):
    _write_batch(tmp_path / "plans" / "amazon_batch.json", batch_id="amazon")
    _write_batch(tmp_path / "plans" / "bucket_batch.json", batch_id="bucket")
    screen = _FakeScreen([ord("a"), 10])
    selected = _batch_picker(screen, tmp_path)
    assert selected == ["plans/amazon_batch.json", "plans/bucket_batch.json"]
