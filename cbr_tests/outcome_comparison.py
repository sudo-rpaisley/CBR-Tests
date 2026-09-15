"""Compare CBR-Tests outcome JSON before and after metric-contract changes."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any


_VOLATILE_KEYS = {
    "run_id",
    "started_at",
    "finished_at",
    "run_started_at",
    "run_finished_at",
    "generated_at",
    "created_at",
    "updated_at",
    "elapsed_seconds",
    "duration_seconds",
    "runtime_seconds",
    "execution_time_seconds",
    "output_path",
}

_LIST_IDENTITY_KEYS = (
    "metric_id",
    "job_id",
    "field",
    "id",
    "name",
    "reason_code",
)

_HIGH_IMPACT_TOKENS = (
    ".status",
    "[status]",
    "runnable",
    "applicable",
    "applicability",
    "_ratio",
    "_score",
    "_distance",
    "_divergence",
    "_deviation",
    "_statistic",
    "decision_rule",
    "measurement_parameters",
)


def load_outcome(path: str | Path) -> dict:
    """Load one JSON outcome object from disk."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Outcome JSON must contain an object at the root: {path}")
    return data


def _identity_key(items: list[Any]) -> str | None:
    if not items or not all(isinstance(item, dict) for item in items):
        return None
    for candidate in _LIST_IDENTITY_KEYS:
        values = []
        for item in items:
            if candidate not in item:
                break
            value = item[candidate]
            if isinstance(value, (dict, list)):
                break
            values.append(str(value))
        else:
            if len(values) == len(set(values)):
                return candidate
    return None


def flatten_outcome(
    value: Any,
    *,
    include_volatile: bool = False,
    path: str = "$",
) -> tuple[dict[str, Any], list[str]]:
    """Flatten JSON into stable paths while tracking ignored run metadata.

    Lists of dictionaries are keyed by a stable identifier such as ``metric_id``
    or ``field`` when possible. This avoids false changes when result ordering
    changes between executions.
    """
    flat: dict[str, Any] = {}
    ignored: list[str] = []

    def visit(node: Any, node_path: str) -> None:
        if isinstance(node, dict):
            if not node:
                flat[node_path] = {}
                return
            for key in sorted(node):
                child_path = f"{node_path}.{key}"
                if not include_volatile and key in _VOLATILE_KEYS:
                    ignored.append(child_path)
                    continue
                visit(node[key], child_path)
            return

        if isinstance(node, list):
            if not node:
                flat[node_path] = []
                return
            identity = _identity_key(node)
            if identity:
                for item in sorted(node, key=lambda row: str(row[identity])):
                    visit(item, f"{node_path}[{identity}={item[identity]}]")
            else:
                for index, item in enumerate(node):
                    visit(item, f"{node_path}[{index}]")
            return

        flat[node_path] = node

    visit(value, path)
    return flat, ignored


def _numbers_equal(before: Any, after: Any, *, abs_tol: float, rel_tol: float) -> bool:
    if isinstance(before, bool) or isinstance(after, bool):
        return False
    if not isinstance(before, (int, float)) or not isinstance(after, (int, float)):
        return False
    before_f = float(before)
    after_f = float(after)
    if math.isnan(before_f) and math.isnan(after_f):
        return True
    return math.isclose(before_f, after_f, abs_tol=abs_tol, rel_tol=rel_tol)


def _impact(path: str) -> str:
    lowered = path.lower()
    return "high" if any(token in lowered for token in _HIGH_IMPACT_TOKENS) else "normal"


def _numeric_delta(before: Any, after: Any) -> float | None:
    if isinstance(before, bool) or isinstance(after, bool):
        return None
    if isinstance(before, (int, float)) and isinstance(after, (int, float)):
        return float(after) - float(before)
    return None


def compare_outcomes(
    before: dict,
    after: dict,
    *,
    abs_tol: float = 1e-12,
    rel_tol: float = 1e-9,
    include_volatile: bool = False,
) -> dict:
    """Return a machine-readable comparison between two outcome objects."""
    before_flat, before_ignored = flatten_outcome(
        before, include_volatile=include_volatile
    )
    after_flat, after_ignored = flatten_outcome(
        after, include_volatile=include_volatile
    )

    changes = []
    all_paths = sorted(set(before_flat) | set(after_flat))
    for path in all_paths:
        in_before = path in before_flat
        in_after = path in after_flat
        if not in_before:
            changes.append(
                {
                    "path": path,
                    "change": "added",
                    "before": None,
                    "after": after_flat[path],
                    "delta": None,
                    "impact": _impact(path),
                }
            )
            continue
        if not in_after:
            changes.append(
                {
                    "path": path,
                    "change": "removed",
                    "before": before_flat[path],
                    "after": None,
                    "delta": None,
                    "impact": _impact(path),
                }
            )
            continue

        before_value = before_flat[path]
        after_value = after_flat[path]
        if before_value == after_value or _numbers_equal(
            before_value,
            after_value,
            abs_tol=abs_tol,
            rel_tol=rel_tol,
        ):
            continue
        changes.append(
            {
                "path": path,
                "change": "changed",
                "before": before_value,
                "after": after_value,
                "delta": _numeric_delta(before_value, after_value),
                "impact": _impact(path),
            }
        )

    changes.sort(key=lambda item: (0 if item["impact"] == "high" else 1, item["path"]))
    added = sum(change["change"] == "added" for change in changes)
    removed = sum(change["change"] == "removed" for change in changes)
    changed = sum(change["change"] == "changed" for change in changes)
    high_impact = sum(change["impact"] == "high" for change in changes)

    return {
        "schema_version": 1,
        "comparison_policy": {
            "abs_tol": abs_tol,
            "rel_tol": rel_tol,
            "include_volatile": include_volatile,
            "volatile_keys": [] if include_volatile else sorted(_VOLATILE_KEYS),
        },
        "before": {
            "status": before.get("status"),
            "case_id": before.get("case_id"),
            "plan_id": before.get("plan_id"),
            "dataset_path": before.get("dataset_path"),
        },
        "after": {
            "status": after.get("status"),
            "case_id": after.get("case_id"),
            "plan_id": after.get("plan_id"),
            "dataset_path": after.get("dataset_path"),
        },
        "summary": {
            "total_changes": len(changes),
            "changed": changed,
            "added": added,
            "removed": removed,
            "high_impact": high_impact,
            "ignored_volatile_paths": len(set(before_ignored) | set(after_ignored)),
        },
        "changes": changes,
    }


def _display_value(value: Any, *, limit: int = 100) -> str:
    if value is None:
        return "—"
    text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    if len(text) > limit:
        text = text[: limit - 1] + "…"
    return text.replace("|", "\\|").replace("`", "\\`")


def render_markdown(report: dict) -> str:
    """Render a comparison report suitable for experiment records and review."""
    summary = report["summary"]
    lines = [
        "# CBR-Tests outcome comparison",
        "",
        "This report compares two authoritative outcome JSON files. Run-specific volatile metadata is ignored by default so the report concentrates on metric, applicability and decision-policy changes.",
        "",
        "## Summary",
        "",
        f"- Total differences: **{summary['total_changes']}**",
        f"- High-impact metric/status differences: **{summary['high_impact']}**",
        f"- Changed values: **{summary['changed']}**",
        f"- Added values: **{summary['added']}**",
        f"- Removed values: **{summary['removed']}**",
        f"- Ignored volatile paths: **{summary['ignored_volatile_paths']}**",
        "",
        "## Run identity",
        "",
        "| Field | Before | After |",
        "|---|---|---|",
    ]
    for key in ("status", "case_id", "plan_id", "dataset_path"):
        lines.append(
            f"| `{key}` | {_display_value(report['before'].get(key))} | {_display_value(report['after'].get(key))} |"
        )

    lines.extend(["", "## Differences", ""])
    if not report["changes"]:
        lines.append("No non-volatile differences were detected within the configured numerical tolerances.")
        return "\n".join(lines) + "\n"

    lines.extend(
        [
            "| Impact | Change | JSON path | Before | After | Δ |",
            "|---|---|---|---|---|---:|",
        ]
    )
    for change in report["changes"]:
        delta = "—" if change["delta"] is None else f"{change['delta']:.12g}"
        lines.append(
            "| {impact} | {kind} | `{path}` | {before} | {after} | {delta} |".format(
                impact=change["impact"],
                kind=change["change"],
                path=change["path"].replace("`", "\\`"),
                before=_display_value(change["before"]),
                after=_display_value(change["after"]),
                delta=delta,
            )
        )
    return "\n".join(lines) + "\n"
