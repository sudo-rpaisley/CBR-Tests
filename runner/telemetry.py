from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


TERMINAL_STATUSES = {"success", "failed", "skipped", "cancelled"}


@dataclass
class RunEvent:
    event_type: str
    message: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metric_id: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = {
            "timestamp": self.timestamp.isoformat(),
            "event_type": self.event_type,
            "message": self.message,
        }
        if self.metric_id is not None:
            data["metric_id"] = self.metric_id
        if self.payload:
            data["payload"] = self.payload
        return data


@dataclass
class MetricState:
    metric_id: str
    taxonomy_path: list[str]
    status: str = "pending"
    started_at: datetime | None = None
    finished_at: datetime | None = None
    elapsed_seconds: float | None = None
    error: str | None = None
    missing_fields: list[str] = field(default_factory=list)

    @property
    def branch(self) -> str:
        return str(self.taxonomy_path[0]) if self.taxonomy_path else "uncategorized"


@dataclass
class RunState:
    case_id: str
    plan_id: str
    plan_name: str
    dataset_path: Path
    output_path: Path
    started_at: datetime
    metrics: dict[str, MetricState]
    events: list[RunEvent] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @classmethod
    def from_plan(
        cls,
        *,
        case_id: str,
        plan: dict,
        metrics: list[dict],
        dataset_path: Path,
        output_path: Path,
        started_at: datetime,
    ) -> "RunState":
        plan_meta = plan.get("plan_meta", {})
        state = cls(
            case_id=case_id,
            plan_id=str(plan_meta.get("plan_id", "unknown_plan")),
            plan_name=str(plan_meta.get("name", "unknown plan")),
            dataset_path=dataset_path,
            output_path=output_path,
            started_at=started_at,
            metrics={
                metric.get("metric_id", "unknown_metric"): MetricState(
                    metric_id=metric.get("metric_id", "unknown_metric"),
                    taxonomy_path=list(metric.get("taxonomy_path", [])),
                )
                for metric in metrics
            },
        )
        state.record_event("run_initialized", f"Initialized run for {state.plan_id}")
        return state

    def record_event(self, event_type: str, message: str, metric_id: str | None = None, **payload: Any) -> None:
        self.events.append(RunEvent(event_type=event_type, message=message, metric_id=metric_id, payload=payload))

    def mark_running(self, metric_id: str, *, started_at: datetime | None = None) -> None:
        metric = self.metrics.get(metric_id)
        if metric is None or metric.status in TERMINAL_STATUSES:
            return
        if metric.status != "running":
            metric.status = "running"
            metric.started_at = started_at or datetime.now(timezone.utc)
            self.record_event("metric_started", f"Started {metric_id}", metric_id=metric_id)

    def mark_completed(
        self,
        metric_id: str,
        status: str,
        *,
        elapsed_seconds: float | None = None,
        error: str | None = None,
        finished_at: datetime | None = None,
    ) -> None:
        metric = self.metrics.get(metric_id)
        if metric is None:
            return
        metric.status = status
        metric.finished_at = finished_at or datetime.now(timezone.utc)
        metric.elapsed_seconds = elapsed_seconds
        metric.error = error
        event_type = "metric_completed" if status == "success" else f"metric_{status}"
        payload: dict[str, Any] = {}
        if elapsed_seconds is not None:
            payload["elapsed_seconds"] = elapsed_seconds
        if error:
            payload["error"] = error
        self.record_event(event_type, f"{status.capitalize()} {metric_id}", metric_id=metric_id, **payload)

    def mark_skipped(self, metric_id: str, missing_fields: list[str]) -> None:
        metric = self.metrics.get(metric_id)
        if metric is None:
            return
        metric.status = "skipped"
        metric.finished_at = datetime.now(timezone.utc)
        metric.missing_fields = list(missing_fields)
        self.record_event(
            "metric_skipped",
            f"Skipped {metric_id}: missing {', '.join(missing_fields)}",
            metric_id=metric_id,
            missing_fields=list(missing_fields),
        )

    def status_counts(self) -> dict[str, int]:
        counts = {"success": 0, "running": 0, "failed": 0, "skipped": 0, "pending": 0, "cancelled": 0, "stopping": 0}
        for metric in self.metrics.values():
            counts[metric.status] = counts.get(metric.status, 0) + 1
        return counts

    def branch_summaries(self) -> dict[str, dict[str, int]]:
        summaries: dict[str, dict[str, int]] = {}
        for metric in self.metrics.values():
            summary = summaries.setdefault(
                metric.branch,
                {"total": 0, "success": 0, "running": 0, "failed": 0, "skipped": 0, "pending": 0, "cancelled": 0, "stopping": 0},
            )
            summary["total"] += 1
            summary[metric.status] = summary.get(metric.status, 0) + 1
        return summaries

    def completed_statuses(self) -> dict[str, str]:
        return {
            metric_id: metric.status
            for metric_id, metric in self.metrics.items()
            if metric.status != "pending"
        }

    def completed_durations(self) -> dict[str, float]:
        return {
            metric_id: metric.elapsed_seconds
            for metric_id, metric in self.metrics.items()
            if metric.elapsed_seconds is not None
        }

    def recent_completed(self, limit: int = 5) -> list[MetricState]:
        completed = [metric for metric in self.metrics.values() if metric.status == "success" and metric.finished_at is not None]
        completed.sort(key=lambda metric: metric.finished_at or datetime.min.replace(tzinfo=timezone.utc))
        return completed[-limit:]

    def attention_metrics(self) -> list[MetricState]:
        return [
            metric
            for metric in self.metrics.values()
            if metric.status in {"running", "failed", "skipped", "stopping", "cancelled"}
        ]
