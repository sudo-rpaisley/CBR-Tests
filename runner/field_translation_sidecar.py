from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from runner.field_translation_schema import SIDECAR_SCHEMA_VERSION, validate_field_translation_payload


def default_field_translation_path(dataset_path: Path) -> Path:
    """Return the standard sidecar translation path for a dataset."""
    return dataset_path.with_name(f"{dataset_path.stem}.field_translation.json")


def ensure_field_translation_file(
    *,
    dataset_path: Path,
    plan: dict,
    detected_dataset_to_test: dict[str, str] | None = None,
) -> Path | None:
    """Create or update the dataset sidecar translation template.

    The sidecar is written next to the dataset as ``<dataset stem>.field_translation.json``.
    Existing sidecars are only updated when enabled plan metrics introduce new
    canonical fields that are not already present in ``test_to_dataset_fields``.
    """
    from runner.field_translation import collect_field_requirements

    field_usage = collect_field_requirements(plan)
    template_fields = sorted(field_usage)
    if not template_fields or not dataset_path.exists():
        return None

    translation_path = default_field_translation_path(dataset_path)
    detected_test_to_dataset = _invert_translation(detected_dataset_to_test or {})
    existing_payload = _load_existing_translation_payload(translation_path)
    existing_test_to_dataset = _payload_to_test_to_dataset_fields(existing_payload)

    if existing_payload:
        missing_template_fields = [field for field in template_fields if field not in existing_test_to_dataset]
        if not missing_template_fields:
            return translation_path
        updated_test_to_dataset = dict(existing_test_to_dataset)
        for field in missing_template_fields:
            updated_test_to_dataset[field] = detected_test_to_dataset.get(field, "")
    else:
        updated_test_to_dataset = {field: detected_test_to_dataset.get(field, "") for field in template_fields}

    payload = {
        "schema_version": SIDECAR_SCHEMA_VERSION,
        "description": "Dataset field translation template. Fill empty values with dataset column names.",
        "dataset": str(dataset_path),
        "plan_id": plan.get("plan_meta", {}).get("plan_id"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "test_to_dataset_fields": {field: updated_test_to_dataset.get(field, "") for field in sorted(updated_test_to_dataset)},
        "field_metadata": {
            field: {
                "required_by": field_usage.get(field, {}).get("required_by", []),
                "optional_for": field_usage.get(field, {}).get("optional_for", []),
                "mapping_source": "auto_detected" if updated_test_to_dataset.get(field) else "template",
            }
            for field in sorted(updated_test_to_dataset)
        },
    }

    translation_path.parent.mkdir(parents=True, exist_ok=True)
    with open(translation_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")
    return translation_path


def _load_existing_translation_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload: Any = json.load(f)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _payload_to_test_to_dataset_fields(payload: dict[str, Any]) -> dict[str, str]:
    if not payload:
        return {}
    validate_field_translation_payload(payload)
    return {
        str(test_field).strip(): str(dataset_field).strip()
        for test_field, dataset_field in payload["test_to_dataset_fields"].items()
        if str(test_field).strip()
    }


def _invert_translation(dataset_to_test: dict[str, str]) -> dict[str, str]:
    return {test_field: dataset_field for dataset_field, test_field in dataset_to_test.items()}
