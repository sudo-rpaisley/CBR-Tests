from __future__ import annotations

from typing import Any

SIDECAR_SCHEMA_VERSION = 1


class FieldTranslationError(ValueError):
    """Raised when a dataset field translation file is invalid or unsafe."""


def validate_unique_targets(mapping: dict[str, str]) -> None:
    """Reject mappings where multiple dataset fields map to one canonical field."""
    targets: dict[str, str] = {}
    duplicates: dict[str, list[str]] = {}
    for source, target in mapping.items():
        if target in targets:
            duplicates.setdefault(target, [targets[target]]).append(source)
        targets[target] = source
    if duplicates:
        duplicate_text = ", ".join(f"{target}: {sources}" for target, sources in sorted(duplicates.items()))
        raise FieldTranslationError(f"Multiple dataset fields map to the same test field: {duplicate_text}")


def validate_field_translation_payload(payload: dict[str, Any]) -> None:
    """Validate the standard field translation sidecar payload shape."""
    if not isinstance(payload, dict):
        raise FieldTranslationError("Field translation payload must be a JSON object.")
    schema_version = payload.get("schema_version")
    if schema_version is not None and schema_version != SIDECAR_SCHEMA_VERSION:
        raise FieldTranslationError(f"Unsupported field translation schema_version: {schema_version}")
    mapping = payload.get("test_to_dataset_fields")
    if mapping is None:
        raise FieldTranslationError("Field translation must include test_to_dataset_fields.")
    if not isinstance(mapping, dict):
        raise FieldTranslationError("test_to_dataset_fields must be an object.")
    for test_field, dataset_field in mapping.items():
        if not isinstance(test_field, str) or not isinstance(dataset_field, str):
            raise FieldTranslationError("test_to_dataset_fields entries must map strings to strings.")
        if not test_field.strip():
            raise FieldTranslationError("test_to_dataset_fields cannot include blank canonical field names.")
    metadata = payload.get("field_metadata")
    if metadata is not None:
        if not isinstance(metadata, dict):
            raise FieldTranslationError("field_metadata must be an object when provided.")
        unknown_metadata = set(metadata) - set(mapping)
        if unknown_metadata:
            raise FieldTranslationError(
                "field_metadata contains fields not listed in test_to_dataset_fields: "
                + ", ".join(sorted(unknown_metadata))
            )


def load_field_translation_from_payload(payload: dict[str, Any]) -> dict[str, str]:
    """Load a dataset-to-test mapping from an already parsed standard sidecar payload."""
    validate_field_translation_payload(payload)
    raw_mapping = payload["test_to_dataset_fields"]

    mapping: dict[str, str] = {}
    for raw_test_field, raw_dataset_field in raw_mapping.items():
        if not isinstance(raw_test_field, str) or not isinstance(raw_dataset_field, str):
            raise FieldTranslationError("Field translation entries must map strings to strings.")
        test_field = raw_test_field.strip()
        dataset_field = raw_dataset_field.strip()
        if not test_field or not dataset_field or test_field == dataset_field:
            continue
        if dataset_field in mapping:
            raise FieldTranslationError(f"Dataset field is mapped more than once: {dataset_field}")
        mapping[dataset_field] = test_field
    validate_unique_targets(mapping)
    return mapping
