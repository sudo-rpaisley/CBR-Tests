from __future__ import annotations

import pandas as pd


def _select_fields(df: pd.DataFrame, metric: dict, key: str) -> list[str]:
    """Resolve the declared schema, not just columns present in the input.

    Silently dropping absent configured fields previously concealed missing
    columns and changed the signature used by duplicate_row_ratio.
    """
    fields = metric.get("input_requirements", {}).get(key)
    if fields is None:
        return list(df.columns)
    return list(dict.fromkeys(str(field) for field in fields))


def compute_missing_value_ratio(df: pd.DataFrame, metric: dict) -> dict:
    fields = _select_fields(df, metric, "candidate_fields")
    row_count = int(len(df))
    field_count = len(fields)
    total_cells = row_count * field_count

    field_results = []
    missing_cells = 0
    for field in fields:
        exists = field in df.columns
        field_missing_count = int(df[field].isna().sum()) if exists else row_count
        missing_cells += field_missing_count
        field_results.append(
            {
                "field": field,
                "exists": exists,
                "missing_column": not exists,
                "missing_count": field_missing_count,
                "total_count": row_count,
                "missing_value_ratio": (
                    round(field_missing_count / row_count, 6) if row_count else None
                ),
            }
        )

    return {
        "fields": field_results,
        "summary": {
            "row_count": row_count,
            "field_count": field_count,
            "missing_column_count": sum(not item["exists"] for item in field_results),
            "missing_columns": [item["field"] for item in field_results if not item["exists"]],
            "total_cells": total_cells,
            "missing_cells": missing_cells,
            "missing_value_ratio": (
                round(missing_cells / total_cells, 6) if total_cells else None
            ),
            "runnable": total_cells > 0,
        },
    }


def compute_duplicate_row_ratio(df: pd.DataFrame, metric: dict) -> dict:
    subset_fields = _select_fields(df, metric, "subset_fields")
    missing_signature_fields = [field for field in subset_fields if field not in df.columns]
    row_count = int(len(df))
    all_fields = list(df.columns)
    duplicate_definition = (
        "full_row"
        if subset_fields and set(subset_fields) == set(all_fields)
        else "configured_signature"
        if subset_fields
        else "no_runnable_signature"
    )

    if not subset_fields or missing_signature_fields:
        duplicate_mask = pd.Series([False] * row_count, index=df.index)
        duplicate_group_count = 0
    else:
        duplicate_mask = df.duplicated(subset=subset_fields, keep="first")
        duplicate_group_count = int(
            df.loc[df.duplicated(subset=subset_fields, keep=False), subset_fields]
            .drop_duplicates()
            .shape[0]
        )

    duplicate_row_count = int(duplicate_mask.sum())
    runnable = row_count > 0 and bool(subset_fields) and not missing_signature_fields
    return {
        "summary": {
            "row_count": row_count,
            "duplicate_definition": duplicate_definition,
            "subset_field_count": len(subset_fields),
            "subset_field_names": ", ".join(subset_fields),
            "subset_fields": subset_fields,
            "missing_signature_fields": missing_signature_fields,
            "reason": ("missing_signature_fields" if missing_signature_fields else "usable" if runnable else "no_runnable_signature"),
            "duplicate_row_count": duplicate_row_count,
            "duplicate_group_count": duplicate_group_count,
            "duplicate_row_ratio": (
                round(duplicate_row_count / row_count, 6) if runnable else None
            ),
            "runnable": runnable,
        }
    }