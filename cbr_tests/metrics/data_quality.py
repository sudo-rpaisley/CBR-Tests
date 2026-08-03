from __future__ import annotations

import pandas as pd


def _select_fields(df: pd.DataFrame, metric: dict, key: str) -> list[str]:
    fields = metric.get("input_requirements", {}).get(key)
    if fields is None:
        return list(df.columns)
    return [field for field in fields if field in df.columns]


def compute_missing_value_ratio(df: pd.DataFrame, metric: dict) -> dict:
    fields = _select_fields(df, metric, "candidate_fields")
    row_count = int(len(df))
    field_count = len(fields)
    total_cells = row_count * field_count

    field_results = []
    missing_cells = 0
    for field in fields:
        field_missing_count = int(df[field].isna().sum())
        missing_cells += field_missing_count
        field_results.append(
            {
                "field": field,
                "missing_count": field_missing_count,
                "total_count": row_count,
                "missing_value_ratio": (
                    round(field_missing_count / row_count, 6) if row_count else 0.0
                ),
            }
        )

    return {
        "fields": field_results,
        "summary": {
            "row_count": row_count,
            "field_count": field_count,
            "total_cells": total_cells,
            "missing_cells": missing_cells,
            "missing_value_ratio": (
                round(missing_cells / total_cells, 6) if total_cells else 0.0
            ),
        },
    }


def compute_duplicate_row_ratio(df: pd.DataFrame, metric: dict) -> dict:
    subset_fields = _select_fields(df, metric, "subset_fields")
    row_count = int(len(df))

    if not subset_fields:
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
    return {
        "summary": {
            "row_count": row_count,
            "subset_fields": subset_fields,
            "duplicate_row_count": duplicate_row_count,
            "duplicate_group_count": duplicate_group_count,
            "duplicate_row_ratio": (
                round(duplicate_row_count / row_count, 6) if row_count else 0.0
            ),
        }
    }
