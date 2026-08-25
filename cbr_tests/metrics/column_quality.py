from __future__ import annotations

import pandas as pd


def compute_column_quality_profile(df: pd.DataFrame, candidate_fields: list[str]) -> dict:
    """Build per-field and aggregate quality details for requested fields."""
    field_results = []
    for field in candidate_fields:
        result = {
            "field": field,
            "exists": field in df.columns,
            "row_count": int(len(df)),
            "non_null_count": 0,
            "non_null_ratio": 0.0,
            "numeric_non_null_count": 0,
            "numeric_non_null_ratio": 0.0,
            "unique_count": 0,
            "unique_ratio": 0.0,
            "constant_column": False,
            "usable_for_analysis": False,
            "reason": None,
        }
        if field not in df.columns:
            result["reason"] = "missing_column"
            field_results.append(result)
            continue

        raw_series = df[field]
        row_count = int(len(df))
        non_null_count = int(raw_series.notna().sum())
        numeric_series = pd.to_numeric(raw_series, errors="coerce")
        numeric_non_null_count = int(numeric_series.notna().sum())
        unique_count = int(numeric_series.dropna().nunique())
        constant_column = numeric_non_null_count > 0 and unique_count < 2
        usable_for_analysis = numeric_non_null_count >= 2 and unique_count >= 2

        result.update(
            {
                "non_null_count": non_null_count,
                "non_null_ratio": round(non_null_count / row_count, 6) if row_count else 0.0,
                "numeric_non_null_count": numeric_non_null_count,
                "numeric_non_null_ratio": (
                    round(numeric_non_null_count / row_count, 6) if row_count else 0.0
                ),
                "unique_count": unique_count,
                "unique_ratio": (
                    round(unique_count / numeric_non_null_count, 6)
                    if numeric_non_null_count
                    else 0.0
                ),
                "constant_column": constant_column,
                "usable_for_analysis": usable_for_analysis,
            }
        )

        if non_null_count == 0:
            result["reason"] = "empty_column"
        elif numeric_non_null_count == 0:
            result["reason"] = "non_numeric"
        elif numeric_non_null_count < 2:
            result["reason"] = "insufficient_numeric_values"
        elif constant_column:
            result["reason"] = "constant_column"
        else:
            result["reason"] = "usable"
        field_results.append(result)

    field_count = len(field_results)
    usable_field_count = sum(result["usable_for_analysis"] for result in field_results)
    constant_field_count = sum(result["constant_column"] for result in field_results)
    missing_field_count = sum(
        result["reason"] == "missing_column" for result in field_results
    )

    def _mean(key: str) -> float | None:
        if not field_count:
            return None
        return round(sum(result[key] for result in field_results) / field_count, 6)

    return {
        "fields": field_results,
        "summary": {
            "field_count": field_count,
            "usable_field_count": usable_field_count,
            "constant_field_count": constant_field_count,
            "missing_field_count": missing_field_count,
            "mean_non_null_ratio": _mean("non_null_ratio"),
            "mean_numeric_non_null_ratio": _mean("numeric_non_null_ratio"),
            "mean_unique_ratio": _mean("unique_ratio"),
            "quality_score": (
                round(usable_field_count / field_count, 6) if field_count else None
            ),
        },
    }
