from itertools import combinations

import pandas as pd

from tests.pearson_profile import validate_candidate_fields


def compute_spearman_profile(df: pd.DataFrame, runnable_fields: list[str]) -> dict:
    corr = df[runnable_fields].corr(method="spearman")

    pairs = []
    for a, b in combinations(runnable_fields, 2):
        overlap_count = int(df[[a, b]].dropna().shape[0])
        value = float(corr.loc[a, b])
        pairs.append({
            "fields": [a, b],
            "value": round(value, 6),
            "overlap_non_null_count": overlap_count,
        })

    mean_abs_correlation = round(
        sum(abs(pair["value"]) for pair in pairs) / len(pairs),
        6,
    ) if pairs else None

    return {
        "fields": runnable_fields,
        "matrix": corr.round(6).to_dict(),
        "summary": {
            "pair_count": len(pairs),
            "mean_absolute_correlation": mean_abs_correlation,
            "pairs": pairs,
        },
    }


def validate_spearman_candidate_fields(df: pd.DataFrame, candidate_fields: list[str]) -> tuple[list[dict], list[str], pd.DataFrame]:
    column_validation, runnable_fields, df = validate_candidate_fields(df, candidate_fields)
    for result in column_validation:
        result["usable_for_spearman"] = result.pop("usable_for_pearson")
    return column_validation, runnable_fields, df
