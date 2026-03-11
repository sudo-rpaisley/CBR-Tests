from itertools import combinations

import pandas as pd



def validate_candidate_fields(df: pd.DataFrame, candidate_fields: list[str]) -> tuple[list[dict], list[str], pd.DataFrame]:

    """

    Validate candidate fields for Pearson correlation.


    Returns:

        column_validation: per-field validation results

        runnable_fields: fields that are usable for Pearson

        df: dataframe with runnable fields coerced to numeric

    """

    column_validation = []

    runnable_fields = []


    for field in candidate_fields:

        result = {

            "field": field,

            "exists": field in df.columns,

            "numeric_non_null_count": 0,

            "numeric_unique_count": 0,

            "usable_for_pearson": False,

            "reason": None

        }


        if field not in df.columns:

            result["reason"] = "missing_column"

            column_validation.append(result)

            continue


        numeric_series = pd.to_numeric(df[field], errors="coerce")

        non_null_count = int(numeric_series.notna().sum())

        unique_count = int(numeric_series.dropna().nunique())


        result["numeric_non_null_count"] = non_null_count

        result["numeric_unique_count"] = unique_count


        if non_null_count == 0:

            result["reason"] = "non_numeric"

        elif non_null_count < 2:

            result["reason"] = "insufficient_numeric_values"

        elif unique_count < 2:

            result["reason"] = "constant_column"

        else:

            result["usable_for_pearson"] = True

            result["reason"] = "usable"

            runnable_fields.append(field)

            df[field] = numeric_series


        column_validation.append(result)


    return column_validation, runnable_fields, df



def compute_pearson_profile(df: pd.DataFrame, runnable_fields: list[str]) -> dict:

    """

    Compute Pearson correlation profile for runnable fields.

    """

    corr = df[runnable_fields].corr(method="pearson")


    pairs = []

    for a, b in combinations(runnable_fields, 2):

        overlap_count = int(df[[a, b]].dropna().shape[0])

        value = float(corr.loc[a, b])

        pairs.append({

            "fields": [a, b],

            "value": round(value, 6),

            "overlap_non_null_count": overlap_count

        })


    mean_abs_correlation = round(

        sum(abs(pair["value"]) for pair in pairs) / len(pairs),

        6

    ) if pairs else None


    return {

        "fields": runnable_fields,

        "matrix": corr.round(6).to_dict(),

        "summary": {

            "pair_count": len(pairs),

            "mean_absolute_correlation": mean_abs_correlation,

            "pairs": pairs

        }

    }
