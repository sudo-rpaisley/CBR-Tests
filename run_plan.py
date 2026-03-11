import json

import argparse

from pathlib import Path


import pandas as pd


from tests.pearson_profile import validate_candidate_fields, compute_pearson_profile

from tests.column_quality_profile import compute_column_quality_profile



def resolve_path(base_dir: Path, path_str: str) -> Path:

    path = Path(path_str)

    if path.is_absolute():

        return path

    return (base_dir / path).resolve()



def load_dataset(dataset_path: Path) -> pd.DataFrame:

    suffix = dataset_path.suffix.lower()


    if suffix == ".csv":

        return pd.read_csv(dataset_path)

    elif suffix == ".tsv":

        return pd.read_csv(dataset_path, sep="\t")

    elif suffix in [".xlsx", ".xls"]:

        return pd.read_excel(dataset_path)

    else:

        raise ValueError(f"Unsupported dataset format: {suffix}")



def run_pearson_metric(df: pd.DataFrame, metric: dict) -> tuple[bool, dict]:

    """

    Execute the Pearson metric and return:

    - success flag

    - metric result payload

    """

    candidate_fields = metric["input_requirements"]["candidate_fields"]

    minimum_runnable_fields = metric["input_requirements"]["minimum_runnable_fields"]


    column_validation, runnable_fields, df = validate_candidate_fields(df, candidate_fields)


    if len(runnable_fields) < minimum_runnable_fields:

        return False, {

            "column_validation": column_validation,

            "error": "Not enough usable numeric columns to compute Pearson correlation."

        }


    pearson_profile = compute_pearson_profile(df, runnable_fields)


    return True, {

        "column_validation": column_validation,

        "test_results": {

            "pearson_correlation_profile": pearson_profile

        }

    }



def run_column_quality_metric(df: pd.DataFrame, metric: dict) -> tuple[bool, dict]:

    """

    Execute the column quality metric and return:

    - success flag

    - metric result payload

    """

    candidate_fields = metric["input_requirements"]["candidate_fields"]

    quality_profile = compute_column_quality_profile(df, candidate_fields)


    return True, {

        "test_results": {

            "column_quality_profile": quality_profile

        }

    }



def dispatch_metric(df: pd.DataFrame, metric: dict) -> tuple[bool, dict]:

    """

    Dispatch execution based on metric_id.

    """

    metric_id = metric["metric_id"]


    if metric_id == "pearson_correlation_profile":

        return run_pearson_metric(df, metric)


    if metric_id == "column_quality_profile":

        return run_column_quality_metric(df, metric)


    raise ValueError(f"Unsupported metric_id: {metric_id}")



def main():

    parser = argparse.ArgumentParser(

        description="Run a minimal test plan from a case JSON."

    )

    parser.add_argument("--case", required=True, help="Path to the case JSON file")

    args = parser.parse_args()


    case_file = Path(args.case).resolve()

    case_dir = case_file.parent


    with open(case_file, "r", encoding="utf-8") as f:

        case = json.load(f)


    plan_path = resolve_path(case_dir, case["test_plan"]["path"])

    dataset_path = resolve_path(case_dir, case["dataset"]["path"])

    output_path = resolve_path(case_dir, case["output"]["path"])


    with open(plan_path, "r", encoding="utf-8") as f:

        plan = json.load(f)


    if not plan.get("metrics"):

        raise ValueError("The plan does not contain any metrics.")


    if len(plan["metrics"]) != 1:

        raise ValueError("This runner currently supports exactly one metric in the plan.")


    metric = plan["metrics"][0]

    df = load_dataset(dataset_path)


    success, metric_payload = dispatch_metric(df, metric)


    if not success:

        outcome = {

            "status": "failed",

            "case_id": case["case_id"],

            "plan_id": plan["plan_meta"]["plan_id"],

            "metric_id": metric["metric_id"],

            "dataset_path": str(dataset_path),

            "error": metric_payload["error"]

        }


        if "column_validation" in metric_payload:

            outcome["column_validation"] = metric_payload["column_validation"]


    else:

        outcome = {

            "status": "success",

            "case_id": case["case_id"],

            "plan_id": plan["plan_meta"]["plan_id"],

            "metric_id": metric["metric_id"],

            "dataset_path": str(dataset_path),

            "test_results": metric_payload["test_results"]

        }


        if "column_validation" in metric_payload:

            outcome["column_validation"] = metric_payload["column_validation"]


    with open(output_path, "w", encoding="utf-8") as f:

        json.dump(outcome, f, indent=2)


    print(f"Done. Wrote {output_path}")



if __name__ == "__main__":

    main()
