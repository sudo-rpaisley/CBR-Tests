from pathlib import Path
import pandas as pd
from runner.tabular import load_tabular_dataset


def _norm(v, case_sensitive: bool):
    if pd.isna(v):
        return None
    s = str(v).strip()
    if s == "":
        return None
    return s if case_sensitive else s.lower()


def _rule_match(field_value, operator, target, case_sensitive):
    fv = _norm(field_value, case_sensitive)
    if fv is None:
        return False
    t = _norm(target, case_sensitive)
    if t is None:
        return False
    if operator == "equals":
        return fv == t
    if operator == "contains":
        return t in fv
    if operator == "starts_with":
        return fv.startswith(t)
    if operator == "ends_with":
        return fv.endswith(t)
    if operator == "in":
        vals = target if isinstance(target, list) else [target]
        vals = [_norm(x, case_sensitive) for x in vals]
        return fv in vals
    return False


def run_slice_identifier_consistency_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    """Slice metadata integrity tests are context-dependent. A valid slice identifier only shows that the slice value belongs to the expected vocabulary. Slice identifier consistency checks whether that value is plausible given other row metadata, such as source file, traffic group, or label. A consistency failure should be interpreted as a possible metadata, labelling, merge, or extraction issue, not automatically as proof that the dataset is unusable."""
    input_req = metric.get("input_requirements", {})
    params = metric.get("calculation", {}).get("parameters", {})
    slice_field = input_req.get("slice_field")
    if not slice_field:
        return False, {"error": "slice_field is required."}
    rules = params.get("rules", [])
    if not rules:
        return False, {"error": "rules is required and must be non-empty."}
    case_sensitive = bool(params.get("case_sensitive", False))
    missing_policy = params.get("missing_policy", "count_invalid")
    max_examples = int(params.get("max_examples", 10))

    df = metric.get("_shared_df")
    if df is None:
        try:
            df = load_tabular_dataset(dataset_path)
        except Exception as exc:
            return False, {"error": f"Failed to load dataset: {exc}"}

    if slice_field not in df.columns:
        return False, {"error": "Slice field does not exist in dataset.", "missing_field": slice_field}

    row_count = len(df)
    checked = consistent = inconsistent = missing = unmatched = rule_app = 0
    rule_unavail = 0
    examples = []
    summary = []

    prepared_rules = []
    for i, r in enumerate(rules):
        wf = r.get("when_field")
        op = r.get("operator")
        val = r.get("value")
        exp = r.get("expected_slice_ids", [])
        if wf not in df.columns:
            rule_unavail += 1
            summary.append({"rule_index": i, "when_field": wf, "operator": op, "value": val, "application_count": 0, "consistent_count": 0, "inconsistent_count": 0})
            prepared_rules.append(None)
            continue
        prepared_rules.append({"when_field": wf, "operator": op, "value": val, "expected": {_norm(x, case_sensitive) for x in exp if _norm(x, case_sensitive) is not None}, "application_count": 0, "consistent_count": 0, "inconsistent_count": 0, "rule_index": i})

    for idx, row in df.iterrows():
        matched = []
        expected = set()
        for pr in prepared_rules:
            if pr is None:
                continue
            if _rule_match(row[pr["when_field"]], pr["operator"], pr["value"], case_sensitive):
                pr["application_count"] += 1
                matched.append(pr["rule_index"])
                expected.update(pr["expected"])
                rule_app += 1

        if not matched:
            unmatched += 1
            continue

        observed = _norm(row[slice_field], case_sensitive)
        if observed is None:
            missing += 1
            if missing_policy == "count_invalid":
                checked += 1
                inconsistent += 1
            continue

        checked += 1
        if observed in expected:
            consistent += 1
            for pr in prepared_rules:
                if pr is not None and pr["rule_index"] in matched:
                    pr["consistent_count"] += 1
        else:
            inconsistent += 1
            for pr in prepared_rules:
                if pr is not None and pr["rule_index"] in matched:
                    pr["inconsistent_count"] += 1
            if len(examples) < max_examples:
                ctx_fields = input_req.get("context_fields", [])
                ctx = {f: row[f] for f in ctx_fields if f in df.columns}
                examples.append({"row_index": int(idx) if isinstance(idx, int) else str(idx), "observed_slice_id": observed, "expected_slice_ids": sorted(expected), "matched_rules": matched, "context": ctx, "reason": "slice_id_does_not_match_expected_context"})

    for pr in prepared_rules:
        if pr is None:
            continue
        summary.append({"rule_index": pr["rule_index"], "when_field": pr["when_field"], "operator": pr["operator"], "value": pr["value"], "application_count": pr["application_count"], "consistent_count": pr["consistent_count"], "inconsistent_count": pr["inconsistent_count"]})

    ratio = round(consistent / checked, 6) if checked else 0.0
    inconsistency_ratio = round(inconsistent / checked, 6) if checked else 0.0
    missing_ratio = round(missing / row_count, 6) if row_count else 0.0

    if checked == 0:
        status = "not_applicable"
    else:
        status = "pass" if ratio >= 0.99 else "warn" if ratio >= 0.95 else "fail"

    return True, {"test_results": {"slice_identifier_consistency_profile": {
        "slice_field": slice_field,
        "row_count": row_count,
        "checked_row_count": checked,
        "consistent_row_count": consistent,
        "inconsistent_row_count": inconsistent,
        "missing_slice_count": missing,
        "unmatched_context_row_count": unmatched,
        "rule_application_count": rule_app,
        "rule_unavailable_count": rule_unavail,
        "slice_identifier_consistency_ratio": ratio,
        "inconsistency_ratio": inconsistency_ratio,
        "missing_slice_ratio": missing_ratio,
        "rules_applied_summary": summary,
        "examples": examples,
        "status": status,
    }}}
