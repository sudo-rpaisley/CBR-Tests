from pathlib import Path
from ipaddress import ip_address, ip_network

from runner.tabular import load_tabular_dataset


IPV4_DOC_NETS = [
    ip_network("192.0.2.0/24"),
    ip_network("198.51.100.0/24"),
    ip_network("203.0.113.0/24"),
]
IPV6_DOC_NETS = [
    ip_network("2001:db8::/32"),
    ip_network("3fff::/20"),
]
SHARED_ADDRESS_SPACE = ip_network("100.64.0.0/10")
BENCHMARKING_NET = ip_network("198.18.0.0/15")
IPV4_MAPPED_NET = ip_network("::ffff:0:0/96")

CATEGORY_PARAMETER = {
    "private": "count_private_as_reserved",
    "loopback": "count_loopback_as_reserved",
    "link_local": "count_link_local_as_reserved",
    "multicast": "count_multicast_as_reserved",
    "documentation": "count_documentation_as_reserved",
    "reserved": "count_reserved_as_reserved",
    "unspecified": "count_unspecified_as_reserved",
    "shared_address_space": "count_shared_address_space_as_reserved",
    "benchmarking": "count_benchmarking_as_reserved",
    "ipv4_mapped": "count_ipv4_mapped_as_reserved",
    "unique_local": "count_unique_local_as_reserved",
}


def get_reserved_categories(addr) -> list[str]:
    categories = []

    if addr.is_private:
        categories.append("private")
    if addr.is_loopback:
        categories.append("loopback")
    if addr.is_link_local:
        categories.append("link_local")
    if addr.is_multicast:
        categories.append("multicast")
    if addr.is_reserved:
        categories.append("reserved")
    if addr.is_unspecified:
        categories.append("unspecified")

    if any(addr in net for net in (IPV4_DOC_NETS if addr.version == 4 else IPV6_DOC_NETS)):
        categories.append("documentation")
    if addr.version == 4 and addr in SHARED_ADDRESS_SPACE:
        categories.append("shared_address_space")
    if addr.version == 4 and addr in BENCHMARKING_NET:
        categories.append("benchmarking")
    if addr.version == 6 and addr in IPV4_MAPPED_NET:
        categories.append("ipv4_mapped")
    if addr.version == 6 and addr in ip_network("fc00::/7"):
        categories.append("unique_local")

    return categories


def _enabled_categories(categories: list[str], params: dict) -> list[str]:
    return [
        category
        for category in categories
        if bool(params.get(CATEGORY_PARAMETER.get(category, ""), False))
    ]


def _diagnostic(status: str, *, invalid_count: int, invalid_ratio: float, reserved_count: int,
                checked_count: int, threshold: float, category_counts: dict, invalid_examples: list,
                reserved_examples: list) -> dict:
    nonzero_categories = {key: value for key, value in category_counts.items() if value}
    evidence = {
        "checked_address_count": checked_count,
        "invalid_address_count": invalid_count,
        "invalid_address_ratio": invalid_ratio,
        "reserved_address_count": reserved_count,
        "reserved_category_counts": nonzero_categories,
    }
    examples = invalid_examples[:3] or reserved_examples[:3]
    if examples:
        evidence["examples"] = examples

    if status == "fail":
        return {
            "reason_code": "invalid_ip_ratio_exceeded",
            "summary": (
                f"{invalid_count} of {checked_count} checked IP values were invalid "
                f"({invalid_ratio:.2%}), exceeding the configured failure threshold of {threshold:.2%}."
            ),
            "evidence": evidence,
            "suggestion": "Inspect the invalid examples and confirm IP field translation and parsing semantics.",
        }
    if status == "warn":
        reason_code = "invalid_or_reserved_ip_observed" if invalid_count else "reserved_ip_addresses_observed"
        return {
            "reason_code": reason_code,
            "summary": (
                f"The IP fields contained {invalid_count} invalid and {reserved_count} counted reserved/special-use values; "
                "the invalid-value rate did not exceed the failure threshold."
            ),
            "evidence": evidence,
            "suggestion": "Review whether the observed special-use address categories are expected for this dataset and capture context.",
        }
    return {
        "reason_code": "ip_profile_within_policy",
        "summary": "No invalid or counted reserved/special-use IP values were observed under the configured policy.",
        "evidence": evidence,
    }


def run_reserved_ip_address_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    import pandas as pd

    params = metric.get("calculation", {}).get("parameters", {})
    invalid_ratio_fail_threshold = float(params.get("invalid_ratio_fail_threshold", 0.01))
    if not 0 <= invalid_ratio_fail_threshold <= 1:
        return False, {
            "error": "invalid_ratio_fail_threshold must be between 0 and 1.",
            "reason_code": "invalid_metric_configuration",
        }

    df = metric.get("_shared_df")
    if df is None:
        try:
            df = load_tabular_dataset(dataset_path)
        except Exception as exc:
            return False, {
                "error": f"Failed to load dataset: {exc}",
                "reason_code": "dataset_load_error",
            }

    candidate_fields = metric.get("input_requirements", {}).get(
        "candidate_fields", ["Source IP", "Destination IP", "Src IP", "Dst IP"]
    )
    checked_fields = [f for f in candidate_fields if f in df.columns]
    missing_fields = [f for f in candidate_fields if f not in df.columns]

    if not checked_fields:
        return False, {
            "error": "No candidate IP fields found in dataset columns.",
            "reason_code": "missing_required_fields",
            "missing_fields": missing_fields or list(candidate_fields),
        }

    row_count = len(df)
    checked_address_count = 0
    valid_address_count = 0
    invalid_address_count = 0
    missing_address_count = 0
    reserved_address_count = 0
    reserved_row_count = 0

    ip_version_counts = {"ipv4": 0, "ipv6": 0}
    reserved_category_counts = {category: 0 for category in CATEGORY_PARAMETER}
    field_summaries = {
        field: {
            "field": field,
            "exists": True,
            "checked_address_count": 0,
            "valid_address_count": 0,
            "invalid_address_count": 0,
            "missing_address_count": 0,
            "reserved_address_count": 0,
        }
        for field in checked_fields
    }

    reserved_examples = []
    invalid_examples = []

    for row_idx, row in df.iterrows():
        row_reserved = False
        for field in checked_fields:
            value = row[field]
            checked_address_count += 1
            field_summaries[field]["checked_address_count"] += 1

            if pd.isna(value) or str(value).strip() == "":
                missing_address_count += 1
                field_summaries[field]["missing_address_count"] += 1
                continue

            value_text = str(value).strip()
            try:
                addr = ip_address(value_text)
            except ValueError:
                invalid_address_count += 1
                field_summaries[field]["invalid_address_count"] += 1
                if len(invalid_examples) < 20:
                    invalid_examples.append({"row_index": int(row_idx), "field": field, "value": value_text})
                continue

            valid_address_count += 1
            field_summaries[field]["valid_address_count"] += 1
            ip_version_counts["ipv4" if addr.version == 4 else "ipv6"] += 1

            observed_categories = get_reserved_categories(addr)
            for category in observed_categories:
                reserved_category_counts[category] += 1

            categories = _enabled_categories(observed_categories, params)
            if categories:
                reserved_address_count += 1
                field_summaries[field]["reserved_address_count"] += 1
                row_reserved = True
                if len(reserved_examples) < 20:
                    reserved_examples.append({
                        "row_index": int(row_idx),
                        "field": field,
                        "value": value_text,
                        "version": addr.version,
                        "categories": categories,
                    })

        if row_reserved:
            reserved_row_count += 1

    invalid_address_ratio = round(invalid_address_count / checked_address_count, 6) if checked_address_count else 0.0
    reserved_address_ratio = round(reserved_address_count / checked_address_count, 6) if checked_address_count else 0.0
    reserved_row_ratio = round(reserved_row_count / row_count, 6) if row_count else 0.0

    for field in checked_fields:
        checked = field_summaries[field]["checked_address_count"]
        field_summaries[field]["reserved_address_ratio"] = round(
            field_summaries[field]["reserved_address_count"] / checked, 6
        ) if checked else 0.0
        field_summaries[field]["invalid_address_ratio"] = round(
            field_summaries[field]["invalid_address_count"] / checked, 6
        ) if checked else 0.0

    if invalid_address_ratio > invalid_ratio_fail_threshold:
        status = "fail"
    elif reserved_address_count > 0 or invalid_address_count > 0:
        status = "warn"
    else:
        status = "pass"

    diagnostic = _diagnostic(
        status,
        invalid_count=invalid_address_count,
        invalid_ratio=invalid_address_ratio,
        reserved_count=reserved_address_count,
        checked_count=checked_address_count,
        threshold=invalid_ratio_fail_threshold,
        category_counts=reserved_category_counts,
        invalid_examples=invalid_examples,
        reserved_examples=reserved_examples,
    )

    return True, {
        "test_results": {
            "reserved_ip_address_profile": {
                "row_count": row_count,
                "checked_fields": checked_fields,
                "missing_fields": missing_fields,
                "checked_address_count": checked_address_count,
                "valid_address_count": valid_address_count,
                "invalid_address_count": invalid_address_count,
                "missing_address_count": missing_address_count,
                "reserved_address_count": reserved_address_count,
                "reserved_row_count": reserved_row_count,
                "invalid_address_ratio": invalid_address_ratio,
                "reserved_address_ratio": reserved_address_ratio,
                "reserved_row_ratio": reserved_row_ratio,
                "invalid_ratio_fail_threshold": invalid_ratio_fail_threshold,
                "ip_version_counts": ip_version_counts,
                "reserved_category_counts": reserved_category_counts,
                "field_summaries": [field_summaries[f] for f in checked_fields],
                "reserved_examples": reserved_examples,
                "invalid_examples": invalid_examples,
                "status": status,
                "diagnostic": diagnostic,
            }
        }
    }
