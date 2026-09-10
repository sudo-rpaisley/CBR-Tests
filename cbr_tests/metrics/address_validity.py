from __future__ import annotations

from ipaddress import ip_address, ip_network

import pandas as pd


DEFAULT_IP_FIELD_CANDIDATES = [
    "Source IP",
    "Destination IP",
    "Src IP",
    "Dst IP",
    "src_ip",
    "dst_ip",
    "source_ip",
    "destination_ip",
]

IPV4_DOCUMENTATION = [
    ip_network("192.0.2.0/24"),
    ip_network("198.51.100.0/24"),
    ip_network("203.0.113.0/24"),
]
IPV6_DOCUMENTATION = [ip_network("2001:db8::/32"), ip_network("3fff::/20")]
SHARED_ADDRESS_SPACE = ip_network("100.64.0.0/10")
BENCHMARKING = ip_network("198.18.0.0/15")
IPV4_MAPPED = ip_network("::ffff:0:0/96")
UNIQUE_LOCAL = ip_network("fc00::/7")

SUPPORTED_MISUSE_CATEGORIES = {
    "private",
    "loopback",
    "link_local",
    "multicast",
    "documentation",
    "reserved",
    "unspecified",
    "shared_address_space",
    "benchmarking",
    "ipv4_mapped",
    "unique_local",
}


def classify_ip_value(value) -> tuple[str, object | None]:
    """Return (classification, parsed-address) for one candidate IP value.

    Missing values are deliberately distinct from invalid values because the
    Valid IP Address Ratio is defined only over non-missing values that were
    actually checked.
    """

    if value is None or pd.isna(value):
        return "missing", None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return "missing", None
    try:
        address = ip_address(text)
    except ValueError:
        return "invalid", None
    return ("ipv4" if address.version == 4 else "ipv6"), address


def _candidate_fields(df: pd.DataFrame, metric: dict) -> tuple[list[str], list[str]]:
    requested = metric.get("input_requirements", {}).get(
        "candidate_fields", DEFAULT_IP_FIELD_CANDIDATES
    )
    existing = [field for field in requested if field in df.columns]
    missing = [field for field in requested if field not in df.columns]
    if not existing:
        raise ValueError("No configured candidate IP fields exist in the dataset.")
    return existing, missing


def compute_valid_ip_address_ratio(df: pd.DataFrame, metric: dict) -> dict:
    """Compute N_valid / N_checked over non-missing candidate IP values."""

    fields, missing_fields = _candidate_fields(df, metric)
    checked = valid = invalid = missing = 0
    ipv4 = ipv6 = 0
    invalid_examples: list[dict] = []
    max_examples = int(metric.get("calculation", {}).get("parameters", {}).get("max_examples", 10))

    field_results = []
    for field in fields:
        f_checked = f_valid = f_invalid = f_missing = 0
        for index, value in df[field].items():
            classification, _ = classify_ip_value(value)
            if classification == "missing":
                missing += 1
                f_missing += 1
                continue

            checked += 1
            f_checked += 1
            if classification in {"ipv4", "ipv6"}:
                valid += 1
                f_valid += 1
                if classification == "ipv4":
                    ipv4 += 1
                else:
                    ipv6 += 1
            else:
                invalid += 1
                f_invalid += 1
                if len(invalid_examples) < max_examples:
                    invalid_examples.append(
                        {
                            "row_index": int(index) if isinstance(index, int) else str(index),
                            "field": field,
                            "value": str(value).strip(),
                        }
                    )

        field_results.append(
            {
                "field": field,
                "checked_address_count": f_checked,
                "valid_address_count": f_valid,
                "invalid_address_count": f_invalid,
                "missing_address_count": f_missing,
                "valid_ip_address_ratio": round(f_valid / f_checked, 6) if f_checked else None,
            }
        )

    ratio = round(valid / checked, 6) if checked else None
    return {
        "summary": {
            "checked_fields": fields,
            "missing_fields": missing_fields,
            "checked_address_count": checked,
            "valid_address_count": valid,
            "invalid_address_count": invalid,
            "missing_address_count": missing,
            "ipv4_count": ipv4,
            "ipv6_count": ipv6,
            "valid_ip_address_ratio": ratio,
            "runnable": checked > 0,
            "denominator_policy": "non_missing_candidate_ip_values",
        },
        "fields": field_results,
        "invalid_examples": invalid_examples,
    }


def special_use_categories(address) -> list[str]:
    categories: list[str] = []
    if address.is_private:
        categories.append("private")
    if address.is_loopback:
        categories.append("loopback")
    if address.is_link_local:
        categories.append("link_local")
    if address.is_multicast:
        categories.append("multicast")
    if address.is_reserved:
        categories.append("reserved")
    if address.is_unspecified:
        categories.append("unspecified")
    if any(address in network for network in (IPV4_DOCUMENTATION if address.version == 4 else IPV6_DOCUMENTATION)):
        categories.append("documentation")
    if address.version == 4 and address in SHARED_ADDRESS_SPACE:
        categories.append("shared_address_space")
    if address.version == 4 and address in BENCHMARKING:
        categories.append("benchmarking")
    if address.version == 6 and address in IPV4_MAPPED:
        categories.append("ipv4_mapped")
    if address.version == 6 and address in UNIQUE_LOCAL:
        categories.append("unique_local")
    return sorted(set(categories))


def _misuse_categories(metric: dict) -> set[str]:
    params = metric.get("calculation", {}).get("parameters", {})
    configured = {str(value).strip() for value in params.get("misuse_categories", []) if str(value).strip()}

    # Compatibility with the old profile's per-category booleans while plans
    # migrate to the clearer misuse_categories field.
    for category in SUPPORTED_MISUSE_CATEGORIES:
        if bool(params.get(f"count_{category}_as_reserved", False)):
            configured.add(category)

    unknown = configured - SUPPORTED_MISUSE_CATEGORIES
    if unknown:
        raise ValueError("Unknown reserved-address misuse categories: " + ", ".join(sorted(unknown)))
    return configured


def compute_reserved_address_misuse_ratio(df: pd.DataFrame, metric: dict) -> dict:
    """Compute policy-inconsistent special-use addresses / valid checked addresses.

    Invalid and missing address values are intentionally excluded from this
    denominator: they are measured by Valid IP Address Ratio and completeness
    metrics respectively. A special-use address is only a misuse when its
    category is explicitly disallowed by the scenario policy.
    """

    fields, missing_fields = _candidate_fields(df, metric)
    misuse_categories = _misuse_categories(metric)
    valid = invalid = missing = misuse = 0
    category_counts = {category: 0 for category in sorted(SUPPORTED_MISUSE_CATEGORIES)}
    misuse_examples: list[dict] = []
    max_examples = int(metric.get("calculation", {}).get("parameters", {}).get("max_examples", 10))

    for field in fields:
        for index, value in df[field].items():
            classification, address = classify_ip_value(value)
            if classification == "missing":
                missing += 1
                continue
            if classification == "invalid":
                invalid += 1
                continue

            valid += 1
            observed = special_use_categories(address)
            for category in observed:
                category_counts[category] += 1
            violated = sorted(set(observed) & misuse_categories)
            if violated:
                misuse += 1
                if len(misuse_examples) < max_examples:
                    misuse_examples.append(
                        {
                            "row_index": int(index) if isinstance(index, int) else str(index),
                            "field": field,
                            "value": str(value).strip(),
                            "misuse_categories": violated,
                        }
                    )

    ratio = round(misuse / valid, 6) if valid and misuse_categories else None
    return {
        "summary": {
            "checked_fields": fields,
            "missing_fields": missing_fields,
            "valid_address_count": valid,
            "invalid_address_count": invalid,
            "missing_address_count": missing,
            "misuse_address_count": misuse,
            "reserved_address_misuse_ratio": ratio,
            "misuse_categories": sorted(misuse_categories),
            "policy_configured": bool(misuse_categories),
            "runnable": valid > 0 and bool(misuse_categories),
            "denominator_policy": "valid_candidate_ip_values_only",
        },
        "special_use_category_counts": category_counts,
        "misuse_examples": misuse_examples,
    }
