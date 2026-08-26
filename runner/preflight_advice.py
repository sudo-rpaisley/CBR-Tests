from __future__ import annotations

from collections import defaultdict
from typing import Any


def _entry(
    action_key: str,
    title: str,
    advice: str,
    *,
    example: str | None = None,
    actionable: bool = True,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "action_key": action_key,
        "title": title,
        "advice": advice,
        "actionable": actionable,
    }
    if example:
        result["example"] = example
    return result


def advice_for_exclusion(
    reason: str | None,
    *,
    status: str,
    dataset_format: str | None,
    missing_fields: list[str] | None = None,
) -> dict[str, Any]:
    """Return a concrete, research-safe remedy for an excluded metric."""

    reason = reason or status
    is_pcap = dataset_format in {"pcap", "pcapng"}
    missing_fields = sorted(set(missing_fields or []))

    if reason == "reference_dataset_required":
        if is_pcap:
            return _entry(
                "independent_reference_pcap",
                "Add an independent reference PCAP",
                "Supply a genuinely independent PCAP/PCAPNG representing the real/reference population. "
                "The candidate capture must not be reused as its own reference.",
                example="python create_plan.py --name \"<plan name>\" --dataset <candidate.pcap> --reference-dataset <reference.pcap>",
            )
        return _entry(
            "explicit_tabular_reference_configuration",
            "Configure an independent tabular reference dataset",
            "Provide a separately collected reference dataset with equivalent feature semantics and configure its path in the reference metric. "
            "The automatic plan builder currently unlocks reference metrics directly only for PCAP/PCAPNG inputs.",
        )

    if reason == "pcap_reference_representation_mismatch":
        return _entry(
            "independent_reference_pcap",
            "Use a PCAP/PCAPNG reference for a raw-PCAP candidate",
            "Choose an independent PCAP or PCAPNG reference so candidate and reference are decoded through the same canonical packet representation.",
            example="python create_plan.py --name \"<plan name>\" --dataset <candidate.pcap> --reference-dataset <reference.pcap>",
        )

    if reason in {"service_definition_required", "pcap_service_population_evidence_required"}:
        if is_pcap:
            return _entry(
                "single_service_evidence",
                "Provide independent single-service evidence and expected ports",
                "Only enable this when the entire capture is independently known to represent one application service. "
                "Then provide the service name and expected port set; do not infer the service from the same ports being tested.",
                example="python create_plan.py --name \"<plan name>\" --dataset <capture.pcap> --single-service <service> --expected-service-ports <port[,port...]>",
            )
        return _entry(
            "service_population_configuration",
            "Define the service population and expected ports",
            "Configure which rows represent the service, the independently established service name, and its expected ports. "
            "Do not infer the expected service from the observed port values being validated.",
        )

    if reason == "allowed_slice_ids_required":
        return _entry(
            "allowed_slice_ids",
            "Provide the allowed slice-ID vocabulary",
            "Supply the experiment's valid slice identifiers from the network/slicing configuration or other authoritative source. "
            "The framework must not invent an allowed slice set from the candidate dataset itself.",
        )

    if reason == "slice_consistency_rules_required":
        return _entry(
            "slice_consistency_rules",
            "Define slice-consistency rules",
            "Provide explicit rules describing which slice identifiers are valid for the relevant traffic, services, labels or other fields in this experiment.",
        )

    if reason == "attack_window_configuration_required":
        return _entry(
            "attack_windows",
            "Provide authoritative attack windows",
            "Supply the attack start/end windows used by the experiment, together with usable timestamp and label fields. "
            "Attack windows should come from experiment ground truth rather than being inferred from the labels being tested.",
        )

    if reason == "split_configuration_required":
        return _entry(
            "train_test_split",
            "Provide train/test split definitions",
            "Identify the training and test populations (or separate datasets) and the stable fields/identifiers used to detect duplicate or identifier leakage across the split.",
        )

    if reason == "benchmark_model_configuration_required":
        return _entry(
            "benchmark_predictions",
            "Provide benchmark truth/prediction configuration",
            "Supply the ground-truth label field, benchmark prediction field and any required class/positive-label semantics. "
            "These metrics score an explicitly configured benchmark result; they must not guess the target class.",
        )

    if reason == "slice_metadata_required":
        return _entry(
            "slice_metadata",
            "Provide slice metadata",
            "Use a dataset or sidecar/export that contains trustworthy per-record slice identifiers and, where required, class labels. "
            "The current raw-PCAP adapter cannot infer 5G slice identity from ordinary IP/TCP/UDP packet evidence.",
        )

    if reason == "flow_segmentation_policy_required":
        return _entry(
            "flow_segmentation_policy",
            "Define independent flow segmentation/exporter semantics",
            "Provide an externally defined flow representation (for example an independent flow export) or explicitly define direction, timeout and segmentation semantics. "
            "CBR-Tests will not silently treat its own reconstructed flows as equivalent to a particular exporter.",
        )

    if reason == "self_derived_pcap_invariant_not_independent":
        return _entry(
            "independent_flow_export",
            "Use independently exported flow fields",
            "Run this metric on an external flow table containing the measured/exported fields it is intended to cross-check. "
            "Deriving both sides from the same PCAP inside CBR-Tests would only test the adapter against itself.",
            actionable=False,
        )

    if reason == "required_fields_not_resolved":
        field_text = ", ".join(missing_fields) if missing_fields else "the required canonical fields"
        return _entry(
            "field_mapping",
            "Map or provide the required fields",
            f"Resolve {field_text}. Map existing equivalent columns with a field-translation sidecar/--field-translation, or use a dataset that genuinely contains the missing information. Do not fabricate absent fields.",
            example="python create_plan.py --name \"<plan name>\" --dataset <dataset.csv> --field-translation <mapping.json>",
        )

    if reason in {"pcap_adapter_fields_missing", "reference_pcap_adapter_fields_missing"}:
        field_text = ", ".join(missing_fields) if missing_fields else "required packet fields"
        return _entry(
            "pcap_adapter_fields",
            "Expose the required fields from the PCAP adapter",
            f"The canonical PCAP representation is missing {field_text}. Confirm that the capture actually contains decodable evidence for those fields, then extend/fix the adapter if appropriate; ordinary CSV field mapping cannot create missing packet evidence.",
            actionable=False,
        )

    if reason in {"pcap_adapter_template_missing", "pcap_reference_template_missing"}:
        return _entry(
            "pcap_metric_template",
            "Add a scientifically defined PCAP metric template",
            "The runtime metric exists, but no safe raw-PCAP configuration template is defined yet. Add explicit canonical fields, parameters and interpretation before enabling it automatically.",
            actionable=False,
        )

    if reason == "pcap_adapter_not_available":
        return _entry(
            "pcap_adapter_support",
            "Add a valid PCAP representation for this metric",
            "This metric has no scientifically justified raw-PCAP adapter yet. Use a compatible tabular/exported representation containing its required evidence, or implement and validate a packet/flow adapter for the metric.",
            actionable=False,
        )

    if reason == "packet_capture_metric_on_tabular_dataset":
        return _entry(
            "raw_packet_capture",
            "Use a raw PCAP/PCAPNG input",
            "This metric needs packet-level evidence that is not available from the selected tabular dataset. Run it against the corresponding raw packet capture if one exists.",
        )

    if reason == "no_metric_template_available":
        return _entry(
            "metric_template",
            "Define the metric's required configuration",
            "The runtime knows this metric, but plan creation has no reusable template defining its fields and parameters. Add a reviewed metric configuration/template before automatic inclusion.",
            actionable=False,
        )

    if reason == "missing_master_taxonomy_entry":
        return _entry(
            "taxonomy_registration",
            "Register the metric in the master taxonomy",
            "Add the metric to taxonomy/master_taxonomy.json at the scientifically appropriate location before automatic plan creation treats it as configured.",
            actionable=False,
        )

    return _entry(
        f"review_{reason}",
        "Review the exclusion reason and configure the metric explicitly",
        f"No dedicated automatic remedy is registered for reason code '{reason}'. Review the metric's required evidence/configuration before enabling it; do not bypass preflight merely to increase the test count.",
        actionable=False,
    )


def build_unlock_actions(metric_statuses: dict[str, dict], *, dataset_format: str | None) -> list[dict[str, Any]]:
    """Group excluded metrics by the user/research action that could unlock them."""

    groups: dict[str, dict[str, Any]] = {}
    missing_by_action: dict[str, set[str]] = defaultdict(set)

    for metric_id, details in metric_statuses.items():
        if details.get("included"):
            continue
        advice = details.get("advice")
        if not isinstance(advice, dict):
            advice = advice_for_exclusion(
                details.get("reason"),
                status=str(details.get("status", "excluded")),
                dataset_format=dataset_format,
                missing_fields=details.get("missing_fields", []),
            )
        action_key = str(advice["action_key"])
        group = groups.setdefault(
            action_key,
            {
                **advice,
                "metric_ids": [],
                "metric_count": 0,
            },
        )
        group["metric_ids"].append(metric_id)
        group["metric_count"] += 1
        missing_by_action[action_key].update(details.get("missing_fields", []))

    output: list[dict[str, Any]] = []
    for action_key, group in groups.items():
        group["metric_ids"] = sorted(group["metric_ids"])
        if missing_by_action[action_key]:
            group["missing_fields"] = sorted(missing_by_action[action_key])
        output.append(group)

    return sorted(output, key=lambda item: (-int(item["metric_count"]), str(item["title"])))
