# Network and flow realism metrics

This page documents every dispatcher metric in the **Network and flow realism** category. Return to the [complete metric index](../metric_reference.md).

## `derived_rate_consistency_profile`

Recomputes packet/s and byte/s from packet counts, byte totals, and flow duration, then compares them with reported rates.

- **Implementation:** Nested modules under `tests/metrics/dataset_heuristics/...` (production implementation awaiting migration)
- **Supplied-plan usage:** none yet; available to plans.
- **Inputs:** explicit `field_map` for duration, forward/backward packet and byte totals, plus at least one reported packet/s or byte/s field.
- **Primary output:** Checked/inconsistent counts, negative/zero-duration violations, rate mismatch counts, bounded examples, consistency ratio and status.
- **Interpretation:** Thresholds are plan-configurable; the duration unit must be declared explicitly.
- **Current caveat:** Post-expert-review addition pending second expert review. Exporter semantics, sampling, timeouts and rounding can explain mismatches. See [Derived rate consistency profile](../derived_rate_consistency_profile.md).

## `flow_duration_consistency_profile`

Checks flow duration against start/end timestamps and configured tolerance.

- **Implementation:** Nested modules under `tests/metrics/dataset_heuristics/...` (production implementations awaiting migration)
- **Supplied-plan usage:** `deepsecure_plan`
- **Inputs:** `field_map` for start, end, duration; calculation tolerance/unit options.
- **Primary output:** Consistent/inconsistent counts, ratio, examples, status.
- **Interpretation:** Pass ≥0.99, warn ≥0.95, else fail.
- **Current caveat:** Timestamp and duration unit configuration must be correct.

## `handshake_plausibility_profile`

Classifies aggregate TCP flows as plausible, suspicious, or uncertain from SYN/ACK/RST/FIN and packet counts.

- **Implementation:** Nested modules under `tests/metrics/dataset_heuristics/...` (production implementations awaiting migration)
- **Supplied-plan usage:** `deepsecure_plan`
- **Inputs:** `field_map`; TCP values; `allow_syn_only`, `allow_rst_flows`, `max_examples`.
- **Primary output:** TCP counts, reason counts, plausibility ratio, examples, status.
- **Interpretation:** Pass ≥0.95, warn ≥0.80, fail below; no TCP rows is `not_applicable`.
- **Current caveat:** Aggregate counts cannot prove an actual ordered three-way handshake.

## `packet_byte_consistency_profile`

Checks packet and byte totals for nonnegative values and plausible byte-per-packet relationships.

- **Implementation:** Nested modules under `tests/metrics/dataset_heuristics/...` (production implementations awaiting migration)
- **Supplied-plan usage:** `deepsecure_plan`
- **Inputs:** `field_map` for forward/backward packet and byte totals plus calculation bounds.
- **Primary output:** Plausible/inconsistent counts, ratio, reasons/examples, status.
- **Interpretation:** Pass ≥0.99, warn ≥0.95, else fail.
- **Current caveat:** Bounds are heuristic and should reflect capture truncation, headers, and dataset aggregation.

## `reserved_ip_address_profile`

Classifies candidate IP fields and reports invalid and special-use/reserved address categories.

- **Implementation:** Nested modules under `tests/metrics/dataset_heuristics/...` (production implementations awaiting migration)
- **Supplied-plan usage:** `deepsecure_plan`, `secure5g_plan`
- **Inputs:** `candidate_fields`, default common source/destination IP names.
- **Primary output:** Counts, per-field summaries, category counts, examples, ratios, domain status.
- **Interpretation:** Pass if no invalid/reserved addresses; warn for any reserved/invalid up to 1% invalid; fail above 1% invalid.
- **Current caveat:** Private/reserved traffic can be legitimate depending on dataset scope, so domain status requires context.

## `service_port_consistency_profile`

Checks whether observed ports match configured expected ports for a service/protocol.

- **Implementation:** Nested modules under `tests/metrics/dataset_heuristics/...` (production implementations awaiting migration)
- **Supplied-plan usage:** `deepsecure_plan`, `secure5g_plan`
- **Inputs:** `port_fields`; calculation expected ports, match mode, thresholds.
- **Primary output:** Eligible/matching row counts, ratio, invalid examples, domain status.
- **Interpretation:** Default pass ≥0.95, warn ≥0.75, else fail; any invalid port downgrades pass to warn.
- **Current caveat:** Expected-port configuration is domain-specific and modern applications may use dynamic/nonstandard ports.

## `slice_identifier_consistency_profile`

Checks slice IDs against conditional contextual rules.

- **Implementation:** Nested modules under `tests/metrics/dataset_heuristics/...` (production implementations awaiting migration)
- **Supplied-plan usage:** `deepsecure_plan`, `secure5g_plan`
- **Inputs:** `slice_field`, `consistency_rules`, optional context fields/case sensitivity.
- **Primary output:** Rule application counts, consistent/inconsistent rows, examples, ratio, status.
- **Interpretation:** Pass ≥0.99, warn ≥0.95, fail below; no matching rules is `not_applicable`.
- **Current caveat:** Rule order/coverage and context normalization determine what can be assessed.

## `tcp_flag_consistency_profile`

Checks aggregate TCP flag counts against packet totals and protocol context.

- **Implementation:** Nested modules under `tests/metrics/dataset_heuristics/...` (production implementations awaiting migration)
- **Supplied-plan usage:** `deepsecure_plan`
- **Inputs:** `field_map` for protocol, packet totals, and available flag counts.
- **Primary output:** Checked/plausible counts, reasons/examples, consistency ratio, status.
- **Interpretation:** Default pass ≥0.99, warn ≥0.95, else fail.
- **Current caveat:** Works on aggregate flow features, not packet sequence order; plausibility rules are heuristic.

## `valid_port_range_profile`

Parses candidate port fields and checks integer range 0–65535.

- **Implementation:** Nested modules under `tests/metrics/dataset_heuristics/...` (production implementations awaiting migration)
- **Supplied-plan usage:** `deepsecure_plan`, `secure5g_plan`
- **Inputs:** `candidate_fields`.
- **Primary output:** Valid/invalid/missing/zero counts, range buckets, examples, ratios, domain status.
- **Interpretation:** Fail above 1% invalid; warn for any invalid or port zero; otherwise pass.
- **Current caveat:** Port zero can be meaningful in some captures; missing values are not counted as checked ports.

## `valid_slice_identifier_profile`

Checks normalized slice IDs against an allowed set and aliases.

- **Implementation:** Nested modules under `tests/metrics/dataset_heuristics/...` (production implementations awaiting migration)
- **Supplied-plan usage:** `deepsecure_plan`, `secure5g_plan`
- **Inputs:** `slice_field`; allowed IDs, aliases, case sensitivity, numeric normalization.
- **Primary output:** Valid/invalid/missing counts, examples, ratio, status.
- **Interpretation:** Pass ≥0.99, warn ≥0.95, else fail.
- **Current caveat:** The allowed set is authoritative only if plan configuration is correct.
