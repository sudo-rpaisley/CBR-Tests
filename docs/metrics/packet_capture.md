# Raw packet capture checks metrics

This page documents every dispatcher metric in the **Raw packet capture checks** category. Return to the [complete metric index](../metric_reference.md).

## `protocol_validity_profile`

Scans raw IPv4/IPv6 packets for valid addresses and basic protocol/port/length/flag plausibility.

- **Implementation:** `tests/metrics/.../valid_ip_address_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** `dad_plan`
- **Inputs:** Raw PCAP/PCAPNG.
- **Primary output:** Packet validity counts, issue counts, field counts, examples, ratio, status.
- **Interpretation:** Pass ≥0.99, warn ≥0.95, else fail.
- **Current caveat:** A broad packet heuristic; it is not full protocol conformance validation.

## `timestamp_coherence_profile`

Scans raw PCAP packet timestamps for backward jumps, zero deltas, and large gaps.

- **Implementation:** `cbr_tests/metrics/timestamp_coherence.py`
- **Supplied-plan usage:** `dad_plan`
- **Inputs:** Raw `.pcap`/`.pcapng`; optional `large_gap_threshold_seconds` default 1.0.
- **Primary output:** Packet/capture/gap counts, mean/max gap, domain status.
- **Interpretation:** Backward jumps yield warn; otherwise pass. Large gaps and zero deltas are reported but do not change status by themselves.
- **Current caveat:** Loads packets sequentially through Scapy; empty/unreadable captures are execution failures.

## Canonical PCAP adapter

PCAP/PCAPNG files are also decoded into a shared packet view containing capture-order index, timestamp, IP addresses, transport ports where present, protocol, IP version, packet length and TCP flags. This allows existing packet-evidence metrics such as `reserved_ip_address_profile` and `valid_port_range_profile` to run without converting the capture to CSV first.

The adapter also exposes a bidirectional 5-tuple flow view for future sequence/reference work. Current automatic plans intentionally exclude self-consistency tests whose compared values would both be derived by this adapter, because those passes would not provide independent evidence about dataset realism.
