# Raw packet capture checks metrics

This page documents every dispatcher metric in the **Raw packet capture checks** category. Return to the [complete metric index](../metric_reference.md).

## `protocol_validity_profile`

Scans decoded IPv4/IPv6 packets for deterministic structural validity and separately reports suspicious-but-potentially-legitimate TCP behaviour.

- **Implementation:** `tests/metrics/.../valid_ip_address_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** `dad_plan`
- **Inputs:** Raw PCAP/PCAPNG.
- **Structural checks:** IP address parseability and family agreement, decoded TCP/UDP port bounds, direct TCP/UDP protocol-to-layer consistency where that can be established safely, positive/credible network-layer length, and decoded IPv4/IPv6 structure.
- **Descriptive checks:** unusual TCP flag combinations such as SYN+FIN and SYN+RST. These are reported but do not reduce the validity ratio by default because attack/security captures can legitimately contain crafted packets.
- **Non-IP frames:** reported separately and excluded from the IPv4/IPv6 validity denominator rather than being silently counted as valid IP packets.
- **IPv4 fragments:** transport-layer consistency is not asserted for non-initial fragments because the transport header may legitimately be absent.
- **Primary output:** checked/valid/structurally-invalid packet counts, address counts, invalid port count, protocol mismatch count, unusual TCP flag evidence, issue examples, validity ratio, and status.
- **Interpretation:** default pass ≥0.99, warn ≥0.95, else fail over checked IPv4/IPv6 packets. Thresholds may be explicitly configured. Suspicious TCP flags affect status only when `suspicious_tcp_flags_affect_status` is explicitly enabled.
- **Current caveat:** This is basic structural plausibility, not full RFC/protocol conformance validation. Successful Scapy decoding itself constrains which malformed fields can be observed.

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
