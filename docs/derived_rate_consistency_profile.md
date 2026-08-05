# Derived rate consistency profile

`derived_rate_consistency_profile` is a post-expert-review Dataset Heuristics metric under Protocol and Network Realism → Flow Semantics.

It recomputes reported flow packet and byte rates from:

- flow duration;
- forward and backward packet counts;
- forward and backward byte totals.

The metric does **not** assume a duration unit. A plan must explicitly declare one of:

- `seconds`
- `milliseconds`
- `microseconds`
- `nanoseconds`

At least one reported rate field must be mapped.

## Example plan metric

```json
{
  "metric_id": "derived_rate_consistency_profile",
  "enabled": true,
  "taxonomy_path": [
    "Dataset Heuristics",
    "Protocol and Network Realism",
    "Flow Semantics",
    "Derived Rate Consistency"
  ],
  "input_requirements": {
    "field_map": {
      "flow_duration": "Flow Duration",
      "total_fwd_packets": "Total Fwd Packets",
      "total_bwd_packets": "Total Bwd Packets",
      "total_len_fwd_packets": "Total Length of Fwd Packets",
      "total_len_bwd_packets": "Total Length of Bwd Packets",
      "flow_packets_per_second": "Flow Packets/s",
      "flow_bytes_per_second": "Flow Bytes/s"
    },
    "field_requirements": {
      "required": [
        "flow_duration",
        "total_fwd_packets",
        "total_bwd_packets",
        "total_len_fwd_packets",
        "total_len_bwd_packets"
      ],
      "optional": [
        "flow_packets_per_second",
        "flow_bytes_per_second"
      ]
    }
  },
  "calculation": {
    "parameters": {
      "duration_unit": "microseconds",
      "relative_tolerance": 0.02,
      "absolute_tolerance": 0.000001,
      "pass_threshold": 0.99,
      "warn_threshold": 0.95,
      "max_examples": 10
    }
  }
}
```

## Interpretation

The metric reports:

- checked and inconsistent row counts;
- negative-value violations;
- zero-duration/non-zero-volume violations;
- packet-rate mismatches;
- byte-rate mismatches;
- a consistency ratio and status;
- bounded examples of inconsistent rows.

A failure shows that supplied derived rates are inconsistent with other supplied flow fields under the declared unit and tolerances. It does not prove that the underlying traffic is unrealistic: the mismatch may originate from exporter definitions, sampling, active/idle timeout behaviour, rounding, or incorrect metadata. Export configuration and rate semantics should be recorded before the result is interpreted.

## Evidence basis

The test is supported as an internal flow-export consistency check by IPFIX flow semantics and flow-monitoring literature:

- RFC 7011, *Specification of the IP Flow Information Export (IPFIX) Protocol for the Exchange of Flow Information*.
- Hofstede et al., *Flow Monitoring Explained: From Packet Capture to Data Analysis With NetFlow and IPFIX*, IEEE Communications Surveys & Tutorials, 2014, DOI 10.1109/COMST.2014.2321898.

These sources support checking relationships among exported flow quantities. They do not supply universal tolerances or pass thresholds.
