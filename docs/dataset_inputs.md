# Dataset input formats

## Supported tabular inputs

The tabular loader supports:

- `.csv`,
- `.tsv`,
- `.xlsx`,
- `.xls`.

Tabular headers are read for field translation detection and sidecar template creation. Dataset columns are preserved; the runner resolves metric inputs rather than renaming the loaded dataframe.

## Packet inputs

Raw packet captures such as `.pcap` and `.pcapng` are intended for packet-aware metrics. Raw packet files are not converted into sidecar templates by the tabular header reader.

## PCAP/tshark CSV exports

CSV/TSV exports with common Wireshark/tshark headings can be auto-detected. Examples include:

- `frame.time_epoch`,
- `ip.src`,
- `ip.dst`,
- `tcp.srcport`,
- `tcp.dstport`,
- `udp.srcport`,
- `udp.dstport`,
- `frame.len`,
- `tcp.flags`.

## Network-flow CSVs

Common aliases such as `Src IP`, `Dst IP`, `source_ip`, `dst_ip`, `Timestamp`, and `Label` can be auto-detected when present.

## Dataset immutability

The runner should not edit supplied datasets. Sidecar templates are separate JSON files used to resolve fields at runtime.
