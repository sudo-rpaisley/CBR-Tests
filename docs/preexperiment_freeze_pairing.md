# Pre-experiment code/paper pairing

This file records the paired revisions used to close the paper-to-code conformance audit before final experiment-plan generation.

## Scientific contract revisions

- **CBR-Tests implementation contract:** `7000ba6d385109a1f763bbd5eb935cc95ef8e964`
- **CBR-Tests audit/status branch:** `audit/paper-metric-traceability`
- **Companion paper contract:** `06e0a5c455c13b59d92d5f630c9573a97df456c1`
- **Companion paper PR:** `sudo-rpaisley/paper-draft#5`

Commits after the implementation-contract SHA on the CBR-Tests audit branch are audit/status documentation only and do not change metric semantics.

## Verified at pairing

- The CBR-Tests full pytest suite passed after the F1–F8 contract fixes and again after PCAP evidence-class propagation.
- Generated CBR-Tests function/test reference documentation was regenerated and verified current.
- Automatic PCAP plans exclude decoder-derived IP validity, decoder-derived port validity, adapter-converted timestamp parseability and generic heterogeneous packet-view missingness from independent evidence.
- Runnable PCAP plan metrics carry an explicit evidence class so independent observations, contextual diagnostics, derived diagnostics, reference-dependent evidence and context-required evidence remain distinguishable.
- The paper taxonomy validator passed all **61 current metric leaves** against the audited CBR-Tests taxonomy.
- The paper metric catalogue was regenerated from its canonical leaf sources.
- The reconciled paper successfully completed `pdflatex -> bibtex -> pdflatex -> pdflatex` compilation.

## Still required before the first final experiment

This pairing is a conformance checkpoint, not the final immutable experiment tag. The remaining freeze work is to verify oracle/boundary coverage for every canonical leaf, freeze reference identity/hash and threshold/tolerance provenance in the final experiment manifests, confirm per-representation applicability, generate the final canonical plans, and tag the exact code and paper revisions used for the first final run.
