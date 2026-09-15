# Fixed PCAP plan matrices

When every experiment dataset uses the same raw packet-capture representation, the scientific test plan does not need to be regenerated for every candidate/reference pair.

CBR-Tests supports a **fixed PCAP plan** workflow:

1. create and validate one PCAP/PCAPNG plan;
2. keep that plan as the scientific definition of the experiment;
3. map candidate captures and optional reference captures onto it;
4. save the mapping as a normal batch manifest;
5. queue those batch manifests in a comparison campaign when several matrices need to run one after another.

## Toolbox

Launch:

```bash
python run_plan.py --tui
```

Under **Prepare experiments**, choose **Map PCAP datasets to existing plan**.

Select:

- the existing PCAP plan;
- one or more candidate `.pcap`/`.pcapng` files;
- optional reference `.pcap`/`.pcapng` files;
- a matrix name and output manifest.

The mapper does not rerun dataset-aware plan generation for each candidate.

## Reference comparisons

Reference metrics store a reference dataset path in their input requirements. If several reference captures are selected, CBR-Tests writes one lightweight binding plan per reference capture. Each binding plan:

- preserves the source plan ID, metric set, thresholds and calculations;
- changes only already-configured reference dataset paths;
- records the source plan path and SHA-256;
- is marked `binding_only` in `plan_creation.fixed_plan_matrix_binding`.

Multiple candidate datasets that use the same reference therefore share the same binding plan. CBR-Tests does **not** generate a separate scientific plan for every candidate/reference job.

If no new reference is selected and the source plan already contains reference metrics, its configured reference is reused and recorded in the batch manifest.

If references are selected but the source plan has no reference-enabled metrics, the mapper refuses the matrix. Build the PCAP plan once with an independent reference first so the reference metric definitions are present.

## CLI equivalent

```bash
python create_fixed_plan_matrix.py \
  --name "Bucket comparison" \
  --plan plans/final_pcap_plan.json \
  --dataset datasets/Bucket_1.pcapng \
  --dataset datasets/Bucket_2.pcapng \
  --reference datasets/Real_1.pcapng \
  --reference datasets/Real_2.pcapng \
  --output plans/bucket_comparison_batch.json
```

Then run the normal batch runner:

```bash
python run_batch.py --batch plans/bucket_comparison_batch.json --experiment-mode
```

The resulting batch can also be added to a comparison campaign.

## Scientific boundary

A fixed plan is appropriate only when the same scientific assumptions genuinely apply to every mapped capture. PCAP versus PCAPNG container differences do not by themselves change the packet-level constructs, but scenario-specific assumptions in a plan (for example an explicitly asserted single-service port population) still need to be true for every dataset to which that plan is mapped.
