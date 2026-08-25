# Creating plans

`create_plan.py` builds test plans from the metric handlers that the runner can actually execute. It does not maintain a separate hard-coded test list, so newly registered metrics are automatically considered during plan creation. This includes post-review additions registered by the dispatcher; their taxonomy/review metadata remains authoritative and does not make an unconfigured metric runnable.

## Core rule

A generated plan contains **only tests that preflight as runnable for the supplied dataset**.

The builder still considers every available metric by default, but metrics that cannot currently run are recorded only in the creation report and are not written into `metrics` in the plan.

This means every metric stored in a generated plan has:

```json
{
  "enabled": true,
  "configuration": {
    "status": "ready"
  }
}
```

Automatic plan creation therefore requires a dataset. Without one, the builder cannot determine which tests are runnable and refuses to create a plan.

"Runnable" here means structurally runnable at plan-creation time: the input format is compatible, the required canonical fields can be resolved, the metric has a usable template, and no required dataset-specific research configuration is missing. A metric may still produce a failing realism result once executed; that is the purpose of the test and is different from being unable to run.

## Preflight states

Every candidate metric is classified as one of:

- `ready`: the dataset, mappings, and available configuration provide enough information to run the metric;
- `needs_mapping`: one or more required canonical fields cannot be resolved from the dataset;
- `needs_configuration`: the metric needs information that must not be guessed, such as a reference dataset, service definition, allowed slice IDs, attack windows, train/test split details, or benchmark-model configuration;
- `not_applicable`: the metric is incompatible with the supplied input type, such as a packet-capture-only metric on a CSV file.

Only `ready` metrics are written into the plan. The other states are shown in the preflight report so users can see why tests were excluded.

Generated plans default to:

```json
{
  "fail_fast": false,
  "allow_skips": false,
  "sample_mode": "full"
}
```

`allow_skips` is false deliberately: if a metric that passed plan preflight later becomes unrunnable, the execution should surface that mismatch instead of silently accepting it.

## Interactive creation

Running:

```bash
python create_plan.py
```

starts a small wizard that asks for a plan ID, name, dataset, and output path. The dataset is required because it is the basis for deciding which tests belong in the plan.

## Dataset-aware creation

```bash
python create_plan.py \
  --plan-id deepsecure-dns \
  --name "DeepSecure DNS" \
  --dataset datasets/DeepSecure/CICDDoS2019/01-12/DrDoS_DNS.csv \
  --output plans/deepsecure_dns_plan.json
```

For supported tabular datasets the builder reads the header, applies existing field-alias detection, chooses the closest available metric template, and includes only metrics whose required fields and configuration can be resolved. It never modifies the dataset.

If the dataset already has a standard `<dataset>.field_translation.json` sidecar, plan creation loads it automatically before deciding which metrics are runnable. An explicit mapping file can also be supplied:

```bash
python create_plan.py \
  --plan-id example \
  --name "Example" \
  --dataset datasets/example.csv \
  --field-translation field_translations/example.json \
  --output plans/example_plan.json
```

## Selection controls

Every available test is considered by default. Narrow the candidate set with:

```bash
python create_plan.py --include valid_port_range_profile,reserved_ip_address_profile ...
python create_plan.py --exclude benchmark_model_accuracy,benchmark_model_f1_score ...
```

`--include` and `--exclude` may be repeated and may contain comma-separated metric IDs. These controls determine what is considered; they do not force an unrunnable test into the plan.

## Discovery and validation

List the live registry and taxonomy location for each metric:

```bash
python create_plan.py --list-tests
```

Validate an existing plan and compare its metric IDs with the current runtime registry:

```bash
python create_plan.py --check plans/my_plan.json
```

## Preflight report and generated metadata

The CLI reports:

- all runtime-discoverable metrics;
- how many metrics were considered;
- how many were runnable and therefore written into the plan;
- how many were excluded;
- the reason for each exclusion;
- unresolved required fields where applicable.

Generated plans include a `plan_creation` object recording the same high-level provenance, including the dataset, input format, field-translation file when used, candidate metric count, runnable metric count, and excluded metric count.

## Why some tests are excluded

Some configuration is a research decision rather than a software default. A service-port consistency test cannot universally assume DNS/53, a slice-ID validity test cannot invent the allowed slice set, and a reference-comparison metric cannot choose its own reference dataset.

Until those inputs are supplied explicitly, such a test is **not runnable**, so it does not belong in the generated plan. The preflight report explains the exclusion instead of silently introducing an assumption or leaving an unusable disabled test in the plan.
