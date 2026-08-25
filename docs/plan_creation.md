# Creating plans

`create_plan.py` builds test plans from the metric handlers that the runner can actually execute. It does not maintain a separate hard-coded test list, so new registered metrics automatically become visible to plan creation.

## Default behaviour

Running:

```bash
python create_plan.py
```

starts a small interactive wizard. Every available metric is selected and included in the generated plan by default. The builder then separates those metrics into configuration states:

- `ready`: the supplied dataset and existing plan templates provide enough information to run the metric;
- `needs_mapping`: the metric is valid for the dataset, but one or more canonical fields could not be resolved;
- `needs_configuration`: the metric needs information that must not be guessed, such as a reference dataset, service definition, allowed slice IDs, attack windows, train/test split details, or benchmark-model configuration;
- `needs_dataset`: no dataset was supplied, so applicability and fields cannot yet be checked;
- `not_applicable`: the metric is incompatible with the supplied dataset type, such as a packet-capture-only metric on a CSV file.

All states remain visible in the plan. Only `ready` metrics are enabled automatically. This is intentionally different from silently creating a plan that contains guessed scientific assumptions and then fails at runtime.

Generated comprehensive plans default to:

```json
{
  "fail_fast": false,
  "allow_skips": true,
  "sample_mode": "full"
}
```

so an inapplicable or unmapped test does not prevent independent realism tests from running.

## Dataset-aware creation

```bash
python create_plan.py \
  --plan-id deepsecure-dns \
  --name "DeepSecure DNS" \
  --dataset datasets/DeepSecure/CICDDoS2019/01-12/DrDoS_DNS.csv \
  --output plans/deepsecure_dns_plan.json
```

For supported tabular datasets the builder reads only the header, applies the existing field-alias detection, chooses the closest existing metric template, and enables metrics whose required fields are resolved. It never modifies the dataset.

## Selection controls

Every available test is selected by default. Narrow or extend that selection with:

```bash
python create_plan.py --include valid_port_range_profile,reserved_ip_address_profile ...
python create_plan.py --exclude benchmark_model_accuracy,benchmark_model_f1_score ...
```

`--include` and `--exclude` may be repeated and may contain comma-separated metric IDs.

Use `--enable-unready` only when you deliberately want incomplete metrics enabled. This can make a generated plan fail at runtime and is therefore not the default.

## Discovery and validation

List the live registry and taxonomy location for each metric:

```bash
python create_plan.py --list-tests
```

Validate an existing plan and compare its metric IDs with the current runtime registry:

```bash
python create_plan.py --check plans/my_plan.json
```

## Generated metadata

Generated plans include a `plan_creation` object describing how many metrics were available, selected and placed in each configuration state. Each metric also includes a `configuration` object containing its state and, where relevant, the reason or missing fields. The runtime ignores this metadata; it is present to make plan provenance and manual review explicit.

## Why some settings are not automatic

Some configuration is a research decision rather than a software default. For example, a service-port consistency test cannot universally assume DNS/53, a slice-ID validity test cannot invent the allowed slice set, and a reference-comparison metric cannot choose its own reference dataset. These metrics are included automatically but remain disabled until those assumptions are supplied explicitly.
