# Field translation workflows

## Create or update a sidecar without running metrics

```bash
python run_plan.py \
  --case plans/example_plan.json \
  --dataset datasets/example.csv \
  --output outcomes/example.json \
  --field-translation-dry-run \
  --yes-field-translation-sidecar
```

## Validate mappings and write reports

```bash
python run_plan.py \
  --case plans/example_plan.json \
  --dataset datasets/example.csv \
  --output outcomes/example.json \
  --field-translation-dry-run \
  --field-translation-report reports/fields.json \
  --field-translation-text-report reports/fields.txt \
  --field-translation-markdown-report reports/fields.md
```

## Run without creating or updating sidecars

```bash
python run_plan.py \
  --case plans/example_plan.json \
  --dataset datasets/example.csv \
  --output outcomes/example.json \
  --no-update-field-translation
```

## Use an explicit non-sidecar mapping

```bash
python run_plan.py \
  --case plans/example_plan.json \
  --dataset datasets/example.csv \
  --output outcomes/example.json \
  --field-translation examples/field_translations/secure5g.json
```


## Full dry-run with all report formats

```bash
python run_plan.py \
  --case plans/deepsecure_plan.json \
  --dataset datasets/DeepSecure/CICDDoS2019/01-12/DrDoS_DNS.csv \
  --output outcomes/deepsecure_trial.json \
  --field-translation-dry-run \
  --yes-field-translation-sidecar \
  --field-translation-report outcomes/deepsecure_fields.json \
  --field-translation-text-report outcomes/deepsecure_fields.txt \
  --field-translation-markdown-report outcomes/deepsecure_fields.md
```

## Display modes while running metrics

```bash
# tmux-friendly default
python run_plan.py --case plans/example_plan.json --dataset datasets/example.csv --output outcomes/example.json --display compact

# full taxonomy list
python run_plan.py --case plans/example_plan.json --dataset datasets/example.csv --output outcomes/example.json --display full

# minimal live output for CI/scripts
python run_plan.py --case plans/example_plan.json --dataset datasets/example.csv --output outcomes/example.json --display quiet
```

See `docs/run_plan_controls.md` for the complete control reference.
