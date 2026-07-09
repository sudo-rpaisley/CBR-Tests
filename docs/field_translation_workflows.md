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
