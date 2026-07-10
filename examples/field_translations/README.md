# Field translation examples

These files are example mappings for common dataset families. They are intended as starting points, not guaranteed universal mappings.

## How to use an example

Copy an example next to your dataset and rename it to match the sidecar convention:

```bash
cp examples/field_translations/deepsecure_cicddos2019.json \
  datasets/DeepSecure/CICDDoS2019/01-12/DrDoS_DNS.field_translation.json
```

Or pass it explicitly:

```bash
python run_plan.py \
  --case plans/deepsecure_plan.json \
  --dataset datasets/DeepSecure/CICDDoS2019/01-12/DrDoS_DNS.csv \
  --output outcomes/deepsecure_trial.json \
  --field-translation examples/field_translations/deepsecure_cicddos2019.json
```

## Important notes

- Examples may need local adjustment for your dataset version.
- Cases are not automatically modified to point at examples.
- Prefer dry-run validation before running metrics:

```bash
python run_plan.py \
  --case plans/deepsecure_plan.json \
  --dataset datasets/DeepSecure/CICDDoS2019/01-12/DrDoS_DNS.csv \
  --output outcomes/deepsecure_trial.json \
  --field-translation-dry-run \
  --field-translation examples/field_translations/deepsecure_cicddos2019.json
```
