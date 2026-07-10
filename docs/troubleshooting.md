# Troubleshooting

## `unrecognized arguments: --field-translation-dry-run`

You are likely running an older branch or checkout. Confirm `python run_plan.py --help` lists the field-translation flags and `--display` modes.

## Dataset path does not exist

The runner now fails early if the dataset path does not exist. Check relative paths from case files and absolute paths passed with `--dataset`.

## Sidecar was not created

Sidecar creation requires one of:

- interactive confirmation in a TTY,
- `--yes-field-translation-sidecar`, or
- `--field-translation-dry-run`.

It is disabled by `--no-update-field-translation`.

## Existing sidecar was not updated

Existing sidecars are only updated when enabled metrics require fields that are not already listed. Use `--field-translation-dry-run --yes-field-translation-sidecar` to validate/update templates without running metrics.

## Metrics were skipped

A metric is skipped when one or more `field_requirements.required` fields are unavailable after identity, explicit, and auto-detected mappings are applied. Check the field translation text/Markdown report for missing fields.

## All metrics were skipped

Fill in the sidecar template values, pass an explicit mapping with `--field-translation`, or update the plan's `field_requirements` if fields were marked required incorrectly.

## Dry-run succeeds but a metric fails during execution

Dry-run checks field availability, not every metric-specific data assumption. Check the outcome JSON `metric_results` error and any `column_validations` details.

## Live output is too tall in tmux

Use `--display compact` for a one-screen summary or `--display quiet` for minimal output. Use `--display full` only when you have enough scrollback or need every metric leaf.

## I do not want files created or changed near datasets

Use `--no-update-field-translation`. Existing sidecars can still be loaded, but new sidecars will not be created or updated.
