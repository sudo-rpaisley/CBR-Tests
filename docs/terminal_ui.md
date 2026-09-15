# Guided terminal UI

The terminal UI is a presentation layer over the normal CBR-Tests runner. It does not change metric calculations, thresholds, plan semantics, outcome formats, or the final-experiment contract.

Start it with:

```bash
python run_plan.py --tui
```

The first screen asks whether you want a **Single dataset run** or a **Batch / comparison run**.

## Single dataset runs

The normal view intentionally shows only the settings needed most often:

- **Case or plan JSON** — choose the prepared case or plan to run.
- **Dataset file** — required when a plan is selected directly; a case may already provide its dataset.
- **Strict experiment mode** — enforces the canonical final-experiment contract and rejects incompatible historical plans.
- **Live display mode** — controls terminal progress output.
- **Field validation only** — validates field mappings without running metrics.

Press `a` to show every advanced option, including output naming, worker overrides, taxonomy ordering, field-translation settings, dataset summaries, and mapping reports.

Press `r` to review the important settings. The run does not start until you confirm the review screen.

### File browser

The dataset browser only shows supported dataset files (`.csv`, `.tsv`, `.xlsx`, `.xls`, `.pcap`, and `.pcapng`). Useful shortcuts are:

| Key | Action |
| --- | --- |
| `↑` / `↓` | Move selection |
| `Enter` | Open directory or select file |
| `Backspace` | Parent directory |
| `e` | Type or paste a path |
| `R` | Repository root |
| `H` | Home directory |
| `M` | `/media` when available |
| `q` / `Esc` | Cancel |

## Batch and comparison runs

The normal batch screen contains only:

- batch name;
- candidate datasets;
- optional independent reference datasets; and
- metric policy.

The screen continuously shows the candidate count, reference count, and resulting job count. Self-comparisons are excluded automatically.

Press `a` to expose worker overrides, display mode, output replacement, dataset-summary controls, and fail-fast behaviour. Press `r` to review the comparison matrix before starting it.

## Common controls

| Key | Action |
| --- | --- |
| `↑` / `↓` | Move between fields |
| `Enter` | Edit or choose the selected field |
| `Space` | Toggle an on/off setting |
| `a` | Show/hide advanced options |
| `r` | Review and run |
| `q` / `Esc` | Leave the setup screen |

Choice fields now open a selectable list instead of requiring repeated Enter presses to cycle through every available value.

## Final experiment runs

For an authoritative single-dataset experiment, enable **Strict experiment mode**. This is the TUI equivalent of `--experiment-mode` and invokes the same validation in the standard runner. It can reject older compatibility plans by design; regenerate those plans from the current taxonomy rather than disabling strict mode for final results.

For the final experiment campaign, field-translation sidecars should also be frozen and reviewed before authoritative execution. Those controls remain available under the advanced view.
