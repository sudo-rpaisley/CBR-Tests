# Display modes

The runner supports live display modes with `--display`. To choose run flags from a terminal menu instead of remembering command-line options, start with `python run_plan.py --tui`.

## `compact`

Default. Recommended for tmux and normal interactive runs. Shows:

- branch status summaries,
- active/attention metrics,
- skipped metrics with missing fields,
- recently completed metrics,
- recent telemetry events,
- overall progress.

## `full`

Shows the full taxonomy tree and every metric leaf. Use for debugging or when terminal scrollback is available.

## `quiet`

Suppresses live taxonomy/status redraws. Use in CI, scripts, or logs where live updates are noisy.

## `interactive`

Shows an ANSI terminal dashboard with boxed sections for run metadata, overall
progress, branch summaries, active/attention metrics, recently completed metrics,
and recent events. During a TUI-launched interactive run, use ↑/↓ to select a
branch and Enter/Space to expand or collapse its metric rows. The dashboard uses
the current terminal size and redraws in place when stdout is an interactive
terminal.

## Operator controls

- `Ctrl-C`: request cancellation.
- `SIGUSR1`: pause.
- `SIGUSR2`: resume.

Labels are kept in text so status remains readable even when color is disabled.

## Launch selector TUI

Run `python run_plan.py --tui` to open a keyboard-driven terminal menu before execution. Use arrow keys to move, Enter to act on the selected field, Space to toggle boolean options, `r` to start the run, and `q` to cancel. The footer explains what the highlighted field does and which key to press; the worker field also shows the detected maximum worker count. The selector covers case/plan paths, a dataset file explorer, output paths, case ID, display mode, worker count, taxonomy controls, field translation controls, and field translation report paths. After completion, the TUI shows an expandable results screen with options to return to the setup menu or quit; dry-run results also include a run-now action, with confirmation when the dry run found issues. Report path fields are optional outputs for field-translation validation: JSON is best for automation, text is best for quick reading/logs, and Markdown is best for review notes or GitHub issues.
