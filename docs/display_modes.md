# Display modes

The runner supports live display modes with `--display`.

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
and recent events. The dashboard uses the current terminal size and redraws in
place when stdout is an interactive terminal.

## Operator controls

- `Ctrl-C`: request cancellation.
- `SIGUSR1`: pause.
- `SIGUSR2`: resume.

Labels are kept in text so status remains readable even when color is disabled.
