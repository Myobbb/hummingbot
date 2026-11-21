"""The one list of `control` actions — shared by the CLI parser, the completer and the handler.

Adding an action used to mean three edits: argparse's `choices` in `client/ui/parser.py`, the
completer in `client/ui/completer.py`, and the `elif action == …` chain in
`strategy_control_command.py`. Missing the parser made `control restore_hold` fail with
`invalid choice: 'restore_hold'` while its handler branch existed (2026-11-21) — the error went
to the terminal, never to a log, and the caller believed the command had run.

Keep this tuple and the handler chain in step: every entry here needs a branch there, and every
branch needs an entry here.
"""

CONTROL_ACTIONS = (
    "list", "pause", "resume", "pause_all", "resume_all", "remove", "add",
    "enable_buyin", "disable_buyin", "enable_selloff", "disable_selloff",
    "enable_hold", "disable_hold", "restore_hold", "set",
    "add_market", "remove_market", "create", "clean",
)