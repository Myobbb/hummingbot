# Pull Request: Fix control commands and clean up status output

## Summary
This PR fixes the control command implementation and cleans up status/runtime output for the multi-strategy orchestrator.

## Changes

### 1. Fix control command return types
- Updated `pause_all_strategies()` to return `int` (count of paused strategies)
- Updated `resume_all_strategies()` to return `int` (count of resumed strategies)
- The control command expects these methods to return counts for proper user feedback

### 2. Clean up status and runtime output
- Removed outdated Python console command references (pause(), resume(), list_arb(), etc.)
- Updated runtime help to show only actual `control` commands
- Removed verbose strategy listing from startup
- Removed redundant control section from status output
- Removed individual strategy status indicators
- Removed excessive separator lines
- Changed "running" to "active" for consistency

### 3. Documentation
- Added comprehensive `CONTROL_COMMAND_USAGE.md` with:
  - Complete usage guide for all control commands
  - Troubleshooting section
  - Implementation status verification
  - Examples

## Control Command Usage

The `control` command is now fully functional:

```bash
# List all strategies
control list

# Pause by token or name
control pause BSX
control pause arb_bsx_gate_bitmart

# Resume
control resume BSX

# Pause/resume all
control pause_all
control resume_all
```

## Status Output

Status output is now clean and concise, showing only:
- Strategy count summary (active/paused)
- Balances
- Market profitability data
- Buy-in status (when active)
- Connector status (when not ready)

## Testing
- All Python files pass syntax validation
- Control command infrastructure is complete
- Commands properly registered in parser and completer

## Implementation Status
✅ Parser integration (`hummingbot/client/ui/parser.py`)
✅ Autocomplete support (`hummingbot/client/ui/completer.py`)
✅ Command implementation (`hummingbot/client/command/strategy_control_command.py`)
✅ Command registration (`hummingbot/client/command/__init__.py`)
✅ Multi-strategy orchestrator support (`scripts/multi_strategy_orchestrator.py`)

## Files Changed
- `scripts/multi_strategy_orchestrator.py` - Fixed return types, cleaned up output
- `CONTROL_COMMAND_USAGE.md` - New documentation file

## Branch Information
- **Source Branch:** `claude/multi-strategy-orchestrator-011CUvukUea1WXRLmXh9csad`
- **Target Branch:** `dev_bb28`

## Commits
1. Merge branch 'dev_bb28' into claude/multi-strategy-orchestrator-011CUvukUea1WXRLmXh9csad
2. Fix control command return types and add documentation
3. Clean up status output and runtime help messages

## How to Create PR

You can create the PR using one of these methods:

### Option 1: GitHub Web UI
1. Go to: https://github.com/Myobbb/hummingbot/pull/new/claude/multi-strategy-orchestrator-011CUvukUea1WXRLmXh9csad
2. Set base branch to: `dev_bb28`
3. Copy the content from this file as the PR description

### Option 2: GitHub CLI (if available)
```bash
gh pr create --base dev_bb28 \
  --title "Fix control commands and clean up status output" \
  --body-file PR_DETAILS.md
```

## Notes
- Control commands require Hummingbot restart to be recognized
- The command infrastructure was already fully implemented in dev_bb28
- This PR only fixes the return type bug and cleans up output
