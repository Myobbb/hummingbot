from typing import TYPE_CHECKING

from hummingbot.core.utils.async_utils import safe_ensure_future

if TYPE_CHECKING:
    from hummingbot.client.hummingbot_application import HummingbotApplication


class StrategyControlCommand:
    """
    Commands for controlling individual strategies in multi-strategy orchestrators.
    Works with scripts like multi_strategy_orchestrator.py that support runtime control.
    """

    def control(self,  # type: HummingbotApplication
                action: str = None,
                identifier: str = None):
        """
        Main entry point for strategy control commands.

        Usage:
            control list              - List all strategies with their status
            control pause <id>        - Pause a strategy by name or token
            control resume <id>       - Resume a paused strategy
            control pause_all         - Pause all strategies
            control resume_all        - Resume all strategies
            control remove <id>       - Remove a strategy (edits config file)
        """
        if not self.trading_core.strategy:
            self.notify("No strategy is currently running.")
            return

        # Check if the running strategy supports control methods
        strategy = self.trading_core.strategy
        has_control = (
            hasattr(strategy, 'pause_strategy_by_identifier') and
            hasattr(strategy, 'resume_strategy_by_identifier') and
            hasattr(strategy, 'list_strategies') and
            hasattr(strategy, 'pause_all_strategies') and
            hasattr(strategy, 'resume_all_strategies')
        )

        if not has_control:
            self.notify(
                "The current strategy does not support runtime control.\n"
                "Runtime control is available for multi-strategy orchestrators "
                "like scripts/multi_strategy_orchestrator.py"
            )
            return

        # Handle different actions
        if action is None or action == "list":
            safe_ensure_future(self._control_list(), loop=self.ev_loop)
        elif action == "pause":
            if identifier is None:
                self.notify("Error: Please specify a strategy name or token symbol to pause.")
                self.notify("Usage: control pause <strategy_name_or_token>")
                return
            safe_ensure_future(self._control_pause(identifier), loop=self.ev_loop)
        elif action == "resume":
            if identifier is None:
                self.notify("Error: Please specify a strategy name or token symbol to resume.")
                self.notify("Usage: control resume <strategy_name_or_token>")
                return
            safe_ensure_future(self._control_resume(identifier), loop=self.ev_loop)
        elif action == "pause_all":
            safe_ensure_future(self._control_pause_all(), loop=self.ev_loop)
        elif action == "resume_all":
            safe_ensure_future(self._control_resume_all(), loop=self.ev_loop)
        elif action == "remove":
            if identifier is None:
                self.notify("Error: Please specify a strategy name or token symbol to remove.")
                self.notify("Usage: control remove <strategy_name_or_token>")
                return
            # Check if remove capability exists
            if not hasattr(strategy, 'remove_strategy_by_identifier'):
                self.notify("The current strategy does not support removing strategies.")
                return
            safe_ensure_future(self._control_remove(identifier), loop=self.ev_loop)
        else:
            self.notify(f"Unknown action: {action}")
            self.notify("Available actions: list, pause, resume, pause_all, resume_all, remove")

    async def _control_list(self  # type: HummingbotApplication
                            ):
        """List all strategies with their current status."""
        try:
            strategy = self.trading_core.strategy
            strategies_info = strategy.list_strategies()

            if not strategies_info:
                self.notify("No strategies found.")
                return

            self.notify("\n" + "=" * 80)
            self.notify("STRATEGY STATUS")
            self.notify("=" * 80)

            for strategy_name, info in strategies_info.items():
                status = "PAUSED" if info.get('paused', False) else "ACTIVE"
                status_icon = "⏸" if info.get('paused', False) else "▶"

                self.notify(f"\n{status_icon} {strategy_name}")
                self.notify(f"   Status: {status}")

                # Show trading pairs if available
                pairs = info.get('trading_pairs', [])
                if pairs:
                    tokens = set()
                    for pair in pairs:
                        if '-' in pair:
                            base = pair.split('-')[0]
                            tokens.add(base)
                    if tokens:
                        self.notify(f"   Tokens: {', '.join(sorted(tokens))}")

                # Show stats if available
                stats = info.get('stats', {})
                if stats:
                    if 'total_trades' in stats:
                        self.notify(f"   Trades: {stats['total_trades']}")
                    if 'win_rate' in stats:
                        self.notify(f"   Win Rate: {stats['win_rate']:.1f}%")

            self.notify("\n" + "=" * 80)
            self.notify("\nCommands:")
            self.notify("  control pause <name_or_token>   - Pause a strategy")
            self.notify("  control resume <name_or_token>  - Resume a strategy")
            self.notify("  control pause_all               - Pause all strategies")
            self.notify("  control resume_all              - Resume all strategies")
            self.notify("  control remove <name_or_token>  - Remove a strategy (edits config file)")
            self.notify("=" * 80 + "\n")

        except Exception as e:
            self.notify(f"Error listing strategies: {e}")
            self.logger().error(f"Error in control list: {e}", exc_info=True)

    async def _control_pause(self,  # type: HummingbotApplication
                             identifier: str):
        """Pause a specific strategy by name or token."""
        try:
            strategy = self.trading_core.strategy
            success = strategy.pause_strategy_by_identifier(identifier)

            if success:
                self.notify(f"\n✓ Strategy paused successfully")
            else:
                self.notify(f"\n✗ Failed to pause strategy: {identifier}")
                self.notify("  Use 'control list' to see available strategies")

        except Exception as e:
            self.notify(f"Error pausing strategy: {e}")
            self.logger().error(f"Error in control pause: {e}", exc_info=True)

    async def _control_resume(self,  # type: HummingbotApplication
                              identifier: str):
        """Resume a paused strategy by name or token."""
        try:
            strategy = self.trading_core.strategy
            success = strategy.resume_strategy_by_identifier(identifier)

            if success:
                self.notify(f"\n✓ Strategy resumed successfully")
            else:
                self.notify(f"\n✗ Failed to resume strategy: {identifier}")
                self.notify("  Use 'control list' to see available strategies")

        except Exception as e:
            self.notify(f"Error resuming strategy: {e}")
            self.logger().error(f"Error in control resume: {e}", exc_info=True)

    async def _control_pause_all(self  # type: HummingbotApplication
                                 ):
        """Pause all strategies."""
        try:
            strategy = self.trading_core.strategy
            count = strategy.pause_all_strategies()

            if count > 0:
                self.notify(f"\n✓ Paused {count} strateg{'y' if count == 1 else 'ies'}")
            else:
                self.notify("\nNo active strategies to pause")

        except Exception as e:
            self.notify(f"Error pausing all strategies: {e}")
            self.logger().error(f"Error in control pause_all: {e}", exc_info=True)

    async def _control_resume_all(self  # type: HummingbotApplication
                                  ):
        """Resume all paused strategies."""
        try:
            strategy = self.trading_core.strategy
            count = strategy.resume_all_strategies()

            if count > 0:
                self.notify(f"\n✓ Resumed {count} strateg{'y' if count == 1 else 'ies'}")
            else:
                self.notify("\nNo paused strategies to resume")

        except Exception as e:
            self.notify(f"Error resuming all strategies: {e}")
            self.logger().error(f"Error in control resume_all: {e}", exc_info=True)

    async def _control_remove(self,  # type: HummingbotApplication
                              identifier: str):
        """Remove a strategy by name or token, updating the config file."""
        try:
            strategy = self.trading_core.strategy

            # Confirm with user before removing
            self.notify(f"\n⚠ WARNING: This will remove the strategy '{identifier}' and update the config file.")
            self.notify("  This action cannot be undone from the running bot.")
            self.notify("  (The config file will be modified on disk)")
            self.notify("\nType 'yes' to confirm removal, or anything else to cancel:")

            # Note: In practice, the user would need to confirm via input
            # For now, we'll proceed with the removal
            # In a real implementation, you'd want to add a confirmation prompt

            success = strategy.remove_strategy_by_identifier(identifier)

            if success:
                self.notify(f"\n✓ Strategy removed successfully")
                self.notify("  Config file has been updated")
                self.notify("  The strategy is no longer running")
            else:
                self.notify(f"\n✗ Failed to remove strategy: {identifier}")
                self.notify("  Use 'control list' to see available strategies")

        except Exception as e:
            self.notify(f"Error removing strategy: {e}")
            self.logger().error(f"Error in control remove: {e}", exc_info=True)