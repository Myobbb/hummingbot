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
                identifier: str = None,
                value: str = None):
        """
        Main entry point for strategy control commands.

        Usage:
            control list                                - List all strategies with their status
            control pause <id>                          - Pause a strategy by name or token
            control resume <id>                         - Resume a paused strategy
            control pause_all                           - Pause all strategies
            control resume_all                          - Resume all strategies
            control remove <id>                         - Remove a strategy (edits config file)
            control add <file>                          - Add a strategy from config file (conf/strategies/)
            control enable_buyin <id>                   - Enable buy-in for position balancer
            control disable_buyin <id>                  - Disable buy-in for position balancer
            control enable_selloff <id>                 - Enable sell-off for position balancer
            control disable_selloff <id>                - Disable sell-off for position balancer
            control set buyin <id> <value>              - Set buy-in target in USD
            control set selloff <id> <value>            - Set sell-off target in USD
            control set buy_spread <id> <value>         - Set buy spread (% or 'min')
            control set sell_spread <id> <value>        - Set sell spread (% or 'min')
            control set order_size <id> <value>         - Set order size in USD
            control set refresh_interval <id> <value>   - Set limit order refresh interval in seconds
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
        elif action == "add":
            if identifier is None:
                self.notify("Error: Please specify a config file to add.")
                self.notify("Usage: control add <config_file.yml>")
                return
            # Check if add capability exists
            if not hasattr(strategy, 'add_strategy_from_file'):
                self.notify("The current strategy does not support adding strategies.")
                return
            safe_ensure_future(self._control_add(identifier), loop=self.ev_loop)
        elif action == "enable_buyin":
            if identifier is None:
                self.notify("Error: Please specify a strategy name or token symbol.")
                self.notify("Usage: control enable_buyin <strategy_name_or_token>")
                return
            # Check if enable_buyin capability exists
            if not hasattr(strategy, 'enable_buyin_by_identifier'):
                self.notify("The current strategy does not support position balancer controls.")
                return
            safe_ensure_future(self._control_enable_buyin(identifier), loop=self.ev_loop)
        elif action == "disable_buyin":
            if identifier is None:
                self.notify("Error: Please specify a strategy name or token symbol.")
                self.notify("Usage: control disable_buyin <strategy_name_or_token>")
                return
            # Check if disable_buyin capability exists
            if not hasattr(strategy, 'disable_buyin_by_identifier'):
                self.notify("The current strategy does not support position balancer controls.")
                return
            safe_ensure_future(self._control_disable_buyin(identifier), loop=self.ev_loop)
        elif action == "enable_selloff":
            if identifier is None:
                self.notify("Error: Please specify a strategy name or token symbol.")
                self.notify("Usage: control enable_selloff <strategy_name_or_token>")
                return
            # Check if enable_selloff capability exists
            if not hasattr(strategy, 'enable_selloff_by_identifier'):
                self.notify("The current strategy does not support position balancer controls.")
                return
            safe_ensure_future(self._control_enable_selloff(identifier), loop=self.ev_loop)
        elif action == "disable_selloff":
            if identifier is None:
                self.notify("Error: Please specify a strategy name or token symbol.")
                self.notify("Usage: control disable_selloff <strategy_name_or_token>")
                return
            # Check if disable_selloff capability exists
            if not hasattr(strategy, 'disable_selloff_by_identifier'):
                self.notify("The current strategy does not support position balancer controls.")
                return
            safe_ensure_future(self._control_disable_selloff(identifier), loop=self.ev_loop)
        elif action == "set":
            if identifier is None:
                self.notify("Error: Please specify what to set (buyin, selloff, buy_spread, sell_spread, order_size)")
                self.notify("Usage: control set <parameter> <strategy_name_or_token> <value>")
                return
            # identifier is the parameter name (buyin, selloff, etc.)
            # value contains the strategy_id and actual value as a list
            if value is None or len(value) == 0:
                self.notify("Error: Missing strategy identifier and value")
                self.notify(f"Usage: control set {identifier} <strategy_name_or_token> <value>")
                return
            # Join the list into a single string
            value_str = " ".join(value) if isinstance(value, list) else str(value)
            safe_ensure_future(self._control_set(identifier, value_str), loop=self.ev_loop)
        else:
            self.notify(f"Unknown action: {action}")
            self.notify("Available actions: list, pause, resume, pause_all, resume_all, remove, add, "
                       "enable_buyin, disable_buyin, enable_selloff, disable_selloff, set")

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
            self.notify("  control pause <name_or_token>                    - Pause a strategy")
            self.notify("  control resume <name_or_token>                   - Resume a strategy")
            self.notify("  control pause_all                                - Pause all strategies")
            self.notify("  control resume_all                               - Resume all strategies")
            self.notify("  control remove <name_or_token>                   - Remove a strategy (edits config file)")
            self.notify("  control add <config_file>                        - Add strategy from conf/strategies/")
            self.notify("  control enable_buyin <name_or_token>             - Enable position balancer buy-in")
            self.notify("  control disable_buyin <name_or_token>            - Disable position balancer buy-in")
            self.notify("  control enable_selloff <name_or_token>           - Enable position balancer sell-off")
            self.notify("  control disable_selloff <name_or_token>          - Disable position balancer sell-off")
            self.notify("  control set buyin <name_or_token> <value>        - Set buy-in target (USD)")
            self.notify("  control set selloff <name_or_token> <value>      - Set sell-off target (USD)")
            self.notify("  control set buy_spread <name> <value>            - Set buy spread (% or 'min')")
            self.notify("  control set sell_spread <name> <value>           - Set sell spread (% or 'min')")
            self.notify("  control set order_size <name> <value>            - Set order size (USD)")
            self.notify("  control set refresh_interval <name> <value>      - Set refresh interval (seconds)")
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
            success = strategy.remove_strategy_by_identifier(identifier)

            if success:
                self.notify(f"\n✓ Strategy removed successfully")
            else:
                self.notify(f"\n✗ Failed to remove strategy: {identifier}")
                self.notify("  Use 'control list' to see available strategies")

        except Exception as e:
            self.notify(f"Error removing strategy: {e}")
            self.logger().error(f"Error in control remove: {e}", exc_info=True)

    async def _control_add(self,  # type: HummingbotApplication
                          config_file: str):
        """Add a strategy from a config file in conf/strategies/."""
        try:
            strategy = self.trading_core.strategy
            success = await strategy.add_strategy_from_file(config_file)

            if success:
                self.notify(f"\n✓ Strategy added successfully from {config_file}")
            else:
                self.notify(f"\n✗ Failed to add strategy from {config_file}")
                self.notify("  Check that the file exists in conf/strategies/")

        except Exception as e:
            self.notify(f"Error adding strategy: {e}")
            self.logger().error(f"Error in control add: {e}", exc_info=True)

    async def _control_enable_buyin(self,  # type: HummingbotApplication
                                    identifier: str):
        """Enable buy-in mode for a strategy's position balancer."""
        try:
            strategy = self.trading_core.strategy
            success = strategy.enable_buyin_by_identifier(identifier)

            if success:
                self.notify(f"\n✓ Buy-in enabled successfully for {identifier}")
                self.notify("  Position balancer will acquire assets to reach target")
            else:
                self.notify(f"\n✗ Failed to enable buy-in for: {identifier}")
                self.notify("  Strategy may not have position balancer enabled")
                self.notify("  Use 'control list' to see available strategies")

        except Exception as e:
            self.notify(f"Error enabling buy-in: {e}")
            self.logger().error(f"Error in control enable_buyin: {e}", exc_info=True)

    async def _control_disable_buyin(self,  # type: HummingbotApplication
                                     identifier: str):
        """Disable buy-in mode for a strategy's position balancer."""
        try:
            strategy = self.trading_core.strategy
            success = strategy.disable_buyin_by_identifier(identifier)

            if success:
                self.notify(f"\n✓ Buy-in disabled successfully for {identifier}")
                self.notify("  All buy orders cancelled and tracking cleared")
            else:
                self.notify(f"\n✗ Failed to disable buy-in for: {identifier}")
                self.notify("  Strategy may not have position balancer enabled")
                self.notify("  Use 'control list' to see available strategies")

        except Exception as e:
            self.notify(f"Error disabling buy-in: {e}")
            self.logger().error(f"Error in control disable_buyin: {e}", exc_info=True)

    async def _control_enable_selloff(self,  # type: HummingbotApplication
                                      identifier: str):
        """Enable sell-off mode for a strategy's position balancer."""
        try:
            strategy = self.trading_core.strategy
            success = strategy.enable_selloff_by_identifier(identifier)

            if success:
                self.notify(f"\n✓ Sell-off enabled successfully for {identifier}")
                self.notify("  Position balancer will reduce assets to reach target")
            else:
                self.notify(f"\n✗ Failed to enable sell-off for: {identifier}")
                self.notify("  Strategy may not have position balancer enabled")
                self.notify("  Use 'control list' to see available strategies")

        except Exception as e:
            self.notify(f"Error enabling sell-off: {e}")
            self.logger().error(f"Error in control enable_selloff: {e}", exc_info=True)

    async def _control_disable_selloff(self,  # type: HummingbotApplication
                                       identifier: str):
        """Disable sell-off mode for a strategy's position balancer."""
        try:
            strategy = self.trading_core.strategy
            success = strategy.disable_selloff_by_identifier(identifier)

            if success:
                self.notify(f"\n✓ Sell-off disabled successfully for {identifier}")
                self.notify("  All sell orders cancelled and tracking cleared")
            else:
                self.notify(f"\n✗ Failed to disable sell-off for: {identifier}")
                self.notify("  Strategy may not have position balancer enabled")
                self.notify("  Use 'control list' to see available strategies")

        except Exception as e:
            self.notify(f"Error disabling sell-off: {e}")
            self.logger().error(f"Error in control disable_selloff: {e}", exc_info=True)

    async def _control_set(self,  # type: HummingbotApplication
                          parameter: str,
                          args: str):
        """
        Set a position balancer parameter at runtime.

        Args:
            parameter: The parameter to set (buyin, selloff, buy_spread, sell_spread, order_size)
            args: String containing "<strategy_id> <value>"
        """
        try:
            # Parse args into strategy_id and value
            if not args or not args.strip():
                self.notify("Error: Missing strategy identifier and value")
                self.notify(f"Usage: control set {parameter} <strategy_name_or_token> <value>")
                return

            parts = args.strip().split(None, 1)
            if len(parts) != 2:
                self.notify("Error: Missing value parameter")
                self.notify(f"Usage: control set {parameter} <strategy_name_or_token> <value>")
                return

            strategy_id = parts[0]
            value_str = parts[1]

            strategy = self.trading_core.strategy

            # Handle different parameters
            if parameter == "buyin":
                # Set buy-in target
                if not hasattr(strategy, 'set_buy_target_by_identifier'):
                    self.notify("The current strategy does not support position balancer controls.")
                    return

                try:
                    value = float(value_str)
                    if value < 0:
                        self.notify("Error: Buy-in target must be non-negative")
                        return
                except ValueError:
                    self.notify(f"Error: Invalid value '{value_str}'. Must be a number.")
                    return

                success = strategy.set_buy_target_by_identifier(strategy_id, value)
                if success:
                    self.notify(f"\n✓ Buy-in target set to {value:.2f} USD for {strategy_id}")
                else:
                    self.notify(f"\n✗ Failed to set buy-in target for: {strategy_id}")
                    self.notify("  Use 'control list' to see available strategies")

            elif parameter == "selloff":
                # Set sell-off target
                if not hasattr(strategy, 'set_sell_target_by_identifier'):
                    self.notify("The current strategy does not support position balancer controls.")
                    return

                try:
                    value = float(value_str)
                    if value < 0:
                        self.notify("Error: Sell-off target must be non-negative")
                        return
                except ValueError:
                    self.notify(f"Error: Invalid value '{value_str}'. Must be a number.")
                    return

                success = strategy.set_sell_target_by_identifier(strategy_id, value)
                if success:
                    self.notify(f"\n✓ Sell-off target set to {value:.2f} USD for {strategy_id}")
                else:
                    self.notify(f"\n✗ Failed to set sell-off target for: {strategy_id}")
                    self.notify("  Use 'control list' to see available strategies")

            elif parameter == "buy_spread":
                # Set buy spread
                if not hasattr(strategy, 'set_buy_spread_by_identifier'):
                    self.notify("The current strategy does not support position balancer controls.")
                    return

                # Handle 'min' or numeric value
                if value_str.lower() == 'min':
                    spread_value = 'min'
                else:
                    try:
                        spread_value = float(value_str)
                        if spread_value < 0:
                            self.notify("Error: Buy spread must be non-negative")
                            return
                    except ValueError:
                        self.notify(f"Error: Invalid value '{value_str}'. Must be a number or 'min'.")
                        return

                success = strategy.set_buy_spread_by_identifier(strategy_id, spread_value)
                if success:
                    if spread_value == 'min':
                        self.notify(f"\n✓ Buy spread set to minimum tick for {strategy_id}")
                    else:
                        self.notify(f"\n✓ Buy spread set to {spread_value}% for {strategy_id}")
                else:
                    self.notify(f"\n✗ Failed to set buy spread for: {strategy_id}")
                    self.notify("  Use 'control list' to see available strategies")

            elif parameter == "sell_spread":
                # Set sell spread
                if not hasattr(strategy, 'set_sell_spread_by_identifier'):
                    self.notify("The current strategy does not support position balancer controls.")
                    return

                # Handle 'min' or numeric value
                if value_str.lower() == 'min':
                    spread_value = 'min'
                else:
                    try:
                        spread_value = float(value_str)
                        if spread_value < 0:
                            self.notify("Error: Sell spread must be non-negative")
                            return
                    except ValueError:
                        self.notify(f"Error: Invalid value '{value_str}'. Must be a number or 'min'.")
                        return

                success = strategy.set_sell_spread_by_identifier(strategy_id, spread_value)
                if success:
                    if spread_value == 'min':
                        self.notify(f"\n✓ Sell spread set to minimum tick for {strategy_id}")
                    else:
                        self.notify(f"\n✓ Sell spread set to {spread_value}% for {strategy_id}")
                else:
                    self.notify(f"\n✗ Failed to set sell spread for: {strategy_id}")
                    self.notify("  Use 'control list' to see available strategies")

            elif parameter == "order_size":
                # Set order size
                if not hasattr(strategy, 'set_order_size_by_identifier'):
                    self.notify("The current strategy does not support position balancer controls.")
                    return

                try:
                    value = float(value_str)
                    if value <= 0:
                        self.notify("Error: Order size must be positive")
                        return
                except ValueError:
                    self.notify(f"Error: Invalid value '{value_str}'. Must be a number.")
                    return

                success = strategy.set_order_size_by_identifier(strategy_id, value)
                if success:
                    self.notify(f"\n✓ Order size set to {value:.2f} USD for {strategy_id}")
                else:
                    self.notify(f"\n✗ Failed to set order size for: {strategy_id}")
                    self.notify("  Use 'control list' to see available strategies")

            elif parameter == "refresh_interval":
                # Set refresh interval
                if not hasattr(strategy, 'set_refresh_interval_by_identifier'):
                    self.notify("The current strategy does not support position balancer controls.")
                    return

                try:
                    value = float(value_str)
                    if value <= 0:
                        self.notify("Error: Refresh interval must be positive")
                        return
                except ValueError:
                    self.notify(f"Error: Invalid value '{value_str}'. Must be a number.")
                    return

                success = strategy.set_refresh_interval_by_identifier(strategy_id, value)
                if success:
                    self.notify(f"\n✓ Refresh interval set to {value:.0f} seconds for {strategy_id}")
                else:
                    self.notify(f"\n✗ Failed to set refresh interval for: {strategy_id}")
                    self.notify("  Use 'control list' to see available strategies")

            else:
                self.notify(f"Unknown parameter: {parameter}")
                self.notify("Available parameters: buyin, selloff, buy_spread, sell_spread, order_size, refresh_interval")

        except Exception as e:
            self.notify(f"Error setting parameter: {e}")
            self.logger().error(f"Error in control set: {e}", exc_info=True)