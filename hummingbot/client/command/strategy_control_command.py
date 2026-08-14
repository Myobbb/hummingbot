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
            control set min_profitability <id> <value>  - Set minimum profitability percentage
            control enable_hold <id>                    - Enable hold-band guardrail (arb size capped near target)
            control disable_hold <id>                   - Disable hold-band guardrail
            control set hold_target <id> <value>        - Set hold target USD (also enables guardrail)
            control set hold_band <id> <value>          - Set hold band half-width USD
            control set hold_escalation <id> <on|off>    - Auto-arm PB after stuck correction (default on)
            control add_market <id> <exchange:PAIR>     - Add market to strategy (e.g., 'mexc:BSX-USDT')
            control remove_market <id> <exchange:PAIR>  - Remove market from strategy
            control clean <id>                          - Set sell-off target to 0 and enable sell-off (sell entire position)
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
        elif action == "add_market":
            if identifier is None or value is None or len(value) == 0:
                self.notify("Error: Missing strategy identifier or market specification")
                self.notify("Usage: control add_market <strategy_name_or_token> <exchange:PAIR>")
                self.notify("Example: control add_market BSX mexc:BSX-USDT")
                return
            # Check if add_market capability exists
            if not hasattr(strategy, 'add_market_by_identifier'):
                self.notify("The current strategy does not support adding markets.")
                return
            market_spec = value[0] if isinstance(value, list) else str(value)
            safe_ensure_future(self._control_add_market(identifier, market_spec), loop=self.ev_loop)
        elif action == "remove_market":
            if identifier is None or value is None or len(value) == 0:
                self.notify("Error: Missing strategy identifier or market specification")
                self.notify("Usage: control remove_market <strategy_name_or_token> <exchange:PAIR>")
                self.notify("Example: control remove_market BSX mexc:BSX-USDT")
                return
            # Check if remove_market capability exists
            if not hasattr(strategy, 'remove_market_by_identifier'):
                self.notify("The current strategy does not support removing markets.")
                return
            market_spec = value[0] if isinstance(value, list) else str(value)
            safe_ensure_future(self._control_remove_market(identifier, market_spec), loop=self.ev_loop)
        elif action == "create":
            # Format: control create <name> <primary_exchange:pair> <secondary_exchange:pair> [options]
            # identifier = name, value = [primary_spec, secondary_spec, ...]
            if identifier is None or value is None or len(value) < 2:
                self.notify("Error: Missing required arguments for create")
                self.notify("Usage: control create <name> <primary_exchange:PAIR> <secondary_exchange:PAIR>")
                self.notify("Example: control create arb_bsx_new gate:BSX-USDT kucoin:BSX-USDT")
                return
            # Check if create capability exists
            if not hasattr(strategy, 'create_strategy'):
                self.notify("The current strategy does not support runtime strategy creation.")
                return
            primary_spec = value[0]
            secondary_spec = value[1]
            # Parse optional min_profitability if provided (e.g., "2.0" as third arg)
            min_profitability = 2.1
            if len(value) > 2:
                try:
                    min_profitability = float(value[2])
                except ValueError:
                    pass  # Keep default
            safe_ensure_future(self._control_create(identifier, primary_spec, secondary_spec, min_profitability), loop=self.ev_loop)
        elif action == "clean":
            if identifier is None:
                self.notify("Error: Please specify a strategy name or token symbol.")
                self.notify("Usage: control clean <strategy_name_or_token>")
                return
            if not hasattr(strategy, 'clean_by_identifier'):
                self.notify("The current strategy does not support the clean command.")
                return
            safe_ensure_future(self._control_clean(identifier), loop=self.ev_loop)
        elif action == "enable_hold":
            if identifier is None:
                self.notify("Error: Please specify a strategy name or token symbol.")
                self.notify("Usage: control enable_hold <strategy_name_or_token>")
                return
            if not hasattr(strategy, 'enable_hold_by_identifier'):
                self.notify("The current strategy does not support hold-band controls.")
                return
            safe_ensure_future(self._control_enable_hold(identifier), loop=self.ev_loop)
        elif action == "disable_hold":
            if identifier is None:
                self.notify("Error: Please specify a strategy name or token symbol.")
                self.notify("Usage: control disable_hold <strategy_name_or_token>")
                return
            if not hasattr(strategy, 'disable_hold_by_identifier'):
                self.notify("The current strategy does not support hold-band controls.")
                return
            safe_ensure_future(self._control_disable_hold(identifier), loop=self.ev_loop)
        else:
            self.notify(f"Unknown action: {action}")
            self.notify("Available actions: list, pause, resume, pause_all, resume_all, remove, add, "
                       "enable_buyin, disable_buyin, enable_selloff, disable_selloff, "
                       "enable_hold, disable_hold, set, add_market, remove_market, create, clean")

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

                # Show hold-band guardrail state (only for strategies that support it)
                hold_target = info.get('hold_target')
                hold_band = info.get('hold_band')
                if 'hold_target' in info:
                    if hold_target is not None:
                        self.notify(f"   Hold: target=${hold_target:.0f} band=±${hold_band:.0f} "
                                    f"(range ${hold_target - hold_band:.0f}–${hold_target + hold_band:.0f})")
                    else:
                        self.notify(f"   Hold: disabled")

                # Position balancer — shown only when a side is actually armed. The hold-band
                # escalation can arm one automatically, so without this there is no indication
                # in `control list` that a strategy is mid buy-in/sell-off.
                pb = info.get('pb')
                if pb and (pb.get('buy_enabled') or pb.get('sell_enabled')):
                    sides = []
                    if pb.get('buy_enabled'):
                        sides.append("buy-in")
                    if pb.get('sell_enabled'):
                        sides.append("sell-off")
                    origin = " (auto-armed by hold-band escalation)" if pb.get('escalated') else ""
                    self.notify(f"   PB: {' + '.join(sides)} ACTIVE{origin}")

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
            self.notify("  control set min_profitability <name> <value>     - Set min profitability (%)")
            self.notify("  control set hold_target <name> <usd>              - Set hold target USD (enables guardrail)")
            self.notify("  control set hold_band <name> <usd>                - Set hold band half-width USD")
            self.notify("  control enable_hold <name_or_token>               - Re-enable after disable_hold")
            self.notify("  control disable_hold <name_or_token>              - Disable hold-band guardrail")
            self.notify("  control add_market <name_or_token> <exchange:PAIR> - Add market to strategy")
            self.notify("  control remove_market <name_or_token> <exchange:PAIR> - Remove market from strategy")
            self.notify("  control clean <name_or_token>                     - Set sell-off target to 0 and enable sell-off")
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

    async def _control_clean(self,  # type: HummingbotApplication
                             identifier: str):
        """Set sell-off target to 0 and enable sell-off (sell entire position)."""
        try:
            strategy = self.trading_core.strategy
            success = strategy.clean_by_identifier(identifier)

            if success:
                self.notify(f"\n✓ Clean initiated for {identifier}")
                self.notify("  Sell-off target set to 0, sell-off enabled — will sell entire position")
            else:
                self.notify(f"\n✗ Failed to initiate clean for: {identifier}")
                self.notify("  Strategy may not have position balancer enabled")
                self.notify("  Use 'control list' to see available strategies")

        except Exception as e:
            self.notify(f"Error in control clean: {e}")
            self.logger().error(f"Error in control clean: {e}", exc_info=True)

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

            elif parameter == "min_profitability":
                # Set min profitability
                if not hasattr(strategy, 'set_min_profitability_by_identifier'):
                    self.notify("The current strategy does not support runtime control.")
                    return

                try:
                    value = float(value_str)
                    # No negative check here as negative profitability might be valid for some strategies (though unlikely for arb)
                    # But let's assume it should be reasonable.
                except ValueError:
                    self.notify(f"Error: Invalid value '{value_str}'. Must be a number.")
                    return

                success = strategy.set_min_profitability_by_identifier(strategy_id, value)
                if success:
                    self.notify(f"\n✓ Min profitability set to {value}% for {strategy_id}")
                else:
                    self.notify(f"\n✗ Failed to set min profitability for: {strategy_id}")
                    self.notify("  Use 'control list' to see available strategies")

            elif parameter == "hold_target":
                if not hasattr(strategy, 'set_hold_target_by_identifier'):
                    self.notify("The current strategy does not support hold-band controls.")
                    return
                try:
                    value = float(value_str)
                    if value < 0:
                        self.notify("Error: Hold target must be non-negative (0 disables the guardrail)")
                        return
                except ValueError:
                    self.notify(f"Error: Invalid value '{value_str}'. Must be a number.")
                    return
                success = strategy.set_hold_target_by_identifier(strategy_id, value)
                if success:
                    self.notify(f"\n✓ Hold target set to {value:.0f} USD for {strategy_id}")
                    self.notify("  Guardrail enabled — arb size will be capped to stay near this target")
                else:
                    self.notify(f"\n✗ Failed to set hold target for: {strategy_id}")

            elif parameter == "hold_escalation":
                if not hasattr(strategy, 'set_hold_escalation_by_identifier'):
                    self.notify("The current strategy does not support hold-band controls.")
                    return
                v = value_str.strip().lower()
                if v in ("on", "true", "1", "yes", "enable", "enabled"):
                    enabled = True
                elif v in ("off", "false", "0", "no", "disable", "disabled"):
                    enabled = False
                else:
                    self.notify(f"Error: Invalid value '{value_str}'. Use on/off (or true/false, 1/0).")
                    return
                success = strategy.set_hold_escalation_by_identifier(strategy_id, enabled)
                if success:
                    if enabled:
                        self.notify(f"\n✓ Hold-band escalation ENABLED for {strategy_id}")
                        self.notify("  Position balancer may be auto-armed after a stuck correction.")
                    else:
                        self.notify(f"\n✓ Hold-band escalation DISABLED for {strategy_id}")
                        self.notify("  Guardrail stays active (arb legs still trimmed); buy-in/sell-off")
                        self.notify("  will NOT be auto-armed. Enable manually if you want PB orders.")
                else:
                    self.notify(f"\n✗ Failed to set hold escalation for: {strategy_id}")

            elif parameter == "hold_band":
                if not hasattr(strategy, 'set_hold_band_by_identifier'):
                    self.notify("The current strategy does not support hold-band controls.")
                    return
                try:
                    value = float(value_str)
                    if value <= 0:
                        self.notify("Error: Hold band must be greater than 0")
                        return
                except ValueError:
                    self.notify(f"Error: Invalid value '{value_str}'. Must be a number.")
                    return
                success = strategy.set_hold_band_by_identifier(strategy_id, value)
                if success:
                    self.notify(f"\n✓ Hold band set to ±{value:.0f} USD for {strategy_id}")
                    self.notify(f"  Acceptable range will be [target - {value:.0f}, target + {value:.0f}]")
                else:
                    self.notify(f"\n✗ Failed to set hold band for: {strategy_id}")

            else:
                self.notify(f"Unknown parameter: {parameter}")
                self.notify("Available parameters: buyin, selloff, buy_spread, sell_spread, order_size, "
                            "refresh_interval, min_profitability, hold_target, hold_band")

        except Exception as e:
            self.notify(f"Error setting parameter: {e}")
            self.logger().error(f"Error in control set: {e}", exc_info=True)

    async def _control_enable_hold(self,  # type: HummingbotApplication
                                   identifier: str):
        """Enable hold-band guardrail for a strategy."""
        try:
            strategy = self.trading_core.strategy
            success = strategy.enable_hold_by_identifier(identifier)
            if success:
                self.notify(f"\n✓ Hold-band guardrail enabled for {identifier}")
                self.notify("  Arb order sizes will be capped to keep total holding near target")
            else:
                self.notify(f"\n✗ Failed to enable hold-band for: {identifier}")
                self.notify("  Check hold_target_usd is set (use 'control set hold_target <id> <usd>')")
        except Exception as e:
            self.notify(f"Error enabling hold-band: {e}")
            self.logger().error(f"Error in control enable_hold: {e}", exc_info=True)

    async def _control_disable_hold(self,  # type: HummingbotApplication
                                    identifier: str):
        """Disable hold-band guardrail for a strategy."""
        try:
            strategy = self.trading_core.strategy
            success = strategy.disable_hold_by_identifier(identifier)
            if success:
                self.notify(f"\n✓ Hold-band guardrail disabled for {identifier}")
                self.notify("  Arb orders will no longer be size-capped by hold target")
            else:
                self.notify(f"\n✗ Failed to disable hold-band for: {identifier}")
        except Exception as e:
            self.notify(f"Error disabling hold-band: {e}")
            self.logger().error(f"Error in control disable_hold: {e}", exc_info=True)

    async def _control_add_market(self,  # type: HummingbotApplication
                                  identifier: str,
                                  market_spec: str):
        """Add a market to a strategy's additional_markets with dynamic websocket subscription."""
        try:
            strategy = self.trading_core.strategy
            
            self.notify(f"Adding market '{market_spec}' to {identifier}...")
            self.notify("  This includes dynamic websocket subscription for order book data")
            
            # Await the async method
            success = await strategy.add_market_by_identifier(identifier, market_spec)

            if success:
                self.notify(f"\n✓ Market '{market_spec}' added successfully to {identifier}")
                self.notify("  New arbitrage pairs created; trading will include the new market")
                self.notify("  Order book data subscription is active")
            else:
                self.notify(f"\n✗ Failed to add market '{market_spec}' to: {identifier}")
                self.notify("  Check logs for details. Use 'control list' to see strategies")

        except Exception as e:
            self.notify(f"Error adding market: {e}")
            self.logger().error(f"Error in control add_market: {e}", exc_info=True)

    async def _control_remove_market(self,  # type: HummingbotApplication
                                     identifier: str,
                                     market_spec: str):
        """Remove a market from a strategy's additional_markets."""
        try:
            strategy = self.trading_core.strategy
            success = strategy.remove_market_by_identifier(identifier, market_spec)

            if success:
                self.notify(f"\n✓ Market '{market_spec}' removed successfully from {identifier}")
                self.notify("  Arbitrage pairs updated; trading will exclude the removed market")
            else:
                self.notify(f"\n✗ Failed to remove market '{market_spec}' from: {identifier}")
                self.notify("  Possible reasons: market not found, or need at least 2 markets remaining")
                self.notify("  Primary/secondary removal requires additional_markets for promotion")
                self.notify("  Use 'control list' to see strategies")

        except Exception as e:
            self.notify(f"Error removing market: {e}")
            self.logger().error(f"Error in control remove_market: {e}", exc_info=True)

    async def _control_create(self,  # type: HummingbotApplication
                              name: str,
                              primary_spec: str,
                              secondary_spec: str,
                              min_profitability: float = 2.1):
        """Create a new arbitrage strategy at runtime (always starts PAUSED)."""
        try:
            strategy = self.trading_core.strategy

            self.notify(f"Creating strategy '{name}'...")
            self.notify("  This includes dynamic websocket subscriptions for order book data")

            success = await strategy.create_strategy(
                name=name,
                primary_spec=primary_spec,
                secondary_spec=secondary_spec,
                min_profitability=min_profitability,
            )

            if success:
                self.notify(f"\n✓ Strategy '{name}' created successfully (PAUSED)")
                self.notify(f"  Primary: {primary_spec}, Secondary: {secondary_spec}")
                self.notify(f"  Min profitability: {min_profitability}%")
                self.notify("  Use 'control resume <name>' to start trading")
            else:
                self.notify(f"\n✗ Failed to create strategy '{name}'")
                self.notify("  Check logs for details. Possible issues:")
                self.notify("  - Strategy name already exists")
                self.notify("  - Exchange not available in connector pool")
                self.notify("  - Invalid exchange:PAIR format")

        except Exception as e:
            self.notify(f"Error creating strategy: {e}")
            self.logger().error(f"Error in control create: {e}", exc_info=True)