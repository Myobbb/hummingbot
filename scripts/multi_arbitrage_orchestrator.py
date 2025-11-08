"""
Multi-Arbitrage Orchestrator Strategy

This script demonstrates how to run multiple arbitrage strategies simultaneously
and control them individually at runtime using the V2 framework.

Features:
- Run multiple arbitrage controllers in parallel
- Pause/resume individual controllers at runtime
- Monitor per-controller performance
- Automatic risk management with drawdown limits
- Interactive controller management

Usage:
1. Create controller configs in conf/controllers/ directory
2. Reference them in your strategy config file
3. Use the built-in methods to control strategies at runtime

Runtime Control Examples:
- self.pause_controller("arb_bsx_gate_bitmart")   # Pause specific arbitrage
- self.resume_controller("arb_bsx_gate_bitmart")  # Resume specific arbitrage
- self.list_controllers()                          # List all controllers
- self.pause_all_controllers()                     # Pause all
- self.resume_all_controllers()                    # Resume all
"""

import os
from decimal import Decimal
from typing import Dict, List, Optional, Set

from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


class MultiArbitrageOrchestratorConfig(StrategyV2ConfigBase):
    """
    Configuration for the multi-arbitrage orchestrator.

    Example configuration file:

    script_file_name: multi_arbitrage_orchestrator.py
    markets: {}  # Auto-populated from controller configs
    candles_config: []
    controllers_config:
      - arb_bsx_gate_bitmart.yml
      - arb_phl_mexc_bitmart.yml
      - arb_token3.yml
    max_controller_drawdown_quote: 100.0
    max_global_drawdown_quote: 500.0
    performance_report_interval: 60
    enable_controller_status_notifications: true
    """
    script_file_name: str = os.path.basename(__file__)
    candles_config: List[CandlesConfig] = []
    markets: Dict[str, Set[str]] = {}
    max_global_drawdown_quote: Optional[float] = None
    max_controller_drawdown_quote: Optional[float] = None
    performance_report_interval: int = 60  # seconds
    enable_controller_status_notifications: bool = True


class MultiArbitrageOrchestrator(StrategyV2Base):
    """
    Multi-Arbitrage Orchestrator Strategy

    This strategy runs multiple arbitrage controllers simultaneously and provides
    runtime control capabilities for individual strategies.

    Features:
    - Individual controller pause/resume without stopping the bot
    - Per-controller performance tracking
    - Automatic drawdown-based risk management
    - Real-time status monitoring
    """

    def __init__(self, connectors: Dict[str, ConnectorBase], config: MultiArbitrageOrchestratorConfig):
        super().__init__(connectors, config)
        self.config = config
        self.max_pnl_by_controller = {}
        self.max_global_pnl = Decimal("0")
        self.drawdown_exited_controllers = []
        self.closed_executors_buffer: int = 30
        self._last_performance_report_timestamp = 0
        self._last_status_notification_timestamp = 0

        # Initialize performance tracking for each controller
        for controller_id in self.controllers.keys():
            self.max_pnl_by_controller[controller_id] = Decimal("0")

    def on_tick(self):
        """
        Main tick handler. Executes control logic and risk management.
        """
        super().on_tick()
        if not self._is_stop_triggered:
            self.check_manual_kill_switch()
            self.control_max_drawdown()
            self.send_performance_report()

            # Example: Auto-control based on custom conditions
            # Uncomment and customize as needed:
            # self.example_conditional_pause()

    def example_conditional_pause(self):
        """
        Example of conditional controller management.
        You can customize this to implement your own logic.
        """
        # Example: Pause controller if it's losing too much
        for controller_id in self.controllers.keys():
            perf = self.get_performance_report(controller_id)
            if perf and perf.global_pnl_quote < Decimal("-50"):
                if self.get_controller_status(controller_id) == RunnableStatus.RUNNING:
                    self.logger().warning(
                        f"Controller {controller_id} has lost $50+, pausing automatically"
                    )
                    self.pause_controller(controller_id)

    def control_max_drawdown(self):
        """Monitor and enforce drawdown limits."""
        if self.config.max_controller_drawdown_quote:
            self.check_max_controller_drawdown()
        if self.config.max_global_drawdown_quote:
            self.check_max_global_drawdown()

    def check_max_controller_drawdown(self):
        """
        Check individual controller drawdowns and pause if limits are exceeded.
        """
        for controller_id, controller in self.controllers.items():
            if controller.status != RunnableStatus.RUNNING:
                continue

            controller_pnl = self.get_performance_report(controller_id).global_pnl_quote
            last_max_pnl = self.max_pnl_by_controller[controller_id]

            if controller_pnl > last_max_pnl:
                self.max_pnl_by_controller[controller_id] = controller_pnl
            else:
                current_drawdown = last_max_pnl - controller_pnl
                if current_drawdown > self.config.max_controller_drawdown_quote:
                    self.logger().info(
                        f"Controller {controller_id} reached max drawdown "
                        f"(${current_drawdown:.2f}). Pausing controller."
                    )
                    self.pause_controller(controller_id)
                    self.drawdown_exited_controllers.append(controller_id)

    def check_max_global_drawdown(self):
        """
        Check global drawdown across all controllers and stop bot if exceeded.
        """
        current_global_pnl = sum([
            self.get_performance_report(controller_id).global_pnl_quote
            for controller_id in self.controllers.keys()
        ])

        if current_global_pnl > self.max_global_pnl:
            self.max_global_pnl = current_global_pnl
        else:
            current_global_drawdown = self.max_global_pnl - current_global_pnl
            if current_global_drawdown > self.config.max_global_drawdown_quote:
                self.drawdown_exited_controllers.extend(list(self.controllers.keys()))
                self.logger().info(
                    f"Global drawdown reached ${current_global_drawdown:.2f}. "
                    f"Stopping the strategy."
                )
                self._is_stop_triggered = True
                HummingbotApplication.main_application().stop()

    def send_performance_report(self):
        """Send performance reports via MQTT if enabled."""
        if (self.current_timestamp - self._last_performance_report_timestamp >=
                self.config.performance_report_interval and self._pub):
            performance_reports = {
                controller_id: self.get_performance_report(controller_id).dict()
                for controller_id in self.controllers.keys()
            }
            self._pub(performance_reports)
            self._last_performance_report_timestamp = self.current_timestamp

    def check_manual_kill_switch(self):
        """
        Check for manual kill switch in controller configs.
        This allows pausing controllers by editing config files.
        """
        for controller_id, controller in self.controllers.items():
            if controller.config.manual_kill_switch and controller.status == RunnableStatus.RUNNING:
                self.logger().info(f"Manual kill switch activated for controller {controller_id}.")
                self.pause_controller(controller_id)
            elif not controller.config.manual_kill_switch and controller.status == RunnableStatus.TERMINATED:
                if controller_id in self.drawdown_exited_controllers:
                    continue
                self.logger().info(f"Manual kill switch deactivated. Resuming controller {controller_id}.")
                self.resume_controller(controller_id)

    def check_executors_status(self):
        """Check if all executors have completed when stopping."""
        active_executors = self.filter_executors(
            executors=self.get_all_executors(),
            filter_func=lambda executor: executor.status == RunnableStatus.RUNNING
        )
        if not active_executors:
            self.logger().info("All executors have finalized their execution. Stopping the strategy.")
            HummingbotApplication.main_application().stop()
        else:
            non_trading_executors = self.filter_executors(
                executors=active_executors,
                filter_func=lambda executor: not executor.is_trading
            )
            self.executor_orchestrator.execute_actions([
                StopExecutorAction(
                    executor_id=executor.id,
                    controller_id=executor.controller_id
                ) for executor in non_trading_executors
            ])

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        """Controllers handle their own executor creation."""
        return []

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        """Controllers handle their own executor stopping."""
        return []

    def apply_initial_setting(self):
        """Apply initial settings for perpetual markets if needed."""
        connectors_position_mode = {}
        for controller_id, controller in self.controllers.items():
            config_dict = controller.config.model_dump()
            if "connector_name" in config_dict:
                if self.is_perpetual(config_dict["connector_name"]):
                    if "position_mode" in config_dict:
                        connectors_position_mode[config_dict["connector_name"]] = config_dict["position_mode"]
                    if "leverage" in config_dict:
                        self.connectors[config_dict["connector_name"]].set_leverage(
                            leverage=config_dict["leverage"],
                            trading_pair=config_dict["trading_pair"]
                        )
        for connector_name, position_mode in connectors_position_mode.items():
            self.connectors[connector_name].set_position_mode(position_mode)

    def format_status(self) -> str:
        """
        Enhanced status display with controller management information.

        This overrides the default status to show:
        - Quick reference for runtime commands
        - Controller status summary
        - Full performance details
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."

        lines = []

        # Controller Management Commands Section
        lines.append("\n" + "=" * 80)
        lines.append("CONTROLLER MANAGEMENT (Python Console)")
        lines.append("=" * 80)
        lines.append("Available commands (use Python console):")
        lines.append("  self.strategy.pause_controller('controller_id')    # Pause specific strategy")
        lines.append("  self.strategy.resume_controller('controller_id')   # Resume specific strategy")
        lines.append("  self.strategy.pause_all_controllers()              # Pause all strategies")
        lines.append("  self.strategy.resume_all_controllers()             # Resume all strategies")
        lines.append("  self.strategy.list_controllers()                   # Show controller summary")
        lines.append("")

        # Controller Status Summary
        controller_summary = self.list_controllers()
        if controller_summary:
            lines.append("Controller Status Summary:")
            summary_lines = []
            for ctrl_id, info in controller_summary.items():
                status_icon = "▶" if info["status"] == RunnableStatus.RUNNING else "⏸"
                summary_lines.append(
                    f"  {status_icon} {ctrl_id}: {info['status'].name} | "
                    f"Active: {info['active_executors']}/{info['total_executors']} | "
                    f"PnL: ${info['global_pnl']:.2f} | "
                    f"Volume: ${info['volume_traded']:.2f}"
                )
            lines.extend(summary_lines)

        # Standard status display
        lines.append("\n" + "=" * 80)
        lines.append("DETAILED STATUS")
        lines.append("=" * 80)

        # Call parent format_status for full details
        lines.append(super().format_status())

        return "\n".join(lines)

    def notify_status_change(self, controller_id: str, old_status: RunnableStatus, new_status: RunnableStatus):
        """
        Notify when a controller status changes.
        Useful for monitoring and logging.
        """
        if self.config.enable_controller_status_notifications:
            msg = (
                f"Controller '{controller_id}' status changed: "
                f"{old_status.name} → {new_status.name}"
            )
            self.logger().info(msg)
            # Uncomment to send to bot interface:
            # self.notify_hb_app_with_timestamp(msg)
