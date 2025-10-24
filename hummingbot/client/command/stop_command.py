import platform
import threading
from typing import TYPE_CHECKING

from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase

if TYPE_CHECKING:
    from hummingbot.client.hummingbot_application import HummingbotApplication  # noqa: F401


class StopCommand:
    def stop(self,  # type: HummingbotApplication
             skip_order_cancellation: bool = False):
        if threading.current_thread() != threading.main_thread():
            self.ev_loop.call_soon_threadsafe(self.stop, skip_order_cancellation)
            return
        safe_ensure_future(self.stop_loop(skip_order_cancellation), loop=self.ev_loop)

    async def stop_loop(self,  # type: HummingbotApplication
                        skip_order_cancellation: bool = False):
        self.logger().info("stop command initiated.")
        self.notify("\nWinding down...")

        # Restore App Nap on macOS.
        if platform.system() == "Darwin":
            import appnope
            appnope.nap()

        # Handle script strategy specific cleanup first
        if self.trading_core.strategy and isinstance(self.trading_core.strategy, ScriptStrategyBase):
            await self.trading_core.strategy.on_stop()

        # Stop strategy if running
        if self.trading_core._strategy_running:
            await self.trading_core.stop_strategy()

        # Cancel outstanding orders
        if not skip_order_cancellation:
            await self.trading_core.cancel_outstanding_orders()

        # Keep clock running to preserve connector state (order books, user streams, etc.)
        # This allows the strategy to be restarted with 'start' without re-importing

        # Note: We do NOT stop the clock or connectors here to allow quick restart
        # The connectors stay connected and ready, enabling seamless restart

        # Preserve strategy metadata and connectors to avoid unloading on stop
        self.notify("Strategy stopped.")
