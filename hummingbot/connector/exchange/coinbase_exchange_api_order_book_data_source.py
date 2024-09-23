import asyncio
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinbase_exchange.coinbase_exchange import CoinbaseExchange

from hummingbot.connector.exchange.coinbase_exchange.coinbase_exchange_order_book import (
    CoinbaseExchangeOrderBook,
)


class CoinbaseExchangeAPIOrderBookDataSource(OrderBookTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0
    TRADE_STREAM_ID = 1
    DIFF_STREAM_ID = 2
    ONE_HOUR = 60 * 60

    _logger: HummingbotLogger | logging.Logger | None = None

    @classmethod
    def logger(cls) -> HummingbotLogger | logging.Logger:
        if cls._logger is None:
            name: str = HummingbotLogger.logger_name_for_class(cls)
            cls._logger = logging.getLogger(name)
        return cls._logger

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'CoinbaseExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = "exchange.coinbase.com"):
        """
        Initialize the CoinbaseExchangeAPIOrderBookDataSource.

        :param trading_pairs: The list of trading pairs to subscribe to.
        :param connector: The CoinbaseExchange instance.
        :param api_factory: The WebAssistantsFactory instance for creating the WSAssistant.
        :param domain: The domain for the WebSocket connection.
        """
        super().__init__(trading_pairs)
        self._domain: str = domain
        self._api_factory: WebAssistantsFactory = api_factory
        self._connector: 'CoinbaseExchange' = connector

        self._subscription_lock: Optional[asyncio.Lock] = None
        self._ws_assistant: Optional[WSAssistant] = None
        self._last_traded_prices: Dict[str, float] = defaultdict(lambda: 0.0)

        # Queue keys for message handling
        self._diff_messages_queue_key = "diff_messages"
        self._trade_messages_queue_key = "trade_messages"
        self._snapshot_messages_queue_key = "snapshot_messages"

    async def _parse_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        order_book_message: OrderBookMessage = await CoinbaseExchangeOrderBook.level2_or_trade_message_from_exchange(
            raw_message,
            self._connector.exchange_symbol_associated_to_pair)
        await message_queue.put(order_book_message)

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        await asyncio.sleep(0)
        return {trading_pair: self._last_traded_prices[trading_pair] or 0.0 for trading_pair in trading_pairs}

    # --- Overriding methods from the Base class ---
    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Reads the order diffs events queue. For each event creates a diff message instance and adds it to the
        output queue

        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created diff messages
        """
        while True:
            try:
                event = await self._message_queue[self._diff_messages_queue_key].get()
                await self._parse_message(raw_message=event, message_queue=output)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public order book updates from exchange")

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Coinbase Exchange provides snapshots through the REST API, not WebSocket.
        The snapshot is retrieved using the REST API.

        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created snapshot messages
        """
        pass

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output_queue: asyncio.Queue):
        """
        Reads the trade events queue.
        For each event creates a trade message instance and adds it to the output queue

        :param ev_loop: the event loop the method will run in
        :param output_queue: a queue to add the created trade messages
        """
        while True:
            try:
                trade_event = await self._message_queue[self._trade_messages_queue_key].get()
                await self._parse_message(raw_message=trade_event, message_queue=output_queue)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public trade updates from exchange")

    async def _connected_websocket_assistant(self) -> WSAssistant:
        self._ws_assistant: WSAssistant = await self._api_factory.get_ws_assistant()
        await self._ws_assistant.connect(ws_url="wss://ws-feed.exchange.coinbase.com")
        return self._ws_assistant

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the order book events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        https://docs.cloud.coinbase.com/exchange/docs/websocket-overview

        Recommended to use several subscriptions
        {
            "type": "subscribe",
            "product_ids": [
                "ETH-USD",
                "BTC-USD"
            ],
            "channels": ["level2", "heartbeat"]
        }
        """
        try:
            symbols = []
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                symbols.append(symbol)

            payload = {
                "type": "subscribe",
                "product_ids": symbols,
                "channels": ["level2", "heartbeat", {"name": "ticker", "product_ids": symbols}]
            }
            await ws.send(WSJSONRequest(payload=payload, is_auth_required=False))

            self.logger().info(f"Subscribed to order book channels for: {', '.join(self._trading_pairs)}")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...",
                exc_info=True
            )
            raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        async for ws_response in websocket_assistant.iter_messages():
            data: Dict[str, Any] = ws_response.data

            if data["type"] == 'error':
                self.logger().error(f"Error received from websocket: {ws_response}")
                raise ValueError(f"Error received from websocket: {ws_response}")

            if data["type"] == "subscriptions":
                self.logger().debug(f"Subscribed to channels: {data}")
                continue

            if data["type"] == "heartbeat":
                self.logger().debug(f"Received heartbeat: {data}")
                continue

            if data["type"] == "match":  # Trade message
                await self._parse_message(raw_message=data, message_queue=self._message_queue[self._trade_messages_queue_key])

            elif data["type"] == "l2update":  # Order book diff message
                await self._parse_message(raw_message=data, message_queue=self._message_queue[self._diff_messages_queue_key])

            else:
                self.logger().warning(f"Unrecognized websocket message type: {data['type']}")

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        params = {
            "product_id": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        snapshot: Dict[str, Any] = await rest_assistant.execute_request(
            url=f"https://api.exchange.coinbase.com/products/{params['product_id']}/book",
            params={"level": 2},
            method=RESTMethod.GET,
            is_auth_required=False,
        )

        snapshot_timestamp: float = self._connector.time_synchronizer.time()

        snapshot_msg: OrderBookMessage = CoinbaseExchangeOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        return snapshot_msg

    # Unused methods for Coinbase Exchange
    async def _parse_order_book_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        raise NotImplementedError("Coinbase Exchange does not implement this method.")

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        raise NotImplementedError("Coinbase Exchange does not implement this method.")

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        raise NotImplementedError("Coinbase Exchange does not implement this method.")

    # You can subscribe to both endpoints, but if ws-direct is your primary connection, we recommend using ws-feed as a failover.

    # Coinbase Market Data production = wss://ws-feed.exchange.coinbase.com / sandbox = wss://ws-feed-public.sandbox.exchange.coinbase.com

    # Coinbase Direct Market Data production = wss://ws-direct.exchange.coinbase.com / sandbox = wss://ws-direct.sandbox.exchange.coinbase.com
