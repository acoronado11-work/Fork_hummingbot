import asyncio
import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any, AsyncGenerator, Dict, List, NamedTuple

from hummingbot.connector.exchange.coinbase_exchange.coinbase_exchange_web_utils import (
    get_timestamp_from_exchange_time,
)
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinbase_exchange.coinbase_exchange import (  # noqa: F401
        CoinbaseExchange,
    )


class CoinbaseExchangeCumulativeUpdate(NamedTuple):
    client_order_id: str
    exchange_order_id: str
    status: str
    trading_pair: str
    fill_timestamp_s: float  # seconds
    average_price: Decimal
    cumulative_base_amount: Decimal
    remainder_base_amount: Decimal
    cumulative_fee: Decimal
    order_type: OrderType
    trade_type: TradeType
    creation_timestamp_s: float = 0.0  # seconds
    is_taker: bool = False  # Coinbase delivers trade events from the maker's perspective


class CoinbaseExchangeAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    UserStreamTrackerDataSource implementation for Coinbase Exchange API.
    """
    _sequence: int = 0
    _logger: HummingbotLogger | logging.Logger | None = None

    @classmethod
    def logger(cls) -> HummingbotLogger | logging.Logger:
        if cls._logger is None:
            name: str = HummingbotLogger.logger_name_for_class(cls)
            cls._logger = logging.getLogger(name)
        return cls._logger

    def __init__(self,
                 auth,
                 trading_pairs: List[str],
                 connector: 'CoinbaseExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = "com"):
        """
        Initialize the CoinbaseExchangeAPIUserStreamDataSource.

        :param auth: The CoinbaseExchangeAuth instance for authentication.
        :param trading_pairs: The list of trading pairs to subscribe to.
        :param connector: The CoinbaseExchange instance.
        :param api_factory: The WebAssistantsFactory instance for creating the WSAssistant.
        :param domain: The domain for the WebSocket connection.
        """
        super().__init__()
        self._domain: str = domain
        self._api_factory: WebAssistantsFactory = api_factory
        self._trading_pairs: List[str] = trading_pairs
        self._connector = connector
        self._ws_assistant: WSAssistant | None = None
        self._reset_recv_time: bool = False

    @property
    def last_recv_time(self) -> float:
        """
        Returns the time of the last received message.

        :return: the timestamp of the last received message in seconds.
        """
        if self._ws_assistant and not self._reset_recv_time:
            return self._ws_assistant.last_recv_time
        return 0

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Create and connect the WebSocket assistant.

        :return: The connected WSAssistant instance.
        """
        self._ws_assistant = await self._api_factory.get_ws_assistant()

        await self._ws_assistant.connect(
            ws_url="wss://ws-feed.exchange.coinbase.com",
            ping_timeout=30.0
        )
        return self._ws_assistant

    async def _subscribe_channels(self, websocket_assistant: WSAssistant) -> None:
        """
        Subscribes to the user events through the provided websocket connection.
        :param websocket_assistant: the websocket assistant used to connect to the exchange.
        """
        await self._subscribe_or_unsubscribe(websocket_assistant, "subscribe")

    async def _unsubscribe_channels(self, websocket_assistant: WSAssistant) -> None:
        """
        Unsubscribes from the user events through the provided websocket connection.
        :param websocket_assistant: the websocket assistant used to connect to the exchange.
        """
        await self._subscribe_or_unsubscribe(websocket_assistant, "unsubscribe")

    async def _subscribe_or_unsubscribe(
            self,
            websocket_assistant: WSAssistant,
            action: str
    ) -> None:
        """
        Subscribes or unsubscribes to the list of channels/pairs through the provided websocket connection.
        :param action: "subscribe" or "unsubscribe".
        :param websocket_assistant: the websocket assistant used to connect to the exchange.
        """
        symbols: List[str] = [
            await self._connector.exchange_symbol_associated_to_pair(trading_pair=pair) for pair in self._trading_pairs
        ]

        try:
            payload = {
                "type": action,
                "product_ids": symbols,
                "channels": ["user"]
            }

            await websocket_assistant.send(WSJSONRequest(payload=payload, is_auth_required=True))
            self.logger().info(f"{action.capitalize()}d to user channels for {self._trading_pairs}.")
        except (asyncio.CancelledError, Exception) as e:
            self.logger().exception(
                f"Unexpected error occurred {action.capitalize()}ing to user channels for {self._trading_pairs}.\n"
                f"Exception: {e}",
                exc_info=True
            )
            raise

    @staticmethod
    async def _try_except_queue_put(item: Any, queue: asyncio.Queue):
        """
        Try to put the order into the queue, except if the queue is full.
        :param queue: The queue to put the order into.
        """
        try:
            queue.put_nowait(item)
            await asyncio.sleep(0)
        except asyncio.QueueFull:
            try:
                await asyncio.wait_for(queue.put(item), timeout=1.0)
            except asyncio.TimeoutError:
                raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        """
        Processes the messages from the websocket connection and puts them into the intermediary queue.
        :param websocket_assistant: the websocket assistant used to connect to the exchange.
        :param queue: The intermediary queue to put the messages into.
        """
        async for ws_response in websocket_assistant.iter_messages():  # type: ignore
            data: Dict[str, Any] = ws_response.data

            if 'type' in data and data["type"] == "error":
                if "authentication failure" in data["message"]:
                    self.logger().error(f"authentication error: {data}")
                    await self._subscribe_channels(self._ws_assistant)
                else:
                    self.logger().error(f"Received error message: {data}")
                self._reset_recv_time = True
                return

            self._process_sequence_number(data)

            channel: str = data.get("channel", "")
            if channel == 'user':
                async for order in self._decipher_message(event_message=data):
                    try:
                        await self._try_except_queue_put(item=order, queue=queue)
                    except asyncio.QueueFull:
                        self.logger().exception("Timeout while waiting to put message into raw queue. Message dropped.")
                        raise
            elif channel == 'subscriptions':
                self._process_subscription_message(data)
            elif channel == 'heartbeat':
                self._process_heartbeat_message(data)

    def _process_sequence_number(self, data: Dict[str, Any]):
        """
        Processes the sequence number from the websocket message.
        :param data: The message received from the websocket connection.
        """
        if "sequence" in data and data["sequence"] > self._sequence:
            self.logger().warning(
                f"Received a message with a higher sequence number than the current one. "
                f"Current sequence: {self._sequence}, received sequence: {data['sequence']}."
            )

        self._sequence = data["sequence"] + 1

    def _process_subscription_message(self, data: Dict[str, Any]):
        """
        Processes the subscription message from the websocket connection.
        :param data: The message received from the websocket connection.
        """
        pass

    def _process_heartbeat_message(self, data: Dict[str, Any]):
        """
        Processes the heartbeat message from the websocket connection.
        :param data: The message received from the websocket connection.
        """
        pass

    async def _decipher_message(self, event_message: Dict[str, Any]) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Streamlines the messages for processing by the exchange.
        :param event_message: The message received from the exchange.
        """
        if not isinstance(event_message["timestamp"], float):
            event_message["timestamp"] = get_timestamp_from_exchange_time(event_message["timestamp"], "s")

        timestamp_s: float = event_message["timestamp"]
        for event in event_message.get("events", []):
            for order in event.get("orders", []):
                try:
                    if order["client_order_id"] != '':
                        order_type: OrderType | None = None
                        if order["order_type"] == "limit":
                            order_type = OrderType.LIMIT
                        elif order["order_type"] == "market":
                            order_type = OrderType.MARKET

                        cumulative_order: CoinbaseExchangeCumulativeUpdate = CoinbaseExchangeCumulativeUpdate(
                            exchange_order_id=order["order_id"],
                            client_order_id=order["client_order_id"],
                            status=order["status"],
                            trading_pair=await self._connector.trading_pair_associated_to_exchange_symbol(
                                symbol=order["product_id"]
                            ),
                            fill_timestamp_s=timestamp_s,
                            average_price=Decimal(order["avg_price"]),
                            cumulative_base_amount=Decimal(order["cumulative_quantity"]),
                            remainder_base_amount=Decimal(order["leaves_quantity"]),
                            cumulative_fee=Decimal(order["total_fees"]),
                            order_type=order_type,
                            trade_type=TradeType.BUY if order["order_side"] == "buy" else TradeType.SELL,
                            creation_timestamp_s=get_timestamp_from_exchange_time(order["creation_time"], "s"),
                        )
                        yield cumulative_order

                except Exception as e:
                    self.logger().exception(f"Failed to create a CumulativeUpdate error {e}\n\t{order}")
                    raise e
