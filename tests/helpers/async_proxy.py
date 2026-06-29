import asyncio
import logging

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger("websockets.proxy")


class AsyncSilenceProxy:
    """
    Async websocket proxy that can be commanded to go silent and then resume.

    It relays application frames between a downstream client and an upstream node. While paused it stops relaying
    application frames in both directions, but the underlying connections stay OPEN because the websockets library keeps
    answering ping/pong automatically. That reproduces the production "poison pill": the client's activity timeout fires
    and its retries exhaust against a socket that never closes, leaving a dead handler on an OPEN connection.

    Usage:
        proxy = await AsyncSilenceProxy(upstream_url).start()
        ...                      # traffic flows
        proxy.pause()            # node goes silent, socket stays open
        ...                      # client retries exhaust -> poison
        proxy.resume()           # traffic flows again
        await proxy.close()

    This is the async analogue of `tests/helpers/proxy_server.py`, using a command-driven pause (an `asyncio.Event`)
    instead of a time-based one so tests are deterministic.
    """

    def __init__(self, upstream: str, host: str = "127.0.0.1"):
        self.upstream = upstream
        self.host = host
        self._server = None
        self._forwarding = asyncio.Event()
        self._forwarding.set()
        self._tasks: set[asyncio.Task] = set()

    @property
    def port(self) -> int:
        """The OS-assigned port the client connects to. Valid after `start()`."""
        return self._server.sockets[0].getsockname()[1]

    @property
    def url(self) -> str:
        return f"ws://{self.host}:{self.port}"

    async def start(self) -> "AsyncSilenceProxy":
        self._server = await serve(self._handle_client, self.host, 0)
        return self

    def pause(self) -> None:
        """Stop relaying application frames; the sockets stay OPEN (go silent)."""
        self._forwarding.clear()

    def resume(self) -> None:
        """Resume relaying application frames."""
        self._forwarding.set()

    async def close(self) -> None:
        # Unblock and cancel the pumps so handlers return promptly, then shut the listener down. Cancelling first avoids
        # waiting on a pump that is blocked reading the still-open (idle) upstream.
        self.resume()
        for task in list(self._tasks):
            task.cancel()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _handle_client(self, client) -> None:
        async with connect(self.upstream) as upstream:
            pumps = [
                asyncio.create_task(self._pump(client, upstream)),
                asyncio.create_task(self._pump(upstream, client)),
            ]
            self._tasks.update(pumps)
            try:
                # When either direction ends (e.g. the client reconnects and drops this connection), stop the sibling so
                # the handler returns instead of blocking forever on the still-open upstream.
                await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for pump in pumps:
                    pump.cancel()
                    self._tasks.discard(pump)

    async def _pump(self, src, dst) -> None:
        try:
            async for message in src:
                # Block here while paused: the frame is held (not relayed) until `resume()`, so the peer sees silence on
                # an otherwise-open socket.
                await self._forwarding.wait()
                await dst.send(message)
        except ConnectionClosed:
            pass
