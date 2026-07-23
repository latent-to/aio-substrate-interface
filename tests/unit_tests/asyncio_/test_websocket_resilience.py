"""
Regression tests for the Websocket resilience fixes.

These cover the failure modes behind the relayed "Max retries exceeded." errors:
- A dead background handler on a still-OPEN socket must be revived on re-entry instead of permanently poisoning the
shared connection ("poison pill").
- A request that fails mid-flight must still balance `_waiting_for_response` and give back the subscription permit that
`send` acquired for it.
- `discard_request` must release that permit, drop the pending future, and burn the id so a late node response can never
be misrouted to a reused id.
- A done future whose `result()` raises must not release its permit inside `retrieve`; the paired `discard_request`
owns that single release, so a mid-flight transport error can never double-release the subscription semaphore.
- Reconnection locking: a forced (handler-driven) reconnect serializes on the connection lock rather than bypassing
it, and an unforced `connect` defers to a live handler instead of cancelling it mid-reconnect.
"""

import asyncio
import json
import socket
from contextlib import suppress
from hashlib import blake2b
from unittest.mock import AsyncMock, MagicMock

import pytest
from websockets.asyncio.server import serve
from websockets.protocol import State

import async_substrate_interface.async_substrate
from async_substrate_interface.async_substrate import (
    AsyncSubstrateInterface,
    Websocket,
)
from async_substrate_interface.errors import SubstrateRequestException


def _make_payload(id_: str) -> dict:
    return {
        "id": id_,
        "payload": {
            "jsonrpc": "2.0",
            "method": "state_getRuntimeVersion",
            "params": [],
        },
    }


class _FakePoisonWs:
    """
    Minimal Websocket stand-in whose `retrieve` always poisons (raises).

    It mirrors the real permit lifecycle so the cleanup path of `_make_rpc_request` can be exercised without real
    sockets: `send` acquires a permit, and on the failure path the only thing that gives it back is `discard_request`
    (exactly what the `_make_rpc_request` finally-block is expected to call).
    """

    def __init__(self, max_subscriptions: int = 1024):
        self._waiting_for_response = 0
        self.max_subscriptions = asyncio.Semaphore(max_subscriptions)
        self.permits_held = 0
        self._sent = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def mark_waiting_for_response(self):
        self._waiting_for_response += 1

    async def mark_response_received(self):
        self._waiting_for_response -= 1

    async def send(self, payload):
        await self.max_subscriptions.acquire()
        self.permits_held += 1
        item_id = f"id{self._sent}"
        self._sent += 1
        return item_id

    async def retrieve(self, item_id):
        raise TimeoutError("Max retries exceeded.")

    async def discard_request(self, item_id):
        self.max_subscriptions.release()
        self.permits_held -= 1


def _interface_with_ws(fake_ws) -> AsyncSubstrateInterface:
    substrate = AsyncSubstrateInterface("ws://localhost", _mock=True)
    substrate.ws = MagicMock()
    substrate.ws.__aenter__ = AsyncMock(return_value=fake_ws)
    substrate.ws.__aexit__ = AsyncMock(return_value=False)
    return substrate


@pytest.mark.asyncio
async def test_dead_handler_is_revived_on_enter():
    """
    A finished `_send_recv_task` on an OPEN socket must be revived by `__aenter__`.

    Without the revive, the socket stays OPEN so neither `__aenter__` nor `connect` rebuild the handler, the dead task
    lingers, and every `retrieve` re-raises its stored error forever.
    """
    ws = Websocket(
        "ws://fake:9944",
        max_retries=1,
        retry_timeout=0.1,
        shutdown_timer=None,
    )

    async def _dead():
        # The real handler returns (does not raise) the error after its retries.
        return TimeoutError("Max retries exceeded.")

    ws._send_recv_task = asyncio.ensure_future(_dead())
    await ws._send_recv_task
    ws.ws = MagicMock(state=State.OPEN)
    ws._attempts = 5

    reconnect_calls = []

    async def fake_connect_internal(force):
        reconnect_calls.append(force)
        ws.ws = MagicMock(state=State.OPEN)
        ws._send_recv_task = asyncio.ensure_future(asyncio.sleep(3600))

    ws._connect_internal = fake_connect_internal

    try:
        await ws.__aenter__()

        assert reconnect_calls == [True]
        assert not ws._send_recv_task.done()
        assert ws._attempts == 0
    finally:
        ws._send_recv_task.cancel()
        with suppress(asyncio.CancelledError):
            await ws._send_recv_task


@pytest.mark.asyncio
async def test_failed_request_balances_waiting_counter():
    """A request that fails mid-flight must still decrement `_waiting_for_response`."""
    fake = _FakePoisonWs()
    substrate = _interface_with_ws(fake)

    with pytest.raises(TimeoutError, match="Max retries exceeded."):
        await substrate._make_rpc_request([_make_payload("a")])

    assert fake._waiting_for_response == 0


@pytest.mark.asyncio
async def test_failed_requests_do_not_leak_subscription_permits():
    """Each failed request must give its `send` permit back via `discard_request`."""
    fake = _FakePoisonWs(max_subscriptions=8)
    substrate = _interface_with_ws(fake)

    for i in range(5):
        with pytest.raises(TimeoutError, match="Max retries exceeded."):
            await substrate._make_rpc_request([_make_payload(f"req{i}")])

    assert fake.permits_held == 0
    assert fake.max_subscriptions._value == 8


@pytest.mark.asyncio
async def test_discard_request_releases_permit_and_burns_id():
    """
    `discard_request` releases the permit and drops the future, the id stays burned, and a late node response for it is
    dropped rather than misrouted to a reused id.
    """
    ws = Websocket("ws://fake:9944", shutdown_timer=None)

    item_id = "Xy1"
    fut = asyncio.get_running_loop().create_future()
    ws._received[item_id] = fut
    ws._inflight[item_id] = '{"id": "Xy1"}'
    ws._in_use_ids.add(item_id)
    await ws.max_subscriptions.acquire()
    permits_after_send = ws.max_subscriptions._value

    await ws.discard_request(item_id)

    assert item_id not in ws._received
    assert item_id not in ws._inflight
    # Burned on purpose: kept in _in_use_ids so it cannot be reissued while a late response for it may still arrive.
    assert item_id in ws._in_use_ids
    assert ws.max_subscriptions._value == permits_after_send + 1
    assert fut.cancelled()

    # A late response for the discarded id must be dropped: no re-created future, no exception.
    await ws._dispatch_response({"id": item_id, "result": "0xLATE"})
    assert item_id not in ws._received

    # Idempotent: a second discard for the same id must not double-release a permit.
    permits_before = ws.max_subscriptions._value
    await ws.discard_request(item_id)
    assert ws.max_subscriptions._value == permits_before


@pytest.mark.asyncio
async def test_failed_retrieve_then_discard_releases_permit_once():
    """
    A done future whose `result()` raises must not release its permit in `retrieve`.

    `retrieve` releases the permit only on the success path; on the exception path it leaves the still-pending id in
    place so the caller's finally-block can hand it to `discard_request`, the single owner of that release. Releasing
    inside `retrieve` here (the pre-fix order) would double-release the subscription semaphore once `discard_request`
    runs.
    """
    ws = Websocket("ws://fake:9944", shutdown_timer=None)

    item_id = "Ab1"
    fut = asyncio.get_running_loop().create_future()
    fut.set_exception(ConnectionError("connection broke mid-flight"))
    ws._received[item_id] = fut
    ws._inflight[item_id] = '{"id": "Ab1"}'
    ws._in_use_ids.add(item_id)
    await ws.max_subscriptions.acquire()
    permits_after_send = ws.max_subscriptions._value

    # `retrieve` hits a done future whose `result()` raises. It must propagate that error WITHOUT releasing the permit,
    # otherwise the paired `discard_request` would release a second time (the double-release this fix prevents).
    with pytest.raises(ConnectionError, match="connection broke mid-flight"):
        await ws.retrieve(item_id)

    assert ws.max_subscriptions._value == permits_after_send
    assert item_id in ws._received

    # The callers' finally-block then discards the still-pending id, which is the single owner of that release.
    await ws.discard_request(item_id)

    assert ws.max_subscriptions._value == permits_after_send + 1
    assert item_id not in ws._received
    assert item_id not in ws._inflight


# --- Subscription recovery after reconnection ---------------------------------------------------


def _sub_message(sub_id: str, result) -> dict:
    return {"jsonrpc": "2.0", "params": {"subscription": sub_id, "result": result}}


@pytest.mark.asyncio
async def test_recovered_subscription_is_aliased_to_original_id():
    """
    A recoverer that re-establishes its subscription returns the new server-side id; messages arriving
    under that id must be routed to the original consumer queue, and `unsubscribe` must both address the
    server by the new id and clean up all recovery state.
    """
    ws = Websocket("ws://fake:9944", shutdown_timer=None)

    async def recoverer():
        return "new-sub"

    ws.register_subscription_recoverer("old-sub", recoverer)
    # tracked (and therefore recoverable/connection-keeping) even before any notification arrives
    assert "old-sub" in ws._received_subscriptions

    await ws._recover_subscriptions()

    await ws._dispatch_response(_sub_message("new-sub", {"inBlock": "0xabc"}))
    queued = ws._received_subscriptions["old-sub"].get_nowait()
    assert queued["params"]["result"] == {"inBlock": "0xabc"}

    await ws.unsubscribe("old-sub")
    sent = await ws._sending.get()
    assert sent["params"] == ["new-sub"]
    assert "old-sub" not in ws._subscription_recoverers
    assert "old-sub" not in ws._received_subscriptions
    assert ws._sub_alias_to_original == {}
    assert ws._sub_original_to_alias == {}


@pytest.mark.asyncio
async def test_recovery_drains_messages_that_raced_in_under_new_id():
    """Notifications that arrive under the new id before the alias is registered must not be lost."""
    ws = Websocket("ws://fake:9944", shutdown_timer=None)

    async def recoverer():
        await ws._dispatch_response(_sub_message("new-sub", "early"))
        return "new-sub"

    ws.register_subscription_recoverer("old-sub", recoverer)
    await ws._recover_subscriptions()

    assert "new-sub" not in ws._received_subscriptions
    early = ws._received_subscriptions["old-sub"].get_nowait()
    assert early["params"]["result"] == "early"


@pytest.mark.asyncio
async def test_recoverer_can_settle_subscription_by_injection():
    """A recoverer that resolves the subscription out-of-band injects terminal messages and returns None."""
    ws = Websocket("ws://fake:9944", shutdown_timer=None)

    async def recoverer():
        await ws.inject_subscription_message(
            "old-sub", _sub_message("old-sub", {"finalized": "0xdef"})
        )
        return None

    ws.register_subscription_recoverer("old-sub", recoverer)
    await ws._recover_subscriptions()

    settled = ws._received_subscriptions["old-sub"].get_nowait()
    assert settled["params"]["result"] == {"finalized": "0xdef"}
    assert ws._sub_alias_to_original == {}


@pytest.mark.asyncio
async def test_failed_recovery_injects_recovery_failed_message():
    """A raising recoverer must surface a recoveryFailed message for the consumer instead of vanishing."""
    ws = Websocket("ws://fake:9944", shutdown_timer=None)

    async def recoverer():
        raise SubstrateRequestException("node unreachable")

    ws.register_subscription_recoverer("old-sub", recoverer)
    await ws._recover_subscriptions()

    message = ws._received_subscriptions["old-sub"].get_nowait()
    assert message["params"]["result"] == {"recoveryFailed": "node unreachable"}
    assert "old-sub" not in ws._recovering_subscriptions


# --- Reconnection locking ------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_forced_connect_waits_for_lock():
    """
    A forced (handler-driven) reconnect must serialize on the connection lock instead of bypassing it:
    racing an unforced `connect` would otherwise create two sockets and orphan one.

    The historical reason for the bypass — `_connect_internal` recursively re-entering `connect()` on
    DNS failure, which would deadlock on the already-held lock — no longer exists; that retry is now a
    loop inside `_connect_internal` that never re-enters `connect`.
    """
    ws = Websocket("ws://fake:9944", shutdown_timer=None)
    calls = []

    async def fake_connect_internal(force):
        calls.append(force)

    ws._connect_internal = fake_connect_internal

    await ws._lock.acquire()
    try:
        task = asyncio.create_task(ws.connect(True))
        await asyncio.sleep(0.05)
        # pre-fix, force=True proceeded without the lock and would already have connected here
        assert calls == []
        assert not task.done()
    finally:
        ws._lock.release()
    await asyncio.wait_for(task, timeout=5)
    assert calls == [True]


@pytest.mark.asyncio
async def test_unforced_connect_defers_to_live_handler():
    """
    While the handler task is alive (e.g. mid-reconnect, in a backoff sleep), an unforced `connect` —
    a consumer's `__aenter__` seeing a CLOSED socket — must leave the connection alone. Pre-fix, its
    `_cancel` killed the reconnecting handler, stranding the resubmitted in-flight requests and
    skipping subscription recovery entirely.
    """
    ws = Websocket("ws://fake:9944", shutdown_timer=None, max_retries=1)
    # stands in for a live handler that is mid-reconnect
    handler = asyncio.create_task(asyncio.sleep(3600))
    ws._send_recv_task = handler

    cancel_calls = []

    async def spy_cancel():
        cancel_calls.append(True)

    ws._cancel = spy_cancel

    async def failing_resolve():
        raise socket.gaierror("connect must not attempt its own connection here")

    ws._resolve_host = failing_resolve

    try:
        await asyncio.wait_for(ws.connect(), timeout=5)
        assert cancel_calls == []
        assert not handler.done()
        assert ws.ws is None
    finally:
        handler.cancel()
        with suppress(asyncio.CancelledError):
            await handler


@pytest.mark.asyncio
async def test_reconnect_survives_concurrent_consumer_connects():
    """
    End-to-end over a loopback server: an abnormal close triggers the handler's reconnect, the pending
    request is resubmitted and answered on the new connection, and consumer-side `connect()` calls fired
    throughout the window neither deadlock against the (now lock-holding) forced reconnect nor kill the
    handler mid-reconnect.
    """
    connections = []

    async def server_handler(server_ws):
        connections.append(server_ws)
        first = len(connections) == 1
        with suppress(Exception):
            async for message in server_ws:
                request = json.loads(message)
                if first:
                    # drop the first connection abruptly instead of answering
                    await server_ws.close(code=1011, reason="restart")
                    return
                await server_ws.send(
                    json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": "ok"})
                )

    async with serve(server_handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        ws = Websocket(f"ws://127.0.0.1:{port}", shutdown_timer=None, retry_timeout=5)
        try:
            async with ws:
                handler_task = ws._send_recv_task
                item_id = await ws.send(
                    {"jsonrpc": "2.0", "method": "echo", "params": []}
                )

                async def consumer_connects():
                    for _ in range(40):
                        await ws.connect()
                        await asyncio.sleep(0.005)

                async def poll_response():
                    while True:
                        if (resp := await ws.retrieve(item_id)) is not None:
                            return resp
                        await asyncio.sleep(0.01)

                response, _ = await asyncio.wait_for(
                    asyncio.gather(poll_response(), consumer_connects()), timeout=15
                )
                assert response["result"] == "ok"
                assert len(connections) >= 2  # actually reconnected
                # the original handler survived both the reconnect and the concurrent connects
                assert ws._send_recv_task is handler_task
                assert not handler_task.done()
        finally:
            await ws.shutdown()


# --- Extrinsic recovery chain queries ------------------------------------------------------------


_EXT_HEX = "0x280403000b63ce64c10c05"
_EXT_HASH = f"0x{blake2b(bytes.fromhex(_EXT_HEX[2:]), digest_size=32).hexdigest()}"


def _block(number: int, parent: str, extrinsics: list[str]) -> dict:
    return {
        "header": {"number": hex(number), "parentHash": parent},
        "extrinsics": extrinsics,
    }


def _substrate_with_chain(blocks: dict[str, dict], heads: list[str]):
    """
    An interface whose `rpc_request` serves a static chain: `blocks` maps block hash to a
    `chain_getBlock` block, and each head request pops the next entry of `heads` (the last is sticky).
    """
    substrate = AsyncSubstrateInterface("ws://localhost", _mock=True)
    remaining_heads = list(heads)

    async def rpc_request(method, params, **kwargs):
        if method == "chain_getBlock":
            return {"result": {"block": blocks[params[0]]}}
        if method in ("chain_getBlockHash", "chain_getFinalizedHead"):
            return {
                "result": remaining_heads.pop(0)
                if len(remaining_heads) > 1
                else remaining_heads[0]
            }
        raise AssertionError(f"Unexpected RPC method {method}")

    substrate.rpc_request = rpc_request
    return substrate


@pytest.mark.asyncio
async def test_scan_recent_blocks_finds_included_extrinsic():
    blocks = {
        "0xb2": _block(2, "0xb1", []),
        "0xb1": _block(1, "0xb0", [_EXT_HEX]),
        "0xb0": _block(0, "0x00", []),
    }
    substrate = _substrate_with_chain(blocks, ["0xb2"])

    scanned: set[str] = set()
    found = await substrate._scan_recent_blocks_for_extrinsic(
        _EXT_HASH, "0xb2", 16, scanned
    )
    assert found == "0xb1"
    assert scanned == {"0xb2", "0xb1"}

    # an unknown extrinsic walks back to genesis and gives up
    assert (
        await substrate._scan_recent_blocks_for_extrinsic("0xdead", "0xb2", 16, set())
        is None
    )
    # already-scanned blocks are not re-fetched
    assert (
        await substrate._scan_recent_blocks_for_extrinsic("0xdead", "0xb2", 16, scanned)
        is None
    )


@pytest.mark.asyncio
async def test_polling_watch_sees_extrinsic_included_in_later_block(monkeypatch):
    """The polling watcher finds an extrinsic that lands in a block only after the first poll."""
    monkeypatch.setattr(
        async_substrate_interface.async_substrate,
        "EXTRINSIC_RECOVERY_POLL_INTERVAL",
        0.01,
    )
    blocks = {
        "0xb2": _block(2, "0xb1", [_EXT_HEX]),
        "0xb1": _block(1, "0xb0", []),
        "0xb0": _block(0, "0x00", []),
    }
    substrate = _substrate_with_chain(blocks, ["0xb1", "0xb2"])

    result = await substrate._wait_for_extrinsic_inclusion_via_polling(
        _EXT_HASH, False, timeout=5
    )
    assert result == {
        "block_hash": "0xb2",
        "extrinsic_hash": _EXT_HASH,
        "finalized": False,
    }


@pytest.mark.asyncio
async def test_polling_watch_times_out(monkeypatch):
    monkeypatch.setattr(
        async_substrate_interface.async_substrate,
        "EXTRINSIC_RECOVERY_POLL_INTERVAL",
        0.01,
    )
    blocks = {"0xb0": _block(0, "0x00", [])}
    substrate = _substrate_with_chain(blocks, ["0xb0"])

    with pytest.raises(SubstrateRequestException, match="not observed within"):
        await substrate._wait_for_extrinsic_inclusion_via_polling(
            _EXT_HASH, True, timeout=0.05
        )
