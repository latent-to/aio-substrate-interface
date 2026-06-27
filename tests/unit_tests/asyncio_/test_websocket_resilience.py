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
"""

import asyncio
from contextlib import suppress
from unittest.mock import AsyncMock, MagicMock

import pytest
from websockets.protocol import State

from async_substrate_interface.async_substrate import (
    AsyncSubstrateInterface,
    Websocket,
)


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
