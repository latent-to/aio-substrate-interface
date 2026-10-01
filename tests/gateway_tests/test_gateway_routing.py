"""
Black-box tests for a gateway that serves a lite node and an archive node behind one
endpoint (default route: lite; archive only when the lite node cannot answer).

Not part of CI. All tests skip unless GATEWAY_ENDPOINT is set:

    GATEWAY_ENDPOINT=wss://gateway.example:443 uv run pytest tests/gateway_tests/ -v

Most checks are self-verifying, so no reference node is needed: `System.Number` read at
the hash of block N must decode to N, `System.ParentHash` to the hash of N-1, and so on.
A wrong route shows as `StateDiscardedError` (lite node asked for pruned state), and a
response paired with the wrong request shows as a wrong number.

Controls (run these first to prove the suite itself):
    GATEWAY_ENDPOINT=<archive node>  -> all tests pass, except the route tests (GW-04, GW-6x)
    GATEWAY_ENDPOINT=<lite node>     -> every test that needs archive state fails

Env vars:
    GATEWAY_ENDPOINT          endpoint under test (required)
    ARCHIVE_ENDPOINT          direct archive node, for reference comparison (see settings.py)
    RPC_ENDPOINT              direct lite node, for reference comparison (see settings.py)
    GATEWAY_PRUNING_WINDOW    state pruning window of the lite node, in blocks (default 256)
    GATEWAY_OLD_DEPTH         depth of the standard "old" block (default 1000)
    GATEWAY_DEEP_DEPTH        depth of the "deep" block (default 100000)
    GATEWAY_SLOW              "1" enables the slow tests (several minutes)
    GATEWAY_ROUTE_FIELD       name of a top-level field the gateway adds to each JSON-RPC
                              response to name the backend that served it; enables GW-6x
    GATEWAY_ROUTE_LITE        value of that field for the lite node (default "lite")
    GATEWAY_ROUTE_ARCHIVE     value of that field for the archive node (default "archive")
    GATEWAY_TEST_KEY_URI      key URI/mnemonic of a funded account; enables GW-70
                              (submits one `System.remark`, which costs a fee)
"""

import asyncio
import contextlib
import os
from types import SimpleNamespace

import pytest
import pytest_asyncio

from async_substrate_interface import AsyncSubstrateInterface
from async_substrate_interface.errors import SubstrateRequestException
from async_substrate_interface.substrate_addons import RetryAsyncSubstrate
from tests.helpers.settings import ARCHIVE_ENTRYPOINT, LATENT_LITE_ENTRYPOINT

GATEWAY_ENDPOINT = os.getenv("GATEWAY_ENDPOINT", "")
PRUNING_WINDOW = int(os.getenv("GATEWAY_PRUNING_WINDOW", "256"))
OLD_DEPTH = int(os.getenv("GATEWAY_OLD_DEPTH", "1000"))
DEEP_DEPTH = int(os.getenv("GATEWAY_DEEP_DEPTH", "100000"))
SLOW = os.getenv("GATEWAY_SLOW") == "1"
ROUTE_FIELD = os.getenv("GATEWAY_ROUTE_FIELD")
ROUTE_LITE = os.getenv("GATEWAY_ROUTE_LITE", "lite")
ROUTE_ARCHIVE = os.getenv("GATEWAY_ROUTE_ARCHIVE", "archive")
TEST_KEY_URI = os.getenv("GATEWAY_TEST_KEY_URI", "")

RECENT_DEPTH = 5
# Depths on each side of the pruning boundary, then well past it.
BOUNDARY_DEPTHS = sorted(
    {
        RECENT_DEPTH,
        PRUNING_WINDOW - 56,
        PRUNING_WINDOW - 6,
        PRUNING_WINDOW - 1,
        PRUNING_WINDOW,
        PRUNING_WINDOW + 1,
        PRUNING_WINDOW + 2,
        PRUNING_WINDOW + 44,
        2 * PRUNING_WINDOW,
        OLD_DEPTH,
        DEEP_DEPTH,
    }
)
# No single request is permitted to hang: a gateway that cannot classify a request
# must fail fast.
REQUEST_TIMEOUT = 60

pytestmark = pytest.mark.skipif(
    not GATEWAY_ENDPOINT, reason="GATEWAY_ENDPOINT is not set"
)
slow = pytest.mark.skipif(not SLOW, reason="set GATEWAY_SLOW=1 to run")


@pytest_asyncio.fixture(scope="module")
async def gateway():
    """One long-lived connection, shared so the tests exercise a multiplexed socket."""
    async with AsyncSubstrateInterface(GATEWAY_ENDPOINT) as substrate:
        yield substrate


@pytest_asyncio.fixture(scope="module")
async def chain(gateway):
    """Block numbers/hashes all tests agree on, anchored at the finalized head."""
    finalized_hash = (await gateway.rpc_request("chain_getFinalizedHead", []))["result"]
    finalized = await gateway.get_block_number(finalized_hash)

    def at_depth(depth: int) -> int:
        return max(finalized - depth, 1)

    async def hash_at_depth(depth: int) -> str:
        return await raw_block_hash(gateway, at_depth(depth))

    return SimpleNamespace(
        genesis=await raw_block_hash(gateway, 0),
        finalized=finalized,
        at_depth=at_depth,
        hash_at_depth=hash_at_depth,
        recent=at_depth(RECENT_DEPTH),
        recent_hash=await hash_at_depth(RECENT_DEPTH),
        old=at_depth(OLD_DEPTH),
        old_hash=await hash_at_depth(OLD_DEPTH),
        deep=at_depth(DEEP_DEPTH),
        deep_hash=await hash_at_depth(DEEP_DEPTH),
        number_key=(await gateway.create_storage_key("System", "Number")).to_hex(),
        events_key=(await gateway.create_storage_key("System", "Events")).to_hex(),
    )


async def _reference(url: str, chain):
    """Direct connection to a backend node; skips if it is another chain or unreachable."""
    try:
        substrate = AsyncSubstrateInterface(url)
        await asyncio.wait_for(substrate.initialize(), REQUEST_TIMEOUT)
    except Exception as e:
        pytest.skip(f"reference node {url} is not reachable: {e!r}")
    if await raw_block_hash(substrate, 0) != chain.genesis:
        await substrate.close()
        pytest.skip(f"reference node {url} is not on the same chain as the gateway")
    return substrate


@pytest_asyncio.fixture(scope="module")
async def archive_ref(chain):
    substrate = await _reference(ARCHIVE_ENTRYPOINT, chain)
    yield substrate
    await substrate.close()


@pytest_asyncio.fixture(scope="module")
async def lite_ref(chain):
    substrate = await _reference(LATENT_LITE_ENTRYPOINT, chain)
    yield substrate
    await substrate.close()


async def raw_block_hash(substrate, block_number: int) -> str:
    """`chain_getBlockHash` without the library cache in front of it."""
    block_hash = (await substrate.rpc_request("chain_getBlockHash", [block_number]))[
        "result"
    ]
    assert block_hash is not None, f"no hash for block {block_number}"
    return block_hash


async def number_at(substrate, block_hash: str) -> int:
    return await asyncio.wait_for(
        substrate.query("System", "Number", block_hash=block_hash), REQUEST_TIMEOUT
    )


# ---------------------------------------------------------------------------
# GW-0x: default path (lite node)
# ---------------------------------------------------------------------------


async def test_gw01_identity_matches_backends(gateway, chain, archive_ref, lite_ref):
    """Gateway, lite and archive are the same chain and the same runtime."""
    for ref in (archive_ref, lite_ref):
        assert await raw_block_hash(ref, 0) == chain.genesis
        assert ref.chain == gateway.chain
        ours = await gateway.get_block_runtime_info(chain.recent_hash)
        theirs = await ref.get_block_runtime_info(chain.recent_hash)
        assert ours["specVersion"] == theirs["specVersion"]


async def test_gw02_head_state_query(gateway, chain):
    """State query with no block hash (lite path)."""
    assert await gateway.query("System", "Number") >= chain.finalized


async def test_gw03_recent_block_state_query(gateway, chain):
    """State query at a block inside the pruning window (lite path)."""
    assert await number_at(gateway, chain.recent_hash) == chain.recent


async def test_gw04_default_route_is_not_archive(gateway, archive_ref):
    """
    A request with no block context is not served by the archive node. Compares libp2p
    peer ids; not valid if ARCHIVE_ENDPOINT is itself a pool of nodes.
    """
    try:
        archive_peer = (await archive_ref.rpc_request("system_localPeerId", []))[
            "result"
        ]
    except SubstrateRequestException as e:
        pytest.skip(f"reference node does not serve system_localPeerId: {e}")
    for _ in range(5):
        peer = (await gateway.rpc_request("system_localPeerId", []))["result"]
        assert peer != archive_peer


async def test_gw05_head_does_not_go_backwards(gateway):
    """
    Best and finalized heads never move backwards between consecutive requests. A
    gateway that alternates between backends at different heights fails this.
    """
    best, finalized = [], []
    for _ in range(30):
        header = (await gateway.rpc_request("chain_getHeader", []))["result"]
        best.append(int(header["number"], 16))
        fin_hash = (await gateway.rpc_request("chain_getFinalizedHead", []))["result"]
        finalized.append(await gateway.get_block_number(fin_hash))
        assert finalized[-1] <= best[-1]
        await asyncio.sleep(0.5)
    assert finalized == sorted(finalized)
    # the best head can step back one block on a fork; more than that is a backend flip
    assert all(b - a >= -1 for a, b in zip(best, best[1:])), best


# ---------------------------------------------------------------------------
# GW-1x: requests that need archive state
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("depth", BOUNDARY_DEPTHS)
async def test_gw10_state_query_at_depth(chain, depth):
    """
    `state_getRuntimeVersion` + `state_getStorage` at blocks on each side of the pruning
    boundary. Uses a new connection, so no library cache hides a request.
    """
    block = chain.at_depth(depth)
    async with AsyncSubstrateInterface(GATEWAY_ENDPOINT) as substrate:
        block_hash = await raw_block_hash(substrate, block)
        assert await number_at(substrate, block_hash) == block


async def test_gw11_old_block_decode(gateway, chain):
    """`chain_getBlock` + runtime/metadata at an old block."""
    block = await gateway.get_block(block_number=chain.old)
    assert block["header"]["number"] == chain.old
    assert len(block["extrinsics"]) > 0


async def test_gw12_old_block_events(gateway, chain):
    """`System.Events` (a large storage value) at an old block."""
    events = await gateway.get_events(block_hash=chain.old_hash)
    assert len(events) > 0


async def test_gw13_old_runtime_call(gateway, chain):
    """`state_call` at an old block."""
    version = await gateway.runtime_call("Core", "version", block_hash=chain.old_hash)
    expected = await gateway.get_block_runtime_info(chain.old_hash)
    assert version["spec_version"] == expected["specVersion"]


async def test_gw14_old_query_map_paged(gateway, chain):
    """
    `state_getKeysPaged` + `state_queryStorageAt` over more than one page at an old
    block. All pages must come from the same state.
    """
    result = await gateway.query_map(
        "System", "BlockHash", block_hash=chain.old_hash, page_size=50, max_results=120
    )
    records = [record async for record in result]
    assert len(records) == min(120, chain.old)
    for number, block_hash in records[::20]:
        assert number < chain.old
        assert block_hash == await raw_block_hash(gateway, number)


async def test_gw15_old_query_map_fully_exhaust(gateway, chain):
    """
    Whole-map read at an old block: `state_getPairs` if served, else `state_getKeys` +
    concurrent `state_queryStorageAt` requests.
    """
    result = await gateway.query_map(
        "System", "BlockHash", block_hash=chain.old_hash, fully_exhaust=True
    )
    records = result.records
    assert len(records) >= min(PRUNING_WINDOW, chain.old)
    for number, block_hash in records[::200]:
        assert block_hash == await raw_block_hash(gateway, number)


async def test_gw16_old_query_multi(gateway, chain):
    """`state_queryStorageAt` with more than one key at an old block."""
    keys = [
        await gateway.create_storage_key("System", "Number", block_hash=chain.old_hash),
        await gateway.create_storage_key(
            "System", "ParentHash", block_hash=chain.old_hash
        ),
    ]
    result = await gateway.query_multi(keys, block_hash=chain.old_hash)
    values = {key.storage_function: value for key, value in result}
    assert values["Number"] == chain.old
    assert values["ParentHash"] == await raw_block_hash(gateway, chain.old - 1)


async def test_gw17_deep_metadata(gateway, chain):
    """Runtime version + full metadata blob (a multi-megabyte frame) at a deep block."""
    version = await gateway.get_block_runtime_info(chain.deep_hash)
    assert version["specVersion"] > 0
    metadata = (await gateway.rpc_request("state_getMetadata", [chain.deep_hash]))[
        "result"
    ]
    assert metadata.startswith("0x6d657461")  # "meta"
    assert len(metadata) > 100_000


async def test_gw18_old_extrinsic_receipt(gateway, chain):
    """Extrinsic lookup by `<block>-<index>`: block body + events at an old block."""
    receipt = await gateway.retrieve_extrinsic_by_identifier(f"{chain.old}-0")
    assert await receipt.is_success is True
    assert len(await receipt.triggered_events) > 0


# ---------------------------------------------------------------------------
# GW-2x: pruning boundary and block-age edge cases
# ---------------------------------------------------------------------------


@slow
async def test_gw20_block_ages_out_of_pruning_window(gateway, chain):
    """
    Follow one block while it moves from inside the pruning window to outside it. The
    route has to change from lite to archive with no failed request in between.
    """
    start_depth = PRUNING_WINDOW - 5
    block = chain.at_depth(start_depth)
    block_hash = await raw_block_hash(gateway, block)
    while True:
        assert await number_at(gateway, block_hash) == block
        depth = await gateway.get_block_number() - block
        if depth > PRUNING_WINDOW + 10:
            break
        await asyncio.sleep(3)


async def test_gw21_state_at_fresh_head(gateway):
    """
    Read the best head, then immediately read state at that hash. Fails if the hash goes
    to a backend that has not imported the block yet.
    """
    for _ in range(20):
        header_hash = (await gateway.rpc_request("chain_getHead", []))["result"]
        header = (await gateway.rpc_request("chain_getHeader", [header_hash]))["result"]
        assert header is not None, f"gateway does not know its own head {header_hash}"
        assert await number_at(gateway, header_hash) == int(header["number"], 16)
        await asyncio.sleep(1)


async def test_gw22_state_at_fresh_finalized_head(gateway):
    """Same as GW-21 for the finalized head."""
    for _ in range(10):
        fin_hash = (await gateway.rpc_request("chain_getFinalizedHead", []))["result"]
        header = (await gateway.rpc_request("chain_getHeader", [fin_hash]))["result"]
        assert header is not None
        assert await number_at(gateway, fin_hash) == int(header["number"], 16)
        await asyncio.sleep(1)


async def test_gw23_unknown_block_hash_fails_fast(gateway, chain):
    """
    A hash that is on neither backend gives the usual node answer (null header, RPC
    error for state) and does not hang or close the socket.
    """
    unknown = "0x" + "ab" * 32
    header = await asyncio.wait_for(
        gateway.rpc_request("chain_getHeader", [unknown]), REQUEST_TIMEOUT
    )
    assert header["result"] is None
    with pytest.raises(SubstrateRequestException):
        await asyncio.wait_for(
            gateway.rpc_request("state_getStorage", [chain.number_key, unknown]),
            REQUEST_TIMEOUT,
        )
    with pytest.raises(SubstrateRequestException):
        await asyncio.wait_for(
            gateway.rpc_request("state_getStorage", [chain.number_key, "0x1234"]),
            REQUEST_TIMEOUT,
        )
    # the connection is still usable
    assert await number_at(gateway, chain.old_hash) == chain.old


# ---------------------------------------------------------------------------
# GW-3x: multiplexing, subscriptions, connections
# ---------------------------------------------------------------------------


async def test_gw30_concurrent_mixed_requests_one_socket(gateway, chain):
    """
    Interleaved lite and archive requests in flight on one socket. Each response must
    come back under the id of its own request.
    """
    blocks = []
    for i in range(30):
        blocks.append(chain.at_depth(RECENT_DEPTH + i))
        blocks.append(chain.at_depth(OLD_DEPTH + i))
    hashes = await asyncio.gather(*(raw_block_hash(gateway, b) for b in blocks))
    numbers = await asyncio.gather(*(number_at(gateway, h) for h in hashes))
    assert list(numbers) == blocks


async def test_gw31_batch_frame_at_old_block(gateway, chain):
    """One JSON-RPC batch frame (array of requests) where all members need the archive."""
    results = await gateway.runtime_calls(
        [("Core", "version", None)] * 5, block_hash=chain.old_hash
    )
    expected = await gateway.get_block_runtime_info(chain.old_hash)
    assert [r["spec_version"] for r in results] == [expected["specVersion"]] * 5


async def test_gw32_batch_frame_mixed_backends(gateway, chain):
    """
    One JSON-RPC batch frame with head, recent and old members. The gateway has to split
    the batch (or send all of it to the archive) and return one response per id.
    """
    targets = [None, chain.recent_hash, chain.old_hash, chain.deep_hash] * 3
    payloads = [
        {
            "jsonrpc": "2.0",
            "method": "state_getStorage",
            "params": [chain.number_key, block_hash],
        }
        for block_hash in targets
    ]
    async with gateway.ws as ws:
        item_ids = await ws.send_batch(payloads)
        responses = await asyncio.wait_for(
            asyncio.gather(*(ws.wait_for_response(i) for i in item_ids)),
            REQUEST_TIMEOUT,
        )
    expected = [None, chain.recent, chain.old, chain.deep] * 3
    for response, block in zip(responses, expected):
        assert "error" not in response, response
        number = int.from_bytes(bytes.fromhex(response["result"][2:]), "little")
        if block is None:
            assert number >= chain.finalized
        else:
            assert number == block


async def test_gw33_head_subscription_with_archive_traffic(gateway, chain):
    """
    A `chain_subscribeNewHeads` subscription stays alive and in order while archive
    requests use the same socket.
    """
    seen: list[int] = []

    async def handler(obj, *_):
        number = obj["header"]["number"]
        seen.append(number)
        if len(seen) >= 4:
            return seen

    async def archive_traffic():
        i = 0
        while True:
            block = chain.at_depth(OLD_DEPTH + i % 50)
            assert (
                await number_at(gateway, await raw_block_hash(gateway, block)) == block
            )
            i += 1
            await asyncio.sleep(0.2)

    traffic = asyncio.create_task(archive_traffic())
    try:
        result = await asyncio.wait_for(
            gateway.subscribe_block_headers(handler), REQUEST_TIMEOUT * 3
        )
    finally:
        traffic.cancel()
    # re-raises if an archive request failed; cancellation is the normal end
    with contextlib.suppress(asyncio.CancelledError):
        await traffic
    assert len(result) == 4
    assert all(b - a in (0, 1) for a, b in zip(result, result[1:])), result


async def test_gw34_storage_subscription_with_archive_traffic(gateway, chain):
    """
    Same as GW-33 for `state_subscribeStorage`, with the archive request made from
    inside the subscription callback.
    """
    seen: list[int] = []

    async def handler(storage_key, value, subscription_id):
        seen.append(value)
        assert await number_at(gateway, chain.old_hash) == chain.old
        if len(seen) >= 3:
            return seen

    key = await gateway.create_storage_key("System", "Number")
    result = await asyncio.wait_for(
        gateway.subscribe_storage([key], handler), REQUEST_TIMEOUT * 3
    )
    assert result == seen
    assert len(seen) == 3
    assert seen == sorted(set(seen))


async def test_gw35_many_connections(chain):
    """Several client sockets at the same time, each with lite and archive requests."""

    async def client(i: int):
        async with AsyncSubstrateInterface(GATEWAY_ENDPOINT) as substrate:
            old = chain.at_depth(OLD_DEPTH + i)
            recent = chain.at_depth(RECENT_DEPTH + i)
            assert (
                await number_at(substrate, await raw_block_hash(substrate, old)) == old
            )
            assert (
                await number_at(substrate, await raw_block_hash(substrate, recent))
                == recent
            )

    await asyncio.wait_for(
        asyncio.gather(*(client(i) for i in range(10))), REQUEST_TIMEOUT * 2
    )


async def test_gw36_reconnect_after_client_idle_close(chain):
    """The library closes an idle socket (5 s default); the next request reconnects."""
    async with AsyncSubstrateInterface(GATEWAY_ENDPOINT) as substrate:
        assert await number_at(substrate, chain.old_hash) == chain.old
        await asyncio.sleep(8)
        assert await number_at(substrate, chain.old_hash) == chain.old
        assert await number_at(substrate, chain.recent_hash) == chain.recent


@slow
async def test_gw37_long_idle_socket_stays_usable(chain):
    """
    A socket the client keeps open and idle for 90 s still reaches both backends. Finds
    idle timeouts on the gateway's upstream connections.
    """
    async with AsyncSubstrateInterface(
        GATEWAY_ENDPOINT, ws_shutdown_timer=None
    ) as substrate:
        assert await number_at(substrate, chain.old_hash) == chain.old
        await asyncio.sleep(90)
        assert await number_at(substrate, chain.old_hash) == chain.old
        assert await number_at(substrate, chain.recent_hash) == chain.recent


# ---------------------------------------------------------------------------
# GW-4x: library compatibility
# ---------------------------------------------------------------------------


async def test_gw40_no_client_side_archive_failover(chain):
    """
    `RetryAsyncSubstrate` with no `archive_nodes` reads old state. `StateDiscardedError`
    must never reach the client, or this raises `MaxRetriesExceeded`.
    """
    async with RetryAsyncSubstrate(GATEWAY_ENDPOINT) as substrate:
        block = await substrate.get_block(block_number=chain.old)
        assert block is not None
        assert block["header"]["number"] == chain.old


async def test_gw41_method_support_is_same_at_all_depths(gateway, chain):
    """
    The library probes `state_getPairs` once and caches the answer for the connection.
    The method must be served for all block ages, or for none.
    """

    async def served(block_hash) -> bool:
        try:
            result = await gateway.rpc_request(
                "state_getPairs", [chain.number_key, block_hash]
            )
        except SubstrateRequestException:
            return False
        return isinstance(result["result"], list)

    at_head = await served(None)
    assert await served(chain.recent_hash) == at_head
    assert await served(chain.old_hash) == at_head


async def test_gw42_rpc_methods_cover_lite_node(gateway, lite_ref):
    """The gateway serves each method the lite node serves."""
    ours = set((await gateway.rpc_request("rpc_methods", []))["result"]["methods"])
    theirs = set((await lite_ref.rpc_request("rpc_methods", []))["result"]["methods"])
    assert not theirs - ours, f"missing on gateway: {sorted(theirs - ours)}"


# ---------------------------------------------------------------------------
# GW-5x: results equal to a direct read from the backend
# ---------------------------------------------------------------------------

REFERENCE_CALLS = [
    ("state_getStorage", lambda c, h: [c.events_key, h]),
    ("state_getRuntimeVersion", lambda c, h: [h]),
    ("state_getKeysPaged", lambda c, h: [c.events_key[:34], 50, c.events_key[:34], h]),
    ("state_queryStorageAt", lambda c, h: [[c.number_key, c.events_key], h]),
    ("chain_getBlock", lambda c, h: [h]),
    ("chain_getHeader", lambda c, h: [h]),
]


@pytest.mark.parametrize(
    "method,params", REFERENCE_CALLS, ids=lambda v: v if isinstance(v, str) else ""
)
@pytest.mark.parametrize("target", ["old", "deep"])
async def test_gw50_matches_archive_reference(
    gateway, chain, archive_ref, method, params, target
):
    """Old-block results are byte-identical to the archive node's own answer."""
    block_hash = getattr(chain, f"{target}_hash")
    ours = await gateway.rpc_request(method, params(chain, block_hash))
    theirs = await archive_ref.rpc_request(method, params(chain, block_hash))
    assert ours["result"] == theirs["result"]


@pytest.mark.parametrize(
    "method,params", REFERENCE_CALLS, ids=lambda v: v if isinstance(v, str) else ""
)
async def test_gw51_matches_lite_reference(gateway, chain, lite_ref, method, params):
    """Recent-block results are byte-identical to the lite node's own answer."""
    ours = await gateway.rpc_request(method, params(chain, chain.recent_hash))
    theirs = await lite_ref.rpc_request(method, params(chain, chain.recent_hash))
    assert ours["result"] == theirs["result"]


# ---------------------------------------------------------------------------
# GW-6x: route assertions (need GATEWAY_ROUTE_FIELD)
# ---------------------------------------------------------------------------

ROUTE_CASES = [
    # (id, method, params builder, backend that must serve it)
    ("system_chain", "system_chain", lambda c: [], ROUTE_LITE),
    ("rpc_methods", "rpc_methods", lambda c: [], ROUTE_LITE),
    ("head", "chain_getHead", lambda c: [], ROUTE_LITE),
    ("finalized_head", "chain_getFinalizedHead", lambda c: [], ROUTE_LITE),
    ("header_at_head", "chain_getHeader", lambda c: [], ROUTE_LITE),
    ("storage_at_head", "state_getStorage", lambda c: [c.number_key], ROUTE_LITE),
    ("runtime_version_at_head", "state_getRuntimeVersion", lambda c: [], ROUTE_LITE),
    (
        "storage_recent",
        "state_getStorage",
        lambda c: [c.number_key, c.recent_hash],
        ROUTE_LITE,
    ),
    (
        "call_recent",
        "state_call",
        lambda c: ["Core_version", "0x", c.recent_hash],
        ROUTE_LITE,
    ),
    (
        "storage_old",
        "state_getStorage",
        lambda c: [c.number_key, c.old_hash],
        ROUTE_ARCHIVE,
    ),
    (
        "storage_deep",
        "state_getStorage",
        lambda c: [c.number_key, c.deep_hash],
        ROUTE_ARCHIVE,
    ),
    (
        "call_old",
        "state_call",
        lambda c: ["Core_version", "0x", c.old_hash],
        ROUTE_ARCHIVE,
    ),
    (
        "runtime_version_old",
        "state_getRuntimeVersion",
        lambda c: [c.old_hash],
        ROUTE_ARCHIVE,
    ),
    (
        "query_storage_old",
        "state_queryStorageAt",
        lambda c: [[c.number_key], c.old_hash],
        ROUTE_ARCHIVE,
    ),
    (
        "keys_paged_old",
        "state_getKeysPaged",
        lambda c: [c.number_key, 10, c.number_key, c.old_hash],
        ROUTE_ARCHIVE,
    ),
]


@pytest.mark.skipif(not ROUTE_FIELD, reason="GATEWAY_ROUTE_FIELD is not set")
@pytest.mark.parametrize(
    "method,params,backend",
    [pytest.param(*case[1:], id=case[0]) for case in ROUTE_CASES],
)
async def test_gw60_route(gateway, chain, method, params, backend):
    """Each request class is served by the cheapest backend that can answer it."""
    response = await gateway.rpc_request(method, params(chain))
    assert ROUTE_FIELD in response, (
        f"gateway did not add '{ROUTE_FIELD}' to the response"
    )
    assert response[ROUTE_FIELD] == backend


@pytest.mark.skipif(not ROUTE_FIELD, reason="GATEWAY_ROUTE_FIELD is not set")
async def test_gw61_archive_use_does_not_stick(gateway, chain):
    """After an archive request, the same socket goes back to the lite node."""
    for _ in range(5):
        old = await gateway.rpc_request(
            "state_getStorage", [chain.number_key, chain.old_hash]
        )
        head = await gateway.rpc_request("state_getStorage", [chain.number_key])
        assert old[ROUTE_FIELD] == ROUTE_ARCHIVE
        assert head[ROUTE_FIELD] == ROUTE_LITE


# ---------------------------------------------------------------------------
# GW-7x: extrinsic submission (needs GATEWAY_TEST_KEY_URI; costs a fee)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not TEST_KEY_URI, reason="GATEWAY_TEST_KEY_URI is not set")
async def test_gw70_submit_and_watch_extrinsic(gateway, chain):
    """
    `author_submitAndWatchExtrinsic` subscription through the gateway, then the receipt
    reads (block + events at the inclusion block). The nonce source and the transaction
    pool must be the same backend: the pending extrinsic has to raise the next index.
    """
    from bittensor_wallet.keypair import Keypair

    keypair = Keypair.create_from_uri(TEST_KEY_URI)
    nonce_before = await gateway.get_account_next_index(keypair.ss58_address)
    call = await gateway.compose_call("System", "remark", {"remark": "0x6777"})
    extrinsic = await gateway.create_signed_extrinsic(call, keypair)
    receipt = await asyncio.wait_for(
        gateway.submit_extrinsic(extrinsic, wait_for_inclusion=True),
        REQUEST_TIMEOUT * 2,
    )
    assert await receipt.is_success, await receipt.error_message
    pool_nonce = (
        await gateway.rpc_request("system_accountNextIndex", [keypair.ss58_address])
    )["result"]
    assert pool_nonce == nonce_before + 1
    # an archive request directly after the submission still works on this socket
    assert await number_at(gateway, chain.old_hash) == chain.old
