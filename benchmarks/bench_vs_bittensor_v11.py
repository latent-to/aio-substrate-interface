"""
Fair benchmark: async-substrate-interface (cyscale) vs bittensor v11 (Rust core).

Fixes the problems in ~/Downloads/test.ipynb:
  - archive endpoint (no state pruning, no aggressive storage-work rate limits)
  - NO tracemalloc during timing (it penalizes Python allocation-heavy code)
  - CPU phases feed IDENTICAL raw RPC response data to both codecs
  - key building measured for both sides (notebook excluded it for asi only)
  - matched page sizes / strategies in e2e phases
"""

import asyncio
import json
import os
import pickle
import statistics
import time
from hashlib import blake2b
from typing import Any

import bittensor as bt
from scalecodec import ScaleBytes

from async_substrate_interface.async_substrate import AsyncSubstrateInterface
from async_substrate_interface.utils.storage import StorageKey
from async_substrate_interface.utils.decoding import try_batch_decode, decode_query_map
from bittensor._transport.storage import decode_storage_values, decode_map_pairs

URL = os.getenv("RPC_ENDPOINT", "wss://archive.sub.latent.to")
# The full query_map scenario costs v11 two RPCs per 100 entries (~3min for a
# 30k-entry map over WAN; seconds against a local node). Disable with FULL_MAPS=0.
FULL_MAPS = os.getenv("FULL_MAPS", "1") == "1"
# UVLOOP=1: run under uvloop instead of the standard asyncio loop (see bottom).

N_ACCOUNTS = 10_000
REPEATS = 5
SCRATCH = "/tmp"

BASE_ACCOUNT_HEX = "d43593c715fdd31c61141abd04a99fd6822c8558854ccde39a5684e7a56da27d"


def account_params(n: int) -> list[list[str]]:
    base = int.from_bytes(bytes.fromhex(BASE_ACCOUNT_HEX), "big")
    return [["0x" + (base ^ i).to_bytes(32, "big").hex()] for i in range(n)]


def unwrap_scale(x: Any) -> Any:
    if x is None:
        return None
    if hasattr(x, "value_serialized"):
        return unwrap_scale(x.value_serialized)
    if hasattr(x, "value_object"):
        return unwrap_scale(x.value_object)
    if hasattr(x, "value"):
        return unwrap_scale(x.value)
    return x


def canonical(x: Any) -> Any:
    x = unwrap_scale(x)
    if x is None:
        return None
    if isinstance(x, bytes):
        return "0x" + x.hex()
    if isinstance(x, dict):
        return {
            str(k): canonical(v)
            for k, v in sorted(x.items(), key=lambda kv: str(kv[0]))
        }
    if isinstance(x, (tuple, list)):
        return [canonical(v) for v in x]
    return x


def digest(x: Any) -> str:
    return blake2b(
        json.dumps(
            canonical(x), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode(),
        digest_size=16,
    ).hexdigest()


results_table: list[tuple[str, float, float]] = []


def bench_pair(label: str, fn_asi, fn_v11, repeats: int = REPEATS, warmup: int = 1):
    out = {}
    for name, fn in (("asi", fn_asi), ("v11", fn_v11)):
        for _ in range(warmup):
            last = fn()
        times = []
        for _ in range(repeats):
            t0 = time.perf_counter()
            last = fn()
            times.append(time.perf_counter() - t0)
        out[name] = (statistics.median(times), min(times), last)
        print(
            f"  {label:<40} {name:<4} median={statistics.median(times) * 1000:9.1f}ms  "
            f"min={min(times) * 1000:9.1f}ms",
            flush=True,
        )
    ratio = out["asi"][0] / out["v11"][0] if out["v11"][0] else float("inf")
    print(f"  {label:<40} asi/v11 = {ratio:.2f}x", flush=True)
    results_table.append((label, out["asi"][0], out["v11"][0]))
    return out["asi"][2], out["v11"][2]


e2e_table: list[tuple[str, float, float]] = []


async def e2e_pair(label, fn_asi, fn_v11, repeats, warmup=1, pause=0.2):
    """Time two async callables end-to-end (network included).

    Repeats are interleaved with alternating order: server-side effects
    (state/trie caches, WASM instances warmed by the first execution of an
    expensive state_call) would otherwise systematically favor whichever
    library runs second.
    """
    for _ in range(warmup):
        await fn_asi()
        await fn_v11()
    times: dict[str, list[float]] = {"asi": [], "v11": []}
    last: dict[str, Any] = {"asi": None, "v11": None}
    for i in range(repeats):
        order = [("asi", fn_asi), ("v11", fn_v11)]
        if i % 2:
            order.reverse()
        for name, fn in order:
            t0 = time.perf_counter()
            last[name] = await fn()
            times[name].append(time.perf_counter() - t0)
            if pause:
                await asyncio.sleep(pause)
    for name in ("asi", "v11"):
        print(
            f"  {label:<40} {name:<4} median={statistics.median(times[name]) * 1000:9.1f}ms  "
            f"min={min(times[name]) * 1000:9.1f}ms",
            flush=True,
        )
    med_asi = statistics.median(times["asi"])
    med_v11 = statistics.median(times["v11"])
    ratio = med_asi / med_v11 if med_v11 else float("inf")
    print(f"  {label:<40} asi/v11 = {ratio:.2f}x", flush=True)
    e2e_table.append((label, med_asi, med_v11))
    return last["asi"], last["v11"]


def prefix_of(pallet: str, item: str, runtime) -> str:
    return StorageKey.create_from_storage_function(
        pallet,
        item,
        [],
        runtime_config=runtime.runtime_config,
        metadata=runtime.metadata,
    ).data.hex()


async def rpc_chunked_query_storage_at(asi, hex_keys, block_hash, chunk=2500):
    by_key = {}
    for i in range(0, len(hex_keys), chunk):
        resp = await asi.rpc_request(
            "state_queryStorageAt", [hex_keys[i : i + chunk], block_hash]
        )
        for group in resp["result"]:
            for k, v in group["changes"]:
                by_key[k] = v
    return [by_key.get(k) for k in hex_keys]


async def main():
    print(f"connecting to {URL}", flush=True)
    async with bt.Subtensor(URL, fallback_endpoints=[], archive_endpoints=[]) as client:
        async with AsyncSubstrateInterface(
            url=URL, ss58_format=42, chain_name="Bittensor"
        ) as asi:
            if pinned_bn := os.getenv("PINNED_BLOCK"):
                block_number = int(pinned_bn)
                block_hash = await asi.get_block_hash(block_number)
            else:
                block_hash = await asi.get_chain_finalised_head()
                header = await asi.rpc_request("chain_getHeader", [block_hash])
                block_number = int(header["result"]["number"], 16)
            print(f"pinned block {block_number} {block_hash}", flush=True)

            runtime = await asi.init_runtime(block_hash=block_hash)
            raw = client._substrate.raw
            codec = await raw._runtimes.codec_at(block_hash)
            entry = codec.storage_entry("System", "Account")

            params = account_params(N_ACCOUNTS)

            # ---------------- Phase 1: key building (CPU) ----------------
            print(
                "\n== Phase 1: build 10k System.Account storage keys (CPU) ==",
                flush=True,
            )

            def asi_keys_batch():
                return StorageKey.create_from_storage_function_batch(
                    "System",
                    "Account",
                    params,
                    runtime_config=runtime.runtime_config,
                    metadata=runtime.metadata,
                )

            def v11_keys():
                return codec.storage_key_batch(entry, params)

            asi_keys, v11_key_bytes = bench_pair(
                "keybuild 10k", asi_keys_batch, v11_keys
            )

            asi_key_bytes = [bytes(k.data) for k in asi_keys]
            same_keys = asi_key_bytes == [bytes(k) for k in v11_key_bytes]
            print(f"  keys byte-identical: {same_keys}", flush=True)
            assert same_keys

            # notebook-style per-key loop, one pass over 1k keys, extrapolated
            t0 = time.perf_counter()
            for p in params[:1000]:
                StorageKey.create_from_storage_function(
                    "System",
                    "Account",
                    p,
                    runtime_config=runtime.runtime_config,
                    metadata=runtime.metadata,
                )
            loop_time = time.perf_counter() - t0
            print(
                f"  (asi notebook-style per-key loop: {loop_time * 10 * 1000:.0f}ms extrapolated to 10k)",
                flush=True,
            )

            # ---------------- Phase 2: fetch raw values once ----------------
            print(
                "\n== Phase 2: fetch raw values (once, shared by both) ==", flush=True
            )
            hex_keys = ["0x" + k.hex() for k in asi_key_bytes]
            t0 = time.perf_counter()
            raw_hexes = await rpc_chunked_query_storage_at(asi, hex_keys, block_hash)
            n_found = sum(1 for r in raw_hexes if r is not None)
            print(
                f"  fetched {len(raw_hexes)} raws ({n_found} exist) in {time.perf_counter() - t0:.2f}s",
                flush=True,
            )

            with open(f"{SCRATCH}/account_raws.pkl", "wb") as f:
                pickle.dump(
                    {
                        "block_hash": block_hash,
                        "hex_keys": hex_keys,
                        "raw_hexes": raw_hexes,
                    },
                    f,
                )

            # ---------------- Phase 3: decode 10k account values (CPU) ----------------
            print(
                "\n== Phase 3: decode 10k System.Account responses (CPU, identical raw data) ==",
                flush=True,
            )

            def asi_decode():
                # mirrors query_multi's post-response processing
                pairs = [
                    (sk, ScaleBytes(rh) if rh is not None else None)
                    for sk, rh in zip(asi_keys, raw_hexes)
                ]
                return try_batch_decode(pairs, runtime)

            def v11_decode():
                # mirrors query_batch's post-response processing
                raws = [
                    bytes.fromhex(rh[2:]) if rh is not None else None
                    for rh in raw_hexes
                ]
                return decode_storage_values(codec, entry, raws)

            asi_vals, v11_vals = bench_pair(
                "decode 10k accounts", asi_decode, v11_decode
            )
            d_asi = [digest(v) for v in asi_vals]
            d_v11 = [digest(v) for v in v11_vals]
            same = d_asi == d_v11
            print(f"  decoded values identical: {same}", flush=True)
            if not same:
                for i, (a, b) in enumerate(zip(d_asi, d_v11)):
                    if a != b:
                        print(f"  first diff at {i}:")
                        print(f"    asi: {canonical(asi_vals[i])}")
                        print(f"    v11: {canonical(v11_vals[i])}")
                        break

            # ---------------- Phase 4: query_map page decode (CPU) ----------------
            for pallet, item in [
                ("System", "Account"),
                ("SubtensorModule", "Keys"),
                ("SubtensorModule", "Bonds"),
            ]:
                print(
                    f"\n== Phase 4: decode one 1000-entry query_map page of {pallet}.{item} (CPU) ==",
                    flush=True,
                )
                entry2 = codec.storage_entry(pallet, item)
                mp = runtime.metadata.get_metadata_pallet(pallet)
                si = mp.get_storage_function(item)
                value_type = si.get_value_type_string()
                param_types = si.get_params_type_string()
                key_hashers = si.get_param_hashers()
                prefix_key = StorageKey.create_from_storage_function(
                    pallet,
                    item,
                    [],
                    runtime_config=runtime.runtime_config,
                    metadata=runtime.metadata,
                )
                prefix = prefix_key.to_hex()

                keys_resp = await asi.rpc_request(
                    "state_getKeysPaged", [prefix, 1000, prefix, block_hash]
                )
                page_keys = keys_resp["result"]
                vals_resp = await asi.rpc_request(
                    "state_queryStorageAt", [page_keys, block_hash]
                )
                changes = []
                for group in vals_resp["result"]:
                    changes.extend(group["changes"])
                print(f"  page has {len(changes)} entries", flush=True)

                def asi_map_decode():
                    return decode_query_map(
                        changes,
                        prefix,
                        runtime,
                        param_types,
                        [],
                        value_type,
                        key_hashers,
                        False,
                    )

                def v11_map_decode():
                    return decode_map_pairs(
                        codec, entry2, [], [tuple(c) for c in changes]
                    )

                asi_page, v11_page = bench_pair(
                    f"map page {pallet}.{item}", asi_map_decode, v11_map_decode
                )
                da = sorted(digest(kv) for kv in asi_page)
                db = sorted(
                    digest((list(kv) if isinstance(kv, tuple) else kv))
                    for kv in [[k, v] for k, v in v11_page]
                )
                print(f"  page decode identical (unordered): {da == db}", flush=True)
                if da != db:
                    print(f"    asi sample: {canonical(asi_page[0])}")
                    print(f"    v11 sample: {canonical(list(v11_page[0]))}")

            # ---------------- Phase 5: end-to-end (network, matched) ----------------
            print(
                "\n== Phase 5: end-to-end query_batch 10k accounts (network, matched, 3 repeats) ==",
                flush=True,
            )

            async def asi_e2e():
                keys = await asi.create_storage_keys(
                    "System", "Account", params, block_hash=block_hash
                )
                return await asi.query_multi(
                    keys, block_hash=block_hash, runtime=runtime
                )

            async def v11_e2e():
                return await client._substrate.query_batch(
                    "System", "Account", params, block_hash=block_hash
                )

            await e2e_pair(
                "query_batch 10k accounts",
                asi_e2e,
                v11_e2e,
                repeats=3,
                warmup=0,
                pause=0.5,
            )

            print(
                "\n== Phase 6: end-to-end get_block (network, 5 repeats) ==", flush=True
            )
            await e2e_pair(
                "get_block",
                lambda: asi.get_block(
                    block_hash=block_hash, ignore_decoding_errors=True
                ),
                lambda: client._substrate.get_block(block_hash=block_hash),
                repeats=5,
                pause=0,
            )

            # ---------------- Phase 7: end-to-end real-world scenarios ----------------
            print(
                "\n== Phase 7: end-to-end real-world scenarios (network) ==", flush=True
            )

            # a real existing account: trailing 32 bytes of the first System.Account key
            acct_keys = await asi.rpc_request(
                "state_getKeysPaged",
                [
                    "0x" + prefix_of("System", "Account", runtime),
                    1,
                    "0x" + prefix_of("System", "Account", runtime),
                    block_hash,
                ],
            )
            real_account = "0x" + acct_keys["result"][0][-64:]

            # single storage read: the most common call in any bot/validator loop
            a_val, v_val = await e2e_pair(
                "single query (System.Account)",
                lambda: asi.query(
                    "System", "Account", [real_account], block_hash=block_hash
                ),
                lambda: client._substrate.query(
                    "System", "Account", [real_account], block_hash
                ),
                repeats=10,
                pause=0,
            )
            print(
                f"  single-query results identical: {digest(a_val) == digest(v_val)}",
                flush=True,
            )

            # sequential burst: 30 dependent reads (e.g. iterating subnets)
            netuids = list(range(30))

            async def asi_seq():
                return [
                    await asi.query(
                        "SubtensorModule", "Tempo", [n], block_hash=block_hash
                    )
                    for n in netuids
                ]

            async def v11_seq():
                return [
                    await client._substrate.query(
                        "SubtensorModule", "Tempo", [n], block_hash
                    )
                    for n in netuids
                ]

            await e2e_pair(
                "30 sequential queries (Tempo)", asi_seq, v11_seq, repeats=3, pause=0
            )

            # concurrent burst: same 30 reads via gather (websocket multiplexing)
            async def asi_gather():
                return await asyncio.gather(
                    *[
                        asi.query(
                            "SubtensorModule", "Tempo", [n], block_hash=block_hash
                        )
                        for n in netuids
                    ]
                )

            async def v11_gather():
                return await asyncio.gather(
                    *[
                        client._substrate.query(
                            "SubtensorModule", "Tempo", [n], block_hash
                        )
                        for n in netuids
                    ]
                )

            await e2e_pair(
                "30 concurrent queries (gather)",
                asi_gather,
                v11_gather,
                repeats=3,
                pause=0,
            )

            # runtime API call
            await e2e_pair(
                "runtime_call (current_alpha_price)",
                lambda: asi.runtime_call(
                    "SwapRuntimeApi", "current_alpha_price", [1], block_hash=block_hash
                ),
                lambda: client._substrate.runtime_call(
                    "SwapRuntimeApi", "current_alpha_price", [1], block_hash
                ),
                repeats=10,
                pause=0,
            )

            # events of a block (fetch + decode Vec<EventRecord>)
            await e2e_pair(
                "get_events",
                lambda: asi.get_events(block_hash=block_hash),
                lambda: client._substrate.events(block_hash),
                repeats=5,
                pause=0,
            )

            # one subnet's Keys map, page size matched at 100 for both
            async def asi_map_subnet():
                qm = await asi.query_map(
                    "SubtensorModule",
                    "Keys",
                    params=[1],
                    block_hash=block_hash,
                    page_size=100,
                )
                return [kv async for kv in qm]

            async def v11_map_subnet():
                return await client._substrate.query_map(
                    "SubtensorModule", "Keys", [1], block_hash
                )

            await e2e_pair(
                "query_map one subnet (Keys, page=100)",
                asi_map_subnet,
                v11_map_subnet,
                repeats=3,
                pause=0,
            )

            if FULL_MAPS:
                # the metagraph-style scan: each library in its idiomatic fast
                # mode (asi: fully_exhaust; v11: its built-in 100-entry pages),
                # so this measures the workflow, not a matched RPC pattern
                async def asi_map_full():
                    qm = await asi.query_map(
                        "SubtensorModule",
                        "Keys",
                        block_hash=block_hash,
                        page_size=1000,
                        fully_exhaust=True,
                    )
                    return [kv async for kv in qm]

                async def v11_map_full():
                    return await client._substrate.query_map(
                        "SubtensorModule", "Keys", None, block_hash
                    )

                # warmup=1 matters here: the first full-map scan pays the
                # node's cold trie walk (seconds), later ones read warm caches
                # (hundreds of ms). With warmup=0 whichever library runs first
                # absorbs the cold cost and the comparison is meaningless.
                a_full, v_full = await e2e_pair(
                    "query_map full (Keys, idiomatic modes)",
                    asi_map_full,
                    v11_map_full,
                    repeats=1,
                    warmup=1,
                    pause=0,
                )
                print(
                    f"  full-map sizes: asi={len(a_full)} v11={len(v_full)}", flush=True
                )

            print("\n==== E2E summary (median, asi vs v11) ====", flush=True)
            for label, a, v in e2e_table:
                print(
                    f"  {label:<40} asi={a * 1000:9.1f}ms  v11={v * 1000:9.1f}ms  ratio={a / v if v else float('inf'):.2f}x",
                    flush=True,
                )

            print("\n==== CPU summary (median, asi vs v11) ====", flush=True)
            for label, a, v in results_table:
                print(
                    f"  {label:<40} asi={a * 1000:9.1f}ms  v11={v * 1000:9.1f}ms  ratio={a / v if v else float('inf'):.2f}x",
                    flush=True,
                )


# UVLOOP=1 runs the whole comparison under uvloop. Both libraries share the
# loop, so this measures the stack's behavior under the recommended loop, not
# an asi-only advantage.
if os.getenv("UVLOOP", "0") == "1":
    import uvloop

    uvloop.run(main())
else:
    asyncio.run(main())
