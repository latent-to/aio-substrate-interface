"""
Benchmark: batched `runtime_calls` vs `asyncio.gather` of individual `runtime_call`s.

128 calls of ("SwapRuntimeApi", "current_alpha_price", [N]) for N in 0..127,
all pinned to the same finalised block so both paths do identical work.
"""

import asyncio
import statistics
import time

from async_substrate_interface.async_substrate import AsyncSubstrateInterface
from tests.helpers.settings import ARCHIVE_ENTRYPOINT

N_CALLS = 128
TRIALS = 7


async def run():
    sub = AsyncSubstrateInterface(
        ARCHIVE_ENTRYPOINT,
        ss58_format=42,
        chain_name="Bittensor",
        ws_shutdown_timer=None,
    )
    await sub.initialize()

    calls = [("SwapRuntimeApi", "current_alpha_price", [n]) for n in range(N_CALLS)]

    try:
        block_hash = await sub.get_chain_finalised_head()
        # Warm up init_runtime/metadata for this block so neither path pays for it.
        await sub.init_runtime(block_hash=block_hash)

        # Correctness check first: both paths must agree.
        batched = await sub.runtime_calls(calls, block_hash=block_hash)
        gathered = await asyncio.gather(
            *[
                sub.runtime_call(api, method, params=params, block_hash=block_hash)
                for api, method, params in calls
            ]
        )
        assert batched == gathered, "batched and gathered results differ!"
        assert len(batched) == N_CALLS
        print(f"correctness OK: {N_CALLS} calls, results identical\n")

        batch_times = []
        gather_times = []
        for i in range(TRIALS):
            t0 = time.perf_counter()
            await sub.runtime_calls(calls, block_hash=block_hash)
            batch_times.append(time.perf_counter() - t0)

            t0 = time.perf_counter()
            await asyncio.gather(
                *[
                    sub.runtime_call(api, method, params=params, block_hash=block_hash)
                    for api, method, params in calls
                ]
            )
            gather_times.append(time.perf_counter() - t0)
            print(
                f"trial {i + 1}: batch={batch_times[-1] * 1000:8.1f} ms   "
                f"gather={gather_times[-1] * 1000:8.1f} ms"
            )

        def report(name, times):
            print(
                f"  {name:7s} median={statistics.median(times) * 1000:8.1f} ms   "
                f"min={min(times) * 1000:8.1f} ms   "
                f"max={max(times) * 1000:8.1f} ms"
            )

        print(f"\n=== {N_CALLS} calls, {TRIALS} trials ===")
        report("batch", batch_times)
        report("gather", gather_times)
        bm = statistics.median(batch_times)
        gm = statistics.median(gather_times)
        faster = "batch" if bm < gm else "gather"
        ratio = max(bm, gm) / min(bm, gm)
        print(f"\n{faster} is {ratio:.2f}x faster (median)")
    finally:
        await sub.close()


if __name__ == "__main__":
    asyncio.run(run())
