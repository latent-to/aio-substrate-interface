"""Isolate asi's per-RPC overhead using an instant-answer local JSON-RPC server."""

import asyncio
import json
import statistics
import time

from websockets.asyncio.server import serve

from async_substrate_interface.async_substrate import AsyncSubstrateInterface

PORT = 9977


async def handler(ws):
    async for raw in ws:
        msg = json.loads(raw)
        msgs = msg if isinstance(msg, list) else [msg]
        out = [{"jsonrpc": "2.0", "id": m["id"], "result": {"ok": True}} for m in msgs]
        await ws.send(json.dumps(out if isinstance(msg, list) else out[0]))


async def main():
    async with serve(handler, "127.0.0.1", PORT):
        asi = AsyncSubstrateInterface(url=f"ws://127.0.0.1:{PORT}", ss58_format=42)
        # warm the connection
        await asi.rpc_request("warmup", [])

        times = []
        for _ in range(300):
            t0 = time.perf_counter()
            await asi.rpc_request("bench_method", [])
            times.append(time.perf_counter() - t0)
        print(
            f"local sequential rpc_request x300: median={statistics.median(times) * 1000:7.3f}ms  "
            f"p95={sorted(times)[int(len(times) * 0.95)] * 1000:7.3f}ms  "
            f"total={sum(times) * 1000:8.1f}ms"
        )
        await asi.close()


asyncio.run(main())
