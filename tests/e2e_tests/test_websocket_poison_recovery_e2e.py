import subprocess

import pytest
from websockets.protocol import State

from async_substrate_interface.async_substrate import AsyncSubstrateInterface
from tests.conftest import start_docker_container
from tests.e2e_tests.test_substrate_addons import wait_for_output
from tests.helpers.async_proxy import AsyncSilenceProxy


@pytest.fixture(scope="function")
def local_chain():
    container = start_docker_container(9955, "poison")
    try:
        if not wait_for_output(container.process, "Imported #1", timeout=60):
            raise TimeoutError(
                "Docker container did not start properly - 'Imported #1' not found"
            )
        yield container
    finally:
        subprocess.run(["docker", "kill", container.name])
        container.process.kill()


@pytest.mark.asyncio
async def test_poison_pill_recovers_after_silence(local_chain):
    """
    A dead handler left on an OPEN socket (the relayed "Max retries exceeded." poison pill) must recover on the next
    call instead of failing forever.

    The AsyncSilenceProxy sits between the client and the localnet. Pausing it makes the node go silent without closing
    the socket, so the client's retries exhaust and the background handler dies while `ws.state` stays OPEN. Resuming
    and issuing one more call must transparently rebuild the connection and succeed.
    """
    proxy = await AsyncSilenceProxy(local_chain.uri).start()
    try:
        substrate = AsyncSubstrateInterface(
            proxy.url,
            retry_timeout=2.0,
            max_retries=2,
            ws_shutdown_timer=None,
        )
        try:
            # Baseline: traffic flows through the proxy.
            head = await substrate.get_chain_head()
            assert head.startswith("0x")

            # Go silent: the socket stays open but no responses come back, so the client's retries exhaust and the
            # handler dies.
            proxy.pause()
            poison = None
            try:
                await substrate.get_chain_head()
            except Exception as exc:  # noqa: BLE001
                poison = exc
            assert poison is not None
            assert "Max retries exceeded" in str(poison)

            # The exact poison condition: a finished handler on an OPEN socket.
            assert substrate.ws._send_recv_task.done()
            assert substrate.ws.state is State.OPEN

            # Resume and prove the next call rebuilds the connection and succeeds.
            proxy.resume()
            recovered = await substrate.get_chain_head()
            assert recovered.startswith("0x")
        finally:
            await substrate.close()
    finally:
        await proxy.close()
