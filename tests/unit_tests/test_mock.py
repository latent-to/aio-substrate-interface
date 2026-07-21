from websockets.exceptions import InvalidURI
import pytest

from async_substrate_interface import AsyncSubstrateInterface


@pytest.mark.asyncio
async def test_async_mock():
    ssi = AsyncSubstrateInterface("notreal")
    assert isinstance(ssi, AsyncSubstrateInterface)
    with pytest.raises(InvalidURI):
        await ssi.initialize()
    async with AsyncSubstrateInterface("notreal", _mock=True) as ssi:
        assert isinstance(ssi, AsyncSubstrateInterface)
    ssi = AsyncSubstrateInterface("notreal", _mock=True)
    async with ssi:
        pass
