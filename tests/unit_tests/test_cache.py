import asyncio
import pytest
from unittest import mock

from async_substrate_interface.utils.cache import CachedFetcher, cached_fetcher


@pytest.mark.asyncio
async def test_cached_fetcher_fetches_and_caches():
    """Tests that CachedFetcher correctly fetches and caches results."""
    # Setup
    mock_method = mock.AsyncMock(side_effect=lambda x: f"result_{x}")
    fetcher = CachedFetcher(max_size=2, method=mock_method)

    # First call should trigger the method
    result1 = await fetcher("key1")
    assert result1 == "result_key1"
    mock_method.assert_awaited_once_with("key1")

    # Second call with the same key should use the cache
    result2 = await fetcher("key1")
    assert result2 == "result_key1"
    # Ensure the method was NOT called again
    assert mock_method.await_count == 1

    # Third call with a new key triggers a method call
    result3 = await fetcher("key2")
    assert result3 == "result_key2"
    assert mock_method.await_count == 2


@pytest.mark.asyncio
async def test_cached_fetcher_handles_inflight_requests():
    """Tests that CachedFetcher waits for in-flight results instead of re-fetching."""
    # Create an event to control when the mock returns
    event = asyncio.Event()

    async def slow_method(x):
        await event.wait()
        return f"slow_result_{x}"

    fetcher = CachedFetcher(max_size=2, method=slow_method)

    # Start first request
    task1 = asyncio.create_task(fetcher("key1"))
    await asyncio.sleep(0.1)  # Let the task start and be inflight

    # Second request for the same key while the first is in-flight
    task2 = asyncio.create_task(fetcher("key1"))
    await asyncio.sleep(0.1)

    # Release the inflight request
    event.set()
    result1, result2 = await asyncio.gather(task1, task2)
    assert result1 == result2 == "slow_result_key1"


@pytest.mark.asyncio
async def test_cached_fetcher_propagates_errors():
    """Tests that CachedFetcher correctly propagates errors."""

    async def error_method(x):
        raise ValueError("Boom!")

    fetcher = CachedFetcher(max_size=2, method=error_method)

    with pytest.raises(ValueError, match="Boom!"):
        await fetcher("key1")


@pytest.mark.asyncio
async def test_cached_fetcher_eviction():
    """Tests that LRU eviction works in CachedFetcher."""

    async def side_effect_method(x):
        return f"val_{x}"

    fetcher = CachedFetcher(max_size=2, method=side_effect_method)

    # Fill cache
    await fetcher("key1")
    await fetcher("key2")
    assert list(fetcher._cache.cache.keys()) == list(fetcher._cache.cache.keys())

    # Insert a new key to trigger eviction
    await fetcher("key3")
    # key1 should be evicted
    assert "key1" not in fetcher._cache.cache
    assert "key2" in fetcher._cache.cache
    assert "key3" in fetcher._cache.cache


@pytest.mark.asyncio
async def test_cached_fetcher_cache_results_false_does_not_memoize():
    """With cache_results=False, results are never stored, so each sequential call re-fetches."""
    calls = 0

    async def method(x):
        nonlocal calls
        calls += 1
        return f"result_{x}_{calls}"

    fetcher = CachedFetcher(max_size=2, method=method, cache_results=False)

    result1 = await fetcher("key1")
    result2 = await fetcher("key1")

    # The stale value is never memoized, so the second call re-fetches.
    assert calls == 2
    assert result1 != result2
    # Nothing is stored in the LRU.
    assert len(fetcher._cache.cache) == 0


@pytest.mark.asyncio
async def test_cached_fetcher_cache_results_false_still_dedups_inflight():
    """With cache_results=False, concurrent calls still share a single in-flight future."""
    calls = 0
    event = asyncio.Event()

    async def slow_method(x):
        nonlocal calls
        calls += 1
        await event.wait()
        return f"slow_{x}_{calls}"

    fetcher = CachedFetcher(max_size=2, method=slow_method, cache_results=False)

    # Two concurrent requests for the same key while the first is in-flight.
    task1 = asyncio.create_task(fetcher("key1"))
    task2 = asyncio.create_task(fetcher("key1"))
    await asyncio.sleep(0.1)

    event.set()
    result1, result2 = await asyncio.gather(task1, task2)

    # Shared a single I/O despite not being cached.
    assert result1 == result2 == "slow_key1_1"
    assert calls == 1

    # The in-flight future is gone, so a later call re-fetches (no memoization).
    result3 = await fetcher("key1")
    assert calls == 2
    assert result3 == "slow_key1_2"


@pytest.mark.asyncio
async def test_cached_fetcher_decorator_no_arg_dedup_only():
    """Mirrors get_chain_head: a no-arg method that dedups concurrent calls but never memoizes."""

    class Chain:
        def __init__(self):
            self.calls = 0
            self.event = asyncio.Event()

        @cached_fetcher(cache_key_index=None, cache_results=False)
        async def get_head(self):
            self.calls += 1
            await self.event.wait()
            return f"head_{self.calls}"

    chain = Chain()

    # Concurrent calls collapse to a single I/O.
    task1 = asyncio.create_task(chain.get_head())
    task2 = asyncio.create_task(chain.get_head())
    await asyncio.sleep(0.1)
    chain.event.set()
    result1, result2 = await asyncio.gather(task1, task2)
    assert result1 == result2 == "head_1"
    assert chain.calls == 1

    # A later call re-fetches the (now stale) chaintip rather than returning a cached value.
    result3 = await chain.get_head()
    assert result3 == "head_2"
    assert chain.calls == 2


@pytest.mark.asyncio
async def test_cached_fetcher_decorator_memoizes_by_default():
    """The cached_fetcher decorator memoizes per-instance by default (cache_results=True)."""

    class Chain:
        def __init__(self):
            self.calls = 0

        @cached_fetcher(max_size=8)
        async def fetch(self, key):
            self.calls += 1
            return f"{key}_{self.calls}"

    chain = Chain()
    result1 = await chain.fetch("a")
    result2 = await chain.fetch("a")
    # Second call with the same key is served from the cache.
    assert result1 == result2 == "a_1"
    assert chain.calls == 1

    # A new key triggers a fetch.
    await chain.fetch("b")
    assert chain.calls == 2

    # Caches are isolated per-instance.
    other = Chain()
    await other.fetch("a")
    assert other.calls == 1
