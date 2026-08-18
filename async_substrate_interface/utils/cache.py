import asyncio
import inspect
import weakref
from collections import OrderedDict
import logging
import os
import pickle
from typing import Callable, Any, Awaitable, Hashable, Optional


SUBSTRATE_CACHE_METHOD_SIZE = int(os.getenv("SUBSTRATE_CACHE_METHOD_SIZE", "512"))

logger = logging.getLogger("async_substrate_interface")


class LRUCache:
    """
    Basic Least-Recently-Used Cache, with simple methods `set` and `get`
    """

    def __init__(self, max_size: int):
        self.max_size = max_size
        self.cache: OrderedDict = OrderedDict()

    def set(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.max_size:
            self.cache.popitem(last=False)

    def get(self, key):
        if key in self.cache:
            # Mark as recently used
            self.cache.move_to_end(key)
            return self.cache[key]
        return None


class CachedFetcher:
    """
    Async caching class that allows the standard async LRU cache system, but also allows for concurrent
    asyncio calls (with the same args) to use the same result of a single call.

    This should only be used for asyncio calls where the result is immutable.

    Concept and usage:
        ```
        async def fetch(self, block_hash: str) -> str:
            return await some_resource(block_hash)

        a1, a2, b = await asyncio.gather(fetch("a"), fetch("a"), fetch("b"))
        ```

        Here, you are making three requests, but you really only need to make two I/O requests
        (one for "a", one for "b"), and while you wouldn't typically make a request like this directly, it's very
        common in using this library to inadvertently make these requests y gathering multiple resources that depend
        on the calls like this under the hood.

        By using

        ```
        @cached_fetcher(max_size=512)
        async def fetch(self, block_hash: str) -> str:
            return await some_resource(block_hash)

        a1, a2, b = await asyncio.gather(fetch("a"), fetch("a"), fetch("b"))
        ```

        You are only making two I/O calls, and a2 will simply use the result of a1 when it lands.
    """

    def __init__(
        self,
        max_size: int,
        method: Callable[..., Awaitable[Any]],
        cache_key_index: Optional[int] = 0,
        cache_results: bool = True,
    ):
        """
        Args:
            max_size: max size of the cache (in items)
            method: the function to cache
            cache_key_index: if the method takes multiple args, this is the index of that cache key in the args list
                (default is the first arg). By setting this to `None`, it will use all args as the cache key.
            cache_results: whether to memoize results in the LRU cache. When `False`, concurrent calls still share
                a single in-flight future (so they de-duplicate to one I/O), but the result is never stored — a
                later call re-fetches. Use this for values that go stale, such as the chaintip from `get_chain_head`.
        """
        self._inflight: dict[Hashable, asyncio.Future] = {}
        self._method = method
        self._max_size = max_size
        self._cache = LRUCache(max_size=max_size)
        self._cache_key_index = cache_key_index
        self._cache_results = cache_results

    def make_cache_key(self, args: tuple, kwargs: dict) -> Hashable:
        bound = inspect.signature(self._method).bind(*args, **kwargs)
        bound.apply_defaults()

        if self._cache_key_index is not None:
            key_name = list(bound.arguments)[self._cache_key_index]
            return bound.arguments[key_name]

        return pickle.dumps(dict(bound.arguments))

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        key = self.make_cache_key(args, kwargs)

        if self._cache_results and (item := self._cache.get(key)) is not None:
            return item

        if key in self._inflight:
            return await self._inflight[key]

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._inflight[key] = future

        try:
            result = await self._method(*args, **kwargs)
            if self._cache_results:
                self._cache.set(key, result)
            future.set_result(result)
            return result
        except Exception:
            self._inflight.pop(key, None)
            future.cancel()
            raise
        finally:
            self._inflight.pop(key, None)
            if not future.done():
                future.cancel()


class _WeakMethod:
    """
    Weak reference to a bound method that allows the instance to be garbage collected.
    Preserves the method's signature for introspection.
    """

    def __init__(self, method):
        self._func = method.__func__
        self._instance_ref = weakref.ref(method.__self__)
        # Store the bound method's signature (without 'self') for inspect.signature() to find.
        # We capture this once at creation time to avoid holding references to the bound method.
        self.__signature__ = inspect.signature(method)

    def __call__(self, *args, **kwargs):
        instance = self._instance_ref()
        if instance is None:
            raise ReferenceError("Instance has been garbage collected")
        return self._func(instance, *args, **kwargs)


class _CachedFetcherMethod:
    """
    Helper class for using CachedFetcher with method caches (rather than functions)
    """

    def __init__(
        self,
        method,
        max_size: int,
        cache_key_index: int,
        cache_results: bool = True,
    ):
        self.method = method
        self.max_size = max_size
        self.cache_key_index = cache_key_index
        self.cache_results = cache_results
        # Use WeakKeyDictionary to avoid preventing garbage collection of instances
        self._instances: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()

    def __get__(self, instance, owner):
        if instance is None:
            return self

        # Cache per-instance (weak references allow GC when instance is no longer used)
        if instance not in self._instances:
            bound_method = self.method.__get__(instance, owner)
            # Use weak reference wrapper to avoid preventing GC of instance
            weak_method = _WeakMethod(bound_method)
            self._instances[instance] = CachedFetcher(
                max_size=self.max_size,
                method=weak_method,
                cache_key_index=self.cache_key_index,
                cache_results=self.cache_results,
            )
        return self._instances[instance]


def cached_fetcher(
    max_size: Optional[int] = None,
    cache_key_index: Optional[int] = 0,
    cache_results: bool = True,
):
    """Wrapper for CachedFetcher. See example in CachedFetcher docstring."""

    def wrapper(method):
        return _CachedFetcherMethod(method, max_size, cache_key_index, cache_results)

    return wrapper
