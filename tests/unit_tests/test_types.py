from async_substrate_interface.types import Runtime, RuntimeCache


def test_runtime_cache():
    fake_block = 2
    fake_hash = "0xignore"
    fake_version = 271

    new_fake_block = 3
    newer_fake_block = 4

    new_fake_hash = "0xnewfakehash"

    runtime = Runtime("", None, None)
    runtime_cache = RuntimeCache()
    # insert our Runtime object into the cache with a set block, hash, and version
    runtime_cache.add_item(runtime, fake_block, fake_hash, fake_version)

    assert runtime_cache.retrieve(fake_block) is not None
    # cache does not yet know that new_fake_block has the same runtime
    assert runtime_cache.retrieve(new_fake_block) is None
    assert (
        runtime_cache.retrieve(
            new_fake_block, new_fake_hash, runtime_version=fake_version
        )
        is not None
    )
    # after checking the runtime with the new block, it now knows this runtime should also map to this block
    assert runtime_cache.retrieve(new_fake_block) is not None
    assert runtime_cache.retrieve(newer_fake_block) is None
    assert runtime_cache.retrieve(newer_fake_block, fake_hash) is not None
    assert runtime_cache.retrieve(newer_fake_block) is not None
    assert runtime_cache.retrieve(fake_block, block_hash=new_fake_hash) is not None
    assert runtime_cache.retrieve(block_hash=new_fake_hash) is not None
