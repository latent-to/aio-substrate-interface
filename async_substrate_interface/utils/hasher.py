"""Helper functions used to calculate keys for Substrate storage items"""

import xxhash

# cyscale's one-shot C BLAKE2b helpers (~2x faster than hashlib for the
# small per-storage-key inputs hashed here).
from scalecodec.utils._ss58 import (  # noqa: F401
    blake2b_digest as _blake2b_digest,
    blake2_128_concat,
)


def blake2_256(data):
    """
    Helper function to calculate a 32 bytes Blake2b hash for provided data, used as key for Substrate storage items
    """
    return _blake2b_digest(data, 32)


def blake2_128(data):
    """
    Helper function to calculate a 16 bytes Blake2b hash for provided data, used as key for Substrate storage items
    """
    return _blake2b_digest(data, 16)


def xxh128(data):
    """
    Helper function to calculate a 2 concatenated xxh64 hash for provided data, used as key for several Substrate
    """
    return xxhash.xxh64_intdigest(data, seed=0).to_bytes(
        8, "little"
    ) + xxhash.xxh64_intdigest(data, seed=1).to_bytes(8, "little")


def two_x64_concat(data):
    """
    Helper function to calculate a xxh64 hash with concatenated data for provided data,
    used as key for several Substrate
    """
    return xxhash.xxh64_intdigest(data, seed=0).to_bytes(8, "little") + data


def xxh64(data):
    return xxhash.xxh64_intdigest(data, seed=0).to_bytes(8, "little")


def identity(data):
    return data
