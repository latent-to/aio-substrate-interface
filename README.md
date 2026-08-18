# AIO Substrate Interface

This project provides an asynchronous interface for interacting with [Substrate](https://substrate.io/)-based
blockchains. Its API is based on the [py-substrate-interface](https://github.com/polkascan/py-substrate-interface)
project, but is not fully compatible.

This is the successor to [async-substrate-interface](https://github.com/RaoFoundation/async-substrate-interface/),
which was archived

Additionally, this project uses [cyscale](https://github.com/latent-to/cyscale) instead
of [py-scale-codec](https://github.com/polkascan/py-scale-codec) for
faster [SCALE](https://docs.substrate.io/reference/scale-codec/) decoding. Since v2.0, because cyscale and
py-scale-codec share the same namespace, we require that only
cyscale is installed to be able to use this. If you run into a runtime error stating that both cannot be installed at
the same time, simply remove them both, and reinstall cyscale:

```shell
pip uninstall cyscale scalecodec -y
pip install cyscale
```

## Installation

To install the package, use the following command:

```bash
pip install aio-substrate-interface
```

## Usage

```python
import asyncio
from async_substrate_interface import AsyncSubstrateInterface


async def main():
    substrate = AsyncSubstrateInterface(
        url="wss://rpc.polkadot.io"
    )
    async with substrate:
        result = await substrate.query(
            module='System',
            storage_function='Account',
            params=['5CZs3T15Ky4jch1sUpSFwkUbYEnsCfe1WCY51fH3SPV6NFnf']
        )

        print(result)


asyncio.run(main())
```

### Event loop: uvloop recommended

The library works on the standard asyncio event loop, but we recommend running your
application under [uvloop](https://github.com/MagicStack/uvloop) when it is available
for your platform:

```bash
pip install uvloop
```

```python
import uvloop

uvloop.run(main())  # instead of asyncio.run(main())
```

In our benchmarks against a local node (`benchmarks/BENCHMARKS.md`), uvloop made
concurrent multiplexed calls (many `asyncio.gather`-ed queries over the single
websocket connection) dramatically faster, and was at worst neutral everywhere else.

### Caching

Caching is used to improve the overall performance of this library. It is applied only on methods whose results
cannot change — such as the block hash for a given block number (small, 512 default max size), or the runtime for a
given runtime version (large, 16 default max size). These cache sizes are user-configurable using the respective env
vars, `SUBSTRATE_CACHE_METHOD_SIZE` and `SUBSTRATE_RUNTIME_CACHE_SIZE`.

Because of the asynchronous nature of the library, rather than something like `functools.lru_cache`, we developed our
own asyncio-friendly LRU cache, `CachedFetcher`. The key difference here is that each request is assigned a future
that is returned when the initial request completes. So, if you were to do:

```python
bn = 5000
bh1, bh2 = await asyncio.gather(
    asi.get_block_hash(bn),
    asi.get_block_hash(bn)
)
```

it would actually only make one single network call, and return the result to both requests.

### ENV VARS

The following environment variables are used within aio-substrate-interface

- SUBSTRATE_CACHE_METHOD_SIZE (default 512): the cache size of the smaller return-size
  methods (see the Caching section for more info)
- SUBSTRATE_RUNTIME_CACHE_SIZE (default 16): the cache size of the larger return-size
  methods (see the Caching section for more info)
- SUBSTRATE_EXTRINSIC_RECOVERY_SCAN_DEPTH (default 16): how many blocks are walked back per check when recovering a
  watched extrinsic whose subscription was severed by a websocket reconnection
- SUBSTRATE_EXTRINSIC_RECOVERY_TIMEOUT (default 120): seconds to wait for inclusion/finalization of an
  already-submitted extrinsic when its watch subscription is being recovered by polling
- SUBSTRATE_EXTRINSIC_RECOVERY_POLL_INTERVAL (default 1): seconds between chain polls while recovering a watched
  extrinsic

## Contributing

Contributions are welcome! Please open an issue or submit a pull request to the `staging` branch.

### Signed Commits

All commits in pull requests must be signed. We require signed commits to verify the authenticity of contributions and
ensure code integrity.

To sign your commits, you must have GPG signing configured in Git:

```bash
git commit -S -m "your commit message"
```

Or configure Git to sign all commits automatically:

```bash
git config --global commit.gpgsign true
```

For instructions on setting up GPG key signing,
see [GitHub's documentation on signing commits](https://docs.github.com/en/authentication/managing-commit-signature-verification/signing-commits).

> **Note:** Pull requests containing unsigned commits will not be merged.

## Cyscale Installation Issue

Because cyscale uses the same namespace as py-scale-codec (scalecodec), there can be some difficulties with
upgrades.

```shell
pip uninstall scalecodec cyscale -y
pip install -U cyscale --force-reinstall
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact

For any questions or inquiries, please open an issue in this repo.

