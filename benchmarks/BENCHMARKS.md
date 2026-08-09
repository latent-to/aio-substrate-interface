# Benchmarks

Comparison of async-substrate-interface (cyscale codec) against the bittensor
v11 stack (Rust `bittensor_core` codec), produced by
`benchmarks/bench_vs_bittensor_v11.py` pinned to block 8806963. Ratios are
asi/v11 medians — lower is better; below 1.0x asi is faster. Every run's
correctness cross-checks passed: storage keys byte-identical, decoded values
identical, full-map sizes equal.

Versions: asi working tree after `e1648a8` (C batch keybuild, websocket
recv-path rework, per-request-future response wait), cyscale 0.8.0 working
tree after `0bc0266` (one-shot C BLAKE2b, limb base58,
`blake2_128_concat_batch`, parametrized-type class cache), bittensor 11.0.2.

## Linux x86_64 — localhost archive node

Linux ubuntu-16gb-ash-1-archive 6.8.0-85-generic x86_64, AMD EPYC-Milan, Python 3.12.3

`RPC_ENDPOINT=ws://127.0.0.1:9944 FULL_MAPS=1` — a local node makes the e2e
phases measure per-request overhead rather than network latency. The
full-map phase warms both libraries first; the node's cold-trie walk costs
seconds and would otherwise be charged to whichever library runs first.

### Standard asyncio loop

Known infra artifact: the "30 concurrent queries" phase stalls ~40ms for
both libraries on this host. Packet captures show it is the Docker
userland-proxy leg (containerized node's Nagle vs the proxy's delayed ACK),
not client behavior; `"userland-proxy": false` or `--network host` on the
node removes it.

```
==== E2E summary (median, asi vs v11) ====
  query_batch 10k accounts                 asi=    119.5ms  v11=    130.7ms  ratio=0.91x
  get_block                                asi=      1.8ms  v11=      1.2ms  ratio=1.46x
  single query (System.Account)            asi=      0.9ms  v11=      0.6ms  ratio=1.33x
  30 sequential queries (Tempo)            asi=     24.2ms  v11=     20.1ms  ratio=1.20x
  30 concurrent queries (gather)           asi=     45.8ms  v11=     43.4ms  ratio=1.05x
  runtime_call (current_alpha_price)       asi=      1.0ms  v11=      0.7ms  ratio=1.39x
  get_events                               asi=      1.2ms  v11=      1.5ms  ratio=0.82x
  query_map one subnet (Keys, page=100)    asi=     10.3ms  v11=      8.6ms  ratio=1.20x
  query_map full (Keys, idiomatic modes)   asi=    359.5ms  v11=   1205.6ms  ratio=0.30x

==== CPU summary (median, asi vs v11) ====
  keybuild 10k                             asi=      5.5ms  v11=     11.7ms  ratio=0.47x
  decode 10k accounts                      asi=     11.0ms  v11=     26.8ms  ratio=0.41x
  map page System.Account                  asi=      2.5ms  v11=      3.1ms  ratio=0.78x
  map page SubtensorModule.Keys            asi=      1.5ms  v11=      2.4ms  ratio=0.63x
  map page SubtensorModule.Bonds           asi=      1.1ms  v11=      1.5ms  ratio=0.74x
```

### uvloop (`UVLOOP=1`)

Both libraries run on the same loop, so this is the whole stack under
uvloop, not an asi-only change. uvloop's tighter write segmentation happens
to dodge the Docker-proxy stall above: asi's 30 gathered queries drop to
~5ms while v11 stays at ~44ms. Everything else is within run-to-run noise.

```
==== E2E summary (median, asi vs v11) ====
  query_batch 10k accounts                 asi=    103.0ms  v11=    110.8ms  ratio=0.93x
  get_block                                asi=      1.4ms  v11=      1.1ms  ratio=1.29x
  single query (System.Account)            asi=      0.7ms  v11=      0.6ms  ratio=1.22x
  30 sequential queries (Tempo)            asi=     21.9ms  v11=     17.8ms  ratio=1.23x
  30 concurrent queries (gather)           asi=      4.9ms  v11=     43.7ms  ratio=0.11x
  runtime_call (current_alpha_price)       asi=      1.0ms  v11=      0.8ms  ratio=1.13x
  get_events                               asi=      1.2ms  v11=      1.7ms  ratio=0.71x
  query_map one subnet (Keys, page=100)    asi=      8.1ms  v11=      7.0ms  ratio=1.14x
  query_map full (Keys, idiomatic modes)   asi=    399.8ms  v11=   1046.9ms  ratio=0.38x

==== CPU summary (median, asi vs v11) ====
  keybuild 10k                             asi=      5.6ms  v11=     12.3ms  ratio=0.46x
  decode 10k accounts                      asi=     12.4ms  v11=     22.6ms  ratio=0.55x
  map page System.Account                  asi=      1.8ms  v11=      3.2ms  ratio=0.58x
  map page SubtensorModule.Keys            asi=      2.0ms  v11=      2.0ms  ratio=0.96x
  map page SubtensorModule.Bonds           asi=      1.4ms  v11=      2.0ms  ratio=0.68x
```

## macOS — WAN archive endpoint

Darwin 25.5.0 arm64, Apple M4 Pro, Python 3.13.6

`RPC_ENDPOINT=wss://archive.sub.latent.to FULL_MAPS=1` — WAN latency
(~250ms RTT) dominates the e2e phases; expect run-to-run noise there. The
full-map ratio is dominated by RPC strategy: v11 walks the 30k-entry map in
~600 sequential 100-entry page requests, each paying the RTT. The CPU
phases feed identical raw RPC data to both codecs and are stable, except
that phases timed while the v11 tokio runtime idles in-process (notably the
Keys map page) read higher than standalone measurements (0.9ms standalone
vs ~1.7ms here).

```
==== E2E summary (median, asi vs v11) ====
  query_batch 10k accounts                 asi=   4545.0ms  v11=   5161.0ms  ratio=0.88x
  get_block                                asi=    256.4ms  v11=    264.8ms  ratio=0.97x
  single query (System.Account)            asi=    261.6ms  v11=    258.8ms  ratio=1.01x
  30 sequential queries (Tempo)            asi=   8924.7ms  v11=   7997.2ms  ratio=1.12x
  30 concurrent queries (gather)           asi=    297.2ms  v11=    297.1ms  ratio=1.00x
  runtime_call (current_alpha_price)       asi=    542.4ms  v11=   3285.6ms  ratio=0.17x
  get_events                               asi=    263.7ms  v11=    278.0ms  ratio=0.95x
  query_map one subnet (Keys, page=100)    asi=   1567.1ms  v11=   2115.2ms  ratio=0.74x
  query_map full (Keys, idiomatic modes)   asi=  10009.5ms  v11= 208676.9ms  ratio=0.05x

==== CPU summary (median, asi vs v11) ====
  keybuild 10k                             asi=      4.0ms  v11=      8.5ms  ratio=0.47x
  decode 10k accounts                      asi=      5.7ms  v11=     13.1ms  ratio=0.43x
  map page System.Account                  asi=      2.2ms  v11=      1.9ms  ratio=1.13x
  map page SubtensorModule.Keys            asi=      1.7ms  v11=      1.2ms  ratio=1.48x
  map page SubtensorModule.Bonds           asi=      1.0ms  v11=      1.0ms  ratio=0.91x
```
