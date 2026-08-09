# Benchmarks

Comparison of async-substrate-interface (cyscale codec) against the bittensor
v11 stack (Rust `bittensor_core` codec), produced by
`benchmarks/bench_vs_bittensor_v11.py` pinned to block 8806963. Ratios are
asi/v11 medians — lower is better; below 1.0x asi is faster. Every run's
correctness cross-checks passed: storage keys byte-identical, decoded values
identical, full-map sizes equal.

Versions: asi working tree after `e1648a8` (C batch keybuild, websocket
recv-path rework), cyscale 0.8.0 working tree after `0bc0266` (one-shot C
BLAKE2b, limb base58, `blake2_128_concat_batch`), bittensor 11.0.2.

## Linux x86_64 — localhost archive node

Linux ubuntu-16gb-ash-1-archive 6.8.0-85-generic x86_64, AMD EPYC-Milan, Python 3.12.3

`RPC_ENDPOINT=ws://127.0.0.1:9944 FULL_MAPS=1` — a local node makes the e2e
phases measure per-request overhead rather than network latency. The
full-map phase warms both libraries first; the node's cold-trie walk costs
seconds and would otherwise be charged to whichever library runs first.

```
==== E2E summary (median, asi vs v11) ====
  query_batch 10k accounts                 asi=    108.0ms  v11=    113.8ms  ratio=0.95x
  get_block                                asi=      2.8ms  v11=      1.8ms  ratio=1.61x
  single query (System.Account)            asi=      1.2ms  v11=      0.9ms  ratio=1.28x
  30 sequential queries (Tempo)            asi=     19.7ms  v11=     16.5ms  ratio=1.19x
  30 concurrent queries (gather)           asi=     49.0ms  v11=     46.9ms  ratio=1.05x
  runtime_call (current_alpha_price)       asi=      0.8ms  v11=      0.6ms  ratio=1.28x
  get_events                               asi=      1.0ms  v11=      1.4ms  ratio=0.75x
  query_map one subnet (Keys, page=100)    asi=      8.5ms  v11=      6.7ms  ratio=1.27x
  query_map full (Keys, idiomatic modes)   asi=    618.0ms  v11=   1846.4ms  ratio=0.33x

==== CPU summary (median, asi vs v11) ====
  keybuild 10k                             asi=      6.0ms  v11=     11.3ms  ratio=0.53x
  decode 10k accounts                      asi=     11.1ms  v11=     25.9ms  ratio=0.43x
  map page System.Account                  asi=      4.1ms  v11=      4.9ms  ratio=0.84x
  map page SubtensorModule.Keys            asi=      1.5ms  v11=      1.6ms  ratio=0.97x
  map page SubtensorModule.Bonds           asi=      1.2ms  v11=      2.1ms  ratio=0.60x
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
vs 1.7ms here).

```
==== E2E summary (median, asi vs v11) ====
  query_batch 10k accounts                 asi=   4573.7ms  v11=   6761.2ms  ratio=0.68x
  get_block                                asi=    258.3ms  v11=    267.7ms  ratio=0.96x
  single query (System.Account)            asi=    258.4ms  v11=    254.8ms  ratio=1.01x
  30 sequential queries (Tempo)            asi=   8520.9ms  v11=   7662.2ms  ratio=1.11x
  30 concurrent queries (gather)           asi=    319.4ms  v11=    294.7ms  ratio=1.08x
  runtime_call (current_alpha_price)       asi=    253.1ms  v11=    256.3ms  ratio=0.99x
  get_events                               asi=    263.5ms  v11=    261.3ms  ratio=1.01x
  query_map one subnet (Keys, page=100)    asi=   1575.5ms  v11=   1583.1ms  ratio=1.00x
  query_map full (Keys, idiomatic modes)   asi=  10149.2ms  v11= 188293.5ms  ratio=0.05x

==== CPU summary (median, asi vs v11) ====
  keybuild 10k                             asi=      3.8ms  v11=      8.5ms  ratio=0.45x
  decode 10k accounts                      asi=      5.5ms  v11=     13.6ms  ratio=0.41x
  map page System.Account                  asi=      1.1ms  v11=      1.9ms  ratio=0.56x
  map page SubtensorModule.Keys            asi=      1.7ms  v11=      0.9ms  ratio=1.88x
  map page SubtensorModule.Bonds           asi=      1.4ms  v11=      1.2ms  ratio=1.09x
```
