# Pending call storage

This repair builds on the existing Flashprofits pin
`19d9e2fa06d38a7dabb0d1618c40763272be62bd`; the package version stays 2.1.1.
The private `EthCall` future retained initialization parameters and space for
the largest `ProviderCall` variant while a boxed batch reply waited. Initialization
parameters now have a separate allocation that releases on first poll. The
running state owns only the active reply variant and reuses an existing box.
Methods, parameters, pins, response mapping, polling time, and cancellation stay
the same. No batching, timeout, or request-policy changes are included.

## Measurements and controls

Four regressions at `7ba68732` first fail with 64,000 requested bytes retained
for 64 pending calls. Four controls pass. The repair retains 4,096 bytes for the
same cohort, including nested allocations, and all eight tests pass. Both
`eth_call` and `eth_estimateGas` cover completion and cancellation, exact
transaction/block/override parameters, borrowed non-Send mappers, cleanup of
every allocation, and a subsequent successful request. Controls cover dropping
unpolled calls, original preparation/reply errors, and Ready/Waiter/RpcCall
replies. Allocation accounting also checks resize and release.

These are System-allocator requested-byte measurements on macOS arm64. They
exclude allocator fragmentation and are not production RSS evidence. The change
adds an initialization allocation and boxes previously inline reply variants;
an existing boxed reply is reused. Whole-application memory and throughput
acceptance remain separate requirements.

## Validation

All commands use `nightly-2026-08-25`, one build job, and
`RUSTFLAGS='--cfg tokio_unstable -Ctarget-cpu=native'`. The unchanged retained
validation lock hashes to
`a407216d11d1d41c1d4d0f106d73fd467885ac60c5b769471d5b5d46438a7dfb`.

```sh
cargo +nightly-2026-08-25 fmt --all --check
cargo +nightly-2026-08-25 nextest run --locked -p alloy-provider -p alloy-contract --all-features --no-fail-fast --retries 0 --test-threads 2
cargo +nightly-2026-08-25 nextest run --locked -p alloy-provider -p alloy-contract --all-features -E 'binary(call_storage)' --retries 0
cargo +nightly-2026-08-25 clippy --locked -p alloy-provider -p alloy-contract --all-features --all-targets -- -D warnings -A clippy::chunks_exact_to_as_chunks -A clippy::useless_borrows_in_formatting -A clippy::question_mark
```

The full provider/contract run has 264 passes, 21 skipped tests, and one failure:
`websocket_tls_setup` uses a hardcoded public endpoint that returns HTTP 401
(`invalid project id`). The unchanged original implementation reproduces that
failure. No failing test was removed or changed. The final focused run passes
all eight tests; the only intervening test change makes a helper `const`.

Formatting and Clippy pass. Unqualified strict Clippy fails on the original
source. Its three existing lint classes above affect `eips/eip7594/sidecar.rs`,
`rpc-types-eth/filter.rs`, and `provider/fillers/mod.rs`; command-line exceptions
are limited to those classes. A new test-helper const warning was fixed, not
exempted. No source lint allowances were added.

Records are retained under Flashprofits' PR113 evidence directory,
`continuation-20260925/alloy-call-storage-*.{json,log}`. The final source/lock
fingerprint is `c4e17b92842574f593aff5ee082a77eb053d513332575b460560cee447b7b022`.
