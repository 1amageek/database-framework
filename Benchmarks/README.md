# database-framework benchmarks

This is an independent Swift package. Its benchmark suites are intentionally
absent from the parent `database-framework` package test graph.

The benchmarks require an isolated FoundationDB cluster. Supply its exact
cluster file through `FDB_CLUSTER_FILE`, then run this package explicitly with
Xcode's test runner and an external timeout. Benchmark results are not
correctness-test evidence and are never included in the parent package's
expected test count.

Correctness, typed failure, cancellation, ownership, and lifecycle contracts
remain in `database-framework/Tests`. This package owns only latency,
throughput, allocation, storage-size, and scale measurements.

The parent package enforces this boundary with
`scripts/verify-benchmark-separation`.
