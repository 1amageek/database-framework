# database-framework benchmarks

This is an independent Swift package. Its benchmark suites are intentionally
absent from the parent `database-framework` package test graph.

The benchmarks require the isolated FoundationDB cluster owned by the parent
package's `scripts/fdb-test-env` harness. The benchmark runtime verifies the
harness identity marker and the single loopback coordinator before any
destructive reset; a missing, system-default, or manually supplied cluster is
rejected. Run the package explicitly with Xcode's test runner and an external
timeout, for example:

```bash
(
  cd Benchmarks
  ../scripts/fdb-test-env run --clean -- \
    perl -e 'alarm shift; exec @ARGV' 3600 \
      xcodebuild test \
        -scheme database-framework-benchmarks-Package \
        -destination 'platform=macOS,arch=arm64'
)
```

Benchmark results are not correctness-test evidence and are never included in
the parent package's expected test count.

Correctness, typed failure, cancellation, ownership, and lifecycle contracts
remain in `database-framework/Tests`. This package owns only latency,
throughput, allocation, storage-size, and scale measurements.

The parent package enforces this boundary with
`scripts/verify-benchmark-separation`.
