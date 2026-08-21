# database-framework benchmarks

This is an independent Swift package. Its benchmark suites are intentionally
absent from the parent `database-framework` package test graph.

The benchmarks require the isolated FoundationDB cluster owned by the parent
package's Apple Container harness. The benchmark runtime verifies the
run-specific identity marker and loopback coordinator before any destructive
reset; a missing, system-default, or manually supplied cluster is rejected.
Run the package through the same version-pinned environment preparation used
by correctness tests, for example:

```bash
(
  cd Benchmarks
  ../scripts/apple-container-test-harness foundationdb-run -- \
    perl -e 'alarm shift; exec @ARGV' 3600 \
      xcodebuild test \
        -scheme database-framework-benchmarks-Package \
        -destination 'platform=macOS,arch=arm64'
)
```

The harness prepares the checksum-verified client, starts the pinned server,
injects the cluster and client paths, preserves service evidence, proves
negative readiness, and removes the disposable environment.

Benchmark results are not correctness-test evidence and are never included in
the parent package's expected test count.

Correctness, typed failure, cancellation, ownership, and lifecycle contracts
remain in `database-framework/Tests`. This package owns only latency,
throughput, allocation, storage-size, and scale measurements.

The parent package enforces this boundary with
`scripts/verify-benchmark-separation`.
