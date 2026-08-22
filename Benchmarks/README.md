# database-framework benchmarks

This is an independent Swift package. Its benchmark suites are intentionally
absent from the parent `database-framework` package test graph.

The main `FrameworkPerformanceBenchmarks` suite requires the isolated
FoundationDB cluster owned by the parent package's Docker harness. Its runtime
verifies the run-specific identity marker and private coordinator before any
destructive reset; a missing, system-default, or manually supplied cluster is
rejected. Run that package through the same version-pinned environment used by
correctness tests, for example:

```bash
(
  cd Benchmarks
  ../scripts/docker-test-harness foundationdb-run -- \
    perl -e 'alarm shift; exec @ARGV' 3600 \
      swift test \
        --only-use-versions-from-resolved-file
)
```

The command runs inside the same pinned Linux runner used by correctness CI.
The harness starts the pinned server, injects the checksum-verified Linux
client and private cluster path, preserves service evidence, proves negative
readiness, and removes the disposable environment.

Benchmark results are not correctness-test evidence and are never included in
the parent package's expected test count.

Correctness, typed failure, cancellation, ownership, and lifecycle contracts
remain in `database-framework/Tests`. This package owns only latency,
throughput, allocation, storage-size, and scale measurements.

The parent package enforces this boundary with
`scripts/verify-benchmark-separation`.

## Bitmap core microbenchmarks

`BitmapCore` is a separate package because its deterministic in-memory set
algebra measurements do not own a FoundationDB lifecycle. Keeping it outside
the FoundationDB benchmark package prevents unrelated backend, vector, graph,
and macro targets from entering the measurement build graph.

Run its release configuration through the benchmark harness. The harness uses
the committed dependency graph, injects the pinned Swift Testing runtime into
the generated `.xctestrun`, and retains the result bundle, summary, and raw
logs:

```bash
scripts/xcode-benchmark-harness bitmap
```

## Full-text query microbenchmarks

`FullTextQuery` measures the real SQLite-backed public full-text execution path
without adding the complete framework test graph to each sample. It exercises
ordered posting-list intersection and union, and then doubles the fixture size
to prove that the optimized path remains inside the default work budget.

```bash
scripts/xcode-benchmark-harness fulltext
```

## 2026-08-22 optimization record

The optimization applies the ordered-list traversal principle from
[arXiv:2601.18747](https://arxiv.org/pdf/2601.18747) only where the framework
already exposes positive Boolean full-text operations. It does not add a
Boolean DAG or NOT API. Roaring array containers and full-text posting lists
now use ordered linear merges instead of rebuilding hash sets and maps at
every operation.

All values below are medians of 15 release samples measured before and after
the implementation change on the same Apple M4 Max (14 cores, 36 GB) running
macOS 27.0 and Swift snapshot 2026-08-14. Absolute timings are machine-specific;
the paired ratios are the regression reference.

| Benchmark | Baseline (us) | Optimized (us) | Speedup |
|---|---:|---:|---:|
| Roaring ascending sparse construction | 19,357.083 | 427.625 | 45.3x |
| Roaring missing-value membership | 1,874.500 | 35.834 | 52.3x |
| Roaring sparse intersection | 8,458.250 | 328.083 | 25.8x |
| Roaring sparse union | 42,105.833 | 968.541 | 43.5x |
| Roaring sparse difference | 7,834.542 | 703.292 | 11.1x |
| Full-text canonical intersection | 10,584.584 | 7,977.250 | 1.33x |
| Full-text canonical union | 19,298.667 | 4,932.959 | 3.91x |
