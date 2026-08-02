# Changelog

Release versions use CalVer in the form YY.MMDD.patch. Detailed historical
implementation notes are kept in Git history rather than in the public
documentation tree.

## Unreleased

### Changed

- Flat, IVF, PQ, HNSW, and Fusion search retain payload owners and borrow
  bounded vector views instead of rematerializing candidate arrays.
- PQ evaluates persisted codebooks and codes directly with metric-correct
  lookup tables.
- IVF and PQ retraining atomically replace stale assignments, inverted lists,
  and compressed codes.
- Namespace transaction borrows retain the container operation lease across
  suspension and reject cross-container transaction use.
- Cloudflare keeps the coherent `VectorIndexes` feature but rejects effective
  HNSW configurations before container opening; Flat, IVF, and PQ remain
  available and there is no algorithm fallback.

### Verification

- The VectorIndex native suite passes 102 tests, the affected online indexing,
  maintenance, Fusion, and schema suites pass 73 tests, and the Cloudflare
  storage response ownership suite passes 4 tests, all without failures or
  skips.
- Normal WASM compiles and links the complete Cloudflare runtime verification
  target with `AllRuntimeFeatures` and swift-hnsw 1.1.4 selected.
- The Embedded Cloudflare release gate executes Flat, IVF, and PQ behavior and
  proves that HNSW is rejected before serving DatabaseWire requests. The
  optimized reactor is 9,084,920 bytes (3,150,215 bytes gzip), reserves 64 MiB,
  and starts in 60.512625 ms in the current verification fixture.

## 26.0731.3 - 2026-07-31

### Changed

- Uniqueness enforcement uses the same Embedded-safe key ownership contract as
  native and WASM builds.

## 26.0731.2 - 2026-07-31

### Changed

- SwiftPM traits select runtime index and relationship products, registrations,
  server capabilities, and the `Database` umbrella exports as one composition.
- Storage engine lifecycle ownership is transferred explicitly into
  `DBContainer` and released exactly once on open failure or shutdown.
- Schema-authoritative index reads retain one caller-owned transaction through
  namespace resolution, lifecycle admission, and physical cursor execution.

## 26.0731.1 - 2026-07-31

### Changed

- Persistent job scheduled-work failures identify whether due-job loading,
  job processing, wake-up scheduling, or both processing and scheduling
  failed.
- Task cancellation now propagates directly and never performs wake-up
  recovery from the cancelled task.

### Verification

- The persistent job service suite passes all 34 tests, including phase
  classification, combined failure preservation, and cancellation behavior.

## 26.0629.0 - 2026-06-29

### Added

- StorageKit-backed FoundationDB, SQLite, and PostgreSQL trait paths.
- DatabaseWire runtime support for client and host adapters.
- Backend-neutral dynamic directory and partition binding APIs.
- Binary vector payload storage for Flat and HNSW paths.
- HNSW graph snapshot chunking and revision-aware read caching.
- Cloud SQL PostgreSQL configuration and readiness integration through
  StorageKit.

### Changed

- Index maintainers use StorageKit contracts instead of direct backend APIs.
- FDB-specific imports are conditionally compiled by the FoundationDB trait.
- VectorIndex production search uses the Swift HNSW backend from swift-hnsw.
- The Database facade re-exports the backend selected by SwiftPM traits.

### Verification

- FoundationDB build and test paths validated with the local cluster harness.
- SQLite build and integration tests validated without libfdb_c.
- PostgreSQL build path validated; integration tests require a configured
  PostgreSQL instance.
- Dependent database-client and database-framework-cloudflare build paths
  validated for the binary vector storage contract.

## Historical Releases

Earlier 0.x releases used SemVer and focused on the initial FoundationDB
execution path, schema catalog, index maintainers, query parsing, migrations,
and the DatabaseCLI. Those release notes remain available in Git history.

[26.0731.3]: https://github.com/1amageek/database-framework/releases/tag/26.0731.3
[26.0731.2]: https://github.com/1amageek/database-framework/releases/tag/26.0731.2
[26.0731.1]: https://github.com/1amageek/database-framework/releases/tag/26.0731.1
[26.0629.0]: https://github.com/1amageek/database-framework/releases/tag/26.0629.0
