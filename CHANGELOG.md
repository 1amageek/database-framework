# Changelog

Release versions use CalVer in the form YY.MMDD.patch. Detailed historical
implementation notes are kept in Git history rather than in the public
documentation tree.

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

[26.0629.0]: https://github.com/1amageek/database-framework/releases/tag/26.0629.0
