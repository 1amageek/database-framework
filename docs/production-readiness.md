# Production Readiness

This checklist describes the conditions required before deploying a
database-framework service.

## Architecture

- Select one StorageKit backend deliberately.
- Select runtime/index capabilities through consuming-package traits; do not
  rely on the default full-host profile for a size-constrained runtime.
- Confirm the backend transaction and isolation semantics.
- Create one long-lived DBContainer per service process or Durable Object
  instance.
- Transfer one initialized engine into that container and register
  `container.shutdown()` with the host's shutdown lifecycle.
- Create a context per request or unit of work.
- Keep web host and database adapter dependencies separate.

## Schema And Indexes

- Version the Schema.
- Define all required directory paths and indexes in model declarations.
- Run migrations before accepting application traffic.
- Validate dynamic-directory fields and require partition bindings.
- Rebuild or validate indexes after storage migration.
- Keep vector payloads in the binary storage format described in
  [Vector Storage](storage/vector-storage-and-hnsw.md).

## Security

- Establish AuthorizationContext at the request boundary.
- Register every enabled entity policy through AuthorizationPolicyHandler.
- Keep SecurityPolicy evaluation enabled in production.
- Test create, read, update, delete, list, and cross-tenant move paths.
- Provision tenant, workspace, and database routing independently from
  authorization credentials.

## Backend Operations

| Backend | Production checks |
|---|---|
| FoundationDB | cluster health, cluster file, transaction limits, backup/restore |
| SQLite | file ownership, locking, backup, single-writer deployment model |
| PostgreSQL | TLS/IAM, pool limits, DDL permissions, serializable retry budget |
| Cloudflare | Worker limits, Durable Object routing, request size, secret rotation |

## Observability

- Emit transaction IDs and request IDs into structured logs.
- Record retry, conflict, latency, and error metrics.
- Alert on migration failure, index build failure, and connection exhaustion.
- Keep benchmark results tied to backend, version, dataset, and hardware.

## Release Gate

~~~bash
export TOOLCHAINS=org.swift.64202607231a

scripts/xcode-test-harness \
  --traits SQLite,AllRuntimeFeatures \
  --only-testing SQLiteTests \
  --expected-count 101 \
  --require-zero-skips \
  --require-zero-expected-failures \
  --require-zero-runtime-warnings

POSTGRES_TEST_HOST=database.test \
POSTGRES_TEST_PORT=5432 \
POSTGRES_TEST_USER=postgres \
POSTGRES_TEST_PASSWORD=test \
POSTGRES_TEST_DB=database_framework_test \
scripts/xcode-test-harness \
  --traits PostgreSQL,AllRuntimeFeatures \
  --only-testing PostgreSQLTests \
  --expected-count 71 \
  --require-zero-skips \
  --require-zero-expected-failures \
  --require-zero-runtime-warnings

scripts/fdb-test-env run --clean -- \
  scripts/xcode-test-harness \
    --traits FoundationDB,AllRuntimeFeatures \
    --skip-testing BenchmarkFrameworkTests \
    --skip-testing PerformanceBenchmarks \
    --expected-count 3918 \
    --require-zero-skips \
    --require-zero-expected-failures \
    --require-zero-runtime-warnings

swift build \
  --swift-sdk swift-6.4.x-DEVELOPMENT-SNAPSHOT-2026-07-23-a_wasm \
  --product Database \
  --disable-default-traits \
  --traits AllRuntimeFeatures \
  -c release \
  -debug-info-format none
swift build \
  --swift-sdk swift-6.4.x-DEVELOPMENT-SNAPSHOT-2026-07-23-a_wasm-embedded \
  --product Database \
  --disable-default-traits \
  --traits AllRuntimeFeatures \
  -c release \
  -debug-info-format none

swift build \
  --swift-sdk swift-6.4.x-DEVELOPMENT-SNAPSHOT-2026-07-23-a_static-linux-0.1.0 \
  --triple aarch64-swift-linux-musl \
  --product DatabaseCLICore \
  --disable-default-traits \
  --traits PostgreSQL \
  -c release \
  -debug-info-format none
~~~

The Xcode harness keeps the source manifest unchanged, selects traits in an
isolated copy, injects the pinned snapshot's testing runtime and backend
environment into `.xctestrun`, preserves build/test logs, and rejects internal
compiler, macro-plugin, or coverage-profiler failures even when Xcode exits
successfully. Do not replace these invocations with a direct package-wide
`xcodebuild test`. Xcode's interactive macro approval is disabled only after
the harness proves that the resolved macro dependency revisions match the
tracked release pins.

The release gates above were last executed against the published dependency
graph on 2026-08-03: database-kit 26.0803.0, storage-kit 26.0803.0, and
swift-hnsw 1.1.4. FoundationDB passed 3,918 tests, SQLite passed 101 tests, and
PostgreSQL passed 71 tests with zero failures, skips, expected failures, or
runtime warnings. Standard WASM, Embedded WASM, and the static
`aarch64-swift-linux-musl` `DatabaseCLICore` product compiled and linked in
release mode.

Release WASM builds disable debug information because reactor artifacts do not
ship it and host-side `dsymutil` cannot reliably verify snapshot-built macro
dependency objects. Compiler diagnostics remain enabled. The static Musl build
compiles and links the Linux CLI/runtime boundary, including `DatabaseMath`,
`DatabaseEngine`, and PostgreSQL storage dependencies; it is a portability
gate, not a substitute for the real PostgreSQL integration suite.

Run PostgreSQL integration tests only when `POSTGRES_TEST_UNIX_SOCKET` or
`POSTGRES_TEST_HOST` identifies an isolated test database. The Unix-socket
form is preferred because it does not expose a host TCP port.
Run Cloudflare Worker smoke and deployment dry-run checks in the
database-framework-cloudflare repository.

A release is not production-ready merely because the default FoundationDB
build passes. Every backend selected for release must have a successful build,
its relevant integration tests, and an explicit operational configuration.
FoundationDB artifacts are released only for macOS and Linux. iOS and
WASI/Embedded compositions must prove their own selected storage adapter path;
the presence of the `FoundationDB` default trait does not provide an adapter on
those targets.

Opening failure, explicit shutdown, and deinitialization must converge on the
same exactly-once engine release. A production host must call
`DBContainer.shutdown()` rather than reaching through to
the engine reference or retaining a second engine owner.
