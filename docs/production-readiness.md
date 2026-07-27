# Production Readiness

This checklist describes the conditions required before deploying a
database-framework service.

## Architecture

- Select one StorageKit backend deliberately.
- Confirm the backend transaction and isolation semantics.
- Create one long-lived DBContainer per service process or Durable Object
  instance.
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
swift build
xcodebuild test -scheme DatabaseCoreFocused -destination 'platform=macOS'
swift build --disable-default-traits --traits SQLite
swift build --disable-default-traits --traits PostgreSQL
swift build \
  --swift-sdk swift-6.4.x-DEVELOPMENT-SNAPSHOT-2026-07-17-a_wasm \
  --product DatabaseRuntime \
  --disable-default-traits
~~~

Run PostgreSQL integration tests only when `POSTGRES_TEST_UNIX_SOCKET` or
`POSTGRES_TEST_HOST` identifies an isolated test database. The Unix-socket
form is preferred because it does not expose a host TCP port.
Run Cloudflare Worker smoke and deployment dry-run checks in the
database-framework-cloudflare repository.

A release is not production-ready merely because the default FoundationDB
build passes. Every backend selected for release must have a successful build,
its relevant integration tests, and an explicit operational configuration.
