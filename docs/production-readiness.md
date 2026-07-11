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

- Establish AuthContext at the request boundary.
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
swift test
swift build --traits SQLite
swift test --traits SQLite
swift build --traits PostgreSQL
~~~

Run PostgreSQL integration tests only when a test database is configured.
Run Cloudflare Worker smoke and deployment dry-run checks in the
database-framework-cloudflare repository.

A release is not production-ready merely because the default FoundationDB
build passes. Every backend selected for release must have a successful build,
its relevant integration tests, and an explicit operational configuration.
