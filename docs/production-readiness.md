# Production Readiness

This checklist describes the conditions required before deploying a
database-framework service.

## Architecture

- For the default composition, inject one StorageEngine and confirm its
  lifecycle. Dedicated backends use the engine root. For FoundationDB or any
  shared physical backend, record the explicitly selected Directory/root and
  keep it stable across restarts. Only a
  `MultiBase` deployment defines a control domain, data domains, and named
  Base placements.
- Select runtime/index capabilities through consuming-package traits. A host
  package's defaults do not change the framework's explicit trait contract.
- Confirm the backend transaction and isolation semantics.
- Create one long-lived DBContainer per service process or Durable Object
  instance.
- Transfer the initialized engine, or the `MultiBase` topology, into that
  container and register `container.shutdown()` with the host lifecycle.
- Create one authorization-bound DatabaseContext per unit of work. With
  `MultiBase`, create a DatabaseSession and explicitly select a Base or
  Composition instead.
- Keep web host and database adapter dependencies separate.

## Schema And Indexes

- Version the Schema.
- Define all required directory paths and indexes in model declarations.
- Run migrations before accepting application traffic. A container opened with
  a migration plan rejects ordinary data operations until the complete plan
  succeeds; a bounded partial run keeps admission closed. With `MultiBase`,
  migrate every active Base; container admission remains closed until all of
  them match the compiled schema fingerprint and physical index generation.
- Validate dynamic-directory fields and require partition bindings.
- Rebuild or validate indexes after storage migration.
- Keep vector payloads in the binary storage format described in
  [Vector Storage](storage/vector-storage-and-hnsw.md).

## Security

- Establish AuthorizationContext at the request boundary.
- With `MultiBase`, persist Base Grants; role names are authenticated
  claims, not an administrative bypass.
- With `MultiBase`, require explicit `.read`, `.write`, and `.administer`
  access independently and authorize every Composition member before output.
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
scripts/apple-container-test-harness doctor
scripts/apple-container-test-harness sqlite
scripts/apple-container-test-harness sqlite --multi-base
scripts/apple-container-test-harness postgresql
scripts/apple-container-test-harness foundationdb

export TOOLCHAINS=org.swift.64202608141a

for product in Database; do
  swift build \
    --swift-sdk swift-6.4.x-DEVELOPMENT-SNAPSHOT-2026-08-14-a_wasm \
    --product "$product" \
    --disable-default-traits \
    --traits AllRuntimeFeatures \
    -c release \
    -debug-info-format none
done

for product in Database; do
  swift build \
    --swift-sdk swift-6.4.x-DEVELOPMENT-SNAPSHOT-2026-08-14-a_wasm-embedded \
    --product "$product" \
    --disable-default-traits \
    --traits AllRuntimeFeatures \
    -c release \
    -debug-info-format none
done

export TOOLCHAINS=org.swift.64202607231a

for product in Database; do
  swift build \
    --swift-sdk swift-6.4.x-DEVELOPMENT-SNAPSHOT-2026-07-23-a_static-linux-0.1.0 \
    --triple aarch64-swift-linux-musl \
    --product "$product" \
    --disable-default-traits \
    --traits AllRuntimeFeatures \
    -c release \
    -debug-info-format none
done

~~~

The backend gate uses the immutable artifact manifest, topology, teardown
contract, and evidence requirements in
[Apple Container Backend Verification](apple-container-testing.md). The
supplemental SQLite Linux run does not replace its macOS Xcode result; both are
required.

The Xcode harness keeps the source manifest unchanged, selects traits in an
isolated copy, injects the pinned snapshot's testing runtime and backend
environment into `.xctestrun`, preserves build/test logs, and rejects internal
compiler, macro-plugin, or coverage-profiler failures even when Xcode exits
successfully. Do not replace these invocations with a direct package-wide
`xcodebuild test`. Xcode's interactive macro approval is disabled only after
the harness proves that the resolved macro dependency revisions match the
tracked release pins.

The framework release gate resolves database-kit 26.0819.0. That published tag
contains the `CompositionSelection`, `CompositionResolution`, and DatabaseWire
v5 contracts consumed by this source revision and resolves to database-kit main
commit `775facbae56d8ccbbf7024d40a8fb36da64093c2`. The remaining release
dependencies are storage-kit 26.0807.0 and swift-hnsw 1.1.4. Record every
resolved version and revision, the framework commit, result bundles, backend
service identities, and platform build logs in the release report; do not
preserve a previous release's results as evidence for a later source revision.

Command-line portability and process verification belong to the independent
[`database-cli`](https://github.com/1amageek/database-cli) package. This
framework does not publish a CLI product or link a command-line executable to
a storage backend.

Release WASM builds disable debug information because reactor artifacts do not
ship it and host-side `dsymutil` cannot reliably verify snapshot-built macro
dependency objects. Compiler diagnostics remain enabled.

Run PostgreSQL integration tests only when `POSTGRES_TEST_UNIX_SOCKET` or
`POSTGRES_TEST_HOST` identifies an isolated test database. The Unix-socket
form is preferred because it does not expose a host TCP port.
Run Cloudflare Worker smoke and deployment dry-run checks in the
database-framework-cloudflare repository.

A release is not production-ready merely because one FoundationDB-selected
build passes. The package has no default backend trait. Every backend selected
for release must have a successful build, its relevant integration tests, and
an explicit operational configuration. FoundationDB artifacts are released
only for macOS and Linux. iOS and WASI/Embedded compositions must prove their
own selected storage adapter path; selecting `FoundationDB` elsewhere does not
provide an adapter on those targets.

Opening failure, explicit shutdown, and deinitialization must converge on the
same exactly-once engine release. A production host must call
`DBContainer.shutdown()` rather than reaching through to
the engine reference or retaining a second engine owner.
