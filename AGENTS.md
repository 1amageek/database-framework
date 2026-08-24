# AGENTS.md

## Responsibility

- This package owns in-process database execution semantics: DBContainer, transactions, persisted models and documents, relationships, indexes, graph and SPARQL behavior, ontology, SHACL, algorithms, migrations, and maintenance primitives.
- It consumes database semantic contracts and an injected StorageEngine. It does not own DatabaseWire operation dispatch, remote command registries, durable server jobs, network transports, native server lifecycle, Cloudflare lifecycle code, or application-specific schemas.
- The default configuration owns one engine and one ordinary database root. Base, Composition, storage topology, placement, persisted Grant evaluation, and target leases exist only when the non-default `MultiBase` trait is selected. `AllRuntimeFeatures` must not enable it.
- Every mutation path must apply preconditions, idempotency, relationship rules, and index updates in the same transaction.

## Naming

- Name declarations for their database-domain responsibility, observable behavior, state transition, ownership, or lifecycle contract.
- Follow the Swift API Design Guidelines at every access level, including tests, generated support, and runtime handlers.
- Do not encode implementation language, ABI, calling convention, module identity, binary layout, toolchain, build mode, or optimization strategy in names.
- Name callbacks and handlers for the operation or event they process. Names such as `regular`, `legacy`, `impl`, `helper`, `manager`, or a bare `callback` are invalid.
- Distinguish database identities, owned values, borrowed views, transaction-scoped state, and persisted state explicitly.

## Runtime and Error Contracts

- Register all runtime capabilities through explicit DBContainer-scoped configuration. Do not use global mutable registration.
- Validate required handlers, readers, maintainers, migrations, and indexes at bootstrap and fail fast when any dependency is missing.
- Bound all externally supplied frames, collections, nesting, object counts, query work, intermediate rows, intermediate bytes, and execution time.
- Keep large persisted values and Wire payloads in owned buffers with range views until a semantic or persistence boundary requires materialization.
- Do not turn unsupported operations, decode failures, authorization failures, conflicts, or resource limits into empty successful results.
- This is version 1. Remove replacement paths and obsolete DTOs rather than preserving compatibility.

## Verification

- Backend verification enters through `scripts/docker-test-harness`. The
  canonical environment is the same pinned `linux/arm64` Docker runner locally
  and in CI. PostgreSQL and FoundationDB run beside it on one run-specific
  Docker network. The harness owns image identity, readiness, exact service
  version, result evidence, stop, negative readiness, and exact container and
  network deletion. It must not request administrator privileges, change host
  DNS, start a Homebrew or launchd backend, publish a host port, or reuse a
  developer service.
- macOS verification is a separate parity lane. `macos-sqlite` delegates to
  `scripts/xcode-test-harness` with the pinned Swift snapshot. Do not replace it
  with direct package-wide `xcodebuild test`; the Xcode harness selects traits
  in an isolated manifest, injects the snapshot testing runtime, enforces
  timeouts and exact counts, and rejects skips, expected failures, runtime
  warnings, and internal compiler, macro-plugin, or profiler errors.
- SQLite remains in-process. Its canonical Linux command links and executes the
  stable SQLite release pinned in `scripts/docker/versions.env`; a SQLite
  service sidecar is not valid evidence for `SQLiteStorageEngine`.
- The harness may skip Xcode's interactive macro approval only after verifying
  that every existing dependency pin, including macro packages, is unchanged.
- Each Docker invocation owns one test runner process and one disposable
  service. Scenario actors coordinate suites inside that process; separate
  harness runs never share a service or network.
- The strict SQLite contracts are 112 tests without `MultiBase` and 115
  tests with it. The existing `MultiBase` backend contracts are 3,416
  FoundationDB tests and 68 PostgreSQL tests. PostgreSQL tests require an
  isolated real server.
- PostgreSQL is addressed as `postgresql:5432` only inside the run network.
  FoundationDB uses the service-generated private cluster file. The runner
  checksum-verifies the FoundationDB Linux client package and uses explicit
  include and library directories. The `foundationdb-client` support command
  separately extracts the pinned macOS client for Xcode compilation without
  installing a system launch daemon.
- Standard WASM, Embedded WASM, and static Musl Linux verification use the exact
  Swift 6.4 snapshot SDK identifiers and release commands in
  `docs/production-readiness.md`.
- Compile/link gates do not replace backend behavioral tests. The final source
  revision must pass both the target build gate and its executable native test
  path before release.
