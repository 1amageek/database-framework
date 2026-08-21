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

- Backend verification enters through `scripts/apple-container-test-harness`.
  It verifies the pinned Apple Container runtime and owns PostgreSQL,
  FoundationDB, SQLite, Swift, run-specific network creation, dedicated-IP
  discovery, private Unix-socket or loopback forwarding, readiness, result
  collection, stop, negative readiness, and exact container and network
  deletion. It must
  not install or update the host runtime, request administrator privileges,
  change host DNS, start a Homebrew or launchd backend, use Docker Desktop,
  publish a host port, or reuse a developer service.
- The Apple Container harness delegates every macOS test execution to
  `scripts/xcode-test-harness` with the pinned Swift snapshot. Do not replace
  it with direct package-wide `xcodebuild test`; the Xcode harness selects
  traits in an isolated manifest, injects the snapshot testing runtime and
  backend environment, enforces timeouts and exact counts, and rejects skips,
  expected failures, runtime warnings, and internal compiler, macro-plugin, or
  profiler errors.
- SQLite remains in-process. Its Apple Container command requires both the
  authoritative macOS Xcode target and a Linux execution of the same target
  linked to the stable SQLite release pinned in
  `scripts/apple-container/versions.env`. A SQLite service sidecar is not valid
  evidence for `SQLiteStorageEngine`.
- The harness may skip Xcode's interactive macro approval only after verifying
  that every existing dependency pin, including macro packages, is unchanged.
- When FoundationDB or PostgreSQL service variables are present, the harness
  disables Xcode test-bundle parallelism. Scenario actors coordinate suites
  only inside one test process, so separate bundles must not concurrently
  share the same disposable service. Target compilation and Swift Testing
  inside each bundle retain their normal parallelism.
- The strict SQLite contracts are 111 tests without `MultiBase` and 114
  tests with it. The existing `MultiBase` backend contracts are 3,432
  FoundationDB tests and 67 PostgreSQL tests. PostgreSQL tests require an
  isolated real server.
- `POSTGRES_TEST_UNIX_SOCKET` is the complete PostgreSQL socket file path, such
  as `<socket-directory>/.s.PGSQL.<port>`, not the containing directory. The
  harness compiles its reviewed relay source with Xcode's `clang`; it must not
  require a Homebrew forwarding or PostgreSQL client utility.
- FoundationDB verification checksum-verifies and expands the pinned macOS C
  client into the user cache. It passes explicit
  `FDB_CLIENT_INCLUDE_DIRECTORY` and `FDB_CLIENT_LIBRARY_DIRECTORY` values and
  never installs or starts the client package's system launch daemon.
- Standard WASM, Embedded WASM, and static Musl Linux verification use the exact
  Swift 6.4 snapshot SDK identifiers and release commands in
  `docs/production-readiness.md`.
- Compile/link gates do not replace backend behavioral tests. The final source
  revision must pass both the target build gate and its executable native test
  path before release.
