# AGENTS.md

## Responsibility

- This package owns database execution semantics: DBContainer, transactions, persisted models and documents, relationships, indexes, graph and SPARQL behavior, ontology, SHACL, algorithms, migrations, maintenance, and jobs.
- It consumes the canonical DatabaseWire contract and an injected StorageEngine. It does not own network transports, Cloudflare lifecycle code, or application-specific schemas.
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

- Native backend verification uses `scripts/xcode-test-harness` with the pinned
  Swift snapshot. Do not replace it with direct package-wide
  `xcodebuild test`; the harness selects traits in an isolated manifest,
  injects the snapshot testing runtime and backend environment, enforces
  timeouts and exact counts, and rejects skips, expected failures, runtime
  warnings, and internal compiler, macro-plugin, or profiler errors.
- The harness may skip Xcode's interactive macro approval only after verifying
  that every existing dependency pin, including macro packages, is unchanged.
- The strict backend contracts are 3,918 FoundationDB tests, 101 SQLite tests,
  and 71 PostgreSQL tests. PostgreSQL tests require an isolated real server.
- FoundationDB verification requires the C SDK header and client library under
  `/usr/local/include` and `/usr/local/lib`, or explicit
  `FDB_CLIENT_INCLUDE_DIRECTORY` and `FDB_CLIENT_LIBRARY_DIRECTORY` values.
- Standard WASM, Embedded WASM, and static Musl Linux verification use the exact
  Swift 6.4 snapshot SDK identifiers and release commands in
  `docs/production-readiness.md`.
- Compile/link gates do not replace backend behavioral tests. The final source
  revision must pass both the target build gate and its executable native test
  path before release.
