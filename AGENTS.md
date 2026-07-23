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
