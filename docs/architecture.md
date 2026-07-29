# Architecture and Ownership

database-framework is the execution layer of the database stack. It consumes
portable contracts from database-kit, applies database semantics, and delegates
physical key-value operations to storage-kit.

## Package Boundaries

~~~text
database-types
  FieldValue, FieldObject, ByteString, Timestamp, civil time values,
  ExactDecimal, UUID, vectors, geographic values, RDFTerm
        |
        v
database-kit
  Persistable contracts, schema metadata, EntityReference, QueryIR,
  mutation contracts, DatabaseWire
        |
        v
database-framework
  DBContainer, DatabaseContext, transaction orchestration, persistence,
  query planning and execution, graph semantics, migrations, indexes
        |
        v
storage-kit
  StorageEngine, transaction and range contracts, Tuple, Subspace,
  directory services, concrete storage adapters
~~~

database-types owns only portable primitive database values. It does not own
models, schemas, queries, indexes, persistence, or storage behavior.

database-kit owns the contracts that describe a database program. It may depend
on database-types, but it does not execute a query or open a storage transaction.

database-framework owns database behavior. It resolves schemas, coordinates one
logical transaction, enforces mutation and relationship rules, maintains indexes,
executes QueryIR, and performs migrations. It does not implement a concrete
storage engine or transport.

storage-kit owns the physical storage contract and backend adapters. It does not
interpret models, QueryIR, graph semantics, or application commands.

## Runtime Forms

The native and standard WASI builds use the same database-framework sources and
the same synchronization contract. A full Cloudflare database runtime links the
application-specific schema and database-framework into a standard WASI reactor.
It is not an Embedded Swift module.

An Embedded application, such as Calendar, communicates through DatabaseWire by
using database-client. It does not link database-framework into its Embedded WASM
artifact.

~~~text
Embedded application WASM
        |
        | DatabaseWire
        v
standard WASI database reactor
        |
        | StorageEngine transaction contract
        v
host storage adapter
~~~

## Value and Ownership Contract

- FieldValue and its primitive members come from database-types. Framework-local
  replacement value types are not permitted.
- FieldObject is the canonical ordered object value. Encoding plans are private
  execution details and must not become competing public value models.
- ByteString is the owned byte primitive. Borrowed storage views are retained only
  for the lifetime guaranteed by their owner.
- Bounded scan pages and typed resumable-operation slices are single-owner
  values. Asynchronous typed boundaries transfer them with `sending`; callers
  consume each page or slice once instead of implicitly copying a potentially
  large work product.
- A typed resumable-operation slice is encoded into its erased persistent form
  immediately. The erased form is a copyable ByteString handle whose backing
  storage remains shared; copying that handle does not copy payload bytes. The
  framework does not cache both representations or materialize an intermediate
  byte array.
- Large binary paths prepare and measure once, write directly into a destination
  buffer, and avoid intermediate Data or Array materialization.
- Copies are allowed at explicit ownership, persistence, or external API
  boundaries where the receiving side must own independent storage.

## Transaction and Index Invariants

- A model mutation, relationship delete rule, derived index update, precondition,
  and idempotency decision belong to one logical transaction.
- Disabled indexes are not maintained or read.
- Write-only indexes are maintained but are not query candidates.
- Readable indexes are maintained and may be selected by the planner.
- An online build validates its configuration before changing lifecycle state.
- An index becomes readable only after its build, uniqueness checks, and progress
  finalization succeed.
- A provider declares whether its index kind supports uniqueness. Runtime
  configuration rejects a unique index when that capability is absent.
- The concrete uniqueness-capable maintainer owns physical key interpretation and
  conflict discovery. DatabaseEngine owns lifecycle policy, violation persistence,
  and the decision to reject a readable-index conflict.
- Mutation maintenance and online builds invoke the same uniqueness policy inside
  the caller's transaction, so a partially built index cannot observe weaker
  uniqueness semantics than ordinary writes.
- Unsupported backend capabilities fail explicitly; they are not silently
  emulated with weaker semantics.

## Dependency Injection

DBContainer owns one DatabaseRuntimeConfiguration. Registries for persistable
types, readers, mutation maintainers, index maintainers, graph executors, and
other runtime services are resolved from that configuration. Global mutable
registration is not part of the runtime contract.

## Synchronization

Shared mutable state uses the same actor or Synchronization.Mutex owner on native
and WASI targets. Target conditions may select an ABI or platform facility, but
must not weaken isolation, Sendable, ownership, or shutdown semantics.

DBContainer and DatabaseTransaction retain actor ownership of asynchronous
database behavior. DatabaseTransactionScope protects only short, in-memory
admission state with `Synchronization.Mutex`: no I/O or suspension occurs while
the lock is held, and close continuations resume after the lock is released.
This synchronization and lifecycle contract is identical on native and WASI
targets.
