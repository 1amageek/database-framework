# SQL Catalog

## Purpose and Scope

This component binds DatabaseKit SQL namespace declarations to one immutable
physical schema/runtime generation. Parent: [DatabaseEngine](../DESIGN.md).
It has no child components. S11 adds catalog declarations, bounded persistence,
name resolution, and catalog-only compatibility checks. S13 extends the immutable
declarations with constraints, defaults, stored/virtual generation, identity,
and sequence metadata. SQLConstraints owns executable row/constraint policy;
SQL DML belongs to S14.

## Responsibilities and Boundaries

`SQLCatalogSnapshot` owns validated logical-to-physical mappings, protected
compiled-model contracts, and canonical catalog identity. `SQLCatalogCodec`
owns the durable storage format. `SQLCatalogStore` reads and writes only in a
caller-owned storage transaction. Schema publication owns atomic persistence,
generation numbers, publication authority, and leases. QueryExecution owns CTE
precedence, query binding, authorization, physical decoding, and logical result
projection. Logical renaming never changes physical entity names, field names,
field numbers, Directory declarations, or persisted data.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | Immutable generation composition | Publishes schema, runtimes, and catalog together. | No independent catalog publication or cache. |
| [DatabaseKit SQL catalog](../../../../database-kit/Sources/DatabaseKit/Schema/SQLCatalog/DESIGN.md) | depends on | Namespace, relation, column, and search-path declarations | Owns declarative meaning and intrinsic validation. | Names and IDs compare exact UTF-8 bytes. |
| [Registry](../Registry/DESIGN.md) | depends on | Entity runtime and durable schema entity codec | Validates executable backing and compiled-model identity. | A schema-driven runtime does not erase persisted protection. |
| [Serialization](../Serialization/DESIGN.md) | depends on | StorageFrameLimits and bounded frame IO | Reuses strict UTF-8, integer, collection, and frame bounds. | Storage metadata is separate from DatabaseWire. |
| [SQLSequences](../SQLSequences/DESIGN.md) | coordinates with | Constant default identity binding | Compiles sequence literal dependencies and consumes them for DROP/default effects. | Direct literal targets must exist before admission. |
| [QueryExecution](../QueryExecution/DESIGN.md) | used by | Ordered relation/column resolution | Maps SQL names to physical fields by number. | Search-path resolution grants no authorization. |

## Architecture

```text
DatabaseKit SQLCatalog + physical Schema + runtime registrations
    -> bounded declaration admission + exact backing validation
        -> immutable SQLCatalogSnapshot
            -> generation lease -> QueryExecution
            -> SQLCatalogCodec -> SQLCatalogStore
                                     -> caller-owned transaction
                                          -> aggregate schema publication
```

## Contracts and Invariants

- Namespace and relation IDs are catalog-wide; column identity is the pair
  `(relationID, columnID)`. Column IDs and physical field numbers are unique
  within a relation. IDs and names use exact UTF-8 equality and byte ordering,
  rather than Swift String canonical-equivalence equality or locale folding.
  Component-local semantic lookup keys retain the declaration's String owner
  and hash/compare its UTF-8 view; lookup does not allocate a second byte owner.
- Every relation resolves exactly one physical entity and a matching runtime
  registration. Every stored column resolves by field number to one physical field. A virtual
  generated column has a declared effective QueryColumnType and no physical
  field, physical field number, stored value, or fabricated backing. The
  ordered mapping follows the declared public column order. Resolved columns
  expose an optional field and a nonoptional effective type.
- A snapshot fixes its physical schema fingerprint. Restoring a persisted
  catalog checks that fingerprint, digest, every backing mapping, and compiled
  protection before publication. A malformed or mismatched pair is a failure.
- Protected contracts persist relation identity, physical entity identity,
  exact canonical durable entity digest, column-to-field identities, and key
  declaration. Generic model registration creates the compiled witness; a
  schema-driven registration does not fabricate one. Restoring with a dynamic
  runtime retains existing protected contracts.
- Catalog-only replacement cannot remove or retarget a protected relation,
  omit or retarget its column IDs, or change its primary-key declaration or existing column-policy/constraint identities.
  New SQL-only virtual generated columns are admissible when every existing
  stored binding, physical entity digest, Directory/index/identity contract,
  and protected SQL declaration remains unchanged. Initial explicit application
  SQL declarations define the protected SQL contract; derived defaults do not
  replace them.
  Logical names and namespace placement may change when backing is unchanged.
  Surviving dynamic relation/column IDs cannot acquire different physical
  meanings. Physical schema migration remains under application/schema
  publication authority and may regenerate compiled contracts for its admitted
  target schema; it does not use catalog-only replacement authority.
- Application-owned schema reconciliation preserves surviving logical IDs,
  names, namespace placement, and column order by exact physical entity identity
  and field number, retaining every surviving constraint, policy, sequence,
  primary-key attribute, and true virtual column. Removal of backing referenced
  by surviving declarations fails; it never silently drops metadata. It appends
  new physical fields, derives public relations
  for new entities, and removes declarations whose physical backing was removed
  by that authorized schema change. Target compiled witnesses regenerate their
  protection; a schema-driven target retains surviving persisted protection.
  Reconciliation preserves the exact namespace array when no new physical
  entity needs a default relation. It creates `public` only when adding such a
  relation requires that namespace.
  Removing a physical primary-key column without an explicit candidate catalog
  is a typed transition failure: reconciliation never silently drops key
  metadata. Application publication may accept an explicit candidate to express
  an intentional key-declaration change under its existing physical-schema
  evolution authority.
- Compiled application upgrades can possess only the target runtime. The
  stored-state application reconciliation seam validates the old durable
  digest, physical fingerprint, mappings, and protection without constructing
  an executable old snapshot or substituting a runtime. It then reconciles to
  the target physical schema and constructs a snapshot with the actual target
  runtime. An optional explicit catalog candidate expresses intentional
  metadata changes such as removal of a formerly keyed physical field.
- A validated migration plan may commit an intermediate physical schema before
  the application's final compiled runtime can execute it. The internal
  `reconciledForMigration` seam validates the old stored metadata, reconciles
  declarations to that stage, and updates surviving protected entity digests
  under migration authority while preserving their source classification.
  It returns stored metadata rather than fabricating an executable snapshot or
  runtime. Codec and Store accept that metadata for the caller-owned aggregate
  transaction. Only a wholly absent initial aggregate may use a derived initial
  migration state; partial or corrupt metadata must fail validation. Schema
  publication owns stage fingerprints and permits executable memory publication
  only for the actual final compiled schema.
- Default derivation creates the `public` namespace and deterministic stable
  IDs from exact physical entity identity and field numbers. Public names are
  exact entity/field names; no aliases or additional case folding are applied.
  It exposes every physical field and derives key metadata from the visible
  physical `id` column. An explicit catalog may omit that column and declare no
  key or another ordered key. This metadata supplies no new grouping dependency
  proof or constraint enforcement.
- Canonical digest input has a format domain/version, physical fingerprint,
  namespaces and relations in declared order, declared column order,
  optional primary-key order and attributes, column backing/policies, every
  declared constraint and sequence in declaration order, and protected contracts
  ordered by relation ID.
  Reordering any public declaration array changes catalog identity while
  preserving the physical index fingerprint. Durable decoding preserves every
  declared array order. The stored digest is recomputed and compared on restore.

## Runtime Flows

```text
qualified name -> exact namespace -> exact relation -> resolved backing
unqualified name -> search path entries in order
                    -> currentUser resolves authenticated principal identifier
                    -> absent user/schema entries skipped
                    -> first matching relation -> resolved backing
```

A missing qualified namespace/relation fails without terminal-name fallback.
An exhausted unqualified search fails. Anonymous current-user entries are
skipped. The caller checks CTEs before relation resolution and applies its
existing entity/field authority after physical resolution.

## State, Ownership, and Lifecycle

The snapshot is an immutable Sendable reference owner; generation leases retain it alongside
the schema and runtime registry. Publication replaces the whole generation in
the existing `DatabaseSchemaGenerationStore` Mutex on Native, WASM, and Embedded.
The component adds no mutable cache, actor, service, shutdown path, or second
publication lock. Storage transactions remain owned and completed by the
schema publication caller. Borrowed byte views never escape a synchronous
borrow; persisted values retain their ByteString owner.
Recursive query binding passes the snapshot reference rather than embedding
its lookup tables or mappings in every query frame.

## Failure, Concurrency, and Constraints

The codec rejects unknown magic/version/tags, invalid UTF-8, invalid integer or
declaration values, truncated/trailing frames, duplicate identities, missing
backing, digest mismatches, and prohibited compatibility changes as typed
failures. Storage failures propagate without synthetic defaults.

StorageFrameLimits owns frame/string/byte/collection bounds. Encode measures
before output allocation. Decode checks the complete frame bound before
materialization, validates each collection count against its minimum remaining
encoded footprint before reserveCapacity, and materializes strings only at
the semantic metadata boundary. Snapshot construction measures/admit declarations
before lookup allocation. Store uses bounded point reads with the same frame
limit. The v2 format rejects v1 frames. Expressions use the existing bounded
QueryIRWireFormat single-expression SELECT envelope, with exactly one unaliased
expression and no FROM, predicates, grouping, windows, CTEs, ordering, set
operations, paging, or other query clauses. Decoding rejects any envelope that
violates that shape. QueryIR string/collection/frame/nesting limits are derived
from the same StorageFrameLimits before encode/decode; no parallel expression
binary format is introduced. Virtual QueryColumnType values use the bounded
QueryIRWireFormat column-type adapter over its existing canonical type codec. No unsafe memory
or target-specific synchronization implementation is added.

For S12 candidate admission, `SQLCatalogCodec.admit` returns the measured
durable frame footprint, including the digest and compiled protection that
snapshot construction will retain. Schema reserves this encoded footprint on
the existing operation meter before constructing the replacement snapshot.
The component does not own reservation lifetime or checkpoint restoration;
those follow the immutable candidate owner in [Schema](../Schema/DESIGN.md).

## Verification and Change Impact

[S11SQLCatalogTests](../../../Tests/DatabaseEngineTests/S11SQLCatalogTests.swift)
owns declaration-to-backing resolution, default derivation, exact Unicode
names/IDs, search paths, qualified failure, canonical digest determinism,
codec round trips, every malformed-frame class, bound admission, mismatched
schema/runtime restore, static protection across schema-driven restoration,
and permitted logical renames versus rejected retarget/removal/key changes.

Schema generation tests own transaction atomicity, durable restart, publication
failure, leases, and concurrent old/new generations. QueryExecution tests own
physical decode followed by public projection and entity/field security. These
integration tests consume this component contract rather than duplicating its
codec tests. Changes to physical backing validation require Registry and schema
publication review; name-resolution changes require QueryExecution review.

## S13 Admission and Intercomponent Contract

```text
SQL declarations + physical fields
  -> SQLCatalogSnapshot: resolve stored/virtual bindings + preserve declaration
  -> SQLConstraints: bind effective types, validate expression class and row policy
  -> immutable generation: declaration + executable policy share one digest
  -> SQLCatalogCodec v2: bounded canonical metadata -> caller transaction
```

SQLCatalog owns durable declaration identity and backing. SQLConstraints owns
executable type/nullability checks, CHECK Boolean/UNKNOWN semantics, immutable
current-row generated expressions without generated-column dependencies, and
row-independent default expressions with the admitted volatile function set.
QueryExecution consumes optional physical fields and explicit effective types;
it may not treat virtual columns as stored fields. No persisted row format,
physical Schema fingerprint, Directory, or index layout is changed by virtual
column admission. Sequence identities and names resolve in catalog namespaces.

| Gap | Required behavioral evidence |
|---|---|
| Metadata ownership | v2 round trip preserves all attributes and declaration order |
| Bounded persistence | truncated, unknown-version/tag, oversized and malformed expression envelope reject |
| Canonical identity | policy, constraint, virtual type, primary-key attributes and sequence changes alter digest |
| Reconciliation | restart and physical reconciliation preserve surviving SQL metadata; orphaned references reject |
| Compiled protection | logical rename succeeds; changing protected policy/constraint or stored binding fails; adding a true virtual column preserves physical identity |

The implementation-facing resolved column exposes `declaration: SQLColumn`,
`field: FieldSchema?`, `type: QueryColumnType`, and `isNullable: Bool`. Stored
columns derive effective type/nullability from their actual FieldSchema; virtual
columns retain the declared type and conservatively permit NULL until row policy
checks. A virtual type never changes the physical field map.

Expression/type encoding uses the shared codec's bounded temporary owner at
the persistence boundary; the outer catalog encoder measures then emits its
final frame. This is not a claim of zero intermediate expression allocation.

Snapshot admission delegates executable expression checks to SQLConstraints
after physical/virtual backing resolution and before returning an executable
snapshot. The compiler accepts declaration plus physical schema rather than a
snapshot, so construction cannot recursively construct itself. Persisted-only
migration states validate target declaration expressions against the physical
schema without inventing an executable runtime for an intermediate schema.


### Constant sequence references in typed defaults

SQLSequences compiles direct string and SQL unknown-string sequence arguments
into an immutable per-relation/per-column literal-to-stable-ID map during
snapshot admission, after scalar policy validation. Typed publication has no
session search path: an unqualified constant uses its relation namespace, and
an explicitly qualified constant uses that namespace. Missing targets fail
admission. The same canonical catalog declarations deterministically rebuild
these bindings on reopen; they are never reconstructed using a DROP or INSERT
caller's search path. An unchanged policy cannot bind to a different sequence ID
during replacement. CASCADE must remove affected policies before snapshot
admission. Explicit text casts and computed target arguments remain late-bound.

```text
Canonical catalog defaults + declaring relation namespace
    -> immutable snapshot literal-to-sequence-ID bindings
        -> DROP dependency comparison by ID
        -> task-local binding during default evaluation / SET DEFAULT
```

SQLSequenceDependencyRegressionTests owns literal-variant admission, same-name
namespace isolation, runtime INSERT defaults, RESTRICT/CASCADE, rollback and
canonical persistence/reopen behavior.
