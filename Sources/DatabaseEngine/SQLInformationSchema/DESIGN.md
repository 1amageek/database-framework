# SQLInformationSchema

## Purpose and Scope

Owns S16 O07 virtual SQL metadata relations. Parent: [DatabaseEngine](../DESIGN.md).
Children: none. The semantic authority is [Kit SQL](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md).

## Responsibilities and Boundaries

Derive tables, columns, table_constraints, key_column_usage,
referential_constraints, views and sequences from the admitted catalog and
principal. Read owns metadata visibility. QueryExecution owns typed binding,
row accounting and relational execution. No physical entity, OID, service or
independent catalog cache is created.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [SQLCatalog](../SQLCatalog/DESIGN.md) | depends on | Immutable resolved snapshot | Supplies actual admitted declarations | Own transaction candidate is authoritative |
| [Read](../Read/DESIGN.md) | depends on | Metadata relation/field visibility | Known denials hide metadata | Operational failures propagate |
| [QueryExecution](../QueryExecution/DESIGN.md) | used by | Explicit virtual descriptor and typed rows | Ordinary relational operators consume metadata | Never dispatch to physical fast paths |
| [SQLViews](../SQLViews/DESIGN.md) | coordinates with | Stable dependency identities | View visibility follows all consumed dependencies | Backing entities remain hidden |

## Architecture

```text
admitted SQLReadResolution (snapshot + principal)
  -> virtual relation descriptor (fixed typed columns)
    -> Read visibility (relation + field + dependency)
      -> bounded row emission -> ordinary SELECT operators
```

## Contracts and Invariants

- Exact information_schema qualification or an explicit search-path entry
  selects the virtual descriptor before ordinary namespace/view lookup.
- Bound plans retain the descriptor explicitly; virtual sources never acquire
  physical SQL relation bindings, statement physical snapshots or list grants.
- Rows reflect static, dynamic and own-transaction catalog declarations;
  rollback restores the existing catalog owner. Materialized views and their backing names are
  excluded, matching PostgreSQL information_schema relation-kind filtering.
- PostgreSQL 18 `table_constraints.constraint_type` projects an intrinsic
  NOT NULL constraint as `CHECK`. The intrinsic `.notNull` declaration and its
  violation SQLSTATE `23502` remain unchanged; this rule owns metadata spelling
  only. Named NOT NULL and CHECK constraints each retain their own metadata row.
- Metadata visibility uses existing principal/list/field policy. Only
  SecurityError and FieldSecurityError.readNotAllowed hide metadata. Other
  errors propagate. Constraints and sequence/view dependencies cannot reveal
  denied relations or fields. No database identity is fabricated: catalog
  identity columns are NULL because this execution foundation has none.
- Durable views capture an explicit informationSchema source kind, descriptor name
  and stable canonical column IDs. Captured names and source ordinals bind before
  caller search paths; no physical catalog identity is fabricated. Ordinary catalog
  and virtual source identities remain disjoint even when strings are identical.
- Empty results retain the same declared column types. All emitted values are
  accounted before retained row construction on the request meter.
- Canonical defaults, generated expressions and view definitions admit text
  capacity on the current request meter before serialization grows output.
  The writer limit is the lesser of available admitted capacity and the existing
  structural output limit. Exhausting request capacity is a typed resource
  failure before row emission; the original serialization cause is retained
  when translating a writer-limit failure.

## Runtime Flows

CTE resolution precedes virtual resolution; a descriptor then bypasses ordinary
view/physical admission and executes through the existing relation producer.
The immutable snapshot remains retained throughout binding and evaluation.

## State, Ownership, and Lifecycle

Descriptors are immutable Sendable values. The admitted SQLReadResolution owns
the snapshot and principal. There is no new mutable state or shutdown path.
The renderer owns the text-capacity reservation while canonical text grows.
Successful rendering releases unused capacity. Before constructing the emitted
values array, the renderer admits the complete positional row footprint and
absorbs the exact text claim into that row owner. The immutable
`SQLInformationSchemaRow` exposes values and a payload lease; its private
initializer prevents callers from inventing an admitted byte count.
QueryExecution retains the lease through ordinary row lifetime and uses the
existing alias footprint for new row/array storage, including durable views
that select a strict source-column subset. It does not charge the same text
again or infer a prepaid-byte discount.
Failure releases the renderer's reservation before returning. Peak reserved
capacity describes admission, not measured allocation.

| Logical state | Native / ordinary WASM / Embedded | Access and release |
|---|---|---|
| Text accumulation | Same function-local value, no shared mutation | Synchronous renderer methods; scope exit releases unfinished claims |
| Emitted row | Same immutable values and payload lease | Consumer retains the lease; final owner release returns the claim |
| Request claims | Existing `DatabaseIntermediateReservation` with `Mutex<State>` | Existing reserve/absorb/release entry points; no target-specific branch |

## Failure, Concurrency, and Constraints

Partitions and physical access paths on virtual sources fail explicitly.
Cancellation, meter/deadline exhaustion and policy operational errors propagate.
Native/WASM/Embedded share the same immutable descriptor and existing meter
synchronization; no target-specific isolation branch is introduced.

## Verification and Change Impact

S16SQLInformationSchemaTests owns real SELECT queries, empty typed metadata,
own DDL and savepoint rollback, static/dynamic/view/materialized metadata,
constraint/key/reference rows and permission filtering/failure propagation.
`serializedDefinitionAdmissionBeforeEmission` creates a real admitted view with
a 1 MiB literal, then reads its definition under a 128 KiB request budget. Its
existing row callback performs ordinary row admission: rejection must occur
before that callback, with no retained-claim leak. The canonical writer's
pre-growth checks provide the corresponding allocation-boundary source proof.
Changes require QueryExecution preflight, physical fast-path and statement
capture checks. Runtime success is reported only after the owning harness runs.

PostgreSQL reference: [information_schema definitions](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/catalog/information_schema.sql) filter tables/columns to ordinary, partitioned, foreign tables and ordinary views; materialized views are absent.
