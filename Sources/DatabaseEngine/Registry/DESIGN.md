# Registry

## Purpose and Scope

Registry owns admitted runtime entity/schema lookup and model-to-execution
adaptation inside [DatabaseEngine](../DESIGN.md). Children: none. This S01
design fixes declared query-field interpretation and positional-row assembly;
it does not introduce the later SQL dynamic catalog or DDL execution.

## Responsibilities and Boundaries

Consume validated immutable Kit schemas and injected runtime registrations,
resolve declared entity/field metadata, and construct admitted query sources.
Model owns static conversion and field identity; Query owns query values and
rich envelopes; Schema owns descriptor validation/fingerprint/evolution.
Read owns session authority; QueryExecution owns slots, rows and metering;
Serialization owns actual primitive persistence bytes. Registry does not
inspect an object to guess SQL meaning or create independent transactions.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | Runtime composition and schema lifetime | Injects registrations with admitted schema generation. | No global mutable registration. |
| [Read](../Read/DESIGN.md) | coordinates with | Schema lease, authority and exact meter | Admits source conversion in a session. | Foreign meter fails before storage/construction. |
| [QueryExecution](../QueryExecution/DESIGN.md) | used by | Ordered typed source schema and slot append | Receives direct model conversion without dictionaries. | Shared schema is charged once; row payload is charged separately. |
| [Serialization](../Serialization/DESIGN.md) | depends on | Retained primitive persisted model | Preserves existing FieldValue bytes and field identity. | Rich interpretation is a declared adapter step after decoding. |
| [Kit Model](../../../../database-kit/Sources/DatabaseKit/Model/DESIGN.md) | depends on | Static wrapper conformance and Field metadata | Supplies generated concrete conversion. | Runtime does not retain metatypes/key paths for adaptation. |
| [Kit Schema](../../../../database-kit/Sources/DatabaseKit/Schema/DESIGN.md) | depends on | FieldSchema.queryType admission | Supplies optional declared query interpretation. | Descriptor changes require migration and a new schema identity. |
| [Kit SQL](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md) | depends on | QueryFieldStorageAdapter and measure | Pure declared envelopes and exact footprint counts. | Plain object/bytes remain primitive without queryType. |

## Architecture

```text
Schema generation + static runtime registration
    -> ordered fields with optional queryType
    -> QueryExecution admitted shared source slots
retained PersistedModel
    -> schema-ordered field borrow
    -> descriptor validation + checked destination measure
    -> footprint-first retained append
        -> declared adapter decode / primitive wrap
        -> positional CanonicalSourceRow
```

## Contracts and Invariants

Current [EntityRuntimeRegistration](EntityRuntimeRegistration.swift) regular
conversion measures model fields before admitted append, then builds an
intermediate QueryRow dictionary. S01 replaces only that assembly with the
purpose-limited shared canonical-slot constructor used by regular and
polymorphic models. Existing transaction, index selection, retained model and
schema generation owners remain unchanged.

- Source slots derive deterministically from admitted entity field order and
  retain stable persisted field numbers separately from output ordinals.
  Query type is field.queryType when present; otherwise the primitive
  FieldSchemaType/isArray declaration. No row supplies missing type metadata.
- QueryFieldStorageAdapter.decode receives the admitted descriptor and
  explicit QueryValueLimits derived from the request's existing resource
  budget. A rich descriptor requires exactly its validated envelope. Plain
  object/bytes with absent descriptor remain primitive even if keys resemble
  that envelope; malformed declared payloads remain typed failures.
- Missing required persisted fields fail. Optional missing fields become
  primitive NULL only under the schema's absence contract. Unknown fields,
  duplicate persisted identity or descriptor mismatch cannot be silently
  coerced into source slots.
- Direct model-to-slot conversion measures before constructing rich values
  and row storage, preserves exact work-meter identity, and runs inside the
  admitted append closure. It does not produce an uncharged intermediate
  dictionary, QueryRow, raw array or general-purpose escaping model borrow.
- Rich field writes and defaults use Kit's declared measurement and encode
  contract before primitive persistence. Generic model traversal continues
  carrying FieldValue. No storage codec tag or primitive algebra changes
  are introduced merely to recognize rich query fields.
- Table and polymorphic scans share the conversion contract. Type-name/code
  annotations and version remain in their existing primitive semantics and
  are included in destination accounting.

## Runtime Flows

A Read session seals authority, schema generation and meter. Registry binds
source metadata without reading a row; retained model fetch uses that
session's transaction. Conversion follows the above measure/admit/build
sequence. Selected rich-field update encodes the descriptor-matched value
before the existing mutation path applies primary/index/relationship writes.
S01 adds no SQL mutation grammar or alternate write authority.

## State, Ownership, and Lifecycle

Container/schema generation owns runtime registrations and immutable field
metadata. Relation owner retains one admitted schema; each retained row owns
its positional values. A retained model borrow ends after destination append
and never escapes. Query type metadata carries no runtime service, engine,
lease or reservation in Kit. Schema cache entries never mix generations.

## Failure, Concurrency, and Constraints

Missing schema/field, corrupt envelope, incompatible type, resource overflow,
foreign meter and stale generation are typed failures with original causes.
Existing container-scoped synchronization remains identical on native, WASM
and Embedded; S01 does not weaken storage/isolation or use shared raw mutable
state. Admission precedes destination allocation; cancellation destroys
partial rows and releases their reservations.

## Verification and Change Impact

DatabaseEngineTests registry/schema and retained-model suites own S01-C09 and
C12: declared-vs-lookalike payloads, missing/extra/mismatched envelopes,
optional/default fields, field-order independence, early budget failure and
partial-owner cleanup. DatabaseTests generated rich-model SQL round trips
own C04–C08 literal/parameter insert/reload/update/projection. Existing
primitive serialization/identity and SPARQL suites own C13 regressions.
Model/schema macros and Wire metadata tests remain owned by Kit. The workspace
progress record owns S01 behavioral evidence for these implemented paths.

SQLite backend persistence evidence is owned by
[`SQLiteStorageTests.richValuesSurviveSQLiteRestart()`](../../../Tests/SQLiteTests/SQLiteStorageTests.swift).
One isolated file-backed container scenario inserts all five declared rich
fields, reloads actual models, updates and saves them, awaits shutdown, then
reopens the same file and verifies model and positional SQL values. The same
scenario checks direct AST literals, typed parameters and empty bound columns.
It uses container-owned engines and shuts them down before directory removal;
no malformed-payload storage injection API is introduced for this fixture.

Changes to field descriptor interpretation revalidate Kit Model/Schema,
canonical manifest identity, Serialization boundaries, QueryExecution row
construction, Read typed model projection and direct server/result consumers.

### S01 admitted source layout

The sealed read authorization selects declared fields in schema order before
canonical source construction. EntityTableRows carries that exact shared
layout even when the source is empty. QueryExecution binds and projects from
this admitted layout, rather than reconstructing a wider entity schema.
Polymorphic union slots come from member declarations and sealed admitted
sets. A field absent from a member declaration can be NULL; an unadmitted
required field cannot be synthesized. Primitive pushdown cannot compare or
order rich persistence envelopes as SQL values.

### S01 retained-source provenance

The retained model/frame origin supplies an explicit shared payload lease to
canonical-slot construction. Alias projections retain that known source
reservation once while charging their new row structures. This is provenance,
not discovery of primitive backing-owner identity. Unknown-capacity raw values
fail admission or use a pre-admitted detachment boundary. Independent roots
retain the conservative known-capacity interpretation specified by
[QueryExecution](../QueryExecution/DESIGN.md#s01-byte-retention-provenance-and-measurement).

### S01 persisted query field descriptors

SchemaEntityEntryCodec owns the engine catalog representation of every admitted
FieldSchema, including the optional queryType. Catalog version 5 retains the
complete primitive/SQL descriptor and its precision, scale, length, time-zone
and array-element metadata. Nested array descriptors use bounded length-prefixed
StorageFrame children; malformed tags, integers and declarations remain typed
failures. Version 4 is rejected rather than interpreted with guessed metadata.
The separate canonical engine projection advances its format identity with this
field representation change. DatabaseWire remains the owner of wire framing.

An initial catalog stores the Kit SchemaManifest fingerprint of the declared
schema. Reopening reconstructs that same semantic schema from catalog entries
before comparing the fingerprint; dropping queryType is therefore a catalog
correctness failure. SQLiteStorageTests.richValuesSurviveSQLiteRestart checks
descriptor and manifest identity across direct catalog codec round-trip, then
requires the real file-backed container reopen and rich data reload to succeed.
