# Index

## Purpose and Scope

This component owns Framework index contracts: resolved index descriptors,
provider dispatch, physical generation identity, lifecycle and uniqueness
boundaries, and descriptor-authoritative key adaptation. Parent:
[DatabaseEngine](../DESIGN.md). Children: none. Specialized providers own their
index algorithms. This document fixes the S06 temporal key adaptation contract;
[existing runtime design](../../../docs/INDEX_RUNTIME_DESIGN.md) describes the
shared provider/lifecycle implementation without duplicating temporal rules.

## Responsibilities and Boundaries

Index adaptation converts an already admitted physical field into an index key
under its exact compiled key-field descriptor. It decodes index keys under that
same descriptor. It does not change persisted or covering payloads, native
primitive types, generic primitive tuple meaning, SQL comparison implementation,
Directory identity, provider algorithms, or transaction authority.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | Installed schema/runtime and transaction ownership | Supplies one admitted descriptor generation. | Never infer metadata from values. |
| [Directory](../Directory/DESIGN.md) | coordinates with | Total physical component identity | Resolves the node containing the index. | Directory physical identity differs from SQL key equality. |
| [Registry](../Registry/DESIGN.md) | depends on | Canonical model and declared value admission | Retains exact field schema and storage payload. | Source dynamic descriptors must survive retirement. |
| [QueryExecution](../QueryExecution/DESIGN.md) | coordinates with | Selected SQL comparison and pushdown equivalence | Determines logical SQL temporal identity/order. | Physical key fast paths require proven equivalence. |
| [Kit SQL](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md) | depends on | QueryFieldStorageAdapter and temporal finite domain | Owns carrier, envelope and declared value validation. | Non-SQL objects never acquire temporal semantics by shape. |

## Architecture

```text
ResolvedIndex.descriptor.keyFieldSchemas + original persisted fields
    -> declared temporal key codec
        -> primitive finite key projection / index-only infinity sentinel / TIME24 key
            -> existing provider key maintenance and uniqueness
                -> descriptor-aware key decoding
                    -> logical declared value (never a synthetic persisted primitive)

original persisted fields -> covering frame (unchanged)
```

## Contracts and Invariants

### Declared temporal key selection

Only a compiled SQL DATE, TIME without zone, TIMESTAMP without zone or TIMESTAMPTZ
key descriptor selects temporal adaptation. Absent or primitive descriptors keep
the existing primitive tuple path. New TIMETZ and existing INTERVAL index fields
remain rejected by Schema admission; no new generic SQL index surface is added.

Kit validates the complete finite SQL domain before key adaptation. In particular,
DATE is bounded by Julian day0 and 5874897-12-31; TIMESTAMP and TIMESTAMPTZ by
PG2000 microseconds [-211813488000000000, 9223371331200000000). This excludes every
index-only sentinel below. New SQL TIME envelopes below 24:00 project to their
exact primitive CivilTime key; the stored/covering envelope remains original.
Equivalent old primitive TIME and new SQL TIME carriers intentionally share one
SQL index key. Its finite inverse is the canonical primitive TIME representative;
only stored/covering payloads preserve the original structural carrier identity. Directory components preserve their distinct physical identity.

| SQL key value | Index-only encoding |
|---|---|
| Finite representable temporal value | Existing primitive temporal tuple bytes, unchanged |
| DATE negative/positive infinity | Existing date encoding of year Int32.min at Jan1 / Int32.max at Dec31 |
| TIMESTAMP negative/positive infinity | Existing dateTime encoding of the same extreme dates with 00:00:00 / 23:59:59.999999999 |
| TIMESTAMPTZ negative/positive infinity | Existing timestamp encoding of seconds Int64.min / Int64.max, nanoseconds0 |
| TIME 24:00:00 | Existing Framework canonical byte header and time tag, hour24/minute0/second0/nanoseconds0 fixed nibble fields |

The sentinels are valid native primitive representations outside the strictly
admitted SQL finite domain. No sentinel is encoded into a record, covering frame,
partition identity, uniqueness-conflict value or other data-facing result.
Decoding selects the exact descriptor first, recognizes only the complete chosen
sentinel, and maps it to the Kit SQL infinity payload. Arbitrary out-of-domain
finite values reject instead of becoming infinity.

TIME24 belongs to the Framework tuple byte owner
`DatabaseEngine/Types/FieldValueTupleCodec.swift`, not DatabaseTypes or StorageKit.
A package-only endpoint factory emits the existing DVB1 header, time tag0x31 and
fixed fields without constructing CivilTime(hour24). Its payload is19 bytes and
complete byte tuple element21 bytes. The factory and exact matcher share the
existing byte grammar owner. Generic native TIME decode continues rejecting hour24;
only the declared SQL TIME key decoder recognizes this endpoint. Every finite
TIME key sorts before it. No generic primitive decoder broadening is permitted.

Maintenance, deletion, uniqueness, ordered reads, physical entry verification and
online build use the same declared projection and inverse. A value is never
silently omitted as NULL/sparse, and a malformed payload produces a typed failure.
Included-only covering fields retain their original payload and do not participate
in key projection. All existing finite key bytes are preserved. A provider layout
change must follow the existing physical generation contract; query-only tuning
must not change layout identity.

### Shared caller boundary

The codec consumes `(physical FieldValue, compiled FieldSchema)` and returns one
TupleElement. Its inverse consumes `(TupleElement, compiled FieldSchema)` and
returns the corresponding declared physical value. Extraction uses
`ResolvedIndex.descriptor.keyFieldSchemas`, never runtime value inspection.
Shared maintainer extraction passes the metadata to this codec; direct scalar
maintenance and scalar uniqueness follow the same path. Key readers/diagnostics
must decode with matching key schemas before exposing values. Generic
`DataAccess` callers without an index descriptor retain their existing behavior.

Providers with their own grouping or key extraction/decoding must consume this
same adapter at the existing boundary. This is mechanical metadata relay for
already admitted keys, not implementation of new SQL aggregation/index forms.
The final implementation inventory must include each affected direct bypass.


### Durable source Directory metadata

`DatabaseIndexStorageScope.entity(name:directoryComponents:directoryFields:)`
retains a nested immutable `DirectoryField` sequence in dynamic component order.
Each element contains only name, `FieldSchemaType` and optional `QueryColumnType`.
The names match dynamic components exactly once. Values are required scalars by
this contract; defaults, security policy, entity authority and unrelated fields
are not retained. Descriptor compatibility and canonical Directory admission
are validated when the scope is constructed and after decoding. The complete
snapshot participates in equality, hashing and stable structural ordering.

A Framework-owned `DatabaseIndexStorageScopeCodec` exposes bounded opaque-byte
encoding/decoding through the existing DatabaseExecution SPI. The retirement
scope frame and Server schema-apply job payload use this same codec. Registry's
existing query-type StorageFrame reader/writer supplies descriptor bytes; no
Server-specific SQL descriptor interpretation or duplicate conversion is added.
The initial-development durable format is updated coherently with all callers;
old version aliases are not retained. Corrupt or incomplete metadata is a typed
failure, never a current-schema or payload-shape fallback.

Transition-plan and publication scope creation capture descriptors from their
source entity at the existing schema-generation boundary. Durable retirement
uses those captured descriptors to resolve exact Directory components even
when the declaring entity has been deleted from the published target schema.
Polymorphic static scopes retain their existing static-path contract.

Server production edits are the opaque scope-byte relay and single durable
format update in `DatabaseSchemaApplyJobPlan.swift`, plus the retirement-specific
partition-page relay in `DatabaseDataOperationExecutor` and its retiring caller
in `DatabaseSchemaApplyResumableOperation`. Build paging remains unchanged. No Server DTO becomes the
owner of Directory, temporal interpretation or physical index policy.

## Runtime Flows

```text
admit source model + descriptor
  -> decode/validate temporal key under exact schema
  -> project only key tuple element
  -> uniqueness + remove old key + add new key in caller transaction
  -> retain original model for covering/persistence
  -> commit or propagate typed failure with transaction rollback
```

Online build uses the same extraction; it cannot publish a readable generation
with missing or incorrectly ordered temporal entries. Reads reconstruct SQL
infinity/TIME24 under the key descriptor, retaining original stored payload when
covering values are available. Selected predicate/order pushdown must separately
prove physical comparator equivalence; object encoding is not SQL temporal order.

## State, Ownership, and Lifecycle

The adapter is stateless. Installed schema/index generation owns descriptors;
original model/frame owns data; provider operation owns temporary tuple elements.
No pointer escapes borrowing. The index-only sentinel is a local encoding choice
and cannot become a retained persisted model. Existing request limits, tuple key
size limits, cancellation, transaction and generation lifetimes remain intact.

## Failure, Concurrency, and Constraints

Invalid descriptors, wrong envelopes, malformed endpoint bytes and SQL finite
range violations propagate typed errors. NULL alone follows existing sparse or
grouping rules; infinity/TIME24 never masquerades as NULL. Shared mutable state
and synchronization are unchanged. Fixed endpoint encoding is bounded; ordinary
payload work uses the existing admitted field/tuple limits.

## Verification and Change Impact

- Hard-coded finite and new endpoint/infinity tuple vectors prove byte spelling,
  strict SQL noncollision, logical round trip and ordering; primitive vectors
  remain unchanged, and generic TIME24 decoding still fails.
- A real declared ordered index proves negative infinity < finite < positive
  infinity, finite TIME <24:00, equivalent TIME carrier uniqueness, insert/update/
  delete and online build, physical decoder and conflict payload correctness.
- Covering data retains the exact original envelope; no extreme synthetic value
  is visible as persisted data. Malformed/foreign descriptor input fails with
  no mutation, followed by a successful valid operation.
- Directory tests separately prove physical injectivity; SQL scan/index predicate
  agreement proves selected comparison and pushdown consistency.
- Scope codec round trips and a real durable schema-job restart prove retirement
  of an infinity/TIME24 partition after source entity deletion. Unknown/corrupt
  descriptor metadata rejects before Directory or index deletion.
- Direct provider extraction/decoding bypasses must be migrated and verified only
  where the installed key schema admits these temporal families. Native primitive
  and unrelated index algorithms are unaffected proof subjects.
