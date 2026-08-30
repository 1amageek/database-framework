# Directory

## Purpose and Scope

The Directory component owns the canonical binding of a `#Directory`
declaration to a StorageKit Directory node. It fixes the node layer tag of
every declared position, the exact textual form of a dynamic component, the
reserved database topology below the storage root, and the admission and
transaction unwrapping required before any `DirectoryAccess` call.

- Parent: [DatabaseEngine](../DESIGN.md).
- Children: none.

This component realizes SPEC sections 8.7, 10.1, 12.1, 12.2, 12.3, and 13. It
is the single normative source for the canonical component grammar; no other
design document restates it.

## Responsibilities and Boundaries

The component owns:

- derivation of the layer tag of every declared node position from the complete
  declaration set, before any data access;
- the canonical `FieldValue` to Directory component string codec and its
  reserved image;
- the reserved topology `default`, `bases/<Base.ID>`, `system`,
  `system/database-framework`, and `data`, and the bootstrap state machine that
  admits or rejects an existing storage root;
- open-existing resolution for reads and open-or-create resolution for writes,
  both executed inside the caller's transaction;
- the declaration-shaped walk that enumerates the partitions of one entity over
  StorageKit's child listing, and the continuation that resumes it;
- the container admission and unwrapping contract that lets an adapter-owned
  `DirectoryAccess` accept a container-issued transaction.

The component does not own key layout, prefix allocation, node existence,
child listing, or recursive removal. Those belong to StorageKit. It computes no
key prefix: every prefix it uses is read from a `Directory` returned by
`DirectoryAccess`. It does not decide query authority, index policy, or
serialization framing.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | Session, transaction, and failure contract | Supplies the transaction and the operation admission this component requires. | This component is not a session boundary and never opens its own transaction. |
| [Core](../Core/DESIGN.md) | used by | Resolved entity Directory | Core reads group and entity keyspaces resolved here. | A resolved node is valid only inside the transaction that resolved it. |
| [Read](../Read/DESIGN.md) | depends on | Operation admission | Directory work is admitted like any other storage operation. | A bare session plus caller assertion is insufficient admission. |
| StorageKit `Directory` | depends on | `DirectoryAccess`, `Directory`, `Partition`, `DirectoryLimits` | Owns catalog transactions, opaque prefixes, layer tags, and root bootstrap. | `FDBDirectoryAccess` downcasts the transaction to its own type, so a container wrapper must be unwrapped first. |
| DatabaseKit `Schema.Entity` | depends on | `directoryComponents`, `directoryLayer`, field schemas | Supplies declaration shape and declared field kinds. | DatabaseKit validates declarations; it never derives node layer tags. |

## Architecture

```text
Schema declarations (all entities)
        |
        v
DirectoryLayerTagMap        shape trie: position -> derived LayerTag
        |
        v
DatabaseDirectoryTopology   default | bases/<Base.ID> -> system | data
        |                                                     |
        |                                                     v
        |                            DirectoryComponentCodec  FieldValue -> component
        |                                                     |
        v                                                     v
ContainerDirectoryAccess  ---- admission + unwrap ----> StorageKit DirectoryAccess
```

## Contracts and Invariants

### Layer-tag derivation

Positions are identified by declaration shape, not by resolved dynamic values.
A position is the ordered sequence of component shapes leading to it, where a
static shape is its literal name and a dynamic shape is its declared field
kind.

- A declaration is an entity `#Directory` or a polymorphic group `#Directory`.
  An entity resolves the directory holding its records; a group resolves the
  directory holding the projection shared by its members. Both assign a layer,
  so derivation reads the complete declaration set rather than the entities
  alone.
- A group declares only static components, because it addresses one directory
  shared by every member and has no record from which a dynamic component could
  be resolved. A dynamic component in a group is
  `DirectoryLayerTagError.dynamicComponentInPolymorphicGroup`.
- A declaration assigns a layer tag only to its own leaf position.
- A position that some declaration resolves as a `.partition` leaf is a
  Partition for every declaration passing through it.
- A position resolved as a `.default` leaf by one declaration and as a
  `.partition` leaf by another is `DirectoryLayerTagError.inconsistentLayer`,
  whether the two declarations are entities, groups, or one of each.
- Declarations sharing a dynamic position must declare the same field kind
  there; a disagreement is
  `DirectoryLayerTagError.inconsistentDynamicFieldKind`.
- A position no declaration resolves as a leaf is a plain Directory.
- Derivation runs once at bootstrap, before any data access, and its result is
  immutable for the schema generation.

### Admitted dynamic field kinds

SPEC 10.1 requires every dynamic component to be a required scalar field with a
canonical textual Directory component form. DatabaseKit validates the required,
scalar, occurs-once, and admitted-kind parts per declaration, so a declaration
this component cannot encode fails where it is written rather than at container
bootstrap; `FieldSchemaType.hasCanonicalDirectoryComponent` owns which kinds are
admitted and `DirectoryLayerTagMap.admitsDynamicComponent` delegates to it. This
component still owns the exact strings and keeps
`DirectoryLayerTagError.unsupportedDynamicFieldKind` as a precondition on a
`package` input it does not construct, so a schema that reached derivation
without that validation is rejected at bootstrap rather than at a later write. The
owner's set and the set `DirectoryComponentCodec` can encode are pinned against
each other by a test naming every `FieldSchemaType` case, because that type is
not `CaseIterable` and a new kind would otherwise be admitted or rejected by
omission.

`FieldSchema` records a field kind, not an enum's raw representation, so
`.enum` is one kind covering both representations an enum value materializes
as, `.string` and `.int64`, per `FieldSchemaValueValidator`. Two declarations
sharing a dynamic position and both declaring `.enum` therefore agree under the
SPEC 10.1 rule even when their raw representations differ. That is safe rather
than merely tolerated: a canonical component carries exactly one `-`, the tag
separator, because `-` is neither unreserved nor the token separator and is
escaped as `%2D` inside a string or bytes body. The tag alphabet is a fixed
disjoint set, so `encode` is injective across kinds and not only within one,
and `s-admin` cannot collide with `i64-3`. Discriminating more finely would
require `FieldSchema` to express enum raw representation, which is a DatabaseKit
metadata change rather than a change to this contract.

A dynamic component naming a field its entity does not declare is
`DirectoryLayerTagError.unknownDynamicField`. `Schema.Entity` validation
already rejects such a declaration, so the case is unreachable through the
schema's public initializer; it exists so that a relaxation there surfaces as a
typed rejection instead of a trap.

### Static and dynamic disjointness

SPEC 12.1 requires a static component to open the node named by that exact
component, so a static name is never escaped. SPEC 12.2 requires the binding to
be injective. Together these leave one gap that SPEC does not name: a static
literal that happens to equal a canonical dynamic component, for example the
static name `s-users` against the string value `users`.

This component closes the gap in the direction that preserves both rules: the
canonical image is reserved. A static `#Directory` component that decodes as a
canonical dynamic component is
`DirectoryLayerTagError.staticComponentInCanonicalImage`. Static names outside
the image, including a static sibling of a dynamic position such as
`["tenants", "global"]` beside `["tenants", <dynamic tenantID>]`, remain legal;
the dynamic value `global` encodes as `s-global` and cannot collide.

### Canonical dynamic component

```text
component  ::= tag "-" body
body       ::= token ( "." token )*
token      ::= ( unreserved | escape )*
unreserved ::= "A"..."Z" / "a"..."z" / "0"..."9" / "_" / "~"
escape     ::= "%" HEXDIGIT-UPPER HEXDIGIT-UPPER
```

- A canonical component contains exactly one `-`, which separates the tag from
  the body. `-` and `.` never occur inside a token; they are escaped as `%2D`
  and `%2E`.
- The tag names the `FieldValue` case, so the codec is injective over all
  values of all kinds, which implies the per-kind injectivity SPEC 12.2
  requires. The declared field kind is used to reject a value whose case the
  declaration does not admit; a `.enum` field admits `.int64` and `.string`,
  matching `FieldSchemaValueValidator`.
- Every hexadecimal body uses uppercase digits, including `uuid`.
- Integer bodies are emitted digit by digit with no leading zero and no plus
  sign; a negative value is prefixed with `n`. Floating-point bodies are the
  big-endian IEEE 754 bit pattern in uppercase hexadecimal, so `+0.0` and
  `-0.0` are distinct components. Swift `description`, `String(_:radix:)`,
  locale formatting, JSON, and untagged interpolation are not used.
- Structured kinds emit a fixed token count, or a count token followed by that
  many sub-values. Decoding is recursive descent driven by the kind, and a
  component with leftover or missing tokens is rejected.
- A `FieldObject` emits its fields in ascending Unicode scalar order of the
  key, so iteration order cannot vary.

| `FieldValue` case | Tag | Body tokens |
|---|---|---|
| `bool` | `b` | `0` or `1` |
| `int8` `int16` `int32` `int64` | `i8` `i16` `i32` `i64` | signed decimal |
| `uint8` `uint16` `uint32` `uint64` | `u8` `u16` `u32` `u64` | decimal |
| `float32` | `f32` | 8 hex digits |
| `float64` | `f64` | 16 hex digits |
| `decimal` | `dec` | coefficient, scale |
| `string` | `s` | escaped UTF-8 |
| `bytes` | `y` | escaped bytes |
| `date` | `d` | year, month, day |
| `time` | `t` | hour, minute, second, nanoseconds |
| `dateTime` | `dt` | year, month, day, hour, minute, second, nanoseconds |
| `timestamp` | `ts` | seconds, nanoseconds |
| `timeSpan` | `sp` | seconds, nanoseconds |
| `calendarPeriod` | `cp` | months, days |
| `geographicPoint` | `gp` | latitude, longitude |
| `geographicPosition` | `gq` | latitude, longitude, height |
| `uuid` | `uu` | 32 hex digits |

`null`, `array`, `object`, `vector`, `reference`, and `rdfTerm` have no
canonical component and are rejected with
`DirectoryComponentCodecError.unsupportedFieldKind`. SPEC 10.1 admits only a
required scalar dynamic component, and each rejected case is either absent,
repeated, nested, or unbounded. Rejecting them is what makes the admitted set
total: an entity declaring such a field as a dynamic component fails at
declaration validation rather than producing a component the codec cannot
invert.

The codec is a bijection between admitted values and canonical strings:
`decode(encode(v)) == v` for every admitted value, and `encode(decode(s)) == s`
for every string that decodes. Non-canonical spellings, including a leading
zero, a lowercase escape, or an escape of an unreserved byte, do not decode.
`decode` enforces the second direction structurally: it re-encodes the value it
built and rejects any input that is not that exact string. This makes
canonicality one check rather than a per-kind obligation, and it is what
rejects inputs the value types normalize away, such as a negative-zero
`geographicPoint` latitude.

The conversion is total in the sense SPEC 12.2 requires: it is defined for
every value of every admitted kind. A value whose canonical component exceeds
`DirectoryLimits.maximumComponentByteCount` fails at the StorageKit address
boundary with `DirectoryAddressError.componentTooLong`. It is never truncated
and never hashed, because either would destroy injectivity.

### Topology and bootstrap

```text
database root                       configured DirectoryPath, plain
├── default                         Partition, the Default Partition
│   ├── system
│   │   └── database-framework
│   └── data                        #Directory binding starts here
└── bases                           plain, present only with MultiBase
    └── <Base.ID>                   Partition, one per Base
        ├── system
        │   └── database-framework
        └── data
```

- The database root is the configured `DirectoryPath` of one storage domain,
  resolved from the engine's Directory root. A container with several storage
  domains has one database root per domain; the layout below each root is the
  same.
- `default` and `bases` are reserved at the database root;
  `system`, `database-framework`, and `data` are reserved at every Tenant
  Partition root. A `#Directory` declaration cannot address any of them: its
  first component is a child of `data`.
- A Base Partition's address is derived, never configured: it is
  `<database root of the placement's domain>/bases/<Base.ID>`. A placement
  selects the storage domain of a newly provisioned Base and contributes no
  path component. Framework metadata may retain that derived address as a
  reference, but the StorageKit catalog remains the only resolver.
- Framework metadata exists only below `system/database-framework`;
  application data, indexes, and relationships exist only below `data`.
- Bootstrap opens the root through StorageKit, whose allocation authority is
  the only witness that a root is initialized (SPEC 8.7). This component
  consumes that verdict; it records no witness of its own.

```text
open root -> initialized  -> open the reserved topology
          -> uninitialized -> create the root and topology in one transaction
          -> foreign        -> DatabaseDirectoryError.incompatibleStorageLayout(r)
```

A root holding data no Directory catalog wrote is never initialized, deleted,
or reinterpreted.

### Binding

- Resolution starts at the `data` Directory of the selected Tenant Partition
  and consumes components in declaration order.
- A read resolves with open-existing only. A missing node yields `nil`, which
  the caller reports as an absent keyspace and therefore no matching data. A
  read never creates a node and never writes catalog metadata.
- A write resolves with open-or-create in the caller's transaction, so node
  creation commits atomically with the data mutation.
- Descent through a Partition needs no special step. `Directory.childLayerRoot`
  selects the nested Directory Layer of a Partition node, so opening a child
  `in:` a Partition node descends into that nested layer. This component never
  computes the nested layer root itself.
- Every open passes the declared tag of the position it addresses, so a node
  stored under another layer is refused by `DirectoryAccess` instead of being
  addressed at the wrong prefix. A layer is part of a node's identity, so this
  holds wherever an address is used, not only on the binding walk.
- Ordinary row deletion never removes a node. Explicit removal is a separate
  destructive operation admitted by authorization and the absence of an active
  lease, and performed by StorageKit.

### Base addresses

`bases` holds one Partition per Base and nothing else, so a child stored under
another layer occupies a Base address without being one.

- `listBaseTenantNames` reports it as
  `DatabaseDirectoryLayoutError.nonPartitionBase`. Omitting it would present a
  shorter listing as a complete one and hide the node.
- `removeBaseTenant` opens the address as a Partition before removing it, so
  the mismatch StorageKit reports stops a deletion that would otherwise destroy
  a subtree this layout never committed. An address no node occupies is the
  state the call establishes, so an absent name returns without failing.

### Partition enumeration

The partitions an entity has are the nodes its declaration resolves at its
dynamic positions. The walk is depth-first, expects the declared tag at every
level, and lists children through StorageKit.

A page's continuation is the path of the last partition it reported, and the
caller hands it back on the next call, so it is bound to the declaration that
issued it: the cursor carries the entity name and the component shape, and a
cursor issued for another declaration, or for a shape this declaration no
longer has, is `DatabasePartitionCatalogError.invalidContinuation`. Without
that binding, two declarations of equal component count accept each other's
cursors and report the remainder of a different tree as this entity's
partitions.

### Durable work that outlives its declaration

Index retirement runs against a path the published schema may no longer
declare, so it cannot re-derive that path's layer when it executes. The durable
marker records the layer of every component of its own path, taken from the
generation the retirement was planned against, and retirement opens the path
with that vector. A node recreated under another layer since the work was
staged is refused rather than cleared.

A scope the source generation cannot type exactly, because the component shape
differs or the declaration is gone, records no layer. Such a marker addresses
its path without verification, which is the state a marker written before
layers were recorded is read back in; it is never given an invented contract.

### Container admission and unwrapping

`ContainerStorageEngine` publishes the underlying engine's
`transactionDomain` object identity unchanged, because `FDBDirectoryAccess`
and `leasePartition` compare domains with `===`.

`ContainerDirectoryAccess` wraps the underlying `DirectoryAccess`. For each
operation it:

1. requires a live `DatabaseStorageLifecycle` operation lease, so a Directory
   call cannot outlive container admission;
2. unwraps the container transaction to the underlying transaction, because
   `FDBDirectoryAccess` resolves its transaction by downcast to
   `FDBStorageTransaction` and rejects any wrapper with
   `StorageError.storageDomainMismatch`;
3. forwards to the underlying access and returns its `Directory` unchanged.

Admission is a contract, not a list of concrete types. Every transaction the
container can lend to a backend Directory capability conforms to
`ContainerAdmittedTransaction`, which owns two facts: whether the transaction
may mutate the Directory catalog, and how to borrow the backend transaction
while retaining every lease that keeps it valid. `ContainerDirectoryAccess`
performs one conditional cast to that protocol, so a read path that reaches a
Directory through any admitted capability is admitted by construction rather
than by enumeration.

| Admitted transaction | Directory mutation | Borrow source |
|---|---|---|
| `ContainerTransaction` | permitted | own operation lease |
| `ContainerTransactionAccess` | permitted | own operation lease |
| `ReadAuthorizedTransactionAccess` | refused | resolved admitting scope |
| `DatabaseReadTransaction` | refused | its `storageAccess` |

A transaction that is not admitted carries no container operation lease, so a
Directory call on it could outlive the storage lifecycle. Directory resolution
therefore rejects a foreign or raw backend transaction with
`StorageError.invalidOperation`, and names the offending type in the message.
A read-only capability asked for a mutating Directory operation fails with
`DatabaseReadTransactionError.mutationRequiresWriteAccess` before any lease is
taken.

`ReadAuthorizedTransactionAccess` resolves recursively: an attenuated child
scope resolves to the scope that admitted it, which is itself a read-authorized
access. Each level contributes one `DatabaseReadScopeOperationLease` to the
borrow, and `ContainerDirectoryTransactionBorrow` retains the whole stack until
`end(for:)` releases it in reverse acquisition order. Recursion terminates
because `admittedReadAccess` never re-wraps an admitted access and `scoped`
always resolves to a distinct parent.

## Runtime Flows

### Bootstrap

```text
open transaction
  -> openOrInitializeRoot (absent) or openRoot (initialized)
  -> open or create default Partition
  -> open or create system, system/database-framework, data
  -> derive DirectoryLayerTagMap from all declarations
  -> commit
```

### Read binding

```text
tenant data Directory
  -> for each declared component
       -> static: name; dynamic: DirectoryComponentCodec.encode(value)
       -> open(name, expecting: derived tag) -> nil ends the walk
  -> leaf Directory -> entity Subspaces
```

### Write binding

```text
tenant data Directory
  -> for each declared component
       -> openOrCreate(name, layer: derived tag)
  -> leaf Directory -> entity Subspaces -> data mutation in the same transaction
```

## State, Ownership, and Lifecycle

| State | Owner | Lifetime |
|---|---|---|
| Derived layer-tag map | Container, per schema generation | Until the schema generation changes |
| Reserved topology handles | Container bootstrap | Container lifetime, revalidated per use |
| Resolved entity `Directory` and `Subspace` | Caller transaction | The resolving transaction |
| Operation lease | `DatabaseStorageLifecycle` | One Directory operation |

The two lifetimes differ because their writers differ. An entity node below
`data` may be created by any concurrent writer, so a handle for it is valid
only inside the transaction that resolved it. A reserved topology node is
written only by container bootstrap and is never removed while the container
is open, so its handle is retained for the container lifetime and revalidated
at each use through the same generation check StorageKit applies in
`leasePartition`: a retained `Partition` whose generation no longer matches is
rejected rather than used.

`DatabaseTransaction` caches resolved subspaces per transaction. One
transaction observes one consistent snapshot including its own writes, so a
cached prefix cannot change under it. The cache stores present results only:
absence is never cached, so a read, a create, and a second read inside one
transaction observe read-your-writes.

## Failure, Concurrency, and Constraints

- `DirectoryLayerTagError` reports declaration defects: `inconsistentLayer`,
  `inconsistentDynamicFieldKind`, `staticComponentInCanonicalImage`,
  `dynamicComponentInPolymorphicGroup`. Each names the declaration it refers to
  as a `DirectoryDeclarationOwner`, because a leaf may be owned by an entity or
  by a polymorphic group.
- `DirectoryComponentCodecError` reports value defects: `unsupportedFieldKind`,
  `valueKindMismatch`, `malformedComponent`.
- `DatabaseDirectoryError.incompatibleStorageLayout` reports a root that
  Version 1 refuses to open.
- `DatabaseDirectoryLayoutError` reports defects of the layout this component
  commits itself: `missingReservedDirectory`, `nonPartitionBase`, and
  `emptyRootPathComponent`. A Tenant Partition and its reserved children commit
  in one transaction, so a Partition that exists without one of them is a
  corrupted layout rather than an absent Tenant, and every read path reports it
  instead of returning absent.
- `missingReservedDirectory` carries the child names actually present beside
  the missing one. Which node was lost is the distinction an operator acts on:
  a Partition holding no child at all was never populated or resolves to a
  prefix that is not its own, while a Partition still holding its siblings lost
  exactly one node. The missing name alone cannot separate those cases, so the
  observed children belong to the failure, bounded by a fixed report limit.
- Address and depth bounds are owned by `DirectoryLimits`; this component
  propagates `DirectoryAddressError` unchanged.
- No failure is converted into empty success. A missing node on read is an
  explicit absent result, not an error and not an empty creation.
- Directory work is serial on the caller's transaction. The derived tag map is
  immutable after bootstrap and therefore shared without synchronization.

## Verification and Change Impact

| Contract | Evidence |
|---|---|
| Canonical form of every kind | Hard-coded golden vectors whose expected strings are literals, never produced by the converter under test. |
| Per-kind injectivity | Discriminator pairs including `+0.0` against `-0.0`, `Int128` extremes, empty string against empty bytes, and values forcing escapes. |
| Bijection | Round-trip in both directions, plus rejection of non-canonical spellings. |
| Layer-tag derivation | Declaration sets producing a Partition position inherited by a declaration passing through it, a position no declaration leaves as a leaf, and both typed disagreements in either supply order. |
| Group declarations | A group Partition leaf is inherited by an entity passing through it, a group disagreeing with an entity or with another group is rejected in either supply order, and a dynamic component in a group is rejected. |
| Admitted kinds | Every `FieldSchemaType` case paired with a materializable value; admission agrees with `DirectoryComponentCodec.encode` for each, and `.enum` is checked in both representations. |
| Reserved image | A static component inside the image fails with `staticComponentInCanonicalImage`; a static sibling outside it succeeds. |
| Topology | Bootstrap creates `default/system/database-framework` and `default/data`, and a declaration binds below `data`. |
| Read never creates | A read of a missing declaration returns absent and leaves the catalog byte-identical. |
| Write atomicity | Node creation and the row mutation are observable only after the same commit. |
| Layout rejection | A nonempty unmarked root fails with `incompatibleStorageLayout` and is not modified. |
| Base addresses | A `bases` child stored under another layer fails both listing and removal, an absent name removes nothing, and a Base Partition is still listed and removed. |
| Reserved-child corruption | A Tenant Partition whose `system` node is removed fails every read path with `missingReservedDirectory`, naming the missing node and listing the children that remain. |
| Enumeration cursor | A cursor issued for one declaration is refused by another declaration of the same shape, and resumes its own walk to exhaustion. |
| Retirement layer identity | A staged retirement records the layer of every source component, a source node recreated under another layer is refused, and a scope the schema cannot type records none and stays addressable. |

Owners:
[DirectoryComponentCodecTests](../../../Tests/DatabaseEngineTests/DirectoryComponentCodecTests.swift)
for the canonical form, injectivity, and bijection;
[DirectoryLayerTagMapTests](../../../Tests/DatabaseEngineTests/DirectoryLayerTagMapTests.swift)
for derivation, the reserved image, and admitted kinds;
[DirectoryBindingTests](../../../Tests/DatabaseEngineTests/DirectoryBindingTests.swift)
for topology, bootstrap, and binding;
[DirectoryReadCreatesNothingTests](../../../Tests/DatabaseEngineTests/DirectoryReadCreatesNothingTests.swift)
for the read direction creating nothing;
[DirectoryLayerIdentityTests](../../../Tests/DatabaseEngineTests/DirectoryLayerIdentityTests.swift)
for Base addresses, the enumeration cursor, and retirement layer identity.

Changing the canonical grammar, the reserved image rule, the derived tag rule,
or the reserved topology is persistent schema identity under SPEC 10.2. It
invalidates stored layouts and requires explicit data movement, so it is never
lightweight schema evolution.
