# Index Runtime Design

## Responsibility boundary

`database-kit` owns `IndexDeclaration`, `IndexDefinition`,
`IndexDescriptor`, validation, and schema evolution. `database-framework`
consumes those values to plan reads, maintain derived data, and manage
lifecycle state. A server or platform adapter may invoke these APIs but does
not reinterpret index semantics.

```text
application schema
    -> DatabaseKit IndexDescriptor
        -> DatabaseEngine ResolvedIndex
            -> IndexType provider
                -> maintainer / reader
                    -> injected StorageEngine transaction
```

Each optional index product implements one semantic family. The engine owns
only the provider registry and transaction coordination; it does not duplicate
feature parameters in generic dictionaries.

## Typed dispatch

`ResolvedIndex` retains the validated descriptor, key expression, and
applicable item types. Runtime registries are keyed by `IndexType`. Providers
must pattern-match the complete `IndexDefinition` before constructing a
maintainer or reader and return a typed mismatch failure for any other case.
Query-plan diagnostics use the same `IndexType`; the execution layer has no
second index-family enum.

Runtime configuration also declares its `IndexType`. Configuration can choose
an execution algorithm, such as a vector implementation, but cannot change the
schema's logical dimensions, metric, fields, uniqueness, or graph model.
Every configuration targets the declaration's explicit `indexName`; runtime
code never reconstructs an index name from an entity or field. Configuration
families that select one exclusive implementation, such as vector algorithms,
reject duplicates rather than choosing by array order.

## Physical generations

Logical name and physical generation are deliberately distinct:

```text
[store]
  [indexes]
    [logical index name]
      [complete definition fingerprint]
        [provider layout fingerprint]
          [physical entries]
  [state]
    [logical index name]
      [complete definition fingerprint]
        [provider layout fingerprint] -> IndexState
  [metadata]
    [_violations]
      [logical index name]
        [complete definition fingerprint]
          [provider layout fingerprint]
```

`DatabaseIndexStorageIdentity` combines the declaration fingerprint with the
physical layout returned by the registered provider. It is paired with a
`DatabaseIndexStorageScope`, which retains the entity directory components or
the resolved polymorphic directory path. Every production maintainer, reader,
online build, lifecycle transition, uniqueness record, and rebuild must use
that scoped physical identity. A logical-name-only path is valid only when an
explicit migration removes every retained generation of a deleted index.

Changing any semantic definition field while retaining the name produces a
new physical generation. The old generation cannot become readable for the
new declaration. Changing a provider parameter that affects persisted bytes
also produces a new generation. Query-only tuning does not. For example,
HNSW `m` and `efConstruction` are physical, while `efSearch` is query tuning.
The provider must validate the complete runtime configuration and return its
layout before storage preparation or schema staging begins.

Each schema generation has two canonical identities. The physical fingerprint
contains the schema fingerprint and sorted provider-layout fingerprints. The
runtime fingerprint contains that physical fingerprint plus the complete
canonical execution options, the security mode, and the application-controlled
execution identity. The application keeps that identity's identifier stable and
increments its revision when executable behavior changes outside canonical
schema metadata. Therefore a query-only or authorization-policy change
publishes a new execution generation without rebuilding bytes, while a layout
change requires an explicit schema transition and physical replacement. Schema,
runtime registry, authorization policy, both fingerprints, and provider layouts
are retained together by every request lease.

The physical fingerprint is pinned in resumable schema-apply plans. Every
staging slice and publication resolves it again and must match the pinned
value. Query-only runtime-factory changes remain valid, while physical drift
fails explicitly instead of mixing layouts across data roots.

When a provider layout changes without a logical definition change, schema
apply builds the new layout as a separate generation and persists an exact
retirement marker for the previous definition-and-layout pair. Cleanup never
uses the definition prefix for this case, because that prefix also owns the
new layout. The active schema lease retains the schema, runtime registry, and
provider-layout map as one immutable value.

## Lifecycle

```text
disabled --enable--> writeOnly --successful build--> readable
    ^                    |
    +------disable-------+
```

Writes maintain `writeOnly` and `readable` generations. Reads admit only a
persisted `readable` state for the exact fingerprint. Missing, corrupt,
unsupported, or mismatched state is a typed failure. No scan, empty result, or
legacy generation is used as fallback.

Online builds store progress inside the physical generation. Unique builds
store violations under the same fingerprint and cannot publish while a
violation remains. Algorithm-specific finalization completes before the
readable transition.

## Schema transitions

| Change | Runtime action |
|---|---|
| added | stage a write-only generation, publish the schema, then backfill it |
| removed | publish removal, then retire the exact previous definition and provider layout |
| replaced | stage and build the new definition or provider layout separately, then retire the exact previous pair |
| unchanged | retain the current readable generation |

Concrete and polymorphic indexes follow the same rules. A polymorphic
fingerprint includes the logical group declaration and every concrete member
projection so a member-field mismatch cannot reuse another generation.

Retirement intent is persisted in each data root before publication. It is
not owned only by a host job checkpoint. The retirement marker remains until
the exact physical generation and its lifecycle, violation, rebuild, and
pending-build state have been cleared in the same data-root transaction.
Both build and retirement markers include `DatabaseIndexStorageScope`; an
entity name alone is not a store locator. This keeps interrupted work distinct
when an entity's directory contract changes and lets cleanup resolve the old
directory after its declaration is no longer present in the target schema.
For a dynamic directory, catalog entries are accepted only when their exact
partition-field set matches the stored scope; entries from an older directory
contract are skipped without being interpreted as the target store.
Re-applying a published target therefore resumes cleanup after cancellation or
restart. Staging a different target reconciles markers against that target and
never retires the fingerprint it selects as active.

```text
stage build state + durable retirement markers
    -> publish target schema
        -> install each data-root snapshot
            -> build target generations
                -> drain data-root retirement markers
                    -> finish schema application
```

Before the first retirement mutation, the runtime waits outside any storage
transaction for every request lease older than the published target generation
to drain. New requests can acquire only the published generation. This keeps
the previous physical index readable for every request that was admitted under
the previous schema and avoids holding a transaction open while waiting.

This lease barrier is owned by one in-process `DBContainer`. A deployment that
opens the same database root through multiple concurrently active containers
must provide cross-container schema-transition exclusion or quiescence in its
host layer before retirement begins. Distributed host admission does not belong
in the framework's index or schema model.

## Compound key order

There is no Permuted runtime product. Alternate order is represented by an
ordinary ordered declaration:

```swift
#Index(.ordered(
    name: "locations_by_city_country_name",
    keys: [
        .ascending(\Location.city),
        .ascending(\Location.country),
        .ascending(\Location.name),
    ]
))
```

This removes a duplicate semantic family and lets the ordered provider own all
ordered key layouts.

## Completion conditions

An index change is complete only when:

- macro expansion and manual construction produce the same descriptor;
- JSON and Wire round trips preserve the complete definition;
- schema diff reports definition changes as replacement;
- runtime bootstrap rejects a missing provider;
- writes, reads, online builds, rebuilds, and uniqueness tracking resolve the
  same fingerprinted subspace;
- pending retirement survives cancellation after publication and is drained by
  a replacement apply;
- build and retirement checkpoints retain the source or target directory scope,
  exact definition fingerprint, and exact layout fingerprint;
- build completion clears a pending marker only after the complete target
  matches the schema, directory scope, definition, and provider layout in the
  active execution generation;
- a durable host plan rejects duplicate logical index targets and any exact
  target present in both its build and retirement sets;
- a target reversal never retires the generation selected by the new target;
- corrupt lifecycle state and unsupported definitions fail explicitly;
- no old metadata DTO, string kind dispatch, Permuted product, or
  logical-name-only active path remains.
