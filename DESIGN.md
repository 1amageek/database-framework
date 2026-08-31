# database-framework

## Purpose and Scope

Package-level design authority and design index for the in-process
`database-framework` package. This document owns package composition,
dependency direction, and the direct-child index. It does not copy the
contracts owned by a child design.

- Parent: the workspace system design at [../DESIGN.md](../DESIGN.md).
- Direct children: [Database](Sources/Database/DESIGN.md),
  [DatabaseEngine](Sources/DatabaseEngine/DESIGN.md),
  [GraphIndex](Sources/GraphIndex/DESIGN.md),
  [BitmapIndex](Sources/BitmapIndex/DESIGN.md),
  [FullTextIndex](Sources/FullTextIndex/DESIGN.md),
  [RankIndex](Sources/RankIndex/DESIGN.md),
  [VectorIndex](Sources/VectorIndex/DESIGN.md), and
  [VersionIndex](Sources/VersionIndex/DESIGN.md).

The child list is an index of design authorities that exist for the current
material. A source target with only caller-signature adaptation and no
independent design contract is not promoted to a new design child.

## Responsibilities and Boundaries

The package owns in-process composition and the dependency boundary between
the database entry module, execution engine, graph execution, and specialized
index modules. `DBContainer` composition selects the runtime features and
injects the storage engine; the child modules own their domain behavior.

The package does not own primitive values, schema/query declarations, storage
backend implementations, wire dispatch, transport, server hosting, or
application schemas. Those contracts remain in their owning packages or
products.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [Database](Sources/Database/DESIGN.md) | child | SQL and SQL-facing SPARQL entry/adaptation | Routes public database calls into the execution and graph contracts. | It must not absorb graph-facing SPARQL physical execution or session authority. |
| [DatabaseEngine](Sources/DatabaseEngine/DESIGN.md) | child | Session, authorization, transaction, persistence, and resource contracts | Owns the in-process execution boundary consumed by feature modules. | Changes to session or retained ownership require every feature child to revalidate its caller path. |
| [GraphIndex](Sources/GraphIndex/DESIGN.md) | child | RDF, SPARQL, graph, and graph-table physical execution | Owns physical graph execution above the DatabaseEngine boundary. | SQL entry/adaptation remains in Database. |
| [BitmapIndex](Sources/BitmapIndex/DESIGN.md) | child | Bitmap read and retained-result contract | Consumes the DatabaseEngine session and result boundary. | Bitmap-specific behavior must not become a package-wide resource contract. |
| [FullTextIndex](Sources/FullTextIndex/DESIGN.md) | child | Full-text read, scoring, facet, and autocomplete contract | Owns full-text execution and its child designs. | Authorization and session creation remain in DatabaseEngine. |
| [RankIndex](Sources/RankIndex/DESIGN.md) | child | Ordered rank read and retained-key contract | Owns rank-specific ordering and result transfer. | It must preserve the DatabaseEngine ownership boundary. |
| [VectorIndex](Sources/VectorIndex/DESIGN.md) | child | Vector algorithm and retained-match contract | Owns vector maintenance and search algorithms. | Session and final row ownership remain in DatabaseEngine. |
| [VersionIndex](Sources/VersionIndex/DESIGN.md) | child | Version-history read and retained-result contract | Owns version ordering and annotations. | It must not define generic transaction or authorization policy. |

External package APIs are dependencies of these children and are not
duplicated in this package index.

## Architecture

Arrows point from a consumer to the contract it consumes:

```text
Database (public entry and adaptation)
    -> GraphIndex (graph/RDF/SPARQL physical execution)
    -> DatabaseEngine (session, authorization, transaction, persistence)
        -> DatabaseKit + DatabaseTypes + StorageKit

BitmapIndex / FullTextIndex / RankIndex / VectorIndex / VersionIndex
    -> DatabaseEngine

DatabaseEngine -X-> specialized feature modules
```

`DatabaseEngine` remains independent of specialized index implementations;
feature targets are composed by the `Database` entry target or an explicit
runtime configuration.

## Contracts and Invariants

- A child design is the sole authority for the responsibility named by its
  scope; parent documents link to it instead of restating its contract.
- Database owns SQL and SQL-facing SPARQL entry/adaptation, GraphIndex owns
  graph-facing SPARQL and RDF physical execution, DatabaseEngine owns
  authenticated session execution, and specialized indexes own their
  algorithm-specific execution.
- Dependency arrows do not reverse through an implementation detail: the
  execution engine cannot import a feature module merely because an entry
  point consumes that feature.
- Canonical session paths preserve authorization, transaction, meter,
  ownership, cancellation, and typed-failure guarantees across module
  boundaries.
- The default composition uses one injected storage engine and one ordinary
  database root. Base/placement and persisted-grant composition are enabled
  only by the explicit `MultiBase` trait.
- Runtime capabilities are injected at composition; global mutable
  registration is not a package contract.

## Runtime Flows

```text
public SQL/graph/index request
    -> Database entry/adaptation or feature entry
    -> DatabaseEngine session and admission
    -> graph or specialized physical executor
    -> retained/canonical result boundary
    -> caller-visible output
```

The exact execution and failure transitions are owned by the corresponding
child design. This package only guarantees that the composition preserves
their declared dependency direction.

## State, Ownership, and Lifecycle

`DBContainer` owns runtime composition, schema state, and its injected storage
engine. DatabaseEngine owns session-scoped authority, transaction, snapshot,
and work-meter lifetimes. GraphIndex and specialized modules own only their
algorithm or physical-operation state; they do not create a second authority
for a canonical session path.

## Failure, Concurrency, and Constraints

Typed authorization, storage, decode, budget, cancellation, conflict, and
cleanup failures cross package boundaries unchanged. Shared mutable runtime
state follows the owning module's actor or mutex contract; package composition
does not weaken synchronization or ownership for a target or feature.

## Verification and Change Impact

Each child design owns its focused behavioral evidence. A package-level change
is complete only after the affected child contracts are reviewed and the
integrated package path is verified once at the converged boundary. A change
to dependency direction, runtime composition, or a child public contract
invalidates the corresponding child and package integration evidence.
