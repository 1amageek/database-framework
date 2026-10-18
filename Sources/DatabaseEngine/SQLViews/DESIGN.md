# SQLViews

## Purpose and Scope

SQLViews is a child of [DatabaseEngine](../DESIGN.md). It owns admission,
stable dependency binding, ordinary view expansion, and materialized view
population. SQLDDL owns statement dispatch; SQLCatalog owns durable generation
publication. This component does not invent a physical entity for an ordinary
view.

## Responsibilities and Boundaries

- Bind each stored SELECT through the canonical type binder before publication.
- Preserve defining relation and column identities across logical renames. Each stored column binding includes
  its admitted input type; candidate publication rejects changes to referenced
  input types before any generation becomes visible.
- Invoke ordinary definitions using the caller's principal, session, temporal
  environment and work meter.
- Persist materialized rows through schema-driven entity runtimes with a hidden
  row identity, retaining duplicate output rows and every supported query type.
- Reject user mutations against ordinary and materialized views.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | Transactional generation and reads | Owns composition | Candidate schema/runtime/catalog are one generation |
| [SQLCatalog](../SQLCatalog/DESIGN.md) | depends on | Immutable snapshot and stable IDs | Stores view definitions | Admission rejects missing/incompatible/cyclic dependencies |
| [QueryExecution](../QueryExecution/DESIGN.md) | coordinates with | Canonical binder and relation producers | Executes definitions | No raw storage or empty-success substitute |
| [SQLBlock](../SQLBlock/DESIGN.md) | used by | Statement scope and transaction | Defines and refreshes views | Publication and population share one transaction |

## Architecture

```text
SQLDDL -> SQLViews definition admission -> canonical binder -> output metadata
                     |                           |
                     v                           v
             SQLCatalog generation      stable source/column IDs
                     |
caller SELECT -> SQLReadResolution -> stored SELECT binding -> canonical producer
                     |
MV refresh -> canonical rows -> QueryFieldStorageAdapter -> persisted models
```

## Contracts and Invariants

| Contract | Guarantee | Rejection |
|---|---|---|
| Definition | Complete bound output names/order/types; nullability metadata remains conservative | Parameters and statement-private relations cannot enter durable definitions |
| Binding | Original schema/table names resolve by stored relation IDs; columns retain original names and stable IDs | Removed referenced columns, missing sources and incompatible output types fail before publication |
| Lexical scope | Canonical CTE resolution precedes durable source bindings; nested views receive isolated bindings | Caller CTEs and search paths cannot retarget a stored definition |
| Replacement | Existing names/order/types remain an unchanged prefix; appended columns are allowed | Existing output rename, reorder or type change |
| Authority | Ordinary views authorize all expanded dependencies as the invoker; materialized reads authorize persisted backing entity/fields, while CREATE/REFRESH authorize the defining query | No definer authority or automatic backing-policy grant is captured or injected |
| Materialization | One internal row ID per output occurrence; typed storage adaptation | WITH NO DATA is explicitly unreadable |
| Atomicity | Catalog, physical backing rows and populated flag commit/rollback together | Partial population is never published |

Stable source bindings retain only the columns actually consumed by the bound
query, including filter/join/order/group/window expressions and wildcard
expansion. A source's original column order is retained; adding an underlying
column cannot change a stored wildcard. A definition-specific resolution
reconstructs logical original-name columns over current physical fields, so
logical renames require no public QueryIR syntax or query-tree rewrite.

## Runtime Flows

```text
CREATE VIEW -> metadata bind -> stable dependency capture -> candidate admission
CREATE MV -> same admission -> candidate backing schema -> SELECT -> typed rows
REFRESH MV -> candidate definition -> SELECT -> replace backing rows -> populated
DROP -> dependency closure -> backing cleanup when materialized -> candidate
```

## State, Ownership, and Lifecycle

Definitions and binding arrays are immutable catalog values. Statement bindings
and canonical plans retain the admitted generation and meter reservations.
Materialized backing entities belong to the durable physical schema and use the
existing runtime lifecycle. No mutable registration or independent transaction
is introduced.

## Failure, Concurrency, and Constraints

Definition expansion, dependency traversal, binding and population consume the
request work budget and honor cancellation. Cycles and unavailable sources are
typed failures. Source snapshots remain guarded by the original statement scope and retain their
definition-specific immutable resolution. Snapshot reuse includes the definition
binding identity, so identical original names in independent view scopes cannot
retarget captured rows.
Population failure rolls back the caller-owned storage transaction and candidate
lease; it does not replace the last complete materialization. CREATE/REFRESH
WITH DATA admits the collected population count against `maximumRows` before
candidate staging or backing-row removal. This is the materialization's affected
row bound, separate from public result-row accounting; internal collection does
not consume public output rows. Work, retained memory, storage reads and deadline
remain charged to the same request meter throughout collection and persistence.

## Verification and Change Impact

The S15 view tests own ordinary rows/metadata, wildcard stability, logical
renames, replacement rejection, invoker security, cycle/dependency rejection,
read-only mutation gates, and MV duplicates/NULL/typed values. SQLite integration
owns rollback/savepoint/reopen and failed-refresh preservation. Catalog codec
roundtrips cover complete durable definitions. Changes to source bindings require
QueryExecution and SQLCatalog admission review; changes to population require
schema-driven runtime and transaction verification.

Physical source admission reads the active physical-schema fingerprint through
its own storage transaction with `snapshot: false`. A retained logical catalog
may read a physical snapshot only when these fingerprints agree. Logical rename
and constraint-only generations do not invalidate compatible reads. A snapshot
pinned before physical DDL remains readable; an old resolution attached to a new
post-DDL storage snapshot fails with a typed generation conflict before Directory
or row access. The schema owner stages candidate metadata before candidate row
validation in the same transaction, so no candidate bypass is required.

The physical fingerprint proof applies to SQL physical DDL's same-domain storage
contract. Under MultiBase, a read in another storage domain retains the existing
read contract: SQL physical DDL rejects retained separate-domain roots before
writing, so this guard does not introduce a cross-domain metadata transaction.

| State owner | Native / WASM / Embedded storage | Read | Mutation | Close / release |
|---|---|---|---|---|
| SQLReadResolution bindings | Immutable `let` arrays and generation owner | Synchronous stable-ID lookup | Construction only | Retained immutable owner |
| SQLStatementSourceSnapshot frame | Same `Mutex<CanonicalRelation?>` | `withLock` after scope validation | `install` through `withLock` | Scope releases snapshot |
| SQLStatementReadScope snapshots | Same `Mutex<State>` | `withLock` | `physicalSource` under `withLock` | `close` removes owners under the same mutex, then awaits outside lock |

No target-specific state, isolation, Sendable conformance or cleanup branch is
introduced. The task's exact Native, ordinary WASM and Embedded runtime lanes
own verification of these unchanged shared-state primitives.

### S16 virtual metadata source composition

`SQLViewSourceBinding.kind` separates catalog identities from
[SQLInformationSchema](../SQLInformationSchema/DESIGN.md) descriptor identities.
The catalog kind retains existing stable relation/view IDs. The informationSchema
kind stores the exact descriptor name and consumed canonical column IDs, names
and types. Candidate validation checks descriptor and column existence/type;
physical dependency closure considers only catalog bindings. Runtime expansion
uses captured kind/identity before caller search paths and preserves the original
source column order through explicit bound source ordinals. Virtual row production
applies current invoker metadata policy; no definer privilege is retained.

Format 4 catalog persistence encodes the source discriminator in each binding,
participating in canonical digest and bounded declaration admission. Unknown
discriminators and old frame versions fail explicitly. S16SQLInformationSchemaTests
owns creation, execution under changed search paths, checkpoint rollback,
reopen, typed binding rejection and metadata-policy propagation.
