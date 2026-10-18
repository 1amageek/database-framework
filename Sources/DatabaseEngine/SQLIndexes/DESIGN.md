# SQL Indexes

## Purpose and Scope

This direct child of [DatabaseEngine](../DESIGN.md) owns D09 SQL index
compilation, transactional physical construction, maintenance and retirement.
It has no child components. It consumes stable catalog declarations from Kit.

## Responsibilities and Boundaries

SQLIndexes owns immutable current-row expression binding, semantic key equality,
partial membership, unique enforcement and its persisted leaf representation.
Leaves retain canonical persisted source-model inputs so comparison can evaluate
all admitted SQL key types without imposing field-storage restrictions on keys.
Schema owns generation publication, partition enumeration, stale writer exclusion
and transaction checkpoint restoration. QueryExecution owns authorization and the
complete residual query. SQLDDL owns statement dispatch and dependency removal.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [Engine](../DESIGN.md) | parent | explicit request composition | Owns execution. | No global registration. |
| [SQLCatalog](../SQLCatalog/DESIGN.md) | depends on | stable index/relation/column identities | Persists declarations and original expression bindings. | Renames preserve binding IDs. |
| [Schema](../Schema/DESIGN.md) | coordinates with | candidate lease and physical staging | Enumerates every existing physical store and builds before publication. | Changed indexes raise the stale writer floor. |
| [Transaction](../Transaction/DESIGN.md) | used by | caller storage transaction | Builds, writes and exact retirement share commit/checkpoint. | No background build completion. |
| [QueryExecution](../QueryExecution/DESIGN.md) | coordinates with | canonical SQL value identity and residual query | Rich key identity uses selected SQL types. | Raw bytes never decide SQL equality. |

## Architecture

```text
SQLIndex declaration -> SQLIndexCompiler -> immutable bound key/predicate
                                      -> SQLIndexRuntime
                                         -> exact generation leaf storage
Schema candidate -> per-store rebuild ------^
DataStore write/delete -> maintain ----------^
QueryExecution -> eligible leaf scan -> authorized base rows -> full residual
```

## Contracts and Invariants

| Subject | Guarantee | Evidence owner |
|---|---|---|
| Binding | Original expression names resolve through frozen column IDs. | Compiler rename tests. |
| Expressions | Keys and predicates are immutable current-row scalar expressions with comparable selected types. | Compiler rejection tests. |
| Temporal expressions | Direct temporal keys and scalar-only immutable operations are supported; character/temporal casts, local/zoned conversions and session/provider-dependent temporal operations reject before writes. | Zoned cast rejection and persisted direct temporal key tests. |
| Identity | SQL numeric, CHAR, temporal, arrays, composites and JSONB use CanonicalSQLValueIdentity. JSON rejects. | Rich identity differential tests. |
| NULL | DISTINCT permits a key containing NULL; NOT DISTINCT compares NULLs as equal. | Unique build/write tests. |
| Atomicity | Build validates existing rows; subsequent maintenance and exact retirement occur in the caller transaction. | Rollback, late failure and reopen tests. |
| Physical identity | Definition fingerprint includes stable index declaration; layout fingerprint fixes leaf revision. | Catalog change and exact retirement tests. |
| Read eligibility | Full indexes are eligible; partial indexes require each partial conjunct to occur exactly in the query predicate after stable binding. | Unsafe partial regression. |
| Narrowed sources | An index is eligible only when every frozen key/predicate column binding remains in the resolved source relation. View/MV source narrowing may omit unrelated indexed columns; that omission does not invalidate the read. Full-catalog admission still validates every index against its complete relation. | Empty/populated narrow view and materialized refresh with unrelated key/predicate indexes. |
| Query binding | Index selection never binds or lowers the caller's filter. A full index requires no predicate proof. Partial selection uses exact stable-name conjunct containment and declines filters with outer source references; parameters and correlation retain their canonical binding/evaluation scope. | Existing parameter/correlated/lateral/SPARQL source tests and parameterized full/partial index assertions. |
| Retained readers | The generation floor is read from the same snapshot before a leaf scan; an old catalog on a newer post-retirement snapshot fails explicitly. | Retained-resolution/drop/new-transaction counterexample. |
| Foreign-key dependencies | Eligible direct-column, nonpartial unique indexes have stable FK dependency IDs; DROP RESTRICT rejects them and CASCADE removes only referencing FK constraints. | Actual unique-index/FK write validation and retained-child post-state test. |

## Runtime Flows

CREATE stages a complete catalog candidate, enumerates existing physical stores,
builds new leaf generations from canonical stored rows, enforces uniqueness, then
permits generation publication. DROP clears only the captured definition/layout
generation. Updates remove the old leaf, validate the new selected key against
remaining leaves, and insert the new leaf. Any failure aborts the caller operation.

Leaf layout revision 2 is an unordered primary-identifier directory containing
the canonical source-model DBRC payload. Unique scans decode those inputs and
reevaluate the admitted immutable key through the current declaration. Physical
scans fetch base rows from the same passed admitted read transaction, which does
not require an ambient TaskLocal transaction binding.
No range, ordering, equality-seek or speedup capability is claimed. SQL equality
is established through CanonicalSQLValueIdentity, never by serialized bytes.
Persisting the source payload duplicates its stored bytes at this explicit
persistence boundary. ItemStorage chunks large leaves within the same generation;
encoding, decoded input ownership, reads and repeated evaluation charge the same
request meter. No old-layout fallback is provided.

## State, Ownership, and Lifecycle

The component has no shared mutable state. Compiled expressions and selected keys
retain admitted metadata/value leases. Physical leaf state belongs to the caller
transaction and exact schema generation. No additional target-specific storage,
Sendable exemption or synchronization primitive is introduced.

## Failure, Concurrency, and Constraints

Invalid binding, volatile/subquery expressions, incomparable types, malformed leaf
payloads, uniqueness, cancellation and work/memory limits remain explicit errors.
Builds and unique scans use the request work meter and non-snapshot reads so
concurrent changes participate in the backend conflict/isolation contract.

The ordinary reader and SQLExplain both consume `selectedSQLIndex`; it performs
the same catalog binding and partial-predicate selection without opening a
directory or pulling an index row. EXPLAIN never substitutes a syntax-derived
index choice.

## Verification and Change Impact

S15SQLIndexTests owns compiler, semantic key, persistence and exact-retirement
behavior. S15 SQL DDL integration tests own real same-transaction build, write,
read equivalence, rollback and reopened catalog behavior. Root owns serial
focused/affected-target/package verification. Changes to this leaf revision
require rebuilding and revisiting Schema physical fingerprints and read leases.
