# SQLDDL

## Purpose and Scope

This direct child of [DatabaseEngine](../DESIGN.md) owns execution of the S15
logical schema, table and column DDL commands. It consumes Kit's DDL values and
composes the existing transaction, schema generation, constraint, sequence,
index and view owners. SQL parsing and operational session commands are outside
this component.

## Responsibilities and Boundaries

SQLDDL resolves logical object names against the admitted request catalog,
constructs candidate declarations, computes dependency removals and old-row
transformations, and submits one complete candidate to Schema. It does not
publish independently, open a transaction, duplicate row/index persistence,
change Directory placement, or infer authority from an SQL namespace.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [Engine](../DESIGN.md) | parent | admitted execution | Owns composition. | No global registry. |
| [SQLBlock](../SQLBlock/DESIGN.md) | used by | request transaction and statement scope | Routes DDL and closes temporary relations. | Results remain private through authoritative close. |
| [Schema](../Schema/DESIGN.md) | depends on | prepared physical candidate and staged generation | Validates coupled physical/logical/runtime candidates. | All rewrites and publication share the caller transaction. |
| [SQLCatalog](../SQLCatalog/DESIGN.md) | depends on | resolved stable identities | Logical names are separate from physical fields. | Protection witnesses survive reopen. |
| [SQLSequences](../SQLSequences/DESIGN.md) | coordinates with | stable identity allocation | Identity and SERIAL columns retain existing semantics. | Literal default targets survive rename. |
| [QueryExecution](../QueryExecution/DESIGN.md) | depends on | bound output metadata and selected expressions | CTAS and USING use actual bound types. | Zero rows never erase output types. |
| [Transaction](../Transaction/DESIGN.md) | depends on | checkpoint journal and schema stage | Rollback restores catalog, rows and indexes. | No independent publication. |

## Architecture

```text
admitted SQLBlock statement
  -> SQLDDLExecutor: resolve names + candidate declarations
      -> QueryExecution: bound CTAS / old-row USING / default
      -> SQLSequences: identity declaration and stable targets
      -> SQLIndexes / SQLViews: owned lifecycle and dependencies
      -> Schema.prepareSQLSchemaCandidate
          -> Transaction.stageSQLSchemaCandidate + old-row rewrites
              -> commit + authoritative close -> publish results
```

## Contracts and Invariants

| Subject | Guarantee | Discriminating evidence |
|---|---|---|
| Authority | DDL requires administer on the admitted transaction; namespace names confer no placement or authority. | Unauthorized request changes no catalog/data. |
| Identity | Rename preserves namespace/relation/column/sequence IDs, physical entity/field identity and Directory. | Query old/new names, reopen, and compare IDs/placement. |
| Field allocation | New physical field numbers exceed the durable relation high-water mark and must be exactly representable as a positive UInt32. Native Int overflow and UInt32 exhaustion produce a typed failure on both 32-bit and 64-bit targets. | Drop highest column, reopen, add and prove no reuse. |
| Static models | Logical renames retain the exact compiled backing contract; physical alteration/removal rejects. | Logical rename success and physical mutation failure. |
| Atomicity | Candidate schema, catalog, transformed rows, indexes and constraints are one transaction/checkpoint effect. | Late expression/constraint failure and savepoint rollback restore old data and names. |
| Name collisions | Relation namespace includes tables, views, indexes and sequences; IF NOT EXISTS skips the existing object without definition comparison or query evaluation; CTAS still performs metadata binding before the no-op. | Mismatched duplicate declaration and failing skipped CTAS expression. |
| Backfill | Sequence-backed values pass through the declared column assignment conversion and storage adapter before row construction, including narrow integer identity columns. ADD COLUMN evaluates nonvolatile defaults once per statement, volatile defaults per existing row; all TYPE USING clauses read the same complete pre-statement row and does not rewrite DEFAULT semantics. | Sequence/clock effects and distinct old-row/default conversion fixtures. |
| Dependencies | Dropping an owned sequence with its column rejects surviving default dependencies under RESTRICT and removes only their input defaults and stable bindings under CASCADE, preserving consumer columns and rows. RESTRICT rejects external dependencies; CASCADE removes dependent objects or referencing FK constraints without deleting referencing tables. | Cross-schema view chain and retained child rows. |
| Temporary tables | Private block lifetime and unqualified shadowing; cleanup occurs before commit, including successful blocks. | Same-block visibility, qualified permanent access and next-request absence. |
| CTAS | An explicit name list may replace an output prefix. Bound ordered names/types exist independently of row count and copied data has no live source dependency. | Empty/NO DATA metadata and independent subsequent source changes. |

## Runtime Flows

A request-scoped immutable identity timeline is reused across sequence-demand
replay. Each data attempt owns its temporary namespace/table inventory; it is
restored by transaction rollback and removed before a successful block returns.
DDL executes inside the same temporal statement and parameter binding scope as
DML. All ALTER actions form one candidate before row effects. Each transformation
retains the original statement row separately from the candidate row, and stored
generated values are recomputed once after the transformations. A transformed
row is decoded with the source runtime and canonicalized with the target runtime, with old index removal preceding new index insertion.
Schema owns that storage operation and its candidate integrity validation.
Sequence-backed backfill resolves through the existing candidate execution
binding; it introduces no separate declaration context. Allocation replay keeps
its original request occurrence and applies the same selected assignment and
encoding on each replay. Sequence consumption retains S13 nonrollback semantics:
a savepoint can restore the column and rows while consumed values remain spent.
Readding the same sequence identity continues from that allocation state.

## State, Ownership, and Lifecycle

| State | Owner | Lifetime |
|---|---|---|
| Stable generated object identities | Immutable SQLDDLRequest | Admitted request, all attempts |
| Physical candidate | Schema | Preparation through transaction publication |
| Old row and replacement row | Schema rewrite invocation | Synchronous ownership / awaited transform |
| Temporary objects | SQLDDLRequest plus transaction journal | One data attempt and block |
| Generation/checkpoint | Transaction | Existing authoritative lifecycle |

No target-specific raw mutable state or weakened Sendable contract is introduced.
The existing transaction and sequence actors retain their ordering contracts;
this component introduces no shared mutable state. Immutable candidate values
pass between owners.

## Failure, Concurrency, and Constraints

Malformed declarations, missing/duplicate/dependent names, static protection,
invalid conversion/default/constraint values, cancellation, resource exhaustion
and publication conflicts remain typed failures. Every scan, retained candidate,
row transformation and dependency traversal consumes the same request work
meter/deadline. A private sequence-allocation demand may restart only after the
outer data transaction has rolled back and closed. Other failures propagate.
No result becomes Ready before temporary cleanup, commit and authoritative close.

Temporary cleanup removes SQL catalog visibility, rows and owned indexes. Empty
Directory nodes and partition-catalog entries retain the existing lifecycle in
[SPEC section 12.3](../../../../SPEC.md#123-directory-catalog-as-partition-catalog).
SQL DROP does not recursively remove a Directory under active leases; a retained
empty node supplies neither SQL lookup visibility nor a mutation target.

## Verification and Change Impact

SQLDDLExecutionTests owns schema/table/column success, failure and rollback
contracts. SQLBlock tests own public text routing, post-close publication,
savepoints and block-only temporary lifetime. SQLSequence tests own stable
literal targets through sequence/schema rename and reopen. Physical staging,
SQL index and view owners supply their focused behavioral evidence; integrated
SQLite/PostgreSQL and exact formal target lanes are task-wide ownership.

The implementation seams are internal: `SQLDDLExecutor.execute` returns only
`SQLCommandKind`; Schema's candidate/stage API owns storage rewrites. No new SPI,
wire operation, public server handler, transaction domain or transport is added.
