# SQL Constraints

## Purpose and Scope
SQLConstraints owns executable SQL constraint and column-policy semantics. Parent: [DatabaseEngine](../DESIGN.md). Children: none.

## Responsibilities and Boundaries
Kit owns immutable declarations. This component binds them to the physical schema, evaluates canonical row expressions and validates keys and references. Transaction owns attempt state, storage reads, mutation actions and timing. Mutation owns policy application before model adaptation. Sequence allocation remains independently committed by SQLBlock.

## Related Designs
| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | execution composition | Composes constraint execution. | No new storage authority. |
| [Transaction](../Transaction/DESIGN.md) | used by | attempt schema lease and retained scans | Validates one admitted transaction overlay. | Never use the globally active schema during final validation. |
| [Mutation](../Mutation/DESIGN.md) | used by | canonical assignment and model adaptation | Applies column policies. | SQL defaults do not replace model defaults. |
| [SQLCatalog](../SQLCatalog/DESIGN.md) | used by | static admission | Calls SQLConstraintCompiler.validate(catalog:physicalSchema:). | The compiler never constructs a snapshot. |
| [QueryExecution](../QueryExecution/DESIGN.md) | depends on | canonical binding/evaluation/comparison | Owns value meaning. | No duplicate scalar evaluator. |

## Architecture
```text
Kit catalog + physical Schema -> SQLConstraintCompiler -> admitted catalog
attempt lease + retained transaction rows -> canonical row values -> constraint check
mutation before/after -> referential action -> same transaction mutation pipeline
SQL input -> defaults / identity -> model adaptation -> generated -> invariant checks
```

## Contracts and Invariants
Primary and unique keys use declared ordered SQL columns independently of entity identity. Primary keys reject NULL. UNIQUE uses declared NULLS DISTINCT/NOT DISTINCT. CHECK rejects only FALSE; NOT NULL rejects NULL. FK MATCH SIMPLE skips any NULL; FULL skips all NULL and rejects partial NULL. Reference targets are validated by Kit and physical type compatibility by this compiler.

Static admission binds all default, generated and CHECK expressions before data access. Defaults cannot reference row columns; generated expressions use the current row, are immutable and cannot reference generated columns. No subquery, parameter, aggregate or window is admitted in stored policies. Virtual generated values are evaluated for query/constraint use and never acquire synthetic physical fields.

Generated admission rejects the unary AGE overload because it reads the transaction date. The binary timestamp-without-time-zone overload remains eligible because both calendar operands are explicit; rejecting the function name alone would discard immutable behavior.

## Runtime Flows
NOT NULL, CHECK, identity nullability and stored-generated consistency validate each newly canonicalized row once before its physical write. Successful CHECK expressions, including admitted volatile or session expressions, are not evaluated again for unchanged rows or at statement/commit boundaries. Nondeferrable primary/unique constraints validate before the next requested row is applied. Deferrable primary/unique constraints in immediate mode and immediate foreign keys validate after the complete outer mutation operation. Deferred primary/unique/FK checks run at commit or retroactively before SET CONSTRAINTS IMMEDIATE changes mode. Primary-key null rejection remains a row obligation even when uniqueness is deferred. RESTRICT executes at the referencing action boundary. CASCADE/SET NULL/SET DEFAULT use actual source rows and the same mutation pipeline; DELETE action subsets affect only declared columns. Physical identity relocation is one logical UPDATE: physical removal/insertion suppress SQL action dispatch and one old-to-new UPDATE drives downstream actions. All row policies and referential defaults consume the admitted session and captured statement temporal environment.

## State, Ownership, and Lifecycle
Catalog admission validates existing rows only for newly introduced or changed constraints/column invariants. Unchanged deferred obligations retain their original commit/SET CONSTRAINTS boundary during unrelated sequence administration. Transaction actor owns timing overrides and pending validation. Checkpoints restore both alongside physical writes and schema staging. Canonical payloads and scanned models retain their request-meter leases. Native, WASM and Embedded share these owners and isolation.

## Failure, Concurrency, and Constraints
All violations are typed SQLConstraintError values. Each throw site records the constraint kind and exact schema/table/column/constraint names from its admitted catalog before rollback. Constraint classification never infers meaning from identifier spelling or message text; primary-key NULL and identity NULL are not-null violations, while duplicate keys are unique violations. Work and retained-memory admission use DatabaseWorkMeter. Integrity scans bypass user row filters only inside the private transaction capability and return no rows to callers. Dynamic entity directories are enumerated using the existing Directory catalog below the admitted tenant/Base data root, with the attempt schema layer map; no sibling authority is entered. Scans are serializable non-snapshot transaction reads so concurrent conflicting inserts cannot both commit. Failures never become empty success; operation and transaction owners retain rollback authority.

## Verification and Change Impact

Canonical row layouts construct optional column-type elements directly and reserve their actual element stride; no runtime array cast is required. Constraint-attribute traversal uses value closures on every target, preserving order and timing without Embedded key-path metadata. Native focused and both portable runtimes must exercise the same row/default/deferred paths after portability changes.

DatabaseEngineTests SQL constraint tests own arbitrary/composite/no-PK, NULL uniqueness, CHECK three-valued behavior, FK matching/actions, deferred and checkpoint behavior, defaults/generated/identity and admission rejection. Root task owns native/portable/backend integration and execution evidence. Source-only inspection is not completion evidence.

`S13SQLReviewScalarAdmissionTests` rejects unary AGE for both timestamp signatures and evaluates admitted binary timestamp AGE through the generated-row path under two transaction dates.
