# SQLCopy

## Purpose and Scope

SQLCopy owns bounded request-contained TEXT/CSV COPY decoding, encoding and its
composition with ordinary statements. Parent: [DatabaseEngine](../DESIGN.md).
It has no child components.

## Responsibilities and Boundaries

SQLCopy owns byte-level field framing, quoting, NULL markers, row cardinality,
transfer admission and typed COPY output. QueryExecution owns selected scalar
text and contextual literal conversion; Mutation owns defaults, constraints,
authorization and transactional effects. SQLBlock owns transaction lifetime,
rollback, retry, settings and post-close publication. No file or PROGRAM access
exists.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [SQL profile](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s16-block-operations-and-diagnostics) | depends on | Immutable COPY declaration | Supplies options and request bytes | Empty CSV and SQL NULL remain distinct |
| [SQLBlock](../SQLBlock/DESIGN.md) | used by | Admitted statement callback | Executes ordinary reads/inserts in the exact transaction | No public executeSQL recursion |
| [QueryExecution](../QueryExecution/DESIGN.md) | depends on | Retained rows and selected scalar text | Reuses actual scalar semantics | Field policies remain active |
| [Mutation](../Mutation/DESIGN.md) | depends on | Ordinary INSERT | Reuses contextual conversion, defaults and constraints | Any late failure aborts the enclosing request |

## Architecture

```text
retained input bytes -> one bounded row -> unknown SQL literals -> ordinary INSERT
ordinary selected retained rows -> scalar text borrow -> escaped bytes -> COPY result
                           same request meter and transaction
```

## Contracts and Invariants

- FROM preserves ordered explicit columns; omitted columns use the existing
  INSERT contract. No row collection or whole-input String is constructed.
- A decoder retains one ByteString owner and bounded offsets. Only one decoded
  row is materialized at the mutation boundary. Input, row, decoded temporary,
  string and output capacity claims precede growth and survive their owners.
- TEXT NULL detection compares the raw field before backslash unescaping. CSV
  NULL detection applies only to unquoted fields. Quoted empty CSV is non-NULL.
- CSV supports quoted newlines, quote/escape options, HEADER and LF, CR or
  CRLF record endings. One input uses one consistent record-ending style;
  mixed endings fail as PostgreSQL COPY requires. Malformed quoting, UTF-8,
  row cardinality or escape input throws a typed failure.
- TO returns bytes and direct row count, never a fabricated SQL row. It borrows
  each typed cell from retained rows and delegates scalar output formatting.

## Runtime Flows

```text
FROM: admit retained input -> decode/header -> admit row -> INSERT -> release row
      -> repeat -> collect count
TO: ordinary SELECT -> retain typed rows -> header -> scalar text/escape each cell
    -> collect bytes/count -> enclosing transaction close -> public result
failure/cancellation -> throw -> SQLBlock rollback/close -> no public output
```

## State, Ownership, and Lifecycle

The decoder index and writer buffer are invocation-local values. Input slices
retain their immutable ByteString owner; no pointer crosses a borrow or await.
The one decoded row retains its reservation through the asynchronous INSERT.
The output reservation travels with the private SQLBlock outcome through close.
No shared mutable state or platform-specific synchronization branch is added.

## Failure, Concurrency, and Constraints

Every scanned byte and appended field/byte consumes the existing request work
meter, including cancellation and deadline checks. Intermediate row and byte
claims precede allocations. COPY output rows consume the existing output-row
budget. Errors after earlier INSERTs propagate unchanged so SQLBlock rolls back
all earlier effects in the same transaction. The component does not catch and
convert failures or start independent transactions.

## Verification and Change Impact

SQLCopy codec tests cover TEXT/CSV NULL versus empty, escapes, quoted multiline,
custom options, headers, CRLF, malformed and late malformed input, exact/one-past
retained/work/output limits and cancellation. SQLBlock public integration tests
own real round-trip, defaults/casts/constraints, denied fields and full rollback
on a late malformed row. Codec-only evidence cannot prove transactional behavior.
Changes to output recheck SQLBlock readiness; changes to coercion recheck Mutation
and QueryExecution; declaration changes recheck Kit and QueryAST.
