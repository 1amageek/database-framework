# Encoded Query Results

## Purpose and Scope

This component owns admitted response bytes and exactly-once final output
promotion. Parent: [DatabaseEngine](../DESIGN.md). Children: none.

## Responsibilities and Boundaries

EncodedQueryPayload retains one exact byte allocation and its original request
reservation. EncodedQueryResponse protects consuming promotion with one Mutex
on every target. Only SQLBlock's private Ready transition and Read's completed
snapshot runner construct it in production. Encoding callbacks never establish
closure or transfer transaction authority.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | Concrete execution ownership | Composes this result owner | No public readiness factory |
| [SQLBlock](../SQLBlock/DESIGN.md) | used by | Post-transaction Ready transition | Produces complete SQL responses | Rollback discards provisional bytes |
| [Read](../Read/DESIGN.md) | used by | Authoritative snapshot closure | Produces SPARQL read responses | Retain all payload claims through encoding |
| [Database Query Execution](../../Database/QueryExecution/DESIGN.md) | used by | Bounded synchronous output projection | Projects canonical values | Callbacks and pointers do not escape |

## Architecture

```text
provisional payload -> exact measurement -> reserve -> synchronous encode
    -> producing runner closes all resources -> EncodedQueryResponse
    -> one promoteToOutput -> caller-owned bytes
```

## Contracts and Invariants

Measurement precedes allocation. Actual byte length equals the measurement.
The original meter accounts for processing and retained bytes through closure;
failures discard the allocation and claim. Generic payloads appear only in
function parameters; no generic authority, root or lifecycle owner is created.

## State, Ownership, and Lifecycle

Native, standard WASM and Embedded share Mutex<EncodedQueryPayload?>, the same
promote entry and the same nil terminal state. Only the producing runners call
the module-internal constructor after authoritative closure. Dropping an
unpromoted owner releases its immutable bytes and reservation.

## Failure, Concurrency, and Constraints

Concurrent promotion has exactly one winner; subsequent calls fail explicitly.
No await, I/O or external callback runs under the promotion mutex. ByteString's
owned allocation is initialized before its bounded synchronous MutableSpan
borrow; neither the span nor its pointer escapes. Overflow and limits fail before
allocation. The existing row/output claims remain live during projection.

## Verification and Change Impact

SQL block streaming and common connection tests exercise successful closure,
pre-commit encoding failure, rollback, cancellation and single promotion. Shared
source changes require Native and exact standard/Embedded runtime validation.
