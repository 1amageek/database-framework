# Autocomplete

## Purpose and Scope

This component owns autocomplete index read traversal and public suggestion
ordering. Its parent is [`FullTextIndex`](../DESIGN.md). It does not own index
writes, tokenization, authorization, transaction creation, or general
full-text search.

## Responsibilities and Boundaries

The component admits cursor entries into a session-provided work meter,
retains decoded suggestions, applies score and term ordering, enforces the
requested limit, and promotes only the final bounded output. The caller owns
the authorized session and transaction. Backend storage and index layout are
owned by `StorageKit` and the parent feature module.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [`FullTextIndex`](../DESIGN.md) | parent | feature read and cleanup contract | Supplies the component boundary and feature verification. | Keep autocomplete independent of polymorphic F0 fetch APIs. |
| [`DatabaseEngine Read`](../../DatabaseEngine/Read/DESIGN.md) | depends on | session transaction, cursor, and work meter | Supplies lifecycle and resource identity. | Do not collect unmetered cursor results. |

## Architecture

```text
authorized DatabaseReadSession
        -> bounded range cursor
        -> retained suggestion buffer + meter
        -> score/term ordering
        -> bounded subrange
        -> public array
```

## Contracts and Invariants

- Prefix and limit validation occurs before storage access.
- Each cursor entry is admitted before its retained bytes are kept.
- Suggestions sort by descending score, then ascending term, and respect the
  normalized prefix.
- The public array is created only after limit selection and is not the
  accounting owner for intermediate cursor state.
- Cursor finish and reservation release happen on success, decode failure,
  cancellation, and storage failure.

## Runtime Flows

`AutocompleteQuery` supplies one session transaction and meter to the reader.
The reader scans the selected term subspace, decodes an admitted entry, keeps
it in the retained buffer, sorts it, retains the requested prefix, promotes
that prefix, and returns after cursor cleanup.

## State, Ownership, and Lifecycle

The retained suggestion buffer owns intermediate reservations until promotion
or failure. The cursor is session-owned and is explicitly finished by the
reader. No borrowed key/value buffer outlives the cursor callback.

## Failure, Concurrency, and Constraints

The reader propagates malformed entries, storage errors, cancellation, and
meter exhaustion. It does not silently skip corrupt values. It performs no
suspending operation while holding a memory lock.

## Verification and Change Impact

Autocomplete tests verify prefix filtering, score-descending and term tie
ordering, limit enforcement, malformed-entry failure, cancellation cleanup,
and zero final meter usage. Changes to the session cursor or retained-buffer
contract require rechecking the parent FullText design.
