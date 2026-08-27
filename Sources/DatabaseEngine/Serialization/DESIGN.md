# Serialization

## Purpose and Scope

The Serialization component owns the stored-item envelope contract and the
bounded retained read that returns one canonical payload with its request
reservation attached.

- Parent: [DatabaseEngine](../DESIGN.md).
- Child [RDF](RDF/DESIGN.md) is outside DF-06F0 and has no design authority in
  this change.

## Responsibilities and Boundaries

Serialization validates item envelopes, reads inline or external content,
assembles chunks, checks sizes and checksums, charges byte-processing work, and
returns retained payload bytes. It consumes a storage transaction capability
and a work meter.

It does not authorize entity/query access, resolve polymorphic runtimes, decode
models, or own query destinations. The unretained public item API is a separate
contract and is not evidence for the retained path.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | Retained storage guarantee | Places bounded bytes below Core decoding. | Compile-only evidence does not prove backend enforcement. |
| [Core](../Core/DESIGN.md) | used by | `ItemStorage.readRetained` | Core consumes the canonical payload. | The returned owner must remain alive through decode. |

## Architecture

```text
bounded envelope point read
    -> validate envelope and declared limits
        +-> inline payload: admit exact retained bytes, then copy/retain
        +-> external payload: admit final buffer
              -> bounded point read for every declared chunk
              -> validate exact chunk sizes and assemble once
    -> checksum and work checkpoint
    -> retained canonical payload
```

## Contracts and Invariants

- `readRetained` uses the work-meter-aware bounded point-read contract for the
  envelope and every external chunk. It never calls raw `getValue` on this
  retained path.
- Declared plain/stored/chunk limits are validated before allocation.
- Final external assembly bytes are reserved before allocation; each backend
  read remains bounded before its bytes are exposed.
- Every chunk has the exact expected size, total assembled size equals the
  envelope declaration, and checksum matches the canonical payload.
- The returned `ByteString` owns the exact reservation for its lifetime.
- Missing envelope returns `nil`. Missing chunk, malformed layout, oversize,
  checksum failure, budget failure, and cancellation are typed failures.

## Runtime Flows

The retained scan keeps cursor envelope ownership alive while this component
decodes the payload and invokes the consumer. Point reads return a retained
payload owner to Core; temporary envelope and chunk owners release after
assembly/decode.

## State, Ownership, and Lifecycle

`ItemStorage` is an operation value over one transaction and immutable
configuration. Inline output retains or copies into an exact admitted owner.
External output has one admitted mutable assembly owner that becomes immutable
exactly once. No pointer escapes a synchronous byte borrow.

## Failure, Concurrency, and Constraints

Chunk reads are serial on the owning transaction. Cancellation or any chunk
failure destroys the assembly owner and releases its reservation before the
error escapes. Cursor cleanup errors preserve both iteration and cleanup
failures rather than replacing one with success.

## Verification and Change Impact

- [ItemStorageResourceTests](../../../Tests/DatabaseEngineTests/ItemStorageResourceTests.swift)
  proves bounded envelope/chunk routing, successful retained bytes, work
  accounting, and release.
- Existing bounded point-read tests own backend rejection and non-poisoning
  semantics; this component test proves that the retained item path reaches
  that contract.
- Changes to envelope format, chunk layout, maximum-byte calculation, retained
  byte ownership, or backend point-read semantics require rechecking Core
  decode and cancellation evidence.
