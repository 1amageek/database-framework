# RDF Serialization

## Purpose and Scope

The RDF Serialization component owns the canonical byte representation and
validation contract for RDF terms persisted or used in graph index keys. It
provides the package-level contract consumed by DatabaseEngine storage codecs
and GraphIndex physical codecs.

- Parent: [Serialization](../DESIGN.md).
- Children: none. The files in this directory are one component contract.

## Responsibilities and Boundaries

This component owns:

- role-aware canonical encoding and decoding of RDF terms, including nested
  RDF-star triples;
- exact encoding measurement plans, final sinks, byte metrics, and bounded
  storage limits;
- constant-space validation of canonical bytes, UTF-8/NUL/canonical-varint
  rules, term-role rules, depth/object-count limits, string metrics, and typed
  errors;
- scoped borrowed validation views and deterministic fingerprints used as
  cache discriminators.

It does not own RDF graph meaning, quad-index ordering, named-graph catalogs,
SPARQL algebra, SQL, SHACL shape semantics, transactions, backend behavior,
or model/schema policy. GraphIndex owns graph physical layout and semantics;
DatabaseEngine Serialization owns envelopes and frame/tuple codecs that
consume this component.

The implementation is centered on [RDFTermStorageFormat](RDFTermStorageFormat.swift),
[RDFTermStorageValidator](RDFTermStorageValidator.swift),
[RDFTermStorageLimits](RDFTermStorageLimits.swift), and
[RDFTermStorageValidation](RDFTermStorageValidation.swift).

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [Serialization](../DESIGN.md) | parent | Stored-item and bounded retained-read composition | Places canonical RDF bytes below DatabaseEngine frame and tuple codecs. | The parent must not duplicate RDF term-format rules. |
| [GraphIndex](../../../GraphIndex/DESIGN.md) | used by | Canonical term bytes, role validation, limits, and scoped views | Uses the format for RDF identities, quad keys, ranges, and graph catalogs. | GraphIndex owns physical key layout, not byte encoding. |

## Architecture

```text
RDF term / role
    -> exact measurement plan
    -> final byte owner or sink
    -> canonical bytes
       |-> withValidatedBytes -> scoped validation + metrics/fingerprint
       `-> decode -> validated RDF term/metrics

StorageFrameEncoder/Decoder and FieldValueTupleCodec
    -> RDF canonical term bytes

GraphIndex quad/catalog codecs
    -> RDF canonical term bytes
```

`RDFTermStorageFormat` is a package-level SPI, not an application-facing
public API. Its callers receive validated values or typed failures; they do
not infer validity from a fingerprint or from a successful allocation.

## Contracts and Invariants

- Encoding is canonical and role-aware. Subject, predicate, object, graph
  name, and nested triple positions accept only the terms permitted by the
  RDF storage contract.
- The encoder measures the exact final size before allocation/sink emission;
  byte count, depth, and object-count limits are enforced before unbounded
  work or storage is performed, while string metrics are collected during
  validation.
- Validation parses bytes with a bounded cursor and rejects truncation,
  trailing bytes, unknown tags, invalid UTF-8/IRI/blank/triple roles,
  noncanonical varints, overflow, and limit violations as typed errors.
- `withValidatedBytes` exposes borrowed views only within its synchronous
  scope. The source owner remains live for the borrow, and no pointer or view
  escapes the closure.
- Decode and encode round trips preserve canonical bytes and semantic role
  invariants. Required decode materialization occurs only at the explicit
  output boundary; validation does not materialize strings or terms merely to
  prove well-formedness.
- Fingerprints are deterministic cache discriminators, not equality proofs;
  callers compare canonical bytes when equality is required.
- There is no empty, synthetic, or lossy fallback for malformed or unsupported
  RDF values. Callers receive the typed `RDFTermStorageError` or limits error.

## Runtime Flows

```text
encode term
    -> validate role and limits
    -> measure exact output
    -> allocate/write one final owner

borrow persisted bytes
    -> bounded validation cursor
    -> return scoped metrics/fingerprint/view
    -> release borrow before owner can escape

decode persisted bytes
    -> validate structure and limits
    -> decode requested term/metrics
    -> propagate typed failure
```

Storage frame, tuple, graph identity, and quad-key callers retain their input
owner through the component's synchronous borrow and apply their own larger
transaction or result lifecycle contracts above it.

## State, Ownership, and Lifecycle

An encoding plan and validation cursor are operation-local values. The final
`ByteString`/sink owner owns emitted bytes; decoded terms own only the data
required by their output contract. `RDFTermStorageValidation` borrows or
records metrics for the validated owner and cannot outlive that owner. The
component keeps no global registry or mutable cache state.

## Failure, Concurrency, and Constraints

Validation and encoding are synchronous bounded memory operations and do not
hold a lock across I/O. Overflow is rejected before integer conversion or
allocation. Cancellation and transaction cleanup belong to the caller that
performs storage I/O; this component preserves typed format failures so that
the caller can release its own owner and reservation. Any unavoidable copy is
limited to the final ownership or explicit decode boundary.

## Verification and Change Impact

| Contract | Evidence |
|---|---|
| RDF metadata/frame round trip and rejection | [SchemaEntityEntryCodecRDFMetadataTests](../../../../Tests/DatabaseEngineTests/SchemaEntityEntryCodecRDFMetadataTests.swift) |
| Nested terms, role rules, and canonical bytes | [RDFStarTests](../../../../Tests/GraphIndexTests/RDFStarTests.swift) |
| Quad key orderings, ranges, and malformed errors | [RDFQuadIndexPhysicalCodecTests](../../../../Tests/GraphIndexTests/RDFQuadIndexPhysicalCodecTests.swift), [MalformedRDFGraphIndexTests](../../../../Tests/GraphIndexTests/MalformedRDFGraphIndexTests.swift) |
| Borrow lifetime, identity sharing, and cached validation | [RDFGraphIdentityStorageSharingTests](../../../../Tests/GraphIndexTests/RDFGraphIdentityStorageSharingTests.swift) |
| Bounded nested traversal and reservation behavior | [RDFDatasetScanReservationTests](../../../../Tests/GraphIndexTests/RDFDatasetScanReservationTests.swift) |
| SHACL storage-format consumer behavior | [SHACLShapesGraphStorageFormatTests](../../../../Tests/GraphIndexTests/SHACLShapesGraphStorageFormatTests.swift) |

Changes to this component invalidate DatabaseEngine frame/tuple evidence and
GraphIndex physical-key, identity, and dataset-scan evidence. Changes only to
GraphIndex layout do not change this component unless the canonical term-byte
contract itself changes.
