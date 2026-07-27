import DatabaseEngine
import DatabaseTypes
import DatabaseKit
import StorageKit
import Synchronization
import Testing
@testable import GraphIndex

@Suite("RDF graph identity storage sharing")
struct RDFGraphIdentityStorageSharingTests {
    @Test("tuple decoding retains the packed key allocation as a byte view")
    func tupleDecodeRetainsPackedKeyAllocation() throws {
        let term = RDFTerm.literal(RDFLiteral(
            lexicalForm: "before\0after",
            datatype: XSDDatatype.string.typedLiteralDatatype
        ))
        let encoded = try RDFTermStorageFormat.encode(term)
        #expect(encoded.allSatisfy { $0 != 0 })

        let packed = Tuple(encoded).pack()
        let tuple = try Tuple.unpack(from: packed)
        let element = try tupleElement(tuple, at: 0)
        guard let decoded = element as? ByteString else {
            Issue.record("Expected a borrowed ByteString tuple element")
            return
        }

        packed.withUnsafeBytes { packedBuffer in
            decoded.withUnsafeBytes { decodedBuffer in
                #expect(
                    decodedBuffer.baseAddress
                        == packedBuffer.baseAddress?.advanced(by: 1)
                )
            }
        }
        #expect(
            try RDFTermStorageFormat.decode(
                decoded
            ) == term
        )
    }

    @Test("a decoded tuple view retains its owner after the packed value leaves scope")
    func tupleViewRetainsOwner() throws {
        let term = try RDFTerm.iri(
            validating: "urn:calendar:event"
        )
        let decoded = try retainedTupleElement(for: term)

        #expect(
            try RDFTermStorageFormat.decode(
                decoded
            ) == term
        )
    }

    @Test("cached literal identity cannot be reused as an RDF subject")
    func cachedLiteralStillValidatesSubjectRole() throws {
        let pool = GraphIdentityPool()
        let encoded = try RDFTermStorageFormat.encode(
            .literal(RDFLiteral(
                lexicalForm: "event",
                datatype: XSDDatatype.string.typedLiteralDatatype
            ))
        )

        _ = try pool.internRDF(encoded, role: .object)
        #expect(throws: GraphIndexError.self) {
            _ = try pool.internRDF(encoded, role: .subject)
        }
    }

    @Test("cached blank-node identity cannot be reused as an RDF predicate")
    func cachedBlankNodeStillValidatesPredicateRole() throws {
        let pool = GraphIdentityPool()
        let encoded = try RDFTermStorageFormat.encode(
            .blankNode(identifier: "event")
        )

        _ = try pool.internRDF(encoded, role: .subject)
        #expect(throws: GraphIndexError.self) {
            _ = try pool.internRDF(encoded, role: .predicate)
        }
    }

    @Test("cached literal identity cannot be reused as an RDF graph name")
    func cachedLiteralStillValidatesGraphRole() throws {
        let pool = GraphIdentityPool()
        let encoded = try RDFTermStorageFormat.encode(
            .literal(RDFLiteral(
                lexicalForm: "calendar",
                datatype: XSDDatatype.string.typedLiteralDatatype
            ))
        )

        _ = try pool.internRDF(encoded, role: .object)
        #expect(throws: GraphIndexError.self) {
            _ = try pool.internRDF(encoded, role: .graph)
        }
    }

    @Test("identity interning validates a cache miss in one owner borrow")
    func identityInterningUsesOneInputBorrow() throws {
        let canonical = try RDFTermStorageFormat.encode(
            .iri(validating: "urn:calendar:event")
        )
        let owner = GraphIdentityBorrowCountingOwner(
            bytes: canonical.copyBytes()
        )
        let pool = GraphIdentityPool()

        let identity = try pool.internRDF(
            ByteString(retaining: owner),
            role: .subject
        )

        #expect(owner.borrowCount == 1)
        #expect(identity.canonicalRDFBytes?.count == canonical.count)
        guard let retainedBytes = identity.canonicalRDFBytes else {
            Issue.record("Expected canonical RDF bytes")
            return
        }
        retainedBytes.withUnsafeBytes { retainedBuffer in
            owner.bytes.withUnsafeBytes { sourceBuffer in
                #expect(
                    retainedBuffer.baseAddress
                        == sourceBuffer.baseAddress
                )
            }
        }
    }

    @Test("nested triple validation errors are not rewritten as outer role errors")
    func nestedTripleErrorRemainsExact() {
        let invalidSubject = ByteString([
            4,
            3, 2, 0x78, 1, 4, 0x75, 0x3A, 0x74,
            2, 4, 0x75, 0x3A, 0x70,
            2, 4, 0x75, 0x3A, 0x6F,
        ])
        let pool = GraphIdentityPool()

        do {
            _ = try pool.internRDF(invalidSubject, role: .object)
            Issue.record("Expected nested RDF validation to fail")
        } catch let error as GraphIndexError {
            guard case .invalidRDFEncoding(let reason) = error else {
                Issue.record("Unexpected graph index error: \(error)")
                return
            }
            #expect(reason == .invalidTripleSubject)
        } catch {
            Issue.record("Unexpected error type: \(error)")
        }
    }

    @Test("clear-key permutation shares canonical component storage")
    func clearPermutationBorrowsEachComponentOncePerFinalKey() throws {
        let base = Subspace(prefix: Tuple("rdf-clear-borrow").pack())
        let codec = RDFQuadIndexPhysicalCodec(baseSubspace: base)
        let quad = RDFQuad(
            subject: .blankNode(
                try RDFBlankNodeIdentifier("subject")
            ),
            predicate: try RDFPredicateIRI(
                "https://example.com/predicate"
            ),
            object: .literal(RDFLiteral(
                lexicalForm: "payload",
                datatype: .xsdString
            )),
            graph: try RDFGraphName(
                iri: "https://example.com/graph"
            )
        )
        var gspoKey: ByteString?
        try RDFQuadIndexWritePlan(quad: quad).forEachEntry { entry in
            guard entry.ordering == .gspo else { return }
            gspoKey = try codec.encode(entry)
        }
        let packedKey = try #require(gspoKey)
        let owner = PhysicalKeyBorrowCountingOwner(
            bytes: packedKey.copyBytes()
        )
        let ownerBackedKey = ByteString(retaining: owner)
        let keyAddressRange = try #require(
            ownerBackedKey.withUnsafeBytes { buffer in
                buffer.baseAddress.map { address in
                    let start = UInt(bitPattern: address)
                    return start..<(start + UInt(buffer.count))
                }
            }
        )
        let encoded = try codec.decodeEncodedQuad(
            key: ownerBackedKey,
            ordering: .gspo
        )
        let graph = try #require(encoded.graph)
        for component in [
            encoded.subject,
            encoded.predicate,
            encoded.object,
            graph,
        ] {
            let componentRange = try #require(
                component.withUnsafeBytes { buffer in
                    buffer.baseAddress.map { address in
                        let start = UInt(bitPattern: address)
                        return start..<(start + UInt(buffer.count))
                    }
                }
            )
            #expect(componentRange.lowerBound >= keyAddressRange.lowerBound)
            #expect(componentRange.upperBound <= keyAddressRange.upperBound)
        }

        let clearPlan = try RDFQuadIndexWritePlan(encodedQuad: encoded)
        let borrowsBeforeEncoding = owner.borrowCount
        var emittedKeyCount = 0
        try clearPlan.forEachEntry { entry in
            guard !entry.ordering.isGraphFirst else { return }
            _ = try codec.encode(entry)
            emittedKeyCount += 1
        }

        #expect(emittedKeyCount == 3)
        #expect(owner.borrowCount - borrowsBeforeEncoding == 12)
    }

    private func retainedTupleElement(
        for term: RDFTerm
    ) throws -> ByteString {
        let packed = Tuple(
            try RDFTermStorageFormat.encode(term)
        ).pack()
        let tuple = try Tuple.unpack(from: packed)
        let element = try tupleElement(tuple, at: 0)
        guard let decoded = element as? ByteString else {
            throw RDFGraphIdentityStorageSharingError.unexpectedElementType
        }
        return decoded
    }

    private func tupleElement(
        _ tuple: [any TupleElement],
        at index: Int
    ) throws -> any TupleElement {
        guard tuple.indices.contains(index) else {
            throw RDFGraphIdentityStorageSharingError.missingElement
        }
        return tuple[index]
    }
}

private final class GraphIdentityBorrowCountingOwner: ByteStringOwner {
    let bytes: [UInt8]
    private let state = Mutex(0)

    init(bytes: [UInt8]) {
        self.bytes = bytes
    }

    var count: Int { bytes.count }

    var borrowCount: Int {
        state.withLock { $0 }
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        state.withLock { $0 += 1 }
        try bytes.withUnsafeBytes(body)
    }
}

private final class PhysicalKeyBorrowCountingOwner: ByteStringOwner {
    let bytes: [UInt8]
    private let state = Mutex(0)

    init(bytes: [UInt8]) {
        self.bytes = bytes
    }

    var count: Int { bytes.count }

    var borrowCount: Int {
        state.withLock { $0 }
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        state.withLock { $0 += 1 }
        try bytes.withUnsafeBytes(body)
    }
}

private enum RDFGraphIdentityStorageSharingError: Error {
    case missingElement
    case unexpectedElementType
}
