import Core
import DatabaseValue
import StorageKit
import Testing
@testable import DatabaseEngine

@Suite("FieldValue canonical tuple codec")
struct FieldValueTupleCodecTests {
    @Test("all value families round-trip through physical tuple bytes")
    func allValueFamiliesRoundTrip() throws {
        let language = try DatabaseRDFLanguageTag("ja")
        let values: [FieldValue] = [
            .null,
            .bool(true),
            .int64(-42),
            .double(4.25),
            .string("calendar"),
            .data([0xFF, 0x00]),
            .rdfTerm(.literal(DatabaseRDFLiteral(
                lexicalForm: "東京\0event",
                language: language
            ))),
            .array([
                .int64(0x44_56_42),
                .string("nested\0string"),
                .data([0xFF, 0x00]),
                .rdfTerm(.iri("urn:calendar:event")),
                .null,
            ]),
        ]

        for value in values {
            let packed = Tuple(try value.toTupleElement()).pack()
            var cursor = TupleCursor(bytes: packed)
            let physicalElement = try cursor.requireNext()

            #expect(try FieldValue(tupleElement: physicalElement) == value)
            #expect(cursor.isAtEnd)
        }
    }

    @Test("composite payloads are zero-free tuple views")
    func compositePayloadsAreZeroFree() throws {
        let values: [FieldValue] = [
            .null,
            .data([0x00, 0xFF, 0x7F]),
            .rdfTerm(.literal(DatabaseRDFLiteral(
                lexicalForm: "a\0b",
                datatype: .xsdString
            ))),
            .array([.string("a\0b"), .data([0x00]), .null]),
        ]

        for value in values {
            let packed = Tuple(try value.toTupleElement()).pack()
            #expect(packed.first == TupleTypeCode.bytes.rawValue)
            #expect(packed.last == 0x00)
            #expect(!packed[1..<(packed.count - 1)].contains(0x00))

            var cursor = TupleCursor(bytes: packed)
            let element = try cursor.requireNext()
            let payload = try #require(element as? Bytes)
            #expect(payload.count == packed.count - 2)
            #expect(try FieldValue(tupleElement: payload) == value)
        }
    }

    @Test("malformed RDF payloads fail with a typed error")
    func malformedRDFPayloadFails() {
        let payload = Bytes([0x44, 0x56, 0x42, 0x01, 0x26, 0xFF])

        #expect(
            throws: FieldValueTupleCodecError.invalidRDFTerm(
                .unknownTag(0xFF)
            )
        ) {
            _ = try FieldValue(tupleElement: payload)
        }
    }

    @Test("untagged byte and tuple values are rejected")
    func untaggedValuesFail() {
        #expect(throws: FieldValueTupleCodecError.invalidEnvelopeMagic) {
            _ = try FieldValue(tupleElement: Bytes([0x01, 0x02]))
        }
        #expect(
            throws: FieldValueTupleCodecError.unsupportedElementType(
                "Tuple"
            )
        ) {
            _ = try FieldValue(tupleElement: Tuple([Int64(1), Int64(2)]))
        }
    }

    @Test("byte, collection, depth, and object limits are symmetric")
    func resourceLimitsAreSymmetric() throws {
        let value = FieldValue.array([
            .array([.null]),
            .rdfTerm(.iri("urn:calendar:event")),
        ])
        let unrestrictedElement = try value.toTupleElement()
        let packed = Tuple(unrestrictedElement).pack()
        let exactLimits = FieldValueTupleCodecLimits(
            maximumEncodedBytes: packed.count,
            maximumCollectionCount: 2,
            maximumDepth: 2,
            maximumObjectCount: 5
        )
        let exactPacked = Tuple(
            try value.toTupleElement(limits: exactLimits)
        ).pack()
        var exactCursor = TupleCursor(bytes: exactPacked)
        let exactElement = try exactCursor.requireNext()
        #expect(
            try FieldValue(
                tupleElement: exactElement,
                limits: exactLimits
            ) == value
        )

        let byteLimits = FieldValueTupleCodecLimits(
            maximumEncodedBytes: packed.count - 1,
            maximumCollectionCount: 2,
            maximumDepth: 2,
            maximumObjectCount: 5
        )
        assertSymmetricFailure(
            value: value,
            packed: packed,
            limits: byteLimits,
            expected: .maximumEncodedBytesExceeded(
                actual: packed.count,
                maximum: packed.count - 1
            )
        )

        let collectionLimits = FieldValueTupleCodecLimits(
            maximumEncodedBytes: packed.count,
            maximumCollectionCount: 1,
            maximumDepth: 2,
            maximumObjectCount: 5
        )
        assertSymmetricFailure(
            value: value,
            packed: packed,
            limits: collectionLimits,
            expected: .maximumCollectionCountExceeded(actual: 2, maximum: 1)
        )

        let depthLimits = FieldValueTupleCodecLimits(
            maximumEncodedBytes: packed.count,
            maximumCollectionCount: 2,
            maximumDepth: 1,
            maximumObjectCount: 5
        )
        assertSymmetricFailure(
            value: value,
            packed: packed,
            limits: depthLimits,
            expected: .maximumDepthExceeded(actual: 2, maximum: 1)
        )

        let objectLimits = FieldValueTupleCodecLimits(
            maximumEncodedBytes: packed.count,
            maximumCollectionCount: 2,
            maximumDepth: 2,
            maximumObjectCount: 4
        )
        assertSymmetricFailure(
            value: value,
            packed: packed,
            limits: objectLimits,
            expected: .maximumObjectCountExceeded(actual: 5, maximum: 4)
        )
    }

    @Test("truncated arrays and malformed nibble payloads fail")
    func malformedCompositePayloadsFail() {
        let truncatedArray = Bytes([0x44, 0x56, 0x42, 0x01, 0x28, 0x20])
        #expect(
            throws: FieldValueTupleCodecError.truncated
        ) {
            _ = try FieldValue(tupleElement: truncatedArray)
        }

        let malformedData = Bytes([0x44, 0x56, 0x42, 0x01, 0x27, 0x02, 0x01])
        #expect(
            throws: FieldValueTupleCodecError.incompleteNibblePair
        ) {
            _ = try FieldValue(tupleElement: malformedData)
        }
    }

    private func assertSymmetricFailure(
        value: FieldValue,
        packed: Bytes,
        limits: FieldValueTupleCodecLimits,
        expected: FieldValueTupleCodecError
    ) {
        #expect(throws: expected) {
            _ = try value.toTupleElement(limits: limits)
        }

        #expect(throws: expected) {
            var cursor = TupleCursor(bytes: packed)
            let element = try cursor.requireNext()
            _ = try FieldValue(tupleElement: element, limits: limits)
        }
    }
}
