import DatabaseKit
import DatabaseTypes
import StorageKit
import Testing
@testable import DatabaseEngine

@Suite("FieldValue canonical tuple codec")
struct FieldValueTupleCodecTests {
    @Test("all value families round-trip through physical tuple bytes")
    func allValueFamiliesRoundTrip() throws {
        let language = try RDFLanguageTag("ja")
        let date = try CivilDate(year: 2026, month: 7, day: 24)
        let timestamp = try Timestamp(
            secondsSinceUnixEpoch: 1_774_483_200,
            nanoseconds: 123_456_789
        )
        let uuid = DatabaseTypes.UUID(high: 1, low: 2)
        let object = FieldValue.object(
            try FieldObject([
                (key: "title", value: .string("Calendar")),
                (
                    key: "priority",
                    value: .decimal(
                        ExactDecimal(coefficient: 125, scale: 2)
                    )
                ),
            ])
        )
        let reference = FieldValue.reference(
            try EntityReference(
                entity: "Event",
                id: .composite([
                    .string("calendar"),
                    .uuid(uuid),
                ]),
                partitions: try FieldObject([
                    (key: "tenant", value: .string("primary")),
                ])
            )
        )
        let values: [FieldValue] = [
            .null,
            .bool(true),
            .int64(-42),
            .uint64(.max),
            .float64(4.25),
            .decimal(ExactDecimal(coefficient: -12_500, scale: 3)),
            .string("calendar"),
            .bytes([0xFF, 0x00]),
            .date(date),
            .timestamp(timestamp),
            .uuid(uuid),
            object,
            reference,
            .rdfTerm(.literal(RDFLiteral(
                lexicalForm: "東京\0event",
                language: language
            ))),
            .array([
                .int64(0x44_56_42),
                .string("nested\0string"),
                .bytes([0xFF, 0x00]),
                object,
                reference,
                .rdfTerm(try .iri(validating: "urn:calendar:event")),
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

    @Test("decimal tuple order matches exact FieldValue order")
    func decimalTupleOrderMatchesFieldValueOrder() throws {
        let values: [FieldValue] = [
            .decimal(ExactDecimal(coefficient: -121, scale: 2)),
            .decimal(ExactDecimal(coefficient: -12, scale: 1)),
            .decimal(ExactDecimal(coefficient: 0, scale: 4)),
            .decimal(ExactDecimal(coefficient: 12, scale: 1)),
            .decimal(ExactDecimal(coefficient: 121, scale: 2)),
            .decimal(ExactDecimal(coefficient: 12, scale: -3)),
        ]

        for pair in zip(values, values.dropFirst()) {
            #expect(pair.0 < pair.1)
            let left = Tuple(try pair.0.toTupleElement()).pack()
            let right = Tuple(try pair.1.toTupleElement()).pack()
            #expect(left.lexicographicallyPrecedes(right))
        }
    }

    @Test("composite payloads are zero-free tuple views")
    func compositePayloadsAreZeroFree() throws {
        let values: [FieldValue] = [
            .null,
            .bytes([0x00, 0xFF, 0x7F]),
            .rdfTerm(.literal(RDFLiteral(
                lexicalForm: "a\0b",
                datatype: .xsdString
            ))),
            .array([.string("a\0b"), .bytes([0x00]), .null]),
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
        let payload = Bytes([
            0x44, 0x56, 0x42, 0x01,
            0x3D,
            0x11, 0x11,
            0x01,
        ])

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
            .rdfTerm(try .iri(validating: "urn:calendar:event")),
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
        let truncatedArray = Bytes([
            0x44, 0x56, 0x42, 0x01,
            0x3A,
            0x20,
        ])
        #expect(
            throws: FieldValueTupleCodecError.truncated
        ) {
            _ = try FieldValue(tupleElement: truncatedArray)
        }

        let malformedData = Bytes([
            0x44, 0x56, 0x42, 0x01,
            0x2F,
            0x02, 0x01,
        ])
        #expect(
            throws: FieldValueTupleCodecError.incompleteNibblePair
        ) {
            _ = try FieldValue(tupleElement: malformedData)
        }

        let invalidUTF8 = Bytes([
            0x44, 0x56, 0x42, 0x01,
            0x3A,
            0x2E, 0x11, 0x11, 0x01,
            0x01,
        ])
        #expect(
            throws: FieldValueTupleCodecError.invalidArrayElement(
                index: 0,
                reason: .invalidUTF8
            )
        ) {
            _ = try FieldValue(tupleElement: invalidUTF8)
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
