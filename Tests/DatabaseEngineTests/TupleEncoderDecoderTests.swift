#if !os(WASI)
import DatabaseKit
import DatabaseTypes
import StorageKit
import TestHeartbeat
import Testing
@testable import DatabaseEngine

@Suite("Tuple encoder canonical value tests", .heartbeat)
struct TupleEncoderTests {
    @Test("supported scalar values encode through FieldValue")
    func supportedScalarsEncodeCanonically() throws {
        let timestamp = try Timestamp(
            secondsSinceUnixEpoch: 1_700_000_000,
            nanoseconds: 123_456_789
        )
        let date = try CivilDate(year: 2026, month: 7, day: 30)
        let uuid = DatabaseTypes.UUID(high: 1, low: 2)
        let cases: [(element: any TupleElement, expected: FieldValue)] = [
            (try TupleEncoder.encode(true), .bool(true)),
            (try TupleEncoder.encode(Int8.min), .int8(.min)),
            (try TupleEncoder.encode(Int16.min), .int16(.min)),
            (try TupleEncoder.encode(Int32.min), .int32(.min)),
            (try TupleEncoder.encode(Int64.min), .int64(.min)),
            (try TupleEncoder.encode(UInt8.max), .uint8(.max)),
            (try TupleEncoder.encode(UInt16.max), .uint16(.max)),
            (try TupleEncoder.encode(UInt32.max), .uint32(.max)),
            (try TupleEncoder.encode(UInt64.max), .uint64(.max)),
            (try TupleEncoder.encode(Float(2.5)), .float32(2.5)),
            (try TupleEncoder.encode(Double.pi), .float64(.pi)),
            (try TupleEncoder.encode("calendar"), .string("calendar")),
            (try TupleEncoder.encode(ByteString([0x00, 0xFF])), .bytes([0x00, 0xFF])),
            (try TupleEncoder.encode(timestamp), .timestamp(timestamp)),
            (try TupleEncoder.encode(date), .date(date)),
            (try TupleEncoder.encode(uuid), .uuid(uuid)),
        ]

        for item in cases {
            #expect(try FieldValue(tupleElement: item.element) == item.expected)
        }
    }

    @Test("FieldValue and RDFTerm overloads preserve their canonical value")
    func semanticOverloadsPreserveValue() throws {
        let object = FieldValue.object(
            try FieldObject([
                (key: "title", value: .string("Event")),
                (key: "priority", value: .int32(3)),
            ])
        )
        let iri = try RDFTerm.iri(validating: "urn:calendar:event")

        #expect(
            try FieldValue(tupleElement: TupleEncoder.encode(object)) == object
        )
        #expect(
            try FieldValue(tupleElement: TupleEncoder.encode(iri)) == .rdfTerm(iri)
        )
    }

    @Test("encodeAll preserves homogeneous values without type erasure")
    func encodeAllPreservesValues() throws {
        let values: [Int32] = [-1, 0, 1]
        let elements = try TupleEncoder.encodeAll(values)

        #expect(elements.count == values.count)
        for (element, value) in zip(elements, values) {
            #expect(try FieldValue(tupleElement: element) == .int32(value))
        }
    }

    @Test("Tuple overload keeps an already physical tuple")
    func tupleOverloadPreservesTuple() {
        let tuple = Tuple(["nested", Int64(42)])
        let encoded = TupleEncoder.encode(tuple)

        #expect((encoded as? Tuple) == tuple)
    }

    @Test("resource limits fail with the canonical typed error")
    func resourceLimitsAreEnforced() {
        let limits = FieldValueTupleCodecLimits(
            maximumEncodedBytes: 4,
            maximumCollectionCount: 1,
            maximumDepth: 1,
            maximumObjectCount: 1
        )

        #expect(throws: FieldValueTupleCodecError.self) {
            _ = try TupleEncoder.encode("too large", limits: limits)
        }
    }
}

@Suite("Tuple decoder canonical value tests", .heartbeat)
struct TupleDecoderTests {
    @Test("canonical values decode to their declared primitive types")
    func canonicalValuesDecode() throws {
        let timestamp = try Timestamp(
            secondsSinceUnixEpoch: -1,
            nanoseconds: 999_999_999
        )
        let date = try CivilDate(year: 2000, month: 2, day: 29)
        let uuid = DatabaseTypes.UUID(high: .max, low: 0)
        let bytes = ByteString([0x00, 0x7F, 0xFF])

        #expect(try TupleDecoder.decodeBool(TupleEncoder.encode(true)))
        #expect(try TupleDecoder.decodeInt8(TupleEncoder.encode(Int8.min)) == .min)
        #expect(try TupleDecoder.decodeInt16(TupleEncoder.encode(Int16.min)) == .min)
        #expect(try TupleDecoder.decodeInt32(TupleEncoder.encode(Int32.min)) == .min)
        #expect(try TupleDecoder.decodeInt64(TupleEncoder.encode(Int64.min)) == .min)
        #expect(try TupleDecoder.decodeUInt8(TupleEncoder.encode(UInt8.max)) == .max)
        #expect(try TupleDecoder.decodeUInt16(TupleEncoder.encode(UInt16.max)) == .max)
        #expect(try TupleDecoder.decodeUInt32(TupleEncoder.encode(UInt32.max)) == .max)
        #expect(try TupleDecoder.decodeUInt64(TupleEncoder.encode(UInt64.max)) == .max)
        #expect(try TupleDecoder.decodeFloat(TupleEncoder.encode(Float(2.5))) == 2.5)
        #expect(try TupleDecoder.decodeDouble(TupleEncoder.encode(Double.pi)) == .pi)
        #expect(try TupleDecoder.decodeString(TupleEncoder.encode("calendar")) == "calendar")
        #expect(try TupleDecoder.decodeBytes(TupleEncoder.encode(bytes)) == bytes)
        #expect(try TupleDecoder.decodeTimestamp(TupleEncoder.encode(timestamp)) == timestamp)
        #expect(try TupleDecoder.decodeCivilDate(TupleEncoder.encode(date)) == date)
        #expect(try TupleDecoder.decodeUUID(TupleEncoder.encode(uuid)) == uuid)
    }

    @Test("physical tuple scalars remain decodable")
    func physicalTupleScalarsDecode() throws {
        #expect(try TupleDecoder.decodeInt64(Int64(42)) == 42)
        #expect(try TupleDecoder.decodeUInt64(UInt64.max) == .max)
        #expect(try TupleDecoder.decodeDouble(Float(2.5)) == 2.5)
        #expect(try TupleDecoder.decodeString("event") == "event")
        #expect(try TupleDecoder.decodeBool(true))
        #expect(try TupleDecoder.decodeBytes(ByteString([0x01])) == [0x01])
        #expect(
            try TupleDecoder.decodeUUID(
                DatabaseTypes.UUID(high: 10, low: 20)
            ) == DatabaseTypes.UUID(high: 10, low: 20)
        )
    }

    @Test("integer narrowing rejects overflow and sign loss")
    func integerNarrowingRejectsInvalidValues() {
        #expect(throws: TupleDecodingError.self) {
            _ = try TupleDecoder.decodeInt8(Int64(Int8.max) + 1)
        }
        #expect(throws: TupleDecodingError.self) {
            _ = try TupleDecoder.decodeUInt64(Int64(-1))
        }
        #expect(throws: TupleDecodingError.self) {
            _ = try TupleDecoder.decodeInt64(UInt64.max)
        }
        #expect(throws: TupleDecodingError.self) {
            _ = try TupleDecoder.decodeInt64(Double(1.5))
        }
    }

    @Test("semantic type mismatches fail instead of coercing")
    func semanticTypeMismatchesFail() throws {
        let encodedInteger = try TupleEncoder.encode(Int64(1))
        let encodedString = try TupleEncoder.encode("true")

        #expect(throws: TupleDecodingError.self) {
            _ = try TupleDecoder.decodeBool(encodedInteger)
        }
        #expect(throws: TupleDecodingError.self) {
            _ = try TupleDecoder.decodeInt64(encodedString)
        }
        #expect(throws: TupleDecodingError.self) {
            _ = try TupleDecoder.decodeBytes(encodedString)
        }
    }

    @Test("generic decode uses explicit TupleDecodable conformance")
    func genericDecodeUsesStaticConformance() throws {
        let encoded = try TupleEncoder.encode(UInt32.max)
        let value = try TupleDecoder.decode(encoded, as: UInt32.self)

        #expect(value == .max)
    }

    @Test("packing and unpacking retain canonical semantics")
    func packedTupleRetainsSemantics() throws {
        let values: [FieldValue] = [
            .string("event"),
            .int64(42),
            .bytes([0x00, 0xFF]),
        ]
        let elements = try FieldValue.toTupleElements(values)
        let packed = Tuple(elements).pack()
        let unpacked = try Tuple.unpack(from: packed)

        #expect(unpacked.count == values.count)
        for (element, expected) in zip(unpacked, values) {
            #expect(try FieldValue(tupleElement: element) == expected)
        }
    }
}
#endif
