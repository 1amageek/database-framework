import DatabaseTypes
import StorageKit
import Testing
@testable import DatabaseEngine

@Suite("Canonical tuple element codec")
struct CanonicalTupleElementCodecTests {
    @Test("Every supported physical scalar round-trips without changing its domain")
    func supportedScalarsRoundTrip() throws {
        let bytes = ByteString([0x00, 0x7F, 0xFF])
        let uuid = DatabaseTypes.UUID(high: 1, low: 2)
        let elements: [any TupleElement & Sendable] = [
            TupleNil(),
            bytes,
            "text",
            true,
            Int64(-42),
            UInt64.max,
            Float(1.25),
            Double(2.5),
            uuid,
        ]

        for element in elements {
            let semanticValue = try CanonicalTupleElementCodec.encode(element)
            let decoded = try CanonicalTupleElementCodec.decode(semanticValue)
            #expect(Tuple(decoded).pack() == Tuple(element).pack())
        }
    }

    @Test("Null and bytes retain their canonical FieldValue domains")
    func nullAndBytesRetainDomains() throws {
        let bytes = ByteString([0x00, 0x01, 0xFF])

        #expect(
            try CanonicalTupleElementCodec.encode(TupleNil()) == .null
        )
        #expect(
            try CanonicalTupleElementCodec.encode(bytes) == .bytes(bytes)
        )
    }

    @Test("Float32 is not widened into an object or Float64")
    func float32RetainsWidth() throws {
        let encoded = try CanonicalTupleElementCodec.encode(Float(1.25))
        #expect(encoded == .float32(1.25))

        let decoded = try CanonicalTupleElementCodec.decode(encoded)
        #expect(decoded is Float)
    }

    @Test("Non-tuple FieldValue domains fail explicitly")
    func unsupportedSemanticDomainFails() throws {
        let date = try CivilDate(year: 2026, month: 8, day: 2)
        #expect(throws: CanonicalTupleElementCodecError.self) {
            try CanonicalTupleElementCodec.decode(.date(date))
        }
    }
}
