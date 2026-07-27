@testable import DatabaseServer
import DatabaseTypes
import StorageKit
import Testing

@Suite("Database UUID tuple encoding")
struct DatabaseUUIDTupleEncodingTests {
    @Test("Database UUID encodes directly into and decodes from a tuple")
    func roundTripsTupleEncoding() throws {
        let expected = DatabaseTypes.UUID(
            high: 0x0011_2233_4455_6677,
            low: 0x8899_AABB_CCDD_EEFF
        )
        let encoded = Tuple(expected).pack()
        var offset = 1

        let decoded = try DatabaseTypes.UUID.decodeTuple(
            from: encoded,
            at: &offset
        )

        #expect(encoded.count == 17)
        #expect(decoded == expected)
        #expect(offset == encoded.count)
    }

    @Test("Database UUID rejects a missing tuple type code")
    func rejectsMissingTypeCode() {
        var offset = 0

        #expect(throws: TupleError.self) {
            _ = try DatabaseTypes.UUID.decodeTuple(from: [], at: &offset)
        }
    }

    @Test("Database UUID rejects offsets outside the encoded bytes")
    func rejectsOutOfBoundsOffsets() {
        let encoded = Bytes([TupleTypeCode.uuid.rawValue])

        for invalidOffset in [2, Int.max] {
            var offset = invalidOffset
            #expect(throws: TupleError.self) {
                _ = try DatabaseTypes.UUID.decodeTuple(
                    from: encoded,
                    at: &offset
                )
            }
        }
    }

    @Test("Database UUID rejects a truncated tuple payload")
    func rejectsTruncatedPayload() {
        let encoded = Bytes(
            [TupleTypeCode.uuid.rawValue] + Array(repeating: 0, count: 15)
        )
        var offset = 1

        #expect(throws: TupleError.self) {
            _ = try DatabaseTypes.UUID.decodeTuple(from: encoded, at: &offset)
        }
    }
}
