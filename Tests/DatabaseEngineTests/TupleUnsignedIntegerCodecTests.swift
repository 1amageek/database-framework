import StorageKit
import Testing
@testable import DatabaseEngine

@Suite("Tuple unsigned integer codec")
struct TupleUnsignedIntegerCodecTests {
    @Test("UInt64 values above Int64 max round-trip exactly")
    func fullWidthUInt64RoundTrip() throws {
        let values: [UInt64] = [
            UInt64(Int64.max) + 1,
            UInt64.max,
        ]

        for value in values {
            let element = try TupleEncoder.encode(value)
            #expect(element is UInt64)

            let packed = Tuple(element).pack()
            let unpacked = try Tuple.unpack(from: packed)
            #expect(unpacked.count == 1)
            let unpackedElement = try #require(unpacked.first)
            let decoded = try TupleDecoder.decode(
                unpackedElement,
                as: UInt64.self
            )
            #expect(decoded == value)
        }
    }

    @Test("Existing positive Int64 tuple representation remains canonical")
    func preservesExistingPositiveIntegerEncoding() throws {
        let unsignedElement = try TupleEncoder.encode(UInt64(42))
        let signedElement = try TupleEncoder.encode(Int64(42))

        #expect(unsignedElement is Int64)
        #expect(Tuple(unsignedElement).pack() == Tuple(signedElement).pack())
        #expect(try TupleDecoder.decode(unsignedElement, as: UInt64.self) == 42)
    }

    @Test("Unsigned tuple keys retain numeric ordering across Int64 boundary")
    func preservesUnsignedKeyOrdering() throws {
        let values: [UInt64] = [
            UInt64(Int64.max),
            UInt64(Int64.max) + 1,
            UInt64.max,
        ]
        let keys = try values.map { value in
            Tuple(try TupleEncoder.encode(value)).pack()
        }

        #expect(keys[0].lexicographicallyPrecedes(keys[1]))
        #expect(keys[1].lexicographicallyPrecedes(keys[2]))
    }

    @Test("Invalid unsigned conversions fail with typed errors")
    func rejectsInvalidUnsignedConversions() {
        #expect(
            throws: TupleDecodingError.integerOverflow(
                value: -1,
                targetType: "UInt64"
            )
        ) {
            try TupleDecoder.decode(Int64(-1), as: UInt64.self)
        }
        #expect(
            throws: TupleDecodingError.unsignedIntegerOverflow(
                value: UInt64.max,
                targetType: "UInt32"
            )
        ) {
            try TupleDecoder.decode(UInt64.max, as: UInt32.self)
        }
        #expect(throws: TupleDecodingError.self) {
            try TupleDecoder.decode(1.5, as: UInt64.self)
        }
    }
}
