import Testing
import DatabaseEngine
@testable import VectorIndex

@Suite("Vector Conversion")
struct VectorConversionTests {

    @Test("Encodes Float32 arrays as little-endian binary payloads")
    func encodesFloat32LittleEndianPayloads() {
        let bytes = VectorConversion.floatArrayToBytes([1.0, -2.5, 0.25])

        #expect(bytes == [
            0x00, 0x00, 0x80, 0x3F,
            0x00, 0x00, 0x20, 0xC0,
            0x00, 0x00, 0x80, 0x3E
        ])
    }

    @Test("Decodes validated Float32 payloads")
    func decodesValidatedFloat32Payloads() throws {
        let payload = VectorConversion.floatArrayToBytes([1.0, -2.5, 0.25])
        let decoded = try VectorConversion.decodeFloatArray(payload, expectedCount: 3)

        #expect(decoded == [1.0, -2.5, 0.25])
    }

    @Test("Rejects malformed Float32 payload lengths")
    func rejectsMalformedFloat32PayloadLengths() {
        #expect(throws: VectorIndexError.self) {
            _ = try VectorConversion.decodeFloatArray([0x00, 0x00, 0x80], expectedCount: 1)
        }
    }

    @Test("Round-trips fixed-width integer payloads")
    func roundTripsFixedWidthIntegerPayloads() throws {
        let unsignedValue = UInt64.max - 42
        let signedValue = Int64.min + 42

        #expect(
            try VectorConversion.bytesToUInt64(
                VectorConversion.uint64ToBytes(unsignedValue)
            ) == unsignedValue
        )
        #expect(
            try VectorConversion.bytesToInt64(
                VectorConversion.int64ToBytes(signedValue)
            ) == signedValue
        )
        #expect(throws: ByteConversionError.self) {
            _ = try VectorConversion.bytesToUInt64([0x01, 0x02])
        }
        #expect(throws: ByteConversionError.self) {
            _ = try VectorConversion.bytesToInt64([0x01, 0x02])
        }
    }

    @Test("Round-trips larger payloads without changing order")
    func roundTripsLargerPayloadsWithoutChangingOrder() throws {
        let vector = (0..<257).map { Float($0) / 10.0 - 12.0 }
        let payload = VectorConversion.floatArrayToBytes(vector)
        let decoded = try VectorConversion.decodeFloatArray(payload, expectedCount: vector.count)

        #expect(payload.count == vector.count * 4)
        #expect(decoded == vector)
    }

    @Test("SIMD distance functions match expected scalar results")
    func simdDistanceFunctionsMatchExpectedScalarResults() {
        let lhs: [Float] = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        let rhs: [Float] = [9, 8, 7, 6, 5, 4, 3, 2, 1]

        let squared = VectorConversion.euclideanDistanceSquaredFloat(lhs, rhs)
        let euclidean = VectorConversion.euclideanDistance(lhs, rhs)
        let dot = VectorConversion.dotProductDistance(lhs, rhs)

        #expect(squared == 240)
        #expect(abs(euclidean - Double(squared).squareRoot()) < 0.0001)
        #expect(dot == -165)
    }
}
