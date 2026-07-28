import DatabaseTypes
import Testing

@testable import DatabaseEngine

@Suite("Item envelope byte ownership")
struct ItemEnvelopeSliceTests {
    @Test("deserialization accepts a retained non-zero-index slice")
    func retainedSlice() throws {
        let payload: ByteString = [0x10, 0x20, 0x30]
        let serialized = try ItemEnvelope.inline(
            payload: payload,
            encoding: .identity,
            plainByteCount: UInt64(payload.count),
            checksum: ItemChecksum.crc32c(payload)
        ).serialize()
        let framed = ByteString([0xFF])
            .appending(contentsOf: serialized)
            .appending(0xEE)
        let frame = framed[1..<(1 + serialized.count)]

        #expect(frame.startIndex == 1)
        #expect(ItemEnvelope.isEnvelope(frame))
        let envelope = try ItemEnvelope.deserialize(frame)
        guard case .inline(let decodedPayload) = envelope.content else {
            Issue.record("Expected inline envelope content")
            return
        }
        #expect(decodedPayload == payload)
        #expect(
            decodedPayload.startIndex
                == frame.startIndex + ItemEnvelope.headerSize
        )
        let frameAddress = frame.withUnsafeBytes {
            $0.baseAddress.map(UInt.init(bitPattern:))
        }
        let payloadAddress = decodedPayload.withUnsafeBytes {
            $0.baseAddress.map(UInt.init(bitPattern:))
        }
        #expect(
            payloadAddress
                == frameAddress.map {
                    $0 + UInt(ItemEnvelope.headerSize)
                }
        )
    }
}
