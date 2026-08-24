import DatabaseTypes
import Testing

@testable import DatabaseEngine

@Suite("Storage frame UTF-8 validation")
struct StorageFrameDecoderUTF8Tests {
    @Test("Borrowed string validation accepts only canonical UTF-8")
    func borrowedStringValidation() throws {
        let validPayloads: [[UInt8]] = [
            [],
            [0x41],
            [0xC2, 0xA2],
            [0xE0, 0xA0, 0x80],
            [0xED, 0x9F, 0xBF],
            [0xEE, 0x80, 0x80],
            [0xF0, 0x90, 0x80, 0x80],
            [0xF4, 0x8F, 0xBF, 0xBF],
        ]
        for payload in validPayloads {
            var decoder = try StorageFrameDecoder(framed(payload))
            let decoded = try decoder.readValidatedStringBytes()
            #expect(decoded.elementsEqual(payload))
            try decoder.ensureFullyRead()
        }

        let invalidPayloads: [[UInt8]] = [
            [0x80],
            [0xC0, 0x80],
            [0xC2],
            [0xE0, 0x9F, 0xBF],
            [0xE1, 0x80],
            [0xED, 0xA0, 0x80],
            [0xF0, 0x8F, 0xBF, 0xBF],
            [0xF1, 0x80, 0x80],
            [0xF4, 0x90, 0x80, 0x80],
            [0xF5, 0x80, 0x80, 0x80],
        ]
        for payload in invalidPayloads {
            var decoder = try StorageFrameDecoder(framed(payload))
            #expect(throws: StorageFrameError.invalidUTF8) {
                _ = try decoder.readValidatedStringBytes()
            }
        }
    }

    private func framed(_ payload: [UInt8]) -> ByteString {
        let count = UInt32(payload.count)
        return ByteString([
            UInt8(truncatingIfNeeded: count),
            UInt8(truncatingIfNeeded: count >> 8),
            UInt8(truncatingIfNeeded: count >> 16),
            UInt8(truncatingIfNeeded: count >> 24),
        ] + payload)
    }
}
