import DatabaseTypes

/// RFC 4648 base64url without padding.
internal enum Base64URLFormat {
    private static let digits: [UInt8] = Array(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".utf8
    )

    static func encode(_ bytes: ByteString) -> String {
        guard !bytes.isEmpty else {
            return ""
        }
        let completeQuartetCount = bytes.count / 3
        let remainder = bytes.count % 3
        let outputCount = completeQuartetCount * 4
            + (remainder == 0 ? 0 : remainder + 1)
        return String(
            unsafeUninitializedCapacity: outputCount
        ) { output in
            var inputOffset = 0
            var outputOffset = 0
            while inputOffset < bytes.count {
                let hasSecond = inputOffset + 1 < bytes.count
                let hasThird = inputOffset + 2 < bytes.count
                let first = UInt32(bytes[inputOffset])
                let second = hasSecond ? UInt32(bytes[inputOffset + 1]) : 0
                let third = hasThird ? UInt32(bytes[inputOffset + 2]) : 0
                let value = (first << 16) | (second << 8) | third
                output[outputOffset] = digits[Int((value >> 18) & 0x3f)]
                output[outputOffset + 1] =
                    digits[Int((value >> 12) & 0x3f)]
                if hasSecond {
                    output[outputOffset + 2] =
                        digits[Int((value >> 6) & 0x3f)]
                }
                if hasThird {
                    output[outputOffset + 3] = digits[Int(value & 0x3f)]
                }
                inputOffset += 3
                outputOffset += hasThird ? 4 : (hasSecond ? 3 : 2)
            }
            return outputCount
        }
    }

    static func decode(
        _ string: String,
        maximumDecodedByteCount: Int
    ) throws -> ByteString {
        let inputCount = string.utf8.count
        guard inputCount % 4 != 1 else {
            throw DatabaseBase64Error.invalidLength
        }
        guard inputCount > 0 else {
            return ByteString()
        }
        let outputCount = (inputCount / 4) * 3
            + max(0, inputCount % 4 - 1)
        guard outputCount <= maximumDecodedByteCount else {
            throw DatabaseBase64Error.decodedValueTooLarge
        }

        var finalSextet: UInt8 = 0
        for byte in string.utf8 {
            guard let value = sextet(for: byte) else {
                throw DatabaseBase64Error.invalidCharacter
            }
            finalSextet = value
        }
        if inputCount % 4 == 2, finalSextet & 0x0f != 0 {
            throw DatabaseBase64Error.invalidPadding
        }
        if inputCount % 4 == 3, finalSextet & 0x03 != 0 {
            throw DatabaseBase64Error.invalidPadding
        }

        return ByteString.copying(count: outputCount) { output in
            var sextets: (UInt8, UInt8, UInt8, UInt8) = (0, 0, 0, 0)
            var inputIndex = 0
            var outputIndex = 0
            for byte in string.utf8 {
                guard let value = sextet(for: byte) else {
                    preconditionFailure(
                        "Validated base64url input became invalid"
                    )
                }
                switch inputIndex & 3 {
                case 0:
                    sextets.0 = value
                case 1:
                    sextets.1 = value
                case 2:
                    sextets.2 = value
                case 3:
                    sextets.3 = value
                    writeDecodedBytes(
                        sextets,
                        count: 3,
                        to: output,
                        at: &outputIndex
                    )
                    sextets = (0, 0, 0, 0)
                default:
                    preconditionFailure("Base64 quartet index is invalid")
                }
                inputIndex += 1
            }
            let remainder = inputCount % 4
            if remainder > 0 {
                writeDecodedBytes(
                    sextets,
                    count: remainder - 1,
                    to: output,
                    at: &outputIndex
                )
            }
        }
    }

    private static func writeDecodedBytes(
        _ sextets: (UInt8, UInt8, UInt8, UInt8),
        count: Int,
        to output: UnsafeMutableRawBufferPointer,
        at outputIndex: inout Int
    ) {
        let combined = (UInt32(sextets.0) << 18)
            | (UInt32(sextets.1) << 12)
            | (UInt32(sextets.2) << 6)
            | UInt32(sextets.3)
        if count >= 1 {
            output[outputIndex] = UInt8(combined >> 16)
            outputIndex += 1
        }
        if count >= 2 {
            output[outputIndex] = UInt8(truncatingIfNeeded: combined >> 8)
            outputIndex += 1
        }
        if count >= 3 {
            output[outputIndex] = UInt8(truncatingIfNeeded: combined)
            outputIndex += 1
        }
    }

    private static func sextet(for byte: UInt8) -> UInt8? {
        switch byte {
        case 0x41...0x5a:
            return byte - 0x41
        case 0x61...0x7a:
            return byte - 0x61 + 26
        case 0x30...0x39:
            return byte - 0x30 + 52
        case 0x2d:
            return 62
        case 0x5f:
            return 63
        default:
            return nil
        }
    }
}
