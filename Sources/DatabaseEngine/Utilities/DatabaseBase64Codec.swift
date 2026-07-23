import StorageKit

internal enum DatabaseBase64Codec {
    private static let digits: [UInt8] = Array(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".utf8
    )

    static func encode(_ bytes: Bytes) -> String {
        guard !bytes.isEmpty else {
            return ""
        }
        let outputCount = ((bytes.count + 2) / 3) * 4
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
                output[outputOffset + 1] = digits[Int((value >> 12) & 0x3f)]
                output[outputOffset + 2] = hasSecond
                    ? digits[Int((value >> 6) & 0x3f)]
                    : 0x3d
                output[outputOffset + 3] = hasThird
                    ? digits[Int(value & 0x3f)]
                    : 0x3d
                inputOffset += 3
                outputOffset += 4
            }
            return outputCount
        }
    }

    static func decode(_ string: String) throws -> Bytes {
        let inputCount = string.utf8.count
        guard inputCount % 4 == 0 else {
            throw DatabaseBase64Error.invalidLength
        }
        guard inputCount > 0 else {
            return Bytes()
        }

        var paddingCount = 0
        var encounteredPadding = false
        var quartetIndex = 0
        var secondSextet: UInt8 = 0
        var thirdSextet: UInt8 = 0
        for byte in string.utf8 {
            if byte == 0x3d {
                encounteredPadding = true
                paddingCount += 1
                guard paddingCount <= 2, quartetIndex >= 2 else {
                    throw DatabaseBase64Error.invalidPadding
                }
            } else {
                guard !encounteredPadding,
                      let value = sextet(for: byte) else {
                    throw encounteredPadding
                        ? DatabaseBase64Error.invalidPadding
                        : DatabaseBase64Error.invalidCharacter
                }
                if quartetIndex == 1 {
                    secondSextet = value
                } else if quartetIndex == 2 {
                    thirdSextet = value
                }
            }
            quartetIndex = (quartetIndex + 1) % 4
        }
        guard quartetIndex == 0 else {
            throw DatabaseBase64Error.invalidLength
        }
        if paddingCount == 2, secondSextet & 0x0f != 0 {
            throw DatabaseBase64Error.invalidPadding
        }
        if paddingCount == 1, thirdSextet & 0x03 != 0 {
            throw DatabaseBase64Error.invalidPadding
        }

        let outputCount = (inputCount / 4) * 3 - paddingCount
        return Bytes.copying(count: outputCount) { output in
            var sextets: (UInt8, UInt8, UInt8, UInt8) = (0, 0, 0, 0)
            var inputIndex = 0
            var outputIndex = 0
            for byte in string.utf8 {
                let value = sextet(for: byte) ?? 0
                switch inputIndex & 3 {
                case 0: sextets.0 = value
                case 1: sextets.1 = value
                case 2: sextets.2 = value
                default:
                    sextets.3 = value
                    let combined = (UInt32(sextets.0) << 18)
                        | (UInt32(sextets.1) << 12)
                        | (UInt32(sextets.2) << 6)
                        | UInt32(sextets.3)
                    if outputIndex < outputCount {
                        output[outputIndex] = UInt8(combined >> 16)
                        outputIndex += 1
                    }
                    if outputIndex < outputCount {
                        output[outputIndex] = UInt8(
                            truncatingIfNeeded: combined >> 8
                        )
                        outputIndex += 1
                    }
                    if outputIndex < outputCount {
                        output[outputIndex] = UInt8(
                            truncatingIfNeeded: combined
                        )
                        outputIndex += 1
                    }
                }
                inputIndex += 1
            }
        }
    }

    private static func sextet(for byte: UInt8) -> UInt8? {
        switch byte {
        case 0x41...0x5a: return byte - 0x41
        case 0x61...0x7a: return byte - 0x61 + 26
        case 0x30...0x39: return byte - 0x30 + 52
        case 0x2b: return 62
        case 0x2f: return 63
        default: return nil
        }
    }
}
