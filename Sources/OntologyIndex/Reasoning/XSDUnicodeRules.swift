/// XML 1.0 Fifth Edition character and name predicates used by XSD datatypes.
enum XSDUnicodeRules {
    static func isXMLCharacter(_ scalar: Unicode.Scalar) -> Bool {
        switch scalar.value {
        case 0x9, 0xA, 0xD,
             0x20...0xD7FF,
             0xE000...0xFFFD,
             0x10000...0x10FFFF:
            true
        default:
            false
        }
    }

    static func isNameStart(_ scalar: Unicode.Scalar) -> Bool {
        switch scalar.value {
        case 0x3A, 0x41...0x5A, 0x5F, 0x61...0x7A,
             0xC0...0xD6, 0xD8...0xF6, 0xF8...0x2FF,
             0x370...0x37D, 0x37F...0x1FFF,
             0x200C...0x200D, 0x2070...0x218F,
             0x2C00...0x2FEF, 0x3001...0xD7FF,
             0xF900...0xFDCF, 0xFDF0...0xFFFD,
             0x10000...0xEFFFF:
            true
        default:
            false
        }
    }

    static func isNameCharacter(_ scalar: Unicode.Scalar) -> Bool {
        if isNameStart(scalar) { return true }
        switch scalar.value {
        case 0x2D, 0x2E, 0x30...0x39, 0xB7,
             0x300...0x36F, 0x203F...0x2040:
            return true
        default:
            return false
        }
    }

    static func allXMLCharacters<Source: StringProtocol>(
        _ source: Source
    ) -> Bool {
        for scalar in source.unicodeScalars {
            guard isXMLCharacter(scalar) else { return false }
        }
        return true
    }

    static func isNormalizedString<Source: StringProtocol>(
        _ source: Source
    ) -> Bool {
        for scalar in source.unicodeScalars {
            guard isXMLCharacter(scalar) else { return false }
            switch scalar.value {
            case 0x9, 0xA, 0xD:
                return false
            default:
                continue
            }
        }
        return true
    }

    static func isToken<Source: StringProtocol>(_ source: Source) -> Bool {
        var previousWasSpace = false
        var isFirst = true
        for scalar in source.unicodeScalars {
            guard isXMLCharacter(scalar) else { return false }
            switch scalar.value {
            case 0x9, 0xA, 0xD:
                return false
            case 0x20:
                if isFirst || previousWasSpace { return false }
                previousWasSpace = true
            default:
                previousWasSpace = false
            }
            isFirst = false
        }
        return !previousWasSpace
    }

    static func isLanguage<Source: StringProtocol>(_ source: Source) -> Bool {
        let bytes = source.utf8
        var index = bytes.startIndex
        var segmentLength = 0
        var isFirstSegment = true
        while index != bytes.endIndex {
            let byte = bytes[index]
            if byte == 45 {
                guard segmentLength >= 1, segmentLength <= 8 else { return false }
                isFirstSegment = false
                segmentLength = 0
            } else if isASCIIAlpha(byte)
                        || (!isFirstSegment && isASCIIDigit(byte)) {
                segmentLength += 1
                guard segmentLength <= 8 else { return false }
            } else {
                return false
            }
            bytes.formIndex(after: &index)
        }
        return segmentLength >= 1 && segmentLength <= 8
    }

    static func isNMTOKEN<Source: StringProtocol>(_ source: Source) -> Bool {
        guard !source.isEmpty else { return false }
        for scalar in source.unicodeScalars {
            guard isNameCharacter(scalar) else { return false }
        }
        return true
    }

    static func isName<Source: StringProtocol>(
        _ source: Source,
        allowsColon: Bool
    ) -> Bool {
        var iterator = source.unicodeScalars.makeIterator()
        guard let first = iterator.next(),
              isNameStart(first),
              allowsColon || first.value != 0x3A else {
            return false
        }
        while let scalar = iterator.next() {
            guard isNameCharacter(scalar),
                  allowsColon || scalar.value != 0x3A else {
                return false
            }
        }
        return true
    }

    private static func isASCIIAlpha(_ byte: UInt8) -> Bool {
        (byte >= 65 && byte <= 90) || (byte >= 97 && byte <= 122)
    }

    private static func isASCIIDigit(_ byte: UInt8) -> Bool {
        byte >= 48 && byte <= 57
    }
}
