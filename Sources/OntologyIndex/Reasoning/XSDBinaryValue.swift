/// Validated zero-copy view of an XSD binary lexical representation.
///
/// Decoding is streamed from the owned source string. No decoded payload buffer
/// is created for length facets or value identity comparisons.
package struct XSDBinaryValue: Sendable {
    package enum Kind: Sendable, Equatable {
        case hexadecimal
        case base64
    }

    let source: String
    let kind: Kind
    let octetCount: Int

    init?(hexadecimal source: String) {
        var nibbleCount = 0
        for byte in source.utf8 {
            guard Self.hexValue(byte) != nil else { return nil }
            nibbleCount += 1
        }
        guard nibbleCount.isMultiple(of: 2) else { return nil }
        self.source = source
        self.kind = .hexadecimal
        self.octetCount = nibbleCount / 2
    }

    init?(base64 source: String) {
        var significantCount = 0
        var paddingCount = 0
        var lastDataValue: UInt8?
        var encounteredPadding = false

        for byte in source.utf8 {
            if byte == 0x20 { continue }
            significantCount += 1
            if byte == 0x3D {
                encounteredPadding = true
                paddingCount += 1
                guard paddingCount <= 2 else { return nil }
                continue
            }
            guard !encounteredPadding,
                  let value = Self.base64Value(byte) else {
                return nil
            }
            lastDataValue = value
        }

        guard significantCount.isMultiple(of: 4) else { return nil }
        if paddingCount == 2 {
            guard let value = lastDataValue, value & 0x0F == 0 else {
                return nil
            }
        } else if paddingCount == 1 {
            guard let value = lastDataValue, value & 0x03 == 0 else {
                return nil
            }
        }

        let quads = significantCount / 4
        self.source = source
        self.kind = .base64
        self.octetCount = quads * 3 - paddingCount
    }

    func isIdentical(to other: XSDBinaryValue) -> Bool {
        guard kind == other.kind, octetCount == other.octetCount else {
            return false
        }
        var lhs = OctetIterator(value: self)
        var rhs = OctetIterator(value: other)
        while let lhsOctet = lhs.next() {
            guard rhs.next() == lhsOctet else { return false }
        }
        return rhs.next() == nil
    }

    private static func hexValue(_ byte: UInt8) -> UInt8? {
        switch byte {
        case 48...57: byte - 48
        case 65...70: byte - 55
        case 97...102: byte - 87
        default: nil
        }
    }

    private static func base64Value(_ byte: UInt8) -> UInt8? {
        switch byte {
        case 65...90: byte - 65
        case 97...122: byte - 71
        case 48...57: byte + 4
        case 43: 62
        case 47: 63
        default: nil
        }
    }

    private struct OctetIterator {
        private let kind: Kind
        private let bytes: String.UTF8View
        private var index: String.UTF8View.Index
        private var bufferedBits: UInt32 = 0
        private var bufferedBitCount = 0

        init(value: XSDBinaryValue) {
            self.kind = value.kind
            self.bytes = value.source.utf8
            self.index = bytes.startIndex
        }

        mutating func next() -> UInt8? {
            while bufferedBitCount < 8 {
                guard let unit = nextUnit() else { return nil }
                let width = kind == .hexadecimal ? 4 : 6
                bufferedBits = (bufferedBits << width) | UInt32(unit)
                bufferedBitCount += width
            }
            let shift = bufferedBitCount - 8
            let octet = UInt8((bufferedBits >> shift) & 0xFF)
            bufferedBitCount -= 8
            if bufferedBitCount == 0 {
                bufferedBits = 0
            } else {
                bufferedBits &= (1 << bufferedBitCount) - 1
            }
            return octet
        }

        private mutating func nextUnit() -> UInt8? {
            while index != bytes.endIndex {
                let byte = bytes[index]
                bytes.formIndex(after: &index)
                if kind == .base64, byte == 0x20 { continue }
                if byte == 0x3D { return nil }
                switch kind {
                case .hexadecimal:
                    return XSDBinaryValue.hexValue(byte)
                case .base64:
                    return XSDBinaryValue.base64Value(byte)
                }
            }
            return nil
        }
    }
}
