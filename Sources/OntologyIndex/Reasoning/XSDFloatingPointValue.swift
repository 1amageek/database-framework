/// Parsed XSD float/double value after strict ASCII lexical validation.
package enum XSDFloatingPointValue: Sendable {
    case finite(Double)
    case positiveInfinity
    case negativeInfinity
    case nan

    package init?(lexicalForm: String, isFloat: Bool) {
        switch lexicalForm {
        case "INF", "+INF":
            self = .positiveInfinity
            return
        case "-INF":
            self = .negativeInfinity
            return
        case "NaN":
            self = .nan
            return
        default:
            break
        }

        guard Self.isFiniteLexicalForm(lexicalForm) else { return nil }
        if isFloat {
            guard let value = Float(lexicalForm) else { return nil }
            if value == .infinity {
                self = .positiveInfinity
            } else if value == -.infinity {
                self = .negativeInfinity
            } else {
                self = .finite(Double(value))
            }
        } else {
            guard let value = Double(lexicalForm) else { return nil }
            if value == .infinity {
                self = .positiveInfinity
            } else if value == -.infinity {
                self = .negativeInfinity
            } else {
                self = .finite(value)
            }
        }
    }

    package var doubleValue: Double {
        switch self {
        case .finite(let value): value
        case .positiveInfinity: .infinity
        case .negativeInfinity: -.infinity
        case .nan: .nan
        }
    }

    private static func isFiniteLexicalForm(_ source: String) -> Bool {
        let bytes = source.utf8
        var index = bytes.startIndex
        if index != bytes.endIndex,
           bytes[index] == 43 || bytes[index] == 45 {
            bytes.formIndex(after: &index)
        }

        var significandDigitCount = 0
        var hasDecimalPoint = false
        while index != bytes.endIndex {
            let byte = bytes[index]
            if byte >= 48, byte <= 57 {
                significandDigitCount += 1
                bytes.formIndex(after: &index)
                continue
            }
            if byte == 46, !hasDecimalPoint {
                hasDecimalPoint = true
                bytes.formIndex(after: &index)
                continue
            }
            break
        }
        guard significandDigitCount > 0 else { return false }
        guard index != bytes.endIndex else { return true }
        guard bytes[index] == 69 || bytes[index] == 101 else { return false }
        bytes.formIndex(after: &index)
        if index != bytes.endIndex,
           bytes[index] == 43 || bytes[index] == 45 {
            bytes.formIndex(after: &index)
        }
        var exponentDigitCount = 0
        while index != bytes.endIndex {
            let byte = bytes[index]
            guard byte >= 48, byte <= 57 else { return false }
            exponentDigitCount += 1
            bytes.formIndex(after: &index)
        }
        return exponentDigitCount > 0
    }
}
