/// Unicode general categories recognized by XSD 1.1 regular expressions.
///
/// XSD permits the one-letter aggregate categories and the two-letter Unicode
/// general categories listed in Appendix G. Swift's scalar properties provide
/// the Unicode database without requiring Foundation or ICU APIs.
package enum XSDRegexUnicodeCategory: Sendable {
    case letters
    case uppercaseLetter
    case lowercaseLetter
    case titlecaseLetter
    case modifierLetter
    case otherLetter
    case marks
    case nonspacingMark
    case spacingCombiningMark
    case enclosingMark
    case numbers
    case decimalDigitNumber
    case letterNumber
    case otherNumber
    case punctuation
    case connectorPunctuation
    case dashPunctuation
    case openPunctuation
    case closePunctuation
    case initialQuotePunctuation
    case finalQuotePunctuation
    case otherPunctuation
    case separators
    case spaceSeparator
    case lineSeparator
    case paragraphSeparator
    case symbols
    case mathSymbol
    case currencySymbol
    case modifierSymbol
    case otherSymbol
    case others
    case control
    case format
    case privateUse
    case unassigned

    package init?(name: Substring) {
        switch name {
        case "L": self = .letters
        case "Lu": self = .uppercaseLetter
        case "Ll": self = .lowercaseLetter
        case "Lt": self = .titlecaseLetter
        case "Lm": self = .modifierLetter
        case "Lo": self = .otherLetter
        case "M": self = .marks
        case "Mn": self = .nonspacingMark
        case "Mc": self = .spacingCombiningMark
        case "Me": self = .enclosingMark
        case "N": self = .numbers
        case "Nd": self = .decimalDigitNumber
        case "Nl": self = .letterNumber
        case "No": self = .otherNumber
        case "P": self = .punctuation
        case "Pc": self = .connectorPunctuation
        case "Pd": self = .dashPunctuation
        case "Ps": self = .openPunctuation
        case "Pe": self = .closePunctuation
        case "Pi": self = .initialQuotePunctuation
        case "Pf": self = .finalQuotePunctuation
        case "Po": self = .otherPunctuation
        case "Z": self = .separators
        case "Zs": self = .spaceSeparator
        case "Zl": self = .lineSeparator
        case "Zp": self = .paragraphSeparator
        case "S": self = .symbols
        case "Sm": self = .mathSymbol
        case "Sc": self = .currencySymbol
        case "Sk": self = .modifierSymbol
        case "So": self = .otherSymbol
        case "C": self = .others
        case "Cc": self = .control
        case "Cf": self = .format
        case "Co": self = .privateUse
        case "Cn": self = .unassigned
        default: return nil
        }
    }

    package func contains(_ scalar: Unicode.Scalar) -> Bool {
        let category = scalar.properties.generalCategory
        switch self {
        case .letters:
            return Self.isLetter(category)
        case .uppercaseLetter:
            return category == .uppercaseLetter
        case .lowercaseLetter:
            return category == .lowercaseLetter
        case .titlecaseLetter:
            return category == .titlecaseLetter
        case .modifierLetter:
            return category == .modifierLetter
        case .otherLetter:
            return category == .otherLetter
        case .marks:
            return Self.isMark(category)
        case .nonspacingMark:
            return category == .nonspacingMark
        case .spacingCombiningMark:
            return category == .spacingMark
        case .enclosingMark:
            return category == .enclosingMark
        case .numbers:
            return Self.isNumber(category)
        case .decimalDigitNumber:
            return category == .decimalNumber
        case .letterNumber:
            return category == .letterNumber
        case .otherNumber:
            return category == .otherNumber
        case .punctuation:
            return Self.isPunctuation(category)
        case .connectorPunctuation:
            return category == .connectorPunctuation
        case .dashPunctuation:
            return category == .dashPunctuation
        case .openPunctuation:
            return category == .openPunctuation
        case .closePunctuation:
            return category == .closePunctuation
        case .initialQuotePunctuation:
            return category == .initialPunctuation
        case .finalQuotePunctuation:
            return category == .finalPunctuation
        case .otherPunctuation:
            return category == .otherPunctuation
        case .separators:
            return Self.isSeparator(category)
        case .spaceSeparator:
            return category == .spaceSeparator
        case .lineSeparator:
            return category == .lineSeparator
        case .paragraphSeparator:
            return category == .paragraphSeparator
        case .symbols:
            return Self.isSymbol(category)
        case .mathSymbol:
            return category == .mathSymbol
        case .currencySymbol:
            return category == .currencySymbol
        case .modifierSymbol:
            return category == .modifierSymbol
        case .otherSymbol:
            return category == .otherSymbol
        case .others:
            return Self.isOther(category)
        case .control:
            return category == .control
        case .format:
            return category == .format
        case .privateUse:
            return category == .privateUse
        case .unassigned:
            return category == .unassigned
        }
    }

    private static func isLetter(_ category: Unicode.GeneralCategory) -> Bool {
        switch category {
        case .uppercaseLetter, .lowercaseLetter, .titlecaseLetter,
             .modifierLetter, .otherLetter:
            return true
        default:
            return false
        }
    }

    private static func isMark(_ category: Unicode.GeneralCategory) -> Bool {
        switch category {
        case .nonspacingMark, .spacingMark, .enclosingMark:
            return true
        default:
            return false
        }
    }

    private static func isNumber(_ category: Unicode.GeneralCategory) -> Bool {
        switch category {
        case .decimalNumber, .letterNumber, .otherNumber:
            return true
        default:
            return false
        }
    }

    private static func isPunctuation(
        _ category: Unicode.GeneralCategory
    ) -> Bool {
        switch category {
        case .connectorPunctuation, .dashPunctuation, .openPunctuation,
             .closePunctuation, .initialPunctuation, .finalPunctuation,
             .otherPunctuation:
            return true
        default:
            return false
        }
    }

    private static func isSeparator(
        _ category: Unicode.GeneralCategory
    ) -> Bool {
        switch category {
        case .spaceSeparator, .lineSeparator, .paragraphSeparator:
            return true
        default:
            return false
        }
    }

    private static func isSymbol(_ category: Unicode.GeneralCategory) -> Bool {
        switch category {
        case .mathSymbol, .currencySymbol, .modifierSymbol, .otherSymbol:
            return true
        default:
            return false
        }
    }

    private static func isOther(_ category: Unicode.GeneralCategory) -> Bool {
        // Unicode scalar values cannot represent the Cs surrogate category.
        switch category {
        case .control, .format, .privateUse, .unassigned:
            return true
        default:
            return false
        }
    }
}
