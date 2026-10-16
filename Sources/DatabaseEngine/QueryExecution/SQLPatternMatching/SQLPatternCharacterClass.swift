struct SQLPatternCharacterClass: Sendable {
    enum Member: Sendable {
        case range(UInt32, UInt32)
        case named(Name)
    }
    enum Name: String, Sendable {
        case alnum, alpha, blank, cntrl, digit, graph, lower, print, punct, space, upper, xdigit
        func contains(_ x: UInt32) -> Bool {
            let alpha = (65...90).contains(x) || (97...122).contains(x)
            let digit = (48...57).contains(x)
            switch self {
            case .alnum: return alpha || digit
            case .alpha: return alpha
            case .blank: return x == 9 || x == 32
            case .cntrl: return x < 32 || x == 127
            case .digit: return digit
            case .graph: return (33...126).contains(x)
            case .lower: return (97...122).contains(x)
            case .print: return (32...126).contains(x)
            case .punct: return (33...126).contains(x) && !alpha && !digit
            case .space: return (9...13).contains(x) || x == 32
            case .upper: return (65...90).contains(x)
            case .xdigit: return digit || (65...70).contains(x) || (97...102).contains(x)
            }
        }
    }
    let members: Range<Int>
    let negative: Bool
    let excludesNewline: Bool
    let insensitive: Bool
}
