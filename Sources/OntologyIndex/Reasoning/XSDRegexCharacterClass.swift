/// An immutable scalar predicate used by the XSD Thompson NFA.
struct XSDRegexCharacterClass: Sendable {
    private indirect enum Expression: Sendable {
        case literal(UInt32)
        case range(UInt32, UInt32)
        case union([Expression])
        case subtraction(Expression, Expression)
        case complement(Expression)
        case category(XSDRegexUnicodeCategory)
        case block(XSDRegexUnicodeBlock)
        case nameStart
        case nameCharacter
        case whitespace
        case word
        case wildcard
    }

    private let expression: Expression

    /// Conservative predicate work charged for every attempted NFA transition.
    let matchWork: Int

    private init(expression: Expression, matchWork: Int) {
        self.expression = expression
        self.matchWork = matchWork
    }

    static func literal(_ scalar: Unicode.Scalar) -> Self {
        Self(expression: .literal(scalar.value), matchWork: 1)
    }

    static func range(
        from lowerBound: Unicode.Scalar,
        through upperBound: Unicode.Scalar
    ) -> Self {
        Self(
            expression: .range(lowerBound.value, upperBound.value),
            matchWork: 1
        )
    }

    static func union(_ members: [Self]) -> Self {
        var work = 1
        for member in members {
            work = saturatedAdd(work, member.matchWork)
        }
        return Self(
            expression: .union(members.map { $0.expression }),
            matchWork: work
        )
    }

    static func subtracting(_ excluded: Self, from included: Self) -> Self {
        Self(
            expression: .subtraction(included.expression, excluded.expression),
            matchWork: saturatedAdd(
                saturatedAdd(1, included.matchWork),
                excluded.matchWork
            )
        )
    }

    static func complement(of member: Self) -> Self {
        Self(
            expression: .complement(member.expression),
            matchWork: saturatedAdd(1, member.matchWork)
        )
    }

    static func category(_ category: XSDRegexUnicodeCategory) -> Self {
        Self(expression: .category(category), matchWork: 1)
    }

    static func block(_ block: XSDRegexUnicodeBlock) -> Self {
        Self(expression: .block(block), matchWork: 1)
    }

    static let nameStart = Self(expression: .nameStart, matchWork: 1)
    static let nameCharacter = Self(expression: .nameCharacter, matchWork: 1)
    static let whitespace = Self(expression: .whitespace, matchWork: 1)
    static let word = Self(expression: .word, matchWork: 1)
    static let wildcard = Self(expression: .wildcard, matchWork: 1)

    func contains(_ scalar: Unicode.Scalar) -> Bool {
        guard Self.isXMLCharacter(scalar.value) else {
            return false
        }
        return Self.evaluate(expression, scalar: scalar)
    }

    private static func evaluate(
        _ expression: Expression,
        scalar: Unicode.Scalar
    ) -> Bool {
        switch expression {
        case .literal(let value):
            return scalar.value == value
        case .range(let lowerBound, let upperBound):
            return scalar.value >= lowerBound && scalar.value <= upperBound
        case .union(let members):
            for member in members where evaluate(member, scalar: scalar) {
                return true
            }
            return false
        case .subtraction(let included, let excluded):
            return evaluate(included, scalar: scalar)
                && !evaluate(excluded, scalar: scalar)
        case .complement(let member):
            return !evaluate(member, scalar: scalar)
        case .category(let category):
            return category.contains(scalar)
        case .block(let block):
            return block.contains(scalar)
        case .nameStart:
            return isXMLNameStart(scalar.value)
        case .nameCharacter:
            return isXMLNameCharacter(scalar.value)
        case .whitespace:
            switch scalar.value {
            case 0x20, 0x09, 0x0A, 0x0D:
                return true
            default:
                return false
            }
        case .word:
            return isXSDWord(scalar)
        case .wildcard:
            return scalar.value != 0x0A && scalar.value != 0x0D
        }
    }

    static func isXMLCharacter(_ value: UInt32) -> Bool {
        switch value {
        case 0x09, 0x0A, 0x0D,
             0x20...0xD7FF,
             0xE000...0xFFFD,
             0x10000...0x10FFFF:
            return true
        default:
            return false
        }
    }

    private static func isXMLNameStart(_ value: UInt32) -> Bool {
        switch value {
        case 0x3A,
             0x41...0x5A,
             0x5F,
             0x61...0x7A,
             0xC0...0xD6,
             0xD8...0xF6,
             0xF8...0x2FF,
             0x370...0x37D,
             0x37F...0x1FFF,
             0x200C...0x200D,
             0x2070...0x218F,
             0x2C00...0x2FEF,
             0x3001...0xD7FF,
             0xF900...0xFDCF,
             0xFDF0...0xFFFD,
             0x10000...0xEFFFF:
            return true
        default:
            return false
        }
    }

    private static func isXMLNameCharacter(_ value: UInt32) -> Bool {
        if isXMLNameStart(value) {
            return true
        }
        switch value {
        case 0x2D,
             0x2E,
             0x30...0x39,
             0xB7,
             0x0300...0x036F,
             0x203F...0x2040:
            return true
        default:
            return false
        }
    }

    private static func isXSDWord(_ scalar: Unicode.Scalar) -> Bool {
        switch scalar.properties.generalCategory {
        case .connectorPunctuation, .dashPunctuation, .openPunctuation,
             .closePunctuation, .initialPunctuation, .finalPunctuation,
             .otherPunctuation, .spaceSeparator, .lineSeparator,
             .paragraphSeparator, .control, .format, .privateUse,
             .unassigned:
            return false
        default:
            return true
        }
    }

    private static func saturatedAdd(_ lhs: Int, _ rhs: Int) -> Int {
        let (result, overflow) = lhs.addingReportingOverflow(rhs)
        return overflow ? Int.max : result
    }
}
