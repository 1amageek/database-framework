import OntologyIndex

/// An immutable Unicode-scalar predicate used by the SPARQL Thompson NFA.
struct SPARQLRegexCharacterClass: Sendable {
    typealias UnicodeCategory = XSDRegexUnicodeCategory
    typealias UnicodeBlock = XSDRegexUnicodeBlock

    private indirect enum Expression: Sendable {
        case literal(UInt32)
        case range(UInt32, UInt32)
        case union([Expression])
        case subtraction(Expression, Expression)
        case complement(Expression)
        case caseInsensitive(Expression)
        case category(UnicodeCategory)
        case block(UnicodeBlock)
        case nameStart
        case nameCharacter
        case whitespace
        case word
        case decimalDigit
        case wildcard(matchesLineSeparators: Bool)
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
        var expressions: [Expression] = []
        expressions.reserveCapacity(members.count)
        for member in members {
            work = saturatedAdd(work, member.matchWork)
            expressions.append(member.expression)
        }
        return Self(expression: .union(expressions), matchWork: work)
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

    static func caseInsensitive(_ member: Self) -> Self {
        Self(
            expression: .caseInsensitive(member.expression),
            matchWork: saturatedAdd(4, saturatedMultiply(member.matchWork, 4))
        )
    }

    static func category(_ category: UnicodeCategory) -> Self {
        Self(expression: .category(category), matchWork: 1)
    }

    static func block(_ block: UnicodeBlock) -> Self {
        Self(expression: .block(block), matchWork: 1)
    }

    static let nameStart = Self(expression: .nameStart, matchWork: 1)
    static let nameCharacter = Self(expression: .nameCharacter, matchWork: 1)
    static let whitespace = Self(expression: .whitespace, matchWork: 1)
    static let word = Self(expression: .word, matchWork: 1)
    static let decimalDigit = Self(expression: .decimalDigit, matchWork: 1)

    static func wildcard(matchesLineSeparators: Bool) -> Self {
        Self(
            expression: .wildcard(
                matchesLineSeparators: matchesLineSeparators
            ),
            matchWork: 1
        )
    }

    func contains(_ scalar: Unicode.Scalar) -> Bool {
        Self.evaluate(expression, scalar: scalar)
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
        case .caseInsensitive(let member):
            return matchesCaseInsensitive(member, scalar: scalar)
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
            return isXPathWord(scalar)
        case .decimalDigit:
            return scalar.properties.generalCategory == .decimalNumber
        case .wildcard(let matchesLineSeparators):
            return matchesLineSeparators || !isLineSeparator(scalar.value)
        }
    }

    private static func matchesCaseInsensitive(
        _ expression: Expression,
        scalar: Unicode.Scalar
    ) -> Bool {
        if evaluate(expression, scalar: scalar) {
            return true
        }

        if scalar.value >= 0x41 && scalar.value <= 0x5A,
           let lowercase = Unicode.Scalar(scalar.value + 0x20) {
            return evaluate(expression, scalar: lowercase)
        }
        if scalar.value >= 0x61 && scalar.value <= 0x7A,
           let uppercase = Unicode.Scalar(scalar.value - 0x20) {
            return evaluate(expression, scalar: uppercase)
        }

        // Swift exposes non-ASCII case mappings as bounded small String
        // values. Materializing those mappings is required for Unicode
        // correctness; the potentially large input itself remains a view.
        return matchesSingleScalarMapping(
            scalar.properties.lowercaseMapping,
            expression: expression,
            excluding: scalar
        ) || matchesSingleScalarMapping(
            scalar.properties.uppercaseMapping,
            expression: expression,
            excluding: scalar
        ) || matchesSingleScalarMapping(
            scalar.properties.titlecaseMapping,
            expression: expression,
            excluding: scalar
        )
    }

    private static func matchesSingleScalarMapping(
        _ mapping: String,
        expression: Expression,
        excluding original: Unicode.Scalar
    ) -> Bool {
        var iterator = mapping.unicodeScalars.makeIterator()
        guard let mapped = iterator.next(),
              iterator.next() == nil,
              mapped != original else {
            return false
        }
        return evaluate(expression, scalar: mapped)
    }

    static func isLineSeparator(_ value: UInt32) -> Bool {
        switch value {
        case 0x0A, 0x0D, 0x2028, 0x2029:
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

    private static func isXPathWord(_ scalar: Unicode.Scalar) -> Bool {
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

    private static func saturatedMultiply(_ lhs: Int, _ rhs: Int) -> Int {
        let (result, overflow) = lhs.multipliedReportingOverflow(by: rhs)
        return overflow ? Int.max : result
    }
}
