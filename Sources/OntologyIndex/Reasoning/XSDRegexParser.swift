/// Bounded Unicode-scalar parser for the XML Schema regex grammar.
struct XSDRegexParser {
    indirect enum Node: Sendable {
        case empty
        case atom(XSDRegexCharacterClass)
        case concatenation([Node])
        case alternation([Node])
        case repetition(Node, minimum: Int, maximum: Int?)

        var matchesOnlyEmpty: Bool {
            switch self {
            case .empty:
                return true
            case .atom:
                return false
            case .concatenation(let children):
                return children.allSatisfy(\.matchesOnlyEmpty)
            case .alternation(let branches):
                return branches.allSatisfy(\.matchesOnlyEmpty)
            case .repetition(let child, _, let maximum):
                return maximum == 0 || child.matchesOnlyEmpty
            }
        }
    }

    private struct ClassAtom {
        let characterClass: XSDRegexCharacterClass
        let rangeEndpoint: Unicode.Scalar?
        let isUnescapedHyphen: Bool
    }

    private let pattern: String
    private let limits: XSDRegularExpression.Limits
    private var index: String.Index
    private var scalarOffset = 0
    private var nodeCount = 0
    private var currentNestingDepth = 0

    init(pattern: String, limits: XSDRegularExpression.Limits) {
        self.pattern = pattern
        self.limits = limits
        index = pattern.unicodeScalars.startIndex
    }

    mutating func parse() throws -> Node {
        let expression = try parseExpression()
        guard currentScalar == nil else {
            throw syntax("unexpected metacharacter")
        }
        return expression
    }

    private mutating func parseExpression() throws -> Node {
        var branches = [try parseBranch()]
        while currentScalar?.value == 0x7C {
            advance()
            branches.append(try parseBranch())
        }
        var retainedEmptyBranch = false
        branches.removeAll { branch in
            guard branch.matchesOnlyEmpty else {
                return false
            }
            if retainedEmptyBranch {
                return true
            }
            retainedEmptyBranch = true
            return false
        }
        guard branches.count > 1 else {
            return branches[0]
        }
        return try makeNode(.alternation(branches))
    }

    private mutating func parseBranch() throws -> Node {
        var pieces: [Node] = []
        while let scalar = currentScalar,
              scalar.value != 0x7C,
              scalar.value != 0x29 {
            pieces.append(try parsePiece())
        }

        let firstPiece = pieces.first
        pieces.removeAll(where: \.matchesOnlyEmpty)
        switch pieces.count {
        case 0:
            return try firstPiece ?? makeNode(.empty)
        case 1:
            return pieces[0]
        default:
            return try makeNode(.concatenation(pieces))
        }
    }

    private mutating func parsePiece() throws -> Node {
        var node = try parseAtom()
        guard let scalar = currentScalar else {
            return node
        }

        switch scalar.value {
        case 0x3F:
            advance()
            node = try makeNode(.repetition(node, minimum: 0, maximum: 1))
        case 0x2A:
            advance()
            node = try makeNode(.repetition(node, minimum: 0, maximum: nil))
        case 0x2B:
            advance()
            node = try makeNode(.repetition(node, minimum: 1, maximum: nil))
        case 0x7B:
            let bounds = try parseQuantifier()
            node = try makeNode(
                .repetition(
                    node,
                    minimum: bounds.minimum,
                    maximum: bounds.maximum
                )
            )
        default:
            return node
        }

        if let next = currentScalar, Self.isQuantifierStart(next.value) {
            throw syntax("multiple quantifiers are not permitted")
        }
        return node.matchesOnlyEmpty ? .empty : node
    }

    private mutating func parseAtom() throws -> Node {
        guard let scalar = currentScalar else {
            throw syntax("expected an atom")
        }

        switch scalar.value {
        case 0x28:
            let openingOffset = scalarOffset
            advance()
            try pushNesting()
            defer { currentNestingDepth -= 1 }
            let expression = try parseExpression()
            guard currentScalar?.value == 0x29 else {
                throw syntax("unterminated group", at: openingOffset)
            }
            advance()
            return expression
        case 0x5B:
            return try makeNode(.atom(parseCharacterClass()))
        case 0x2E:
            advance()
            return try makeNode(.atom(.wildcard))
        case 0x5C:
            return try makeNode(.atom(parseEscape().characterClass))
        case 0x29, 0x7C, 0x3F, 0x2A, 0x2B, 0x7B, 0x7D, 0x5D:
            throw syntax("unexpected metacharacter")
        default:
            guard XSDRegexCharacterClass.isXMLCharacter(scalar.value) else {
                throw syntax("pattern contains a non-XML character")
            }
            advance()
            return try makeNode(.atom(.literal(scalar)))
        }
    }

    private mutating func parseQuantifier() throws -> (
        minimum: Int,
        maximum: Int?
    ) {
        let openingOffset = scalarOffset
        advance()
        let minimum = try parseQuantifierInteger()

        if currentScalar?.value == 0x7D {
            advance()
            return (minimum, minimum)
        }

        guard currentScalar?.value == 0x2C else {
            throw syntax("expected ',' or '}' in quantifier")
        }
        advance()
        if currentScalar?.value == 0x7D {
            advance()
            return (minimum, nil)
        }

        let maximum = try parseQuantifierInteger()
        guard currentScalar?.value == 0x7D else {
            throw syntax("unterminated quantifier", at: openingOffset)
        }
        guard minimum <= maximum else {
            throw syntax(
                "quantifier minimum exceeds maximum",
                at: openingOffset
            )
        }
        advance()
        return (minimum, maximum)
    }

    private mutating func parseQuantifierInteger() throws -> Int {
        guard let first = currentScalar, Self.isASCIIDigit(first.value) else {
            throw syntax("expected a decimal quantifier bound")
        }

        var value = 0
        while let scalar = currentScalar, Self.isASCIIDigit(scalar.value) {
            let digit = Int(scalar.value - 0x30)
            let (multiplied, multiplyOverflow) = value.multipliedReportingOverflow(
                by: 10
            )
            let (candidate, addOverflow) = multiplied.addingReportingOverflow(
                digit
            )
            if multiplyOverflow || addOverflow {
                throw XSDRegularExpression.Error.resourceLimit(
                    name: "quantifier",
                    limit: limits.quantifier,
                    actual: Int.max
                )
            }
            value = candidate
            advance()
        }

        guard value <= limits.quantifier else {
            throw XSDRegularExpression.Error.resourceLimit(
                name: "quantifier",
                limit: limits.quantifier,
                actual: value
            )
        }
        return value
    }

    private mutating func parseCharacterClass() throws
        -> XSDRegexCharacterClass
    {
        let openingOffset = scalarOffset
        advance()
        try pushNesting()
        defer { currentNestingDepth -= 1 }

        var isNegative = false
        if currentScalar?.value == 0x5E {
            isNegative = true
            advance()
        }

        var members: [XSDRegexCharacterClass] = []
        while let scalar = currentScalar {
            if scalar.value == 0x5D {
                guard !members.isEmpty else {
                    throw syntax("character class must not be empty")
                }
                advance()
                return finishCharacterClass(
                    members: members,
                    subtraction: nil,
                    isNegative: isNegative
                )
            }

            if scalar.value == 0x2D,
               peekNextScalar()?.value == 0x5B,
               !members.isEmpty {
                advance()
                let excluded = try parseCharacterClass()
                guard currentScalar?.value == 0x5D else {
                    throw syntax(
                        "character class subtraction must be the final member"
                    )
                }
                advance()
                return finishCharacterClass(
                    members: members,
                    subtraction: excluded,
                    isNegative: isNegative
                )
            }

            let lower = try parseClassAtom()
            if currentScalar?.value == 0x2D,
               lower.rangeEndpoint != nil,
               let following = peekNextScalar(),
               following.value != 0x5B,
               following.value != 0x5D,
               !(following.value == 0x2D
                    && peekScalar(aheadBy: 2)?.value == 0x5B) {
                let rangeOffset = scalarOffset
                advance()
                let upper = try parseClassAtom()
                guard let lowerScalar = lower.rangeEndpoint,
                      let upperScalar = upper.rangeEndpoint,
                      !lower.isUnescapedHyphen,
                      !upper.isUnescapedHyphen else {
                    throw syntax(
                        "range endpoints must be literal scalars",
                        at: rangeOffset
                    )
                }
                guard lowerScalar.value <= upperScalar.value else {
                    throw syntax("reversed character range", at: rangeOffset)
                }
                members.append(
                    .range(from: lowerScalar, through: upperScalar)
                )
            } else {
                members.append(lower.characterClass)
            }
        }

        throw syntax("unterminated character class", at: openingOffset)
    }

    private func finishCharacterClass(
        members: [XSDRegexCharacterClass],
        subtraction: XSDRegexCharacterClass?,
        isNegative: Bool
    ) -> XSDRegexCharacterClass {
        var result = XSDRegexCharacterClass.union(members)
        if isNegative {
            result = .complement(of: result)
        }
        if let subtraction {
            result = .subtracting(subtraction, from: result)
        }
        return result
    }

    private mutating func parseClassAtom() throws -> ClassAtom {
        guard let scalar = currentScalar else {
            throw syntax("expected a character-class member")
        }
        switch scalar.value {
        case 0x5C:
            return try parseEscape()
        case 0x5B, 0x5D:
            throw syntax("unescaped bracket in character class")
        default:
            guard XSDRegexCharacterClass.isXMLCharacter(scalar.value) else {
                throw syntax("pattern contains a non-XML character")
            }
            advance()
            return ClassAtom(
                characterClass: .literal(scalar),
                rangeEndpoint: scalar,
                isUnescapedHyphen: scalar.value == 0x2D
            )
        }
    }

    private mutating func parseEscape() throws -> ClassAtom {
        let slashOffset = scalarOffset
        advance()
        guard let escaped = currentScalar else {
            throw syntax("trailing escape", at: slashOffset)
        }
        advance()

        switch escaped.value {
        case 0x6E:
            return literalEscape("\n")
        case 0x72:
            return literalEscape("\r")
        case 0x74:
            return literalEscape("\t")
        case 0x5C, 0x7C, 0x2E, 0x2D, 0x5E, 0x3F, 0x2A, 0x2B,
             0x7B, 0x7D, 0x28, 0x29, 0x5B, 0x5D:
            return ClassAtom(
                characterClass: .literal(escaped),
                rangeEndpoint: escaped,
                isUnescapedHyphen: false
            )
        case 0x73:
            return classEscape(.whitespace)
        case 0x53:
            return classEscape(.complement(of: .whitespace))
        case 0x69:
            return classEscape(.nameStart)
        case 0x49:
            return classEscape(.complement(of: .nameStart))
        case 0x63:
            return classEscape(.nameCharacter)
        case 0x43:
            return classEscape(.complement(of: .nameCharacter))
        case 0x64:
            return classEscape(.category(.decimalDigitNumber))
        case 0x44:
            return classEscape(
                .complement(of: .category(.decimalDigitNumber))
            )
        case 0x77:
            return classEscape(.word)
        case 0x57:
            return classEscape(.complement(of: .word))
        case 0x70:
            return try parsePropertyEscape(isComplement: false, at: slashOffset)
        case 0x50:
            return try parsePropertyEscape(isComplement: true, at: slashOffset)
        default:
            throw syntax("invalid escape", at: slashOffset)
        }
    }

    private mutating func parsePropertyEscape(
        isComplement: Bool,
        at slashOffset: Int
    ) throws -> ClassAtom {
        guard currentScalar?.value == 0x7B else {
            throw syntax("expected '{' after property escape", at: slashOffset)
        }
        advance()
        let nameStart = index
        while let scalar = currentScalar, scalar.value != 0x7D {
            guard Self.isPropertyNameScalar(scalar.value) else {
                throw syntax("invalid Unicode property name")
            }
            advance()
        }
        let nameEnd = index
        guard nameStart != nameEnd else {
            throw syntax("Unicode property name must not be empty")
        }
        guard currentScalar?.value == 0x7D else {
            throw syntax("unterminated Unicode property escape", at: slashOffset)
        }
        advance()

        let name = pattern[nameStart..<nameEnd]
        let property: XSDRegexCharacterClass
        if let category = XSDRegexUnicodeCategory(name: name) {
            property = .category(category)
        } else if name.hasPrefix("Is") {
            let blockName = name.dropFirst(2)
            guard !blockName.isEmpty,
                  let block = XSDRegexUnicodeBlock(name: blockName) else {
                throw syntax("unknown Unicode block", at: slashOffset)
            }
            property = .block(block)
        } else {
            throw syntax("unknown Unicode category", at: slashOffset)
        }

        return classEscape(
            isComplement ? .complement(of: property) : property
        )
    }

    private func literalEscape(_ scalar: Unicode.Scalar) -> ClassAtom {
        return ClassAtom(
            characterClass: .literal(scalar),
            rangeEndpoint: scalar,
            isUnescapedHyphen: false
        )
    }

    private func classEscape(
        _ characterClass: XSDRegexCharacterClass
    ) -> ClassAtom {
        ClassAtom(
            characterClass: characterClass,
            rangeEndpoint: nil,
            isUnescapedHyphen: false
        )
    }

    private mutating func makeNode(_ node: Node) throws -> Node {
        let (actual, overflow) = nodeCount.addingReportingOverflow(1)
        guard !overflow, actual <= limits.astNodes else {
            throw XSDRegularExpression.Error.resourceLimit(
                name: "astNodes",
                limit: limits.astNodes,
                actual: overflow ? Int.max : actual
            )
        }
        nodeCount = actual
        return node
    }

    private mutating func pushNesting() throws {
        let (actual, overflow) = currentNestingDepth.addingReportingOverflow(1)
        guard !overflow, actual <= limits.nestingDepth else {
            throw XSDRegularExpression.Error.resourceLimit(
                name: "nestingDepth",
                limit: limits.nestingDepth,
                actual: overflow ? Int.max : actual
            )
        }
        currentNestingDepth = actual
    }

    private var currentScalar: Unicode.Scalar? {
        guard index != pattern.unicodeScalars.endIndex else {
            return nil
        }
        return pattern.unicodeScalars[index]
    }

    private func peekNextScalar() -> Unicode.Scalar? {
        peekScalar(aheadBy: 1)
    }

    private func peekScalar(aheadBy distance: Int) -> Unicode.Scalar? {
        guard index != pattern.unicodeScalars.endIndex else {
            return nil
        }
        var lookaheadIndex = index
        for _ in 0..<distance {
            pattern.unicodeScalars.formIndex(after: &lookaheadIndex)
            if lookaheadIndex == pattern.unicodeScalars.endIndex {
                return nil
            }
        }
        return pattern.unicodeScalars[lookaheadIndex]
    }

    @discardableResult
    private mutating func advance() -> Unicode.Scalar? {
        guard let scalar = currentScalar else {
            return nil
        }
        pattern.unicodeScalars.formIndex(after: &index)
        scalarOffset += 1
        return scalar
    }

    private func syntax(
        _ reason: String,
        at offset: Int? = nil
    ) -> XSDRegularExpression.Error {
        .invalidSyntax(offset: offset ?? scalarOffset, reason: reason)
    }

    private static func isQuantifierStart(_ value: UInt32) -> Bool {
        value == 0x3F || value == 0x2A || value == 0x2B || value == 0x7B
    }

    private static func isASCIIDigit(_ value: UInt32) -> Bool {
        value >= 0x30 && value <= 0x39
    }

    private static func isPropertyNameScalar(_ value: UInt32) -> Bool {
        (value >= 0x41 && value <= 0x5A)
            || (value >= 0x61 && value <= 0x7A)
            || (value >= 0x30 && value <= 0x39)
            || value == 0x2D
    }

}
