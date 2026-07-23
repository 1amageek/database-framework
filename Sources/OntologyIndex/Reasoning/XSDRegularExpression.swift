/// A bounded, immutable XML Schema 1.1 regular expression.
///
/// Matching is implicitly anchored to the entire input. Compilation uses a
/// Thompson NFA, so the runtime never performs recursive regex backtracking.
struct XSDRegularExpression: Sendable {
    struct Limits: Sendable, Equatable {
        let patternUTF8Bytes: Int
        let patternScalars: Int
        let nestingDepth: Int
        let astNodes: Int
        let nfaStates: Int
        let quantifier: Int
        let activeTransitionWork: Int

        init(
            patternUTF8Bytes: Int = 16_384,
            patternScalars: Int = 16_384,
            nestingDepth: Int = 64,
            astNodes: Int = 8_192,
            nfaStates: Int = 16_384,
            quantifier: Int = 4_096,
            activeTransitionWork: Int = 1_000_000
        ) {
            self.patternUTF8Bytes = patternUTF8Bytes
            self.patternScalars = patternScalars
            self.nestingDepth = nestingDepth
            self.astNodes = astNodes
            self.nfaStates = nfaStates
            self.quantifier = quantifier
            self.activeTransitionWork = activeTransitionWork
        }

        static let `default` = Self()
    }

    enum Error: Swift.Error, Sendable, Equatable {
        case invalidSyntax(offset: Int, reason: String)
        case resourceLimit(name: String, limit: Int, actual: Int)
    }

    private let nfa: XSDRegexNFA

    init(pattern: String, limits: Limits = .default) throws {
        try Self.checkLimit(
            name: "patternUTF8Bytes",
            limit: limits.patternUTF8Bytes,
            actual: 0
        )
        try Self.checkLimit(
            name: "patternScalars",
            limit: limits.patternScalars,
            actual: 0
        )
        try Self.checkLimit(
            name: "nestingDepth",
            limit: limits.nestingDepth,
            actual: 0
        )
        try Self.checkLimit(name: "astNodes", limit: limits.astNodes, actual: 0)
        try Self.checkLimit(name: "nfaStates", limit: limits.nfaStates, actual: 0)
        try Self.checkLimit(
            name: "quantifier",
            limit: limits.quantifier,
            actual: 0
        )
        try Self.checkLimit(
            name: "activeTransitionWork",
            limit: limits.activeTransitionWork,
            actual: 0
        )
        try Self.checkPatternLimits(pattern, limits: limits)

        var parser = XSDRegexParser(pattern: pattern, limits: limits)
        let syntaxTree = try parser.parse()
        var compiler = XSDRegexCompiler(limits: limits)
        nfa = try compiler.compile(syntaxTree)
    }

    func wholeMatch<Input: StringProtocol>(_ input: Input) throws -> Bool {
        try nfa.wholeMatch(input)
    }

    private static func checkLimit(
        name: String,
        limit: Int,
        actual: Int
    ) throws {
        guard limit >= 0, actual <= limit else {
            throw Error.resourceLimit(name: name, limit: limit, actual: actual)
        }
    }

    private static func checkPatternLimits(
        _ pattern: String,
        limits: Limits
    ) throws {
        var utf8Bytes = 0
        var scalarCount = 0
        for scalar in pattern.unicodeScalars {
            let (nextScalarCount, scalarOverflow) =
                scalarCount.addingReportingOverflow(1)
            let (nextUTF8Bytes, byteOverflow) = utf8Bytes.addingReportingOverflow(
                utf8Width(of: scalar.value)
            )

            if byteOverflow || nextUTF8Bytes > limits.patternUTF8Bytes {
                throw Error.resourceLimit(
                    name: "patternUTF8Bytes",
                    limit: limits.patternUTF8Bytes,
                    actual: byteOverflow ? Int.max : nextUTF8Bytes
                )
            }
            if scalarOverflow || nextScalarCount > limits.patternScalars {
                throw Error.resourceLimit(
                    name: "patternScalars",
                    limit: limits.patternScalars,
                    actual: scalarOverflow ? Int.max : nextScalarCount
                )
            }

            utf8Bytes = nextUTF8Bytes
            scalarCount = nextScalarCount
        }
    }

    private static func utf8Width(of value: UInt32) -> Int {
        switch value {
        case 0...0x7F: return 1
        case 0x80...0x7FF: return 2
        case 0x800...0xFFFF: return 3
        default: return 4
        }
    }
}
