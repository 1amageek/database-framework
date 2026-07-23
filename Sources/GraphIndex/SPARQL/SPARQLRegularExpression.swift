/// A bounded SPARQL/XPath regular expression compiled to a Thompson NFA.
///
/// Supported syntax is alternation, concatenation, capturing and non-capturing
/// groups, greedy quantifiers, anchors, wildcard, character classes (including
/// subtraction), XML Schema character-class escapes, Unicode categories, and
/// Unicode blocks. Backreferences, lookarounds, and non-greedy or possessive
/// quantifiers are rejected instead of entering a backtracking implementation.
struct SPARQLRegularExpression: Sendable {
    struct Limits: Sendable, Equatable {
        let patternUTF8Bytes: Int
        let patternScalars: Int
        let flagScalars: Int
        let inputUTF8Bytes: Int
        let nestingDepth: Int
        let astNodes: Int
        let nfaStates: Int
        let quantifier: Int
        let captureGroups: Int
        let activeTransitionWork: Int
        let replacementTokens: Int
        let replacementMatches: Int
        let outputUTF8Bytes: Int

        init(
            patternUTF8Bytes: Int = SPARQLExecutionLimits.maximumRegularExpressionPatternUTF8Count,
            patternScalars: Int = SPARQLExecutionLimits.maximumRegularExpressionPatternScalarCount,
            flagScalars: Int = SPARQLExecutionLimits.maximumRegularExpressionFlagScalarCount,
            inputUTF8Bytes: Int = SPARQLExecutionLimits.maximumLiteralUTF8Count,
            nestingDepth: Int = SPARQLExecutionLimits.maximumRegularExpressionNestingDepth,
            astNodes: Int = SPARQLExecutionLimits.maximumRegularExpressionASTNodeCount,
            nfaStates: Int = SPARQLExecutionLimits.maximumRegularExpressionNFAStateCount,
            quantifier: Int = SPARQLExecutionLimits.maximumRegularExpressionQuantifier,
            captureGroups: Int = SPARQLExecutionLimits.maximumRegularExpressionCaptureGroupCount,
            activeTransitionWork: Int = SPARQLExecutionLimits.maximumRegularExpressionActiveTransitionWork,
            replacementTokens: Int = SPARQLExecutionLimits.maximumRegularExpressionReplacementTokenCount,
            replacementMatches: Int = SPARQLExecutionLimits.maximumRegularExpressionReplacementMatchCount,
            outputUTF8Bytes: Int = SPARQLExecutionLimits.maximumLiteralUTF8Count
        ) {
            self.patternUTF8Bytes = patternUTF8Bytes
            self.patternScalars = patternScalars
            self.flagScalars = flagScalars
            self.inputUTF8Bytes = inputUTF8Bytes
            self.nestingDepth = nestingDepth
            self.astNodes = astNodes
            self.nfaStates = nfaStates
            self.quantifier = quantifier
            self.captureGroups = captureGroups
            self.activeTransitionWork = activeTransitionWork
            self.replacementTokens = replacementTokens
            self.replacementMatches = replacementMatches
            self.outputUTF8Bytes = outputUTF8Bytes
        }

        static let `default` = Self()
    }

    struct Options: OptionSet, Sendable {
        let rawValue: UInt8

        static let caseInsensitive = Self(rawValue: 1 << 0)
        static let multiline = Self(rawValue: 1 << 1)
        static let dotMatchesLineSeparators = Self(rawValue: 1 << 2)
        static let extended = Self(rawValue: 1 << 3)
    }

    enum Error: Swift.Error, Sendable, Equatable {
        case invalidFlags(offset: Int, flag: Unicode.Scalar)
        case invalidSyntax(offset: Int, reason: String)
        case invalidReplacement(offset: Int, reason: String)
        case resourceLimit(name: String, limit: Int, actual: Int)
    }

    private let nfa: SPARQLRegexNFA
    private let limits: Limits
    private let captureGroupCount: Int

    init(
        pattern: String,
        flags: String? = nil,
        limits: Limits = .default
    ) throws {
        try Self.validate(limits: limits)
        try Self.checkBoundedString(
            pattern,
            byteLimitName: "patternUTF8Bytes",
            byteLimit: limits.patternUTF8Bytes,
            scalarLimitName: "patternScalars",
            scalarLimit: limits.patternScalars
        )
        let options = try Self.parseOptions(flags ?? "", limits: limits)
        let parsedPattern: String
        if options.contains(.extended) {
            // Extended-mode trivia must be removed before parsing. This bounded
            // pattern copy is independent of the potentially large input path.
            parsedPattern = Self.removingExtendedModeTrivia(from: pattern)
        } else {
            parsedPattern = pattern
        }

        var parser = SPARQLRegexParser(
            pattern: parsedPattern,
            options: options,
            limits: limits
        )
        let parsed = try parser.parse()
        var compiler = SPARQLRegexCompiler(options: options, limits: limits)
        nfa = try compiler.compile(parsed.node)
        captureGroupCount = parsed.captureGroupCount
        self.limits = limits
    }

    func matches(_ input: String) throws -> Bool {
        try matches(input) { _ in }
    }

    func matches(
        _ input: String,
        onValidatedInput: (Int) throws -> Void
    ) throws -> Bool {
        let inputUTF8ByteCount = try checkedInputUTF8ByteCount(input)
        try onValidatedInput(inputUTF8ByteCount)
        var budget = SPARQLRegexWorkBudget(
            limit: limits.activeTransitionWork
        )
        var scratch = SPARQLRegexNFA.Scratch(stateCount: nfa.states.count)
        return try nfa.firstMatch(
            in: input,
            from: input.startIndex,
            budget: &budget,
            scratch: &scratch
        ) != nil
    }

    func replacingMatches(
        in input: String,
        with replacement: String
    ) throws -> String {
        _ = try checkedInputUTF8ByteCount(input)
        let template = try SPARQLRegexReplacementTemplate(
            replacement,
            captureGroupCount: captureGroupCount,
            tokenLimit: limits.replacementTokens,
            byteLimit: limits.outputUTF8Bytes
        )
        var budget = SPARQLRegexWorkBudget(
            limit: limits.activeTransitionWork
        )
        var scratch = SPARQLRegexNFA.Scratch(stateCount: nfa.states.count)
        var output = SPARQLRegexOutputBuilder(
            capacity: min(input.utf8.count, limits.outputUTF8Bytes),
            byteLimit: limits.outputUTF8Bytes
        )
        var searchStart = input.startIndex
        var emissionStart = input.startIndex
        var matchCount = 0

        while let match = try nfa.firstMatch(
            in: input,
            from: searchStart,
            budget: &budget,
            scratch: &scratch
        ) {
            matchCount = try Self.checkedIncrement(
                matchCount,
                name: "replacementMatches",
                limit: limits.replacementMatches
            )
            try output.append(input[emissionStart..<match.range.lowerBound])
            try template.append(match: match, input: input, to: &output)
            emissionStart = match.range.upperBound

            if match.range.isEmpty {
                guard match.range.upperBound != input.endIndex else {
                    searchStart = input.endIndex
                    break
                }
                searchStart = input.unicodeScalars.index(
                    after: match.range.upperBound
                )
            } else {
                guard match.range.upperBound != input.endIndex else {
                    searchStart = input.endIndex
                    break
                }
                searchStart = match.range.upperBound
            }
        }

        try output.append(input[emissionStart..<input.endIndex])
        return output.finish()
    }

    static func evaluateMatch(
        _ input: String,
        pattern: String,
        flags: String?
    ) throws -> Bool {
        do {
            return try Self(pattern: pattern, flags: flags).matches(input)
        } catch let error as Error {
            throw map(error, pattern: pattern, function: "REGEX")
        }
    }

    static func evaluateReplacement(
        _ input: String,
        pattern: String,
        replacement: String,
        flags: String?
    ) throws -> String {
        do {
            return try Self(pattern: pattern, flags: flags)
                .replacingMatches(in: input, with: replacement)
        } catch let error as Error {
            throw map(error, pattern: pattern, function: "REPLACE")
        }
    }

    private func checkedInputUTF8ByteCount(_ input: String) throws -> Int {
        try Self.checkedUTF8ByteCount(
            input,
            name: "inputUTF8Bytes",
            limit: limits.inputUTF8Bytes
        )
    }

    private static func parseOptions(
        _ flags: String,
        limits: Limits
    ) throws -> Options {
        var options: Options = []
        var offset = 0
        for flag in flags.unicodeScalars {
            guard offset < limits.flagScalars else {
                throw Error.resourceLimit(
                    name: "flagScalars",
                    limit: limits.flagScalars,
                    actual: offset + 1
                )
            }
            switch flag.value {
            case 0x69: options.insert(.caseInsensitive)
            case 0x6D: options.insert(.multiline)
            case 0x73: options.insert(.dotMatchesLineSeparators)
            case 0x78: options.insert(.extended)
            default: throw Error.invalidFlags(offset: offset, flag: flag)
            }
            offset += 1
        }
        return options
    }

    private static func removingExtendedModeTrivia(
        from pattern: String
    ) -> String {
        var result = ""
        result.reserveCapacity(pattern.utf8.count)
        var escaped = false
        var insideCharacterClass = false
        var insideComment = false

        for scalar in pattern.unicodeScalars {
            if insideComment {
                if scalar.value == 0x0A || scalar.value == 0x0D {
                    insideComment = false
                }
                continue
            }
            if escaped {
                result.unicodeScalars.append(scalar)
                escaped = false
                continue
            }
            if scalar.value == 0x5C {
                result.unicodeScalars.append(scalar)
                escaped = true
                continue
            }
            if insideCharacterClass {
                result.unicodeScalars.append(scalar)
                if scalar.value == 0x5D {
                    insideCharacterClass = false
                }
                continue
            }
            if scalar.value == 0x5B {
                insideCharacterClass = true
                result.unicodeScalars.append(scalar)
            } else if scalar.value == 0x23 {
                insideComment = true
            } else if !isExtendedModeWhitespace(scalar.value) {
                result.unicodeScalars.append(scalar)
            }
        }
        return result
    }

    private static func isExtendedModeWhitespace(_ value: UInt32) -> Bool {
        switch value {
        case 0x09, 0x0A, 0x0D, 0x20:
            return true
        default:
            return false
        }
    }

    private static func validate(limits: Limits) throws {
        let namedLimits: [(String, Int)] = [
            ("patternUTF8Bytes", limits.patternUTF8Bytes),
            ("patternScalars", limits.patternScalars),
            ("flagScalars", limits.flagScalars),
            ("inputUTF8Bytes", limits.inputUTF8Bytes),
            ("nestingDepth", limits.nestingDepth),
            ("astNodes", limits.astNodes),
            ("nfaStates", limits.nfaStates),
            ("quantifier", limits.quantifier),
            ("captureGroups", limits.captureGroups),
            ("activeTransitionWork", limits.activeTransitionWork),
            ("replacementTokens", limits.replacementTokens),
            ("replacementMatches", limits.replacementMatches),
            ("outputUTF8Bytes", limits.outputUTF8Bytes),
        ]
        for (name, limit) in namedLimits where limit < 0 {
            throw Error.resourceLimit(name: name, limit: limit, actual: 0)
        }
    }

    private static func checkBoundedString(
        _ value: String,
        byteLimitName: String,
        byteLimit: Int,
        scalarLimitName: String,
        scalarLimit: Int
    ) throws {
        var bytes = 0
        var scalars = 0
        for scalar in value.unicodeScalars {
            bytes = try checkedAdd(
                bytes,
                Self.utf8Width(of: scalar.value),
                name: byteLimitName,
                limit: byteLimit
            )
            scalars = try checkedIncrement(
                scalars,
                name: scalarLimitName,
                limit: scalarLimit
            )
        }
    }

    static func checkedAdd(
        _ lhs: Int,
        _ rhs: Int,
        name: String,
        limit: Int
    ) throws -> Int {
        let (result, overflow) = lhs.addingReportingOverflow(rhs)
        guard !overflow, result <= limit else {
            throw Error.resourceLimit(
                name: name,
                limit: limit,
                actual: overflow ? Int.max : result
            )
        }
        return result
    }

    static func checkedMultiply(
        _ lhs: Int,
        _ rhs: Int,
        name: String,
        limit: Int
    ) throws -> Int {
        let (result, overflow) = lhs.multipliedReportingOverflow(by: rhs)
        guard !overflow, result <= limit else {
            throw Error.resourceLimit(
                name: name,
                limit: limit,
                actual: overflow ? Int.max : result
            )
        }
        return result
    }

    static func checkedIncrement(
        _ value: Int,
        name: String,
        limit: Int
    ) throws -> Int {
        try checkedAdd(value, 1, name: name, limit: limit)
    }

    static func checkedUTF8ByteCount(
        _ value: String,
        name: String,
        limit: Int
    ) throws -> Int {
        var count = 0
        for scalar in value.unicodeScalars {
            count = try checkedAdd(
                count,
                utf8Width(of: scalar.value),
                name: name,
                limit: limit
            )
        }
        return count
    }

    private static func utf8Width(of value: UInt32) -> Int {
        switch value {
        case 0...0x7F: return 1
        case 0x80...0x7FF: return 2
        case 0x800...0xFFFF: return 3
        default: return 4
        }
    }

    private static func map(
        _ error: Error,
        pattern: String,
        function: String
    ) -> SPARQLExpressionEvaluationError {
        switch error {
        case .invalidFlags:
            return .invalidFunctionArguments("\(function) flags")
        case .invalidSyntax:
            return .invalidRegularExpression(pattern)
        case .invalidReplacement:
            return .invalidFunctionArguments("REPLACE replacement")
        case .resourceLimit(let name, let limit, let actual):
            return .resourceLimitExceeded(
                stage: "regular expression \(name)",
                required: UInt64(exactly: actual),
                maximum: UInt64(exactly: limit)
            )
        }
    }
}
