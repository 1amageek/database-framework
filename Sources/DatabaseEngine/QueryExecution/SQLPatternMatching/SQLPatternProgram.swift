import DatabaseKit

struct SQLPatternProgram: Sendable {
    enum Instruction: Sendable {
        case consume(Int, next: Int)
        case split(Int, Int)
        case saveStart(Int, next: Int)
        case saveEnd(Int, next: Int)
        case start(next: Int)
        case end(next: Int)
        case invalidEscape
        case accept
    }
    struct Options: Sendable {
        var insensitive = false
        var literal = false
        var expanded = false
        var newlineStops = false
        var newlineAnchors = false
        var global = false
    }
    let instructions: DatabaseSharedRetainedArray<Instruction>
    let classes: DatabaseSharedRetainedArray<SQLPatternCharacterClass>
    let members: DatabaseSharedRetainedArray<SQLPatternCharacterClass.Member>
    let pattern: SQLPatternInput
    let startState: Int
    let captureCount: Int
    let options: Options
    let wholeString: Bool
    var meter: DatabaseWorkMeter { pattern.meter }
    var isGlobal: Bool { options.global }

    static func escape(_ text: String, workMeter: DatabaseWorkMeter) throws -> Unicode.Scalar? {
        try workMeter.consume(at: .expressionEvaluation)
        var iterator = text.unicodeScalars.makeIterator()
        let scalar = iterator.next()
        guard iterator.next() == nil else { throw SQLPatternError.invalidEscape }
        return scalar
    }

    static func compile(pattern: SQLPatternInput, syntax: SQLPatternSyntax,
                        flags: SQLPatternInput?, operation: SQLPatternOperation) throws -> Self {
        if let flags, flags.meter !== pattern.meter { throw SQLPatternError.meterMismatch }
        var options = Options()
        if case .ere(let insensitive) = syntax { options.insensitive = insensitive }
        if case .like(let insensitive, _) = syntax { options.insensitive = insensitive }
        if let flags {
            guard case .ere = syntax else { throw SQLPatternError.invalidFlag(offset: 0, flag: " ") }
            var offset = 0
            for flag in flags.text.unicodeScalars {
                try pattern.meter.consume(at: .expressionEvaluation)
                switch flag {
                case "c": options.insensitive = false
                case "i": options.insensitive = true
                case "e": options.literal = false
                case "q": options.literal = true
                case "t": options.expanded = false
                case "x": options.expanded = true
                case "s": options.newlineStops = false; options.newlineAnchors = false
                case "n", "m": options.newlineStops = true; options.newlineAnchors = true
                case "p": options.newlineStops = true; options.newlineAnchors = false
                case "w": options.newlineStops = false; options.newlineAnchors = true
                case "g":
                    guard operation == .matches || operation == .replace else {
                        throw SQLPatternError.invalidFlag(offset: offset, flag: flag)
                    }
                    options.global = true
                case "b": throw SQLPatternError.unsupportedFlag(offset: offset, flag: flag)
                default: throw SQLPatternError.invalidFlag(offset: offset, flag: flag)
                }
                offset += 1
            }
        }
        var parser = try SQLPatternParser(pattern: pattern, syntax: syntax, options: options)
        return try parser.compile()
    }

    func matches(_ input: SQLPatternInput) throws -> Bool {
        var iterator = try makeMatchIterator(in: input)
        return try iterator.next() != nil
    }
    func makeMatchIterator(in input: SQLPatternInput, start: Int32 = 1) throws -> SQLPatternMatchIterator {
        try SQLPatternMatchIterator(program: self, input: input, start: start)
    }
    func makeSplitIterator(in input: SQLPatternInput) throws -> SQLPatternSplitIterator {
        try SQLPatternSplitIterator(program: self, input: input)
    }

    func accepts(_ classIndex: Int, scalar: Unicode.Scalar) throws -> Bool {
        let characterClass = classes[classIndex]
        if characterClass.excludesNewline && scalar.value == 10 { return false }
        let x = scalar.value
        let folded: UInt32 = (65...90).contains(x) ? x + 32 : ((97...122).contains(x) ? x - 32 : x)
        var matched = false
        for index in characterClass.members {
            try meter.consume(at: .expressionEvaluation)
            switch members[index] {
            case .range(let lower, let upper):
                matched = (lower...upper).contains(x) || (characterClass.insensitive && (lower...upper).contains(folded))
            case .named(let name):
                matched = name.contains(x) || (characterClass.insensitive && name.contains(folded))
            }
            if matched { break }
        }
        return characterClass.negative ? !matched : matched
    }
}
