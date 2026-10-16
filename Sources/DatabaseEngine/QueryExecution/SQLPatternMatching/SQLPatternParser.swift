import DatabaseKit

/// Iterative grammar and compilation; nesting consumes admitted storage, not the call stack.
struct SQLPatternParser: ~Copyable {
    private enum Node: Sendable {
        case empty, start, end, invalidEscape
        case atom(Int)
        case concatenate(Int, Int)
        case alternate(Int, Int)
        case group(Int, Int)
        case repetition(Int, Int, Int?)
    }
    private struct Entry: Sendable { let node: Node; let cost: UInt64 }
    private struct Frame: Sendable {
        var branches: Int?
        var prefix: Int?
        var last: Int?
        let group: Int
    }
    private enum Task: Sendable {
        case node(Int, Int)
        case left(Int)
        case alternate(Int, Int)
        case split(Int)
        case saveStart(Int)
        case repetitions(Int, Int, Int)
        case optional(Int)
        case loop(Int, Int, Int)
    }
    private let input: SQLPatternInput
    private let syntax: SQLPatternSyntax
    private let options: SQLPatternProgram.Options
    private var index: String.Index
    private var offset = 0
    private var groups = 0
    private var nodes: SQLPatternStorage<Entry>
    private var frames: SQLPatternStorage<Frame>
    private var classes: SQLPatternStorage<SQLPatternCharacterClass>
    private var members: SQLPatternStorage<SQLPatternCharacterClass.Member>

    init(pattern: SQLPatternInput, syntax: SQLPatternSyntax, options: SQLPatternProgram.Options) throws {
        let preparedNodes = try SQLPatternStorage<Entry>(meter: pattern.meter)
        var preparedFrames = try SQLPatternStorage<Frame>(meter: pattern.meter)
        let preparedClasses = try SQLPatternStorage<SQLPatternCharacterClass>(meter: pattern.meter)
        let preparedMembers = try SQLPatternStorage<SQLPatternCharacterClass.Member>(meter: pattern.meter)
        try preparedFrames.append(Frame(group: 0))
        input = pattern; self.syntax = syntax; self.options = options
        index = pattern.text.unicodeScalars.startIndex
        nodes = consume preparedNodes
        frames = consume preparedFrames
        classes = consume preparedClasses
        members = consume preparedMembers
    }
    private var current: Unicode.Scalar? {
        index == input.text.endIndex ? nil : input.text.unicodeScalars[index]
    }
    private var followingScalar: Unicode.Scalar? {
        guard current != nil else { return nil }
        let following = input.text.unicodeScalars.index(after: index)
        return following == input.text.endIndex ? nil : input.text.unicodeScalars[following]
    }
    private var specialBracketMarker: Unicode.Scalar? {
        guard current == "[", let marker = followingScalar,
              marker == ":" || marker == "." || marker == "=" else { return nil }
        return marker
    }
    @discardableResult private mutating func advance() throws -> Unicode.Scalar? {
        guard let scalar = current else { return nil }
        try input.meter.consume(at: .expressionEvaluation)
        input.text.unicodeScalars.formIndex(after: &index); offset += 1
        return scalar
    }
    private func fail(_ reason: String) -> SQLPatternError { .invalidPattern(offset: offset, reason: reason) }
    private mutating func add(_ node: Node, cost: UInt64) throws -> Int {
        let result = nodes.count
        try nodes.append(Entry(node: node, cost: cost))
        return result
    }
    private mutating func combine(_ left: Int, _ right: Int, alternate: Bool = false) throws -> Int {
        let cost = try DatabaseIntermediateFootprint(bytes: nodes[left].cost).adding(.init(bytes: nodes[right].cost))
            .adding(.init(bytes: alternate ? 1 : 0)).bytes
        return try add(alternate ? .alternate(left, right) : .concatenate(left, right), cost: cost)
    }
    private mutating func appendAtom(_ node: Int) throws {
        var frame = frames[frames.count - 1]
        if let last = frame.last { frame.prefix = try frame.prefix.map { try combine($0, last) } ?? last }
        frame.last = node; frames[frames.count - 1] = frame
    }
    private mutating func branch(_ frame: Frame) throws -> Int {
        if let last = frame.last { return try frame.prefix.map { try combine($0, last) } ?? last }
        return try add(.empty, cost: 0)
    }
    private mutating func finished(_ frame: Frame) throws -> Int {
        let tail = try branch(frame)
        return try frame.branches.map { try combine($0, tail, alternate: true) } ?? tail
    }
    private mutating func atom(_ scalar: Unicode.Scalar) throws -> Int {
        let begin = members.count
        try members.append(.range(scalar.value, scalar.value))
        let number = classes.count
        try classes.append(.init(members: begin..<members.count, negative: false,
                                 excludesNewline: false, insensitive: options.insensitive))
        return try add(.atom(number), cost: 1)
    }
    private mutating func wildcard() throws -> Int {
        let begin = members.count
        try members.append(.range(0, 0x10ffff))
        let number = classes.count
        let stops: Bool
        if case .ere = syntax { stops = options.newlineStops } else { stops = false }
        try classes.append(.init(members: begin..<members.count, negative: false,
                                 excludesNewline: stops, insensitive: false))
        return try add(.atom(number), cost: 1)
    }
    private mutating func repeatNode(_ child: Int, minimum: Int, maximum: Int?) throws -> Int {
        let repetitions = UInt64(maximum ?? minimum + 1)
        let expanded = try DatabaseIntermediateFootprint(bytes: nodes[child].cost).multiplied(by: repetitions)
        let splits = UInt64(maximum.map { $0 - minimum } ?? 1)
        return try add(.repetition(child, minimum, maximum), cost: expanded.adding(.init(bytes: splits)).bytes)
    }

    mutating func compile() throws -> SQLPatternProgram {
        let escapedBy: Unicode.Scalar?
        let like: Bool
        let similar: Bool
        switch syntax {
        case .like(_, let escape): escapedBy = escape; like = true; similar = false
        case .similar(let escape): escapedBy = escape; like = false; similar = true
        case .ere: escapedBy = "\\"; like = false; similar = false
        }
        while let scalar = current {
            if options.literal { try advance(); try appendAtom(atom(scalar)); continue }
            if options.expanded && !like && !similar {
                if scalar == " " || (9...13).contains(scalar.value) { try advance(); continue }
                if scalar == "#" { while let s = current, s != "\n" { try advance() }; continue }
            }
            if scalar == escapedBy {
                let slash = offset
                try advance()
                guard let escaped = current else {
                    if like { try appendAtom(add(.invalidEscape, cost: 1)); continue }
                    throw fail("trailing escape")
                }
                if !like && !similar {
                    if (48...57).contains(escaped.value) {
                        throw SQLPatternError.unsupportedPattern(offset: slash, construct: "pattern backreference")
                    }
                    if "dDsSwW".unicodeScalars.contains(escaped) ||
                        ((escaped == "p" || escaped == "P") && followingScalar == "{") {
                        throw SQLPatternError.unsupportedPattern(offset: slash, construct: "property or class escape")
                    }
                }
                try advance(); try appendAtom(atom(escaped)); continue
            }
            if (like || similar) && (scalar == "%" || scalar == "_") {
                try advance(); let any = try wildcard()
                try appendAtom(scalar == "%" ? repeatNode(any, minimum: 0, maximum: nil) : any)
                continue
            }
            if like { try advance(); try appendAtom(atom(scalar)); continue }
            if scalar == "{" {
                let after = input.text.unicodeScalars.index(after: index)
                if after == input.text.endIndex || !(48...57).contains(input.text.unicodeScalars[after].value) {
                    try advance(); try appendAtom(atom(scalar)); continue
                }
            }
            switch scalar {
            case "(":
                try advance()
                if current == "?" { throw SQLPatternError.unsupportedPattern(offset: offset, construct: "ARE group") }
                groups += 1; try frames.append(Frame(group: groups))
            case ")":
                guard frames.count > 1 else { throw fail("unmatched closing group") }
                try advance()
                let frame = frames.popLast()!
                let child = try finished(frame)
                let cost = try DatabaseIntermediateFootprint(bytes: nodes[child].cost).adding(.init(bytes: 2)).bytes
                try appendAtom(add(.group(child, frame.group), cost: cost))
            case "|":
                try advance()
                var frame = frames[frames.count - 1]
                let tail = try branch(frame)
                frame.branches = try frame.branches.map { try combine($0, tail, alternate: true) } ?? tail
                frame.prefix = nil; frame.last = nil; frames[frames.count - 1] = frame
            case "*", "+", "?", "{":
                var frame = frames[frames.count - 1]
                guard let child = frame.last else { throw fail("quantifier requires an atom") }
                if case .repetition = nodes[child].node { throw SQLPatternError.unsupportedPattern(offset: offset, construct: "multiple or non-greedy quantifier") }
                let minimum: Int; let maximum: Int?
                if scalar == "{" {
                    try advance(); minimum = try integer()
                    if current == "}" { maximum = minimum; try advance() }
                    else {
                        guard current == "," else { throw fail("invalid repetition") }; try advance()
                        maximum = current == "}" ? nil : try integer()
                        guard current == "}", maximum == nil || minimum <= maximum! else { throw fail("invalid repetition bounds") }
                        try advance()
                    }
                } else {
                    try advance(); minimum = scalar == "+" ? 1 : 0; maximum = scalar == "?" ? 1 : nil
                }
                frame.last = try repeatNode(child, minimum: minimum, maximum: maximum)
                frames[frames.count - 1] = frame
            case "[": try appendAtom(characterClass())
            case ".": try advance(); try appendAtom(similar ? atom(scalar) : wildcard())
            case "^": try advance(); try appendAtom(similar ? atom(scalar) : add(.start, cost: 1))
            case "$": try advance(); try appendAtom(similar ? atom(scalar) : add(.end, cost: 1))
            case "}", "]": try advance(); try appendAtom(atom(scalar))
            default: try advance(); try appendAtom(atom(scalar))
            }
        }
        guard frames.count == 1 else { throw fail("unterminated group") }
        let root = try finished(frames.popLast()!)
        let cost = try DatabaseIntermediateFootprint(bytes: nodes[root].cost).adding(.init(bytes: 1)).bytes
        guard let stateCount = Int(exactly: cost) else {
            throw DatabaseWorkLimitError.maximumIntermediateBytes(stage: .expressionEvaluation,
                consumed: input.meter.retainedIntermediateBytes, requested: UInt64.max,
                maximum: input.meter.budget.maximumIntermediateBytes)
        }
        var code = try SQLPatternStorage<SQLPatternProgram.Instruction>(meter: input.meter, count: stateCount)
        try code.append(.accept)
        var tasks = try SQLPatternStorage<Task>(meter: input.meter)
        try tasks.append(.node(root, 0))
        var result = 0
        while let task = tasks.popLast() {
            try input.meter.consume(at: .expressionEvaluation)
            switch task {
            case .node(let node, let next):
                switch nodes[node].node {
                case .empty: result = next
                case .atom(let number): result = code.count; try code.append(.consume(number, next: next))
                case .start: result = code.count; try code.append(.start(next: next))
                case .end: result = code.count; try code.append(.end(next: next))
                case .invalidEscape: result = code.count; try code.append(.invalidEscape)
                case .concatenate(let left, let right): try tasks.append(.left(left)); try tasks.append(.node(right, next))
                case .alternate(let left, let right): try tasks.append(.alternate(left, next)); try tasks.append(.node(right, next))
                case .group(let child, let group):
                    let end = code.count; try code.append(.saveEnd(group, next: next))
                    try tasks.append(.saveStart(group)); try tasks.append(.node(child, end))
                case .repetition(let child, let minimum, let maximum):
                    result = next
                    if let maximum { try tasks.append(.repetitions(child, minimum, maximum - minimum)) }
                    else {
                        let loop = code.count; try code.append(.split(next, next))
                        try tasks.append(.loop(loop, child, minimum)); try tasks.append(.node(child, loop))
                    }
                }
            case .left(let child): try tasks.append(.node(child, result))
            case .alternate(let child, let next): let right = result; try tasks.append(.split(right)); try tasks.append(.node(child, next))
            case .split(let right): let left = result; result = code.count; try code.append(.split(left, right))
            case .saveStart(let group): let child = result; result = code.count; try code.append(.saveStart(group, next: child))
            case .repetitions(let child, let required, let optional):
                if optional > 0 {
                    let next = result; try tasks.append(.repetitions(child, required, optional - 1))
                    try tasks.append(.optional(next)); try tasks.append(.node(child, next))
                } else if required > 0 {
                    try tasks.append(.repetitions(child, required - 1, 0)); try tasks.append(.node(child, result))
                }
            case .optional(let next): let child = result; result = code.count; try code.append(.split(child, next))
            case .loop(let loop, let child, let required):
                guard case .split(_, let next) = code[loop] else { preconditionFailure("Loop instruction changed") }
                code[loop] = .split(result, next); result = loop
                try tasks.append(.repetitions(child, required, 0))
            }
        }
        return try SQLPatternProgram(instructions: code.finish(), classes: classes.finish(), members: members.finish(),
            pattern: input, startState: result, captureCount: groups, options: options, wholeString: like || similar)
    }

    private mutating func integer() throws -> Int {
        guard let first = current, (48...57).contains(first.value) else { throw fail("expected repetition bound") }
        var value = 0
        while let scalar = current, (48...57).contains(scalar.value) {
            let product = value.multipliedReportingOverflow(by: 10)
            let sum = product.partialValue.addingReportingOverflow(Int(scalar.value - 48))
            guard !product.overflow, !sum.overflow, sum.partialValue < Int.max else { throw fail("repetition bound overflow") }
            guard sum.partialValue <= 255 else { throw fail("repetition bound must be between 0 and 255") }
            value = sum.partialValue; try advance()
        }
        return value
    }
    private mutating func characterClass() throws -> Int {
        try advance()
        let negative = current == "^"
        if negative { try advance() }
        let begin = members.count
        var first = true
        while let scalar = current {
            if scalar == "]" && !first { try advance(); break }
            first = false
            if let marker = specialBracketMarker {
                try advance()
                try advance(); let nameStart = index
                while let s = current, s != marker { try advance() }
                guard current == marker else { throw fail("unterminated bracket member") }
                let spelling = input.text[nameStart..<index]
                try advance(); guard current == "]" else { throw fail("invalid bracket member") }; try advance()
                if marker == ":" {
                    guard let name = SQLPatternCharacterClass.Name(rawValue: String(spelling)) else { throw fail("unknown POSIX character class") }
                    try members.append(.named(name))
                } else {
                    var values = spelling.unicodeScalars.makeIterator()
                    guard let value = values.next(), values.next() == nil else {
                        throw SQLPatternError.unsupportedPattern(offset: offset, construct: "multi-codepoint collating element")
                    }
                    try members.append(.range(value.value, value.value))
                }
            } else {
                let lower = scalar.value; try advance()
                if current == "-" {
                    try advance()
                    if current == "]" { try members.append(.range(lower, lower)); try members.append(.range(45, 45)); continue }
                    guard let upper = current, upper.value >= lower, specialBracketMarker == nil else { throw fail("invalid character range") }
                    try advance(); try members.append(.range(lower, upper.value))
                } else { try members.append(.range(lower, lower)) }
            }
            if current == nil { throw fail("unterminated bracket expression") }
        }
        guard members.count > begin else { throw fail("empty bracket expression") }
        let number = classes.count
        try classes.append(.init(members: begin..<members.count, negative: negative,
                                 excludesNewline: negative && options.newlineStops, insensitive: options.insensitive))
        return try add(.atom(number), cost: 1)
    }
}
