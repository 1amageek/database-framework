/// Builds a bounded Thompson NFA from the parsed SPARQL regex syntax tree.
struct SPARQLRegexCompiler {
    private let options: SPARQLRegularExpression.Options
    private let limits: SPARQLRegularExpression.Limits
    private var states: [SPARQLRegexNFA.State] = []

    init(
        options: SPARQLRegularExpression.Options,
        limits: SPARQLRegularExpression.Limits
    ) {
        self.options = options
        self.limits = limits
    }

    mutating func compile(
        _ node: SPARQLRegexParser.Node
    ) throws(SPARQLRegularExpression.Error) -> SPARQLRegexNFA {
        let requiredStateCount = try SPARQLRegularExpression.checkedAdd(
            1,
            stateCost(of: node),
            name: "nfaStates",
            limit: limits.nfaStates
        )
        states.reserveCapacity(requiredStateCount)
        let accept = try append(.accept)
        let start = try compile(node, continuingAt: accept)
        return SPARQLRegexNFA(
            states: states,
            startState: start,
            multiline: options.contains(.multiline)
        )
    }

    private mutating func compile(
        _ node: SPARQLRegexParser.Node,
        continuingAt continuation: Int
    ) throws(SPARQLRegularExpression.Error) -> Int {
        switch node {
        case .empty:
            return continuation
        case .atom(let characterClass):
            return try append(.consume(characterClass, next: continuation))
        case .startAssertion:
            return try append(.assertStart(next: continuation))
        case .endAssertion:
            return try append(.assertEnd(next: continuation))
        case .capture(let group, let child):
            let end = try append(.saveEnd(group: group, next: continuation))
            let childStart = try compile(child, continuingAt: end)
            return try append(.saveStart(group: group, next: childStart))
        case .concatenation(let children):
            var start = continuation
            for child in children.reversed() {
                start = try compile(child, continuingAt: start)
            }
            return start
        case .alternation(let branches):
            guard let last = branches.last else {
                return continuation
            }
            var start = try compile(last, continuingAt: continuation)
            for branch in branches.dropLast().reversed() {
                let branchStart = try compile(
                    branch,
                    continuingAt: continuation
                )
                start = try append(.split(first: branchStart, second: start))
            }
            return start
        case .repetition(let child, let minimum, let maximum):
            return try compileRepetition(
                child,
                minimum: minimum,
                maximum: maximum,
                continuingAt: continuation
            )
        }
    }

    private mutating func compileRepetition(
        _ child: SPARQLRegexParser.Node,
        minimum: Int,
        maximum: Int?,
        continuingAt continuation: Int
    ) throws(SPARQLRegularExpression.Error) -> Int {
        if maximum == 0 {
            return continuation
        }

        var start = continuation
        if let maximum {
            for _ in minimum..<maximum {
                let optionalStart = try compile(child, continuingAt: start)
                start = try append(.split(first: optionalStart, second: start))
            }
        } else {
            let splitIndex = try append(.epsilon(next: continuation))
            let repeatingStart = try compile(child, continuingAt: splitIndex)
            states[splitIndex] = .split(
                first: repeatingStart,
                second: continuation
            )
            start = splitIndex
        }

        if minimum > 0 {
            for _ in 0..<minimum {
                start = try compile(child, continuingAt: start)
            }
        }
        return start
    }

    private func stateCost(
        of node: SPARQLRegexParser.Node
    ) throws(SPARQLRegularExpression.Error) -> Int {
        switch node {
        case .empty:
            return 0
        case .atom, .startAssertion, .endAssertion:
            return 1
        case .capture(_, let child):
            return try SPARQLRegularExpression.checkedAdd(
                2,
                stateCost(of: child),
                name: "nfaStates",
                limit: limits.nfaStates
            )
        case .concatenation(let children):
            var total = 0
            for child in children {
                total = try SPARQLRegularExpression.checkedAdd(
                    total,
                    stateCost(of: child),
                    name: "nfaStates",
                    limit: limits.nfaStates
                )
            }
            return total
        case .alternation(let branches):
            var total = branches.isEmpty ? 0 : branches.count - 1
            for branch in branches {
                total = try SPARQLRegularExpression.checkedAdd(
                    total,
                    stateCost(of: branch),
                    name: "nfaStates",
                    limit: limits.nfaStates
                )
            }
            return total
        case .repetition(let child, let minimum, let maximum):
            guard maximum != 0 else {
                return 0
            }
            let childCost = try stateCost(of: child)
            if let maximum {
                let copies = try SPARQLRegularExpression.checkedMultiply(
                    childCost,
                    maximum,
                    name: "nfaStates",
                    limit: limits.nfaStates
                )
                return try SPARQLRegularExpression.checkedAdd(
                    copies,
                    maximum - minimum,
                    name: "nfaStates",
                    limit: limits.nfaStates
                )
            }
            let copies = try SPARQLRegularExpression.checkedAdd(
                minimum,
                1,
                name: "nfaStates",
                limit: limits.nfaStates
            )
            let repeated = try SPARQLRegularExpression.checkedMultiply(
                childCost,
                copies,
                name: "nfaStates",
                limit: limits.nfaStates
            )
            return try SPARQLRegularExpression.checkedAdd(
                repeated,
                1,
                name: "nfaStates",
                limit: limits.nfaStates
            )
        }
    }

    private mutating func append(_ state: SPARQLRegexNFA.State) throws(SPARQLRegularExpression.Error) -> Int {
        _ = try SPARQLRegularExpression.checkedIncrement(
            states.count,
            name: "nfaStates",
            limit: limits.nfaStates
        )
        let index = states.count
        states.append(state)
        return index
    }
}
