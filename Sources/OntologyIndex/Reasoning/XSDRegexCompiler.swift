/// Builds a bounded Thompson NFA from the parsed XSD regex syntax tree.
struct XSDRegexCompiler {
    private let limits: XSDRegularExpression.Limits
    private var states: [XSDRegexNFA.State] = []

    init(limits: XSDRegularExpression.Limits) {
        self.limits = limits
    }

    mutating func compile(_ node: XSDRegexParser.Node) throws -> XSDRegexNFA {
        let requiredStateCount = try checkedAdd(1, stateCost(of: node))
        states.reserveCapacity(requiredStateCount)
        let accept = try append(.accept)
        let start = try compile(node, continuingAt: accept)
        return XSDRegexNFA(
            states: states,
            startState: start,
            activeTransitionWorkLimit: limits.activeTransitionWork
        )
    }

    private mutating func compile(
        _ node: XSDRegexParser.Node,
        continuingAt continuation: Int
    ) throws -> Int {
        switch node {
        case .empty:
            return continuation
        case .atom(let characterClass):
            return try append(.consume(characterClass, next: continuation))
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
        _ child: XSDRegexParser.Node,
        minimum: Int,
        maximum: Int?,
        continuingAt continuation: Int
    ) throws -> Int {
        if maximum == 0 || child.matchesOnlyEmpty {
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

    private func stateCost(of node: XSDRegexParser.Node) throws -> Int {
        switch node {
        case .empty:
            return 0
        case .atom:
            return 1
        case .concatenation(let children):
            var total = 0
            for child in children {
                total = try checkedAdd(total, stateCost(of: child))
            }
            return total
        case .alternation(let branches):
            var total = branches.isEmpty ? 0 : branches.count - 1
            for branch in branches {
                total = try checkedAdd(total, stateCost(of: branch))
            }
            return total
        case .repetition(let child, let minimum, let maximum):
            guard maximum != 0, !child.matchesOnlyEmpty else {
                return 0
            }
            let childCost = try stateCost(of: child)
            if let maximum {
                let copies = try checkedMultiply(childCost, maximum)
                return try checkedAdd(copies, maximum - minimum)
            }
            let copies = try checkedAdd(minimum, 1)
            return try checkedAdd(
                checkedMultiply(childCost, copies),
                1
            )
        }
    }

    private func checkedAdd(_ lhs: Int, _ rhs: Int) throws -> Int {
        let (result, overflow) = lhs.addingReportingOverflow(rhs)
        guard !overflow, result <= limits.nfaStates else {
            throw XSDRegularExpression.Error.resourceLimit(
                name: "nfaStates",
                limit: limits.nfaStates,
                actual: overflow ? Int.max : result
            )
        }
        return result
    }

    private func checkedMultiply(_ lhs: Int, _ rhs: Int) throws -> Int {
        let (result, overflow) = lhs.multipliedReportingOverflow(by: rhs)
        guard !overflow, result <= limits.nfaStates else {
            throw XSDRegularExpression.Error.resourceLimit(
                name: "nfaStates",
                limit: limits.nfaStates,
                actual: overflow ? Int.max : result
            )
        }
        return result
    }

    private mutating func append(_ state: XSDRegexNFA.State) throws -> Int {
        let (actual, overflow) = states.count.addingReportingOverflow(1)
        guard !overflow, actual <= limits.nfaStates else {
            throw XSDRegularExpression.Error.resourceLimit(
                name: "nfaStates",
                limit: limits.nfaStates,
                actual: overflow ? Int.max : actual
            )
        }
        let index = states.count
        states.append(state)
        return index
    }
}
