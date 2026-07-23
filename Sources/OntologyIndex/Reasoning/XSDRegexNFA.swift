/// Immutable Thompson NFA produced by the XSD regular-expression compiler.
struct XSDRegexNFA: Sendable {
    enum State: Sendable {
        case consume(XSDRegexCharacterClass, next: Int)
        case epsilon(next: Int)
        case split(first: Int, second: Int)
        case accept
    }

    let states: [State]
    let startState: Int
    let activeTransitionWorkLimit: Int

    func wholeMatch<Input: StringProtocol>(_ input: Input) throws -> Bool {
        var budget = WorkBudget(limit: activeTransitionWorkLimit)
        var marks = [Int](repeating: 0, count: states.count)
        var generation = 1
        var current: [Int] = []
        var next: [Int] = []
        var closureStack: [Int] = []
        current.reserveCapacity(states.count)
        next.reserveCapacity(states.count)
        closureStack.reserveCapacity(states.count)

        try addClosure(
            from: startState,
            to: &current,
            marks: &marks,
            generation: generation,
            stack: &closureStack,
            budget: &budget
        )

        for scalar in input.unicodeScalars {
            next.removeAll(keepingCapacity: true)
            advanceGeneration(&generation, marks: &marks)

            for stateIndex in current {
                try budget.consume(1)
                guard case .consume(let characterClass, let successor) =
                    states[stateIndex]
                else {
                    continue
                }

                try budget.consume(characterClass.matchWork)
                guard characterClass.contains(scalar) else {
                    continue
                }

                try addClosure(
                    from: successor,
                    to: &next,
                    marks: &marks,
                    generation: generation,
                    stack: &closureStack,
                    budget: &budget
                )
            }

            guard !next.isEmpty else {
                return false
            }
            swap(&current, &next)
        }

        for stateIndex in current {
            try budget.consume(1)
            if case .accept = states[stateIndex] {
                return true
            }
        }
        return false
    }

    private func addClosure(
        from initialState: Int,
        to destination: inout [Int],
        marks: inout [Int],
        generation: Int,
        stack: inout [Int],
        budget: inout WorkBudget
    ) throws {
        stack.append(initialState)
        while let stateIndex = stack.popLast() {
            try budget.consume(1)
            guard marks[stateIndex] != generation else {
                continue
            }
            marks[stateIndex] = generation

            switch states[stateIndex] {
            case .consume, .accept:
                destination.append(stateIndex)
            case .epsilon(let successor):
                stack.append(successor)
            case .split(let first, let second):
                stack.append(second)
                stack.append(first)
            }
        }
    }

    private func advanceGeneration(
        _ generation: inout Int,
        marks: inout [Int]
    ) {
        if generation == Int.max {
            for index in marks.indices {
                marks[index] = 0
            }
            generation = 1
        } else {
            generation += 1
        }
    }

    private struct WorkBudget {
        let limit: Int
        private(set) var consumed = 0

        mutating func consume(_ amount: Int) throws {
            let (actual, overflow) = consumed.addingReportingOverflow(amount)
            guard !overflow, actual <= limit else {
                throw XSDRegularExpression.Error.resourceLimit(
                    name: "activeTransitionWork",
                    limit: limit,
                    actual: overflow ? Int.max : actual
                )
            }
            consumed = actual
        }
    }
}
