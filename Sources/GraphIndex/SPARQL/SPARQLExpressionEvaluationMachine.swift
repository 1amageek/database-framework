import DatabaseTypes
import DatabaseEngine

/// Bounded, stack-independent evaluator shared by synchronous and
/// dataset-aware SPARQL execution.
///
/// The machine owns only small control frames and one contiguous operand
/// stack. An owned argument array is materialized only when values cross an
/// asynchronous runtime-resolver boundary.
struct SPARQLExpressionEvaluationMachine {
    typealias Outcome = SPARQLExpressionEvaluationOutcome<FieldValue>

    enum Action {
        case exists(handle: Int)
        case function(name: String, arguments: [FieldValue])
    }

    enum Step {
        case action(Action)
        case finished(Outcome)
    }

    private enum BooleanOutcome {
        case value(Bool)
        case expressionError(SPARQLExpressionEvaluationError)
    }

    private enum Task {
        case evaluate(Int)
        case continueStrict(
            node: Int,
            nextChild: Int,
            operandStart: Int
        )
        case conjunctionLeft(node: Int)
        case conjunctionRight(
            leftError: SPARQLExpressionEvaluationError?
        )
        case disjunctionLeft(node: Int)
        case disjunctionRight(
            leftError: SPARQLExpressionEvaluationError?
        )
        case conditionalBranch(node: Int)
        case caseCondition(node: Int, pairIndex: Int)
        case coalesce(node: Int, nextChild: Int)
        case membershipOperand(node: Int)
        case membershipCandidate(
            node: Int,
            nextChild: Int,
            left: FieldValue,
            firstError: SPARQLExpressionEvaluationError?
        )
    }

    private let program: SPARQLExpressionProgram
    private let binding: VariableBinding
    private var tasks: [Task]
    private var outcomes: [Outcome]
    private var operands: [FieldValue]
    private var awaitsRuntimeResolution: Bool

    init(
        program: SPARQLExpressionProgram,
        binding: VariableBinding
    ) {
        self.program = program
        self.binding = binding
        self.tasks = program.nodes.isEmpty ? [] : [.evaluate(0)]
        self.outcomes = []
        self.operands = []
        self.awaitsRuntimeResolution = false
        self.tasks.reserveCapacity(min(program.nodes.count, 64))
        self.outcomes.reserveCapacity(min(program.nodes.count, 16))
        self.operands.reserveCapacity(min(program.nodes.count, 16))
    }

    mutating func advance() -> Step {
        advance(beforeNodeEvaluation: {})
    }

    mutating func advance(
        workMeter: DatabaseWorkMeter
    ) throws -> Step {
        try advance {
            try workMeter.consume(at: .expressionEvaluation)
        }
    }

    private mutating func advance<Failure: Error>(
        beforeNodeEvaluation: () throws(Failure) -> Void
    ) throws(Failure) -> Step {
        guard !awaitsRuntimeResolution else {
            return .finished(
                .expressionError(
                    .runtimeInvariant(
                        "expression machine advanced before resolver completion"
                    )
                )
            )
        }

        while let task = tasks.popLast() {
            switch task {
            case .evaluate(let node):
                try beforeNodeEvaluation()
                if let action = evaluate(node: node) {
                    awaitsRuntimeResolution = true
                    return .action(action)
                }

            case .continueStrict(
                let node,
                let nextChild,
                let operandStart
            ):
                guard let outcome = outcomes.popLast() else {
                    return finishWithInvariant(
                        "strict expression child produced no outcome"
                    )
                }
                switch outcome {
                case .value(let value):
                    operands.append(value)
                case .expressionError(let error):
                    removeOperands(from: operandStart)
                    outcomes.append(.expressionError(error))
                    continue
                }

                let childCount = program.childCount(of: node)
                if nextChild < childCount {
                    tasks.append(
                        .continueStrict(
                            node: node,
                            nextChild: nextChild + 1,
                            operandStart: operandStart
                        )
                    )
                    tasks.append(
                        .evaluate(
                            program.child(of: node, at: nextChild)
                        )
                    )
                    continue
                }
                if let action = applyStrict(
                    node: node,
                    operandStart: operandStart
                ) {
                    awaitsRuntimeResolution = true
                    return .action(action)
                }

            case .conjunctionLeft(let node):
                guard let left = takeBooleanOutcome() else {
                    return finishWithInvariant(
                        "conjunction left operand produced no outcome"
                    )
                }
                switch left {
                case .value(false):
                    appendBoolean(false)
                case .value(true):
                    tasks.append(
                        .conjunctionRight(
                            leftError: nil
                        )
                    )
                    tasks.append(
                        .evaluate(program.child(of: node, at: 1))
                    )
                case .expressionError(let error):
                    guard error.isSPARQLEvaluationError else {
                        outcomes.append(.expressionError(error))
                        continue
                    }
                    tasks.append(
                        .conjunctionRight(
                            leftError: error
                        )
                    )
                    tasks.append(
                        .evaluate(program.child(of: node, at: 1))
                    )
                }

            case .conjunctionRight(let leftError):
                guard let right = takeBooleanOutcome() else {
                    return finishWithInvariant(
                        "conjunction right operand produced no outcome"
                    )
                }
                finishConjunction(
                    leftError: leftError,
                    right: right
                )

            case .disjunctionLeft(let node):
                guard let left = takeBooleanOutcome() else {
                    return finishWithInvariant(
                        "disjunction left operand produced no outcome"
                    )
                }
                switch left {
                case .value(true):
                    appendBoolean(true)
                case .value(false):
                    tasks.append(
                        .disjunctionRight(
                            leftError: nil
                        )
                    )
                    tasks.append(
                        .evaluate(program.child(of: node, at: 1))
                    )
                case .expressionError(let error):
                    guard error.isSPARQLEvaluationError else {
                        outcomes.append(.expressionError(error))
                        continue
                    }
                    tasks.append(
                        .disjunctionRight(
                            leftError: error
                        )
                    )
                    tasks.append(
                        .evaluate(program.child(of: node, at: 1))
                    )
                }

            case .disjunctionRight(let leftError):
                guard let right = takeBooleanOutcome() else {
                    return finishWithInvariant(
                        "disjunction right operand produced no outcome"
                    )
                }
                finishDisjunction(
                    leftError: leftError,
                    right: right
                )

            case .conditionalBranch(let node):
                guard let condition = takeBooleanOutcome() else {
                    return finishWithInvariant(
                        "conditional expression produced no condition"
                    )
                }
                switch condition {
                case .value(let value):
                    tasks.append(
                        .evaluate(
                            program.child(
                                of: node,
                                at: value ? 1 : 2
                            )
                        )
                    )
                case .expressionError(let error):
                    outcomes.append(.expressionError(error))
                }

            case .caseCondition(let node, let pairIndex):
                guard let condition = takeBooleanOutcome() else {
                    return finishWithInvariant(
                        "CASE expression produced no condition"
                    )
                }
                switch condition {
                case .value(true):
                    tasks.append(
                        .evaluate(
                            program.child(
                                of: node,
                                at: pairIndex * 2 + 1
                            )
                        )
                    )
                case .value(false):
                    scheduleCaseCondition(
                        node: node,
                        pairIndex: pairIndex + 1
                    )
                case .expressionError(let error):
                    outcomes.append(.expressionError(error))
                }

            case .coalesce(let node, let nextChild):
                guard let outcome = outcomes.popLast() else {
                    return finishWithInvariant(
                        "COALESCE child produced no outcome"
                    )
                }
                switch outcome {
                case .value(let value) where value != .null:
                    outcomes.append(.value(value))
                case .value:
                    scheduleCoalesce(node: node, child: nextChild)
                case .expressionError(let error):
                    if error.isSPARQLEvaluationError {
                        scheduleCoalesce(
                            node: node,
                            child: nextChild
                        )
                    } else {
                        outcomes.append(.expressionError(error))
                    }
                }

            case .membershipOperand(let node):
                guard let outcome = outcomes.popLast() else {
                    return finishWithInvariant(
                        "membership operand produced no outcome"
                    )
                }
                switch outcome {
                case .expressionError(let error):
                    outcomes.append(.expressionError(error))
                case .value(let left):
                    guard program.childCount(of: node) > 1 else {
                        appendMembershipBoolean(
                            node: node,
                            matched: false
                        )
                        continue
                    }
                    tasks.append(
                        .membershipCandidate(
                            node: node,
                            nextChild: 2,
                            left: left,
                            firstError: nil
                        )
                    )
                    tasks.append(
                        .evaluate(program.child(of: node, at: 1))
                    )
                }

            case .membershipCandidate(
                let node,
                let nextChild,
                let left,
                let existingError
            ):
                guard let outcome = outcomes.popLast() else {
                    return finishWithInvariant(
                        "membership candidate produced no outcome"
                    )
                }
                var firstError = existingError
                switch outcome {
                case .expressionError(let error):
                    guard error.isSPARQLEvaluationError else {
                        outcomes.append(.expressionError(error))
                        continue
                    }
                    if firstError == nil {
                        firstError = error
                    }
                case .value(let right):
                    do throws(SPARQLExpressionEvaluationError) {
                        if try ExpressionEvaluator.equalFieldValues(
                            left,
                            right
                        ) {
                            appendMembershipBoolean(
                                node: node,
                                matched: true
                            )
                            continue
                        }
                    } catch let error {
                        guard error.isSPARQLEvaluationError else {
                            outcomes.append(.expressionError(error))
                            continue
                        }
                        if firstError == nil {
                            firstError = error
                        }
                    }
                }

                if nextChild < program.childCount(of: node) {
                    tasks.append(
                        .membershipCandidate(
                            node: node,
                            nextChild: nextChild + 1,
                            left: left,
                            firstError: firstError
                        )
                    )
                    tasks.append(
                        .evaluate(
                            program.child(of: node, at: nextChild)
                        )
                    )
                } else if let firstError {
                    outcomes.append(.expressionError(firstError))
                } else {
                    appendMembershipBoolean(
                        node: node,
                        matched: false
                    )
                }
            }
        }

        guard operands.isEmpty else {
            return finishWithInvariant(
                "expression machine retained completed operands"
            )
        }
        guard outcomes.count == 1, let outcome = outcomes.popLast() else {
            return finishWithInvariant(
                "expression machine completed without one result"
            )
        }
        return .finished(outcome)
    }

    mutating func resume(with outcome: Outcome) {
        guard awaitsRuntimeResolution else {
            awaitsRuntimeResolution = false
            tasks.removeAll(keepingCapacity: true)
            operands.removeAll(keepingCapacity: true)
            outcomes = [
                .expressionError(
                    .runtimeInvariant(
                        "expression resolver completed without a pending action"
                    )
                ),
            ]
            return
        }
        awaitsRuntimeResolution = false
        outcomes.append(outcome)
    }

    private mutating func evaluate(node: Int) -> Action? {
        guard program.nodes.indices.contains(node) else {
            outcomes.append(
                .expressionError(
                    .runtimeInvariant(
                        "expression program referenced an invalid node"
                    )
                )
            )
            return nil
        }
        let opcode = program.nodes[node].opcode
        switch opcode {
        case .conjunction:
            guard requireChildCount(2, node: node) else {
                return nil
            }
            tasks.append(.conjunctionLeft(node: node))
            tasks.append(.evaluate(program.child(of: node, at: 0)))

        case .disjunction:
            guard requireChildCount(2, node: node) else {
                return nil
            }
            tasks.append(.disjunctionLeft(node: node))
            tasks.append(.evaluate(program.child(of: node, at: 0)))

        case .conditional:
            guard requireChildCount(3, node: node) else {
                return nil
            }
            tasks.append(.conditionalBranch(node: node))
            tasks.append(.evaluate(program.child(of: node, at: 0)))

        case .caseSelection:
            scheduleCaseCondition(node: node, pairIndex: 0)

        case .coalesce:
            scheduleCoalesce(node: node, child: 0)

        case .membership:
            guard program.childCount(of: node) >= 1 else {
                outcomes.append(
                    .expressionError(
                        .runtimeInvariant(
                            "membership expression has no operand"
                        )
                    )
                )
                return nil
            }
            tasks.append(.membershipOperand(node: node))
            tasks.append(.evaluate(program.child(of: node, at: 0)))

        case .exists(let handle):
            guard program.childCount(of: node) == 0 else {
                outcomes.append(
                    .expressionError(
                        .runtimeInvariant(
                            "EXISTS expression unexpectedly has children"
                        )
                    )
                )
                return nil
            }
            return .exists(handle: handle)

        default:
            let childCount = program.childCount(of: node)
            let operandStart = operands.count
            guard childCount > 0 else {
                return applyStrict(
                    node: node,
                    operandStart: operandStart
                )
            }
            tasks.append(
                .continueStrict(
                    node: node,
                    nextChild: 1,
                    operandStart: operandStart
                )
            )
            tasks.append(.evaluate(program.child(of: node, at: 0)))
        }
        return nil
    }

    private mutating func applyStrict(
        node: Int,
        operandStart: Int
    ) -> Action? {
        let opcode = program.nodes[node].opcode
        let operandRange = operandStart..<operands.count
        if case .function(
            let name,
            let identifier,
            let distinct
        ) = opcode,
           requiresRuntimeResolver(identifier) {
            guard !distinct else {
                removeOperands(from: operandStart)
                outcomes.append(
                    .expressionError(
                        .invalidFunctionArguments(name)
                    )
                )
                return nil
            }

            // The async resolver may retain arguments after this synchronous
            // machine step. Materialization is therefore the ownership
            // boundary, not an intermediate evaluation copy.
            let arguments = Array(operands[operandRange])
            removeOperands(from: operandStart)
            return .function(name: name, arguments: arguments)
        }

        do throws(SPARQLExpressionEvaluationError) {
            let value = try ExpressionEvaluator.evaluateImmediate(
                opcode,
                operands: operands[operandRange],
                binding: binding
            )
            removeOperands(from: operandStart)
            outcomes.append(.value(value))
        } catch let error {
            removeOperands(from: operandStart)
            outcomes.append(.expressionError(error))
        }
        return nil
    }

    private func requiresRuntimeResolver(
        _ identifier: SPARQLFunctionIdentifier
    ) -> Bool {
        switch identifier {
        case .extensionFunction:
            return true
        case .datatypeConstructor:
            return false
        case .builtIn(let builtIn):
            switch builtIn {
            case .now, .rand, .uuid, .strUUID, .blankNode:
                return true
            default:
                return false
            }
        }
    }

    private mutating func scheduleCaseCondition(
        node: Int,
        pairIndex: Int
    ) {
        guard case .caseSelection(
            let pairCount,
            let hasElseResult
        ) = program.nodes[node].opcode else {
            outcomes.append(
                .expressionError(
                    .runtimeInvariant(
                        "CASE frame references a non-CASE opcode"
                    )
                )
            )
            return
        }
        let (pairChildren, pairCountOverflow) = pairCount
            .multipliedReportingOverflow(by: 2)
        let (expectedChildren, childCountOverflow) = pairChildren
            .addingReportingOverflow(hasElseResult ? 1 : 0)
        guard !pairCountOverflow, !childCountOverflow else {
            outcomes.append(
                .expressionError(
                    .runtimeInvariant(
                        "CASE expression child count overflowed"
                    )
                )
            )
            return
        }
        guard program.childCount(of: node) == expectedChildren else {
            outcomes.append(
                .expressionError(
                    .runtimeInvariant(
                        "CASE expression child count is inconsistent"
                    )
                )
            )
            return
        }
        if pairIndex < pairCount {
            tasks.append(
                .caseCondition(node: node, pairIndex: pairIndex)
            )
            tasks.append(
                .evaluate(
                    program.child(of: node, at: pairIndex * 2)
                )
            )
        } else if hasElseResult {
            tasks.append(
                .evaluate(
                    program.child(of: node, at: pairCount * 2)
                )
            )
        } else {
            outcomes.append(.value(.null))
        }
    }

    private mutating func scheduleCoalesce(
        node: Int,
        child: Int
    ) {
        let childCount = program.childCount(of: node)
        guard child < childCount else {
            outcomes.append(
                .expressionError(
                    .typeError(
                        "COALESCE has no expression without an error"
                    )
                )
            )
            return
        }
        tasks.append(.coalesce(node: node, nextChild: child + 1))
        tasks.append(.evaluate(program.child(of: node, at: child)))
    }

    private mutating func takeBooleanOutcome() -> BooleanOutcome? {
        guard let outcome = outcomes.popLast() else {
            return nil
        }
        switch outcome {
        case .expressionError(let error):
            return .expressionError(error)
        case .value(let value):
            do throws(SPARQLExpressionEvaluationError) {
                return .value(
                    try ExpressionEvaluator.effectiveBooleanValue(value)
                )
            } catch let error {
                return .expressionError(error)
            }
        }
    }

    private mutating func finishConjunction(
        leftError: SPARQLExpressionEvaluationError?,
        right: BooleanOutcome
    ) {
        guard let leftError else {
            appendBooleanOutcome(right)
            return
        }
        switch right {
        case .value(false):
            appendBoolean(false)
        case .value(true):
            outcomes.append(.expressionError(leftError))
        case .expressionError(let rightError):
            outcomes.append(
                .expressionError(
                    rightError.isSPARQLEvaluationError
                        ? leftError
                        : rightError
                )
            )
        }
    }

    private mutating func finishDisjunction(
        leftError: SPARQLExpressionEvaluationError?,
        right: BooleanOutcome
    ) {
        guard let leftError else {
            appendBooleanOutcome(right)
            return
        }
        switch right {
        case .value(true):
            appendBoolean(true)
        case .value(false):
            outcomes.append(.expressionError(leftError))
        case .expressionError(let rightError):
            outcomes.append(
                .expressionError(
                    rightError.isSPARQLEvaluationError
                        ? leftError
                        : rightError
                )
            )
        }
    }

    private mutating func appendBooleanOutcome(
        _ outcome: BooleanOutcome
    ) {
        switch outcome {
        case .value(let value):
            appendBoolean(value)
        case .expressionError(let error):
            outcomes.append(.expressionError(error))
        }
    }

    private mutating func appendBoolean(_ value: Bool) {
        do throws(SPARQLExpressionEvaluationError) {
            outcomes.append(
                .value(
                    try ExpressionEvaluator.canonicalBoolean(value)
                )
            )
        } catch let error {
            outcomes.append(.expressionError(error))
        }
    }

    private mutating func appendMembershipBoolean(
        node: Int,
        matched: Bool
    ) {
        guard case .membership(let negated)
                = program.nodes[node].opcode else {
            outcomes.append(
                .expressionError(
                    .runtimeInvariant(
                        "membership frame references another opcode"
                    )
                )
            )
            return
        }
        appendBoolean(matched ? !negated : negated)
    }

    private mutating func requireChildCount(
        _ expected: Int,
        node: Int
    ) -> Bool {
        guard program.childCount(of: node) == expected else {
            outcomes.append(
                .expressionError(
                    .runtimeInvariant(
                        "expression opcode expected \(expected) children"
                    )
                )
            )
            return false
        }
        return true
    }

    private mutating func removeOperands(from start: Int) {
        guard start < operands.endIndex else {
            return
        }
        operands.removeSubrange(start..<operands.endIndex)
    }

    private mutating func finishWithInvariant(
        _ detail: String
    ) -> Step {
        tasks.removeAll(keepingCapacity: true)
        operands.removeAll(keepingCapacity: true)
        outcomes.removeAll(keepingCapacity: true)
        return .finished(.expressionError(.runtimeInvariant(detail)))
    }
}
