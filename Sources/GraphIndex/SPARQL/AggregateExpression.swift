import DatabaseTypes
import DatabaseKit
import DatabaseEngine

/// Canonical SPARQL aggregate algebra. Aggregate operands remain QueryIR plans
/// so every solution is evaluated by the same runtime expression semantics used
/// by FILTER, BIND, projection, and ORDER BY.
public enum AggregateExpression: Sendable {
    case count(
        expression: SPARQLExpressionPlan?,
        distinct: Bool,
        alias: String
    )
    case sum(
        expression: SPARQLExpressionPlan,
        distinct: Bool,
        alias: String
    )
    case avg(
        expression: SPARQLExpressionPlan,
        distinct: Bool,
        alias: String
    )
    case min(expression: SPARQLExpressionPlan, alias: String)
    case max(expression: SPARQLExpressionPlan, alias: String)
    case sample(expression: SPARQLExpressionPlan, alias: String)
    case groupConcat(
        expression: SPARQLExpressionPlan,
        separator: String,
        distinct: Bool,
        alias: String
    )

    public var alias: String {
        switch self {
        case .count(_, _, let alias),
             .sum(_, _, let alias),
             .avg(_, _, let alias),
             .min(_, let alias),
             .max(_, let alias),
             .sample(_, let alias),
             .groupConcat(_, _, _, let alias):
            return alias
        }
    }

    public var inputExpression: SPARQLExpressionPlan? {
        switch self {
        case .count(let expression, _, _):
            return expression
        case .sum(let expression, _, _),
             .avg(let expression, _, _),
             .min(let expression, _),
             .max(let expression, _),
             .sample(let expression, _),
             .groupConcat(let expression, _, _, _):
            return expression
        }
    }

    public var inputVariable: String? {
        inputExpression?.directVariable.map(Self.normalizedVariable)
    }

    public var isDistinct: Bool {
        switch self {
        case .count(_, let distinct, _),
             .sum(_, let distinct, _),
             .avg(_, let distinct, _),
             .groupConcat(_, _, let distinct, _):
            return distinct
        case .min, .max, .sample:
            return false
        }
    }

    func resultOwnership() throws -> SPARQLExpressionResultOwnership {
        switch self {
        case .min, .max, .sample:
            return .borrowed
        case .count, .sum, .avg, .groupConcat:
            return .produced(
                maximumFootprint: try CanonicalRelationalFootprintMeter
                    .maximumRDFTermValueFootprint(
                        maximumUTF8ByteCount: UInt64(
                            SPARQLExecutionLimits.maximumLiteralUTF8Count
                        )
                    )
            )
        }
    }

    /// Evaluates exactly one aggregate expression for a group. The supplied
    /// evaluator owns query-scoped state and the caller's transaction.
    func evaluate(
        groupIndex: Int,
        in partition: borrowing SPARQLGroupPartition,
        workMeter: DatabaseWorkMeter,
        evaluateExpression: @Sendable (
            SPARQLExpressionPlan,
            VariableBinding
        ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue>
    ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue?> {
        switch self {
        case .count(let expression, let distinct, _):
            return try await evaluateCount(
                groupIndex: groupIndex,
                in: partition,
                expression: expression,
                distinct: distinct,
                workMeter: workMeter,
                evaluateExpression: evaluateExpression
            )
        case .sum(let expression, let distinct, _):
            return try await evaluateSum(
                groupIndex: groupIndex,
                in: partition,
                expression: expression,
                distinct: distinct,
                workMeter: workMeter,
                evaluateExpression: evaluateExpression
            )
        case .avg(let expression, let distinct, _):
            return try await evaluateAverage(
                groupIndex: groupIndex,
                in: partition,
                expression: expression,
                distinct: distinct,
                workMeter: workMeter,
                evaluateExpression: evaluateExpression
            )
        case .min(let expression, _):
            return try await evaluateExtremum(
                groupIndex: groupIndex,
                in: partition,
                expression: expression,
                findMinimum: true,
                workMeter: workMeter,
                evaluateExpression: evaluateExpression
            )
        case .max(let expression, _):
            return try await evaluateExtremum(
                groupIndex: groupIndex,
                in: partition,
                expression: expression,
                findMinimum: false,
                workMeter: workMeter,
                evaluateExpression: evaluateExpression
            )
        case .sample(let expression, _):
            return try await evaluateSample(
                groupIndex: groupIndex,
                in: partition,
                expression: expression,
                workMeter: workMeter,
                evaluateExpression: evaluateExpression
            )
        case .groupConcat(
            let expression,
            let separator,
            let distinct,
            _
        ):
            return try await evaluateGroupConcat(
                groupIndex: groupIndex,
                in: partition,
                expression: expression,
                separator: separator,
                distinct: distinct,
                workMeter: workMeter,
                evaluateExpression: evaluateExpression
            )
        }
    }

    private func evaluateCount(
        groupIndex: Int,
        in partition: borrowing SPARQLGroupPartition,
        expression: SPARQLExpressionPlan?,
        distinct: Bool,
        workMeter: DatabaseWorkMeter,
        evaluateExpression: @Sendable (
            SPARQLExpressionPlan,
            VariableBinding
        ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue>
    ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue?> {
        var count: UInt64 = 0
        let memberRange = partition.memberRange(at: groupIndex)
        if let expression {
            let distinctValues = distinct
                ? try SPARQLRetainedFieldValueSet.make(workMeter: workMeter)
                : nil
            for memberIndex in memberRange {
                try workMeter.consume(at: .aggregateInput)
                let evaluated = try await partition.withMember(
                    at: memberIndex,
                    { binding in
                        try await recoverExpressionValue(
                            expression,
                            binding: copy binding,
                            evaluateExpression: evaluateExpression
                        )
                    }
                )
                let value: FieldValue
                switch evaluated {
                case .value(.some(let evaluatedValue)):
                    value = evaluatedValue
                case .value(.none):
                    continue
                case .expressionError(let error):
                    return .expressionError(error)
                }
                if let distinctValues {
                    try workMeter.consume(at: .deduplication)
                    guard try distinctValues.insert(value) else { continue }
                }
                count = try incrementedCount(count)
            }
        } else if distinct {
            var distinctSolutions = try SPARQLRetainedBindingSet.make(
                workMeter: workMeter,
                stage: .deduplication,
                expectedCount: memberRange.count
            )
            for memberIndex in memberRange {
                try workMeter.consume(at: .aggregateInput)
                let inserted = try await partition.withMember(
                    at: memberIndex
                ) { binding in
                    try workMeter.consume(at: .deduplication)
                    return try distinctSolutions.insert(binding)
                }
                guard inserted else { continue }
                count = try incrementedCount(count)
            }
        } else {
            try workMeter.consume(
                UInt64(memberRange.count),
                at: .aggregateInput
            )
            guard let exactCount = UInt64(exactly: memberRange.count) else {
                throw SPARQLQueryError.aggregateResultOutOfRange
            }
            count = exactCount
        }

        guard count <= UInt64(Int64.max),
              let numeric = SPARQLNumericValue(.int64(Int64(count))) else {
            throw SPARQLQueryError.aggregateResultOutOfRange
        }
        do throws(SPARQLNumericError) {
            return .value(try numeric.fieldValue())
        } catch {
            throw SPARQLQueryError.aggregateResultOutOfRange
        }
    }

    private func evaluateSum(
        groupIndex: Int,
        in partition: borrowing SPARQLGroupPartition,
        expression: SPARQLExpressionPlan,
        distinct: Bool,
        workMeter: DatabaseWorkMeter,
        evaluateExpression: @Sendable (
            SPARQLExpressionPlan,
            VariableBinding
        ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue>
    ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue?> {
        var sum: SPARQLNumericValue?
        let distinctValues = distinct
            ? try SPARQLRetainedFieldValueSet.make(workMeter: workMeter)
            : nil
        for memberIndex in partition.memberRange(at: groupIndex) {
            try workMeter.consume(at: .aggregateInput)
            let evaluated = try await partition.withMember(
                at: memberIndex
            ) { binding in
                try await evaluateRequiredValue(
                    expression,
                    binding: copy binding,
                    evaluateExpression: evaluateExpression
                )
            }
            let value: FieldValue
            switch evaluated {
            case .value(let evaluatedValue):
                value = evaluatedValue
            case .expressionError(let error):
                return .expressionError(error)
            }
            guard let numeric = SPARQLNumericValue(value) else {
                return .expressionError(
                    .typeError("SUM requires a numeric aggregate input")
                )
            }
            if let distinctValues {
                try workMeter.consume(at: .deduplication)
                guard try distinctValues.insert(value) else { continue }
            }
            do {
                sum = try sum?.applying(.add, to: numeric) ?? numeric
            } catch {
                throw SPARQLQueryError.aggregateResultOutOfRange
            }
        }
        guard let value = sum ?? SPARQLNumericValue(.int64(0)) else {
            throw SPARQLQueryError.aggregateResultOutOfRange
        }
        do throws(SPARQLNumericError) {
            return .value(try value.fieldValue())
        } catch {
            throw SPARQLQueryError.aggregateResultOutOfRange
        }
    }

    private func evaluateAverage(
        groupIndex: Int,
        in partition: borrowing SPARQLGroupPartition,
        expression: SPARQLExpressionPlan,
        distinct: Bool,
        workMeter: DatabaseWorkMeter,
        evaluateExpression: @Sendable (
            SPARQLExpressionPlan,
            VariableBinding
        ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue>
    ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue?> {
        var sum: SPARQLNumericValue?
        var count: Int64 = 0
        let distinctValues = distinct
            ? try SPARQLRetainedFieldValueSet.make(workMeter: workMeter)
            : nil
        for memberIndex in partition.memberRange(at: groupIndex) {
            try workMeter.consume(at: .aggregateInput)
            let evaluated = try await partition.withMember(
                at: memberIndex
            ) { binding in
                try await evaluateRequiredValue(
                    expression,
                    binding: copy binding,
                    evaluateExpression: evaluateExpression
                )
            }
            let value: FieldValue
            switch evaluated {
            case .value(let evaluatedValue):
                value = evaluatedValue
            case .expressionError(let error):
                return .expressionError(error)
            }
            guard let numeric = SPARQLNumericValue(value) else {
                return .expressionError(
                    .typeError("AVG requires a numeric aggregate input")
                )
            }
            if let distinctValues {
                try workMeter.consume(at: .deduplication)
                guard try distinctValues.insert(value) else { continue }
            }
            do {
                sum = try sum?.applying(.add, to: numeric) ?? numeric
            } catch {
                throw SPARQLQueryError.aggregateResultOutOfRange
            }
            let (next, overflow) = count.addingReportingOverflow(1)
            guard !overflow else {
                throw SPARQLQueryError.aggregateResultOutOfRange
            }
            count = next
        }

        guard count > 0, let sum,
              let divisor = SPARQLNumericValue(.int64(count)) else {
            guard let zero = SPARQLNumericValue(.int64(0)) else {
                throw SPARQLQueryError.aggregateResultOutOfRange
            }
            do throws(SPARQLNumericError) {
                return .value(try zero.fieldValue())
            } catch {
                throw SPARQLQueryError.aggregateResultOutOfRange
            }
        }
        do {
            return .value(
                try sum.applying(.divide, to: divisor).fieldValue()
            )
        } catch {
            throw SPARQLQueryError.aggregateResultOutOfRange
        }
    }

    private func evaluateExtremum(
        groupIndex: Int,
        in partition: borrowing SPARQLGroupPartition,
        expression: SPARQLExpressionPlan,
        findMinimum: Bool,
        workMeter: DatabaseWorkMeter,
        evaluateExpression: @Sendable (
            SPARQLExpressionPlan,
            VariableBinding
        ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue>
    ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue?> {
        var extremum: FieldValue?
        for memberIndex in partition.memberRange(at: groupIndex) {
            try workMeter.consume(at: .aggregateInput)
            let evaluated = try await partition.withMember(
                at: memberIndex
            ) { binding in
                try await evaluateRequiredValue(
                    expression,
                    binding: copy binding,
                    evaluateExpression: evaluateExpression
                )
            }
            let value: FieldValue
            switch evaluated {
            case .value(let evaluatedValue):
                value = evaluatedValue
            case .expressionError(let error):
                return .expressionError(error)
            }
            guard let current = extremum else {
                extremum = value
                continue
            }
            let comparison: SPARQLComparisonOrder
            do throws(SPARQLExpressionEvaluationError) {
                comparison = try SPARQLTermOrdering.compare(value, current)
            } catch let error {
                return .expressionError(error)
            }
            if (findMinimum && comparison == .ascending)
                || (!findMinimum && comparison == .descending) {
                extremum = value
            }
        }
        return .value(extremum)
    }

    private func evaluateSample(
        groupIndex: Int,
        in partition: borrowing SPARQLGroupPartition,
        expression: SPARQLExpressionPlan,
        workMeter: DatabaseWorkMeter,
        evaluateExpression: @Sendable (
            SPARQLExpressionPlan,
            VariableBinding
        ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue>
    ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue?> {
        for memberIndex in partition.memberRange(at: groupIndex) {
            try workMeter.consume(at: .aggregateInput)
            let outcome = try await partition.withMember(
                at: memberIndex
            ) { binding in
                try await evaluateExpression(expression, copy binding)
            }
            switch outcome {
            case .value(let value):
                if value != .null {
                    return .value(value)
                }
            case .expressionError(let evaluationError):
                if evaluationError.isSPARQLEvaluationError {
                    continue
                }
                return .expressionError(evaluationError)
            }
        }
        return .value(nil)
    }

    private func evaluateGroupConcat(
        groupIndex: Int,
        in partition: borrowing SPARQLGroupPartition,
        expression: SPARQLExpressionPlan,
        separator: String,
        distinct: Bool,
        workMeter: DatabaseWorkMeter,
        evaluateExpression: @Sendable (
            SPARQLExpressionPlan,
            VariableBinding
        ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue>
    ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue?> {
        let seen = distinct
            ? try SPARQLRetainedFieldValueSet.make(workMeter: workMeter)
            : nil
        var requiredUTF8Count: UInt64 = 0
        var result = ""
        var hasValue = false

        for memberIndex in partition.memberRange(at: groupIndex) {
            try workMeter.consume(at: .aggregateInput)
            let evaluated = try await partition.withMember(
                at: memberIndex
            ) { binding in
                try await evaluateRequiredValue(
                    expression,
                    binding: copy binding,
                    evaluateExpression: evaluateExpression
                )
            }
            let value: FieldValue
            switch evaluated {
            case .value(let evaluatedValue):
                value = evaluatedValue
            case .expressionError(let error):
                return .expressionError(error)
            }
            guard let lexicalForm = Self.stringValue(value) else {
                return .expressionError(
                    .typeError(
                        "GROUP_CONCAT requires an RDF term with a lexical form"
                    )
                )
            }
            if let seen {
                try workMeter.consume(at: .deduplication)
                guard try seen.insert(value) else { continue }
            }
            let separatorCount = hasValue ? separator.utf8.count : 0
            let required = UInt64(separatorCount) + UInt64(lexicalForm.utf8.count)
            let (next, overflow) = requiredUTF8Count.addingReportingOverflow(required)
            guard !overflow else {
                return .expressionError(
                    .resourceLimitExceeded(
                        stage: "GROUP_CONCAT",
                        required: UInt64.max,
                        maximum: UInt64(
                            SPARQLExecutionLimits.maximumLiteralUTF8Count
                        )
                    )
                )
            }
            guard next <= SPARQLExecutionLimits.maximumLiteralUTF8Count else {
                return .expressionError(
                    .resourceLimitExceeded(
                        stage: "GROUP_CONCAT",
                        required: next,
                        maximum: UInt64(
                            SPARQLExecutionLimits.maximumLiteralUTF8Count
                        )
                    )
                )
            }
            try workMeter.consume(required, at: .resultMaterialization)
            requiredUTF8Count = next
            if hasValue { result.append(separator) }
            result.append(lexicalForm)
            hasValue = true
        }
        do throws(SPARQLExpressionEvaluationError) {
            return .value(
                try ExpressionEvaluator.evaluate(
                    .literal(.string(result)),
                    binding: VariableBinding()
                )
            )
        } catch let error {
            return .expressionError(error)
        }
    }

    private func recoverExpressionValue(
        _ expression: SPARQLExpressionPlan,
        binding: VariableBinding,
        evaluateExpression: @Sendable (
            SPARQLExpressionPlan,
            VariableBinding
        ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue>
    ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue?> {
        switch try await evaluateExpression(expression, binding) {
        case .value(let value):
            return .value(value == .null ? nil : value)
        case .expressionError(let evaluationError):
            if evaluationError.isSPARQLEvaluationError {
                return .value(nil)
            }
            return .expressionError(evaluationError)
        }
    }

    private func incrementedCount(_ count: UInt64) throws -> UInt64 {
        let (next, overflow) = count.addingReportingOverflow(1)
        guard !overflow else {
            throw SPARQLQueryError.aggregateResultOutOfRange
        }
        return next
    }

    /// Every aggregate except COUNT evaluates its input as a regular SPARQL
    /// expression. A local expression error makes that aggregate result an
    /// error; the grouping operator decides whether the projection alias is
    /// left unbound. It must not silently discard the failing solution.
    private func evaluateRequiredValue(
        _ expression: SPARQLExpressionPlan,
        binding: VariableBinding,
        evaluateExpression: @Sendable (
            SPARQLExpressionPlan,
            VariableBinding
        ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue>
    ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue> {
        switch try await evaluateExpression(expression, binding) {
        case .value(let evaluated):
            guard evaluated != .null else {
                return .expressionError(
                    .typeError(
                        "aggregate input evaluated to a non-RDF null value"
                    )
                )
            }
            return .value(evaluated)
        case .expressionError(let evaluationError):
            return .expressionError(evaluationError)
        }
    }

    private static func stringValue(_ value: FieldValue) -> String? {
        switch value {
        case .rdfTerm(.iri(let value)):
            return value.rawValue
        case .rdfTerm(.literal(let literal)):
            return literal.lexicalForm
        case .string(let value):
            return value
        default:
            return nil
        }
    }

    private static func normalizedVariable(_ variable: String) -> String {
        variable.first == "?" ? variable : "?\(variable)"
    }
}

extension AggregateExpression {
    private static func variablePlan(
        _ variable: String
    ) throws -> SPARQLExpressionPlan {
        guard variable.first == "?" else {
            throw SPARQLQueryError.invalidVariable(variable)
        }
        let rawName = String(variable.dropFirst())
        do {
            _ = try SPARQLVariableName(rawName)
        } catch {
            throw SPARQLQueryError.invalidVariable(variable)
        }
        return try SPARQLExpressionPlan(
            .variable(Variable(rawName))
        )
    }

    public static func countAll(as alias: String) -> AggregateExpression {
        .count(expression: nil, distinct: false, alias: alias)
    }

    public static func countAllDistinct(as alias: String) -> AggregateExpression {
        .count(expression: nil, distinct: true, alias: alias)
    }

    public static func count(
        _ variable: String,
        as alias: String
    ) throws -> AggregateExpression {
        .count(
            expression: try variablePlan(variable),
            distinct: false,
            alias: alias
        )
    }

    public static func countDistinct(
        _ variable: String,
        as alias: String
    ) throws -> AggregateExpression {
        .count(
            expression: try variablePlan(variable),
            distinct: true,
            alias: alias
        )
    }

    public static func sum(
        _ variable: String,
        distinct: Bool = false,
        as alias: String
    ) throws -> AggregateExpression {
        .sum(
            expression: try variablePlan(variable),
            distinct: distinct,
            alias: alias
        )
    }

    public static func avg(
        _ variable: String,
        distinct: Bool = false,
        as alias: String
    ) throws -> AggregateExpression {
        .avg(
            expression: try variablePlan(variable),
            distinct: distinct,
            alias: alias
        )
    }

    public static func min(
        _ variable: String,
        as alias: String
    ) throws -> AggregateExpression {
        .min(expression: try variablePlan(variable), alias: alias)
    }

    public static func max(
        _ variable: String,
        as alias: String
    ) throws -> AggregateExpression {
        .max(expression: try variablePlan(variable), alias: alias)
    }

    public static func sample(
        _ variable: String,
        as alias: String
    ) throws -> AggregateExpression {
        .sample(expression: try variablePlan(variable), alias: alias)
    }

    public static func groupConcat(
        _ variable: String,
        separator: String = " ",
        as alias: String
    ) throws -> AggregateExpression {
        .groupConcat(
            expression: try variablePlan(variable),
            separator: separator,
            distinct: false,
            alias: alias
        )
    }

    public static func groupConcatDistinct(
        _ variable: String,
        separator: String = " ",
        as alias: String
    ) throws -> AggregateExpression {
        .groupConcat(
            expression: try variablePlan(variable),
            separator: separator,
            distinct: true,
            alias: alias
        )
    }
}

extension AggregateExpression: CustomStringConvertible {
    public var description: String {
        switch self {
        case .count(let expression, let distinct, let alias):
            return "(COUNT(\(distinct ? "DISTINCT " : "")\(expression.map(String.init(describing:)) ?? "*")) AS \(alias))"
        case .sum(let expression, let distinct, let alias):
            return "(SUM(\(distinct ? "DISTINCT " : "")\(expression)) AS \(alias))"
        case .avg(let expression, let distinct, let alias):
            return "(AVG(\(distinct ? "DISTINCT " : "")\(expression)) AS \(alias))"
        case .min(let expression, let alias):
            return "(MIN(\(expression)) AS \(alias))"
        case .max(let expression, let alias):
            return "(MAX(\(expression)) AS \(alias))"
        case .sample(let expression, let alias):
            return "(SAMPLE(\(expression)) AS \(alias))"
        case .groupConcat(let expression, let separator, let distinct, let alias):
            return "(GROUP_CONCAT(\(distinct ? "DISTINCT " : "")\(expression); separator=\"\(separator)\") AS \(alias))"
        }
    }
}
