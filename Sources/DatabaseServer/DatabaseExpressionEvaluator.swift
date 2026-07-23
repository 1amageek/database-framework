import DatabaseValue
import QueryIR

struct DatabaseExpressionEvaluator: Sendable {
    let fields: [String: DatabaseValue]

    init(fields: [String: DatabaseValue]) {
        self.fields = fields
    }

    func predicate(_ expression: Expression) throws -> Bool {
        try truth(evaluate(expression)) == .true
    }

    func evaluate(_ expression: Expression) throws -> DatabaseValue {
        switch expression {
        case .literal(let literal):
            return try value(literal)
        case .column(let column):
            if let value = fields[column.displayName] ?? fields[column.column] {
                return value
            }
            throw DatabaseExpressionEvaluationError.missingColumn(column.displayName)
        case .variable(let variable):
            guard let value = fields[variable.name] else {
                throw DatabaseExpressionEvaluationError.missingColumn(variable.name)
            }
            return value
        case .parameter:
            throw DatabaseExpressionEvaluationError.unboundParameter
        case .add(let lhs, let rhs):
            return try arithmetic(lhs, rhs, operation: .add)
        case .subtract(let lhs, let rhs):
            return try arithmetic(lhs, rhs, operation: .subtract)
        case .multiply(let lhs, let rhs):
            return try arithmetic(lhs, rhs, operation: .multiply)
        case .divide(let lhs, let rhs):
            return try arithmetic(lhs, rhs, operation: .divide)
        case .modulo(let lhs, let rhs):
            return try arithmetic(lhs, rhs, operation: .modulo)
        case .negate(let nested):
            return try negate(evaluate(nested))
        case .equal(let lhs, let rhs):
            return try equality(lhs, rhs, expectedEqual: true)
        case .notEqual(let lhs, let rhs):
            return try equality(lhs, rhs, expectedEqual: false)
        case .lessThan(let lhs, let rhs):
            return try ordering(lhs, rhs) { $0 == .less }
        case .lessThanOrEqual(let lhs, let rhs):
            return try ordering(lhs, rhs) { $0 != .greater }
        case .greaterThan(let lhs, let rhs):
            return try ordering(lhs, rhs) { $0 == .greater }
        case .greaterThanOrEqual(let lhs, let rhs):
            return try ordering(lhs, rhs) { $0 != .less }
        case .and(let lhs, let rhs):
            return truthValue(try truth(evaluate(lhs)).and(truth(evaluate(rhs))))
        case .or(let lhs, let rhs):
            return truthValue(try truth(evaluate(lhs)).or(truth(evaluate(rhs))))
        case .not(let nested):
            return truthValue(try truth(evaluate(nested)).not)
        case .isNull(let nested):
            return .bool(try evaluate(nested).isNull)
        case .isNotNull(let nested):
            return .bool(!((try evaluate(nested)).isNull))
        case .bound(let variable):
            return .bool(fields[variable.name]?.isNull == false)
        case .like(let nested, let pattern):
            let candidate = try evaluate(nested)
            if candidate.isNull { return .null }
            guard case .string(let value) = candidate else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "LIKE")
            }
            return .bool(Self.matchesLike(value, pattern: pattern))
        case .regex:
            throw DatabaseExpressionEvaluationError.unsupportedExpression("REGEX")
        case .between(let nested, let low, let high):
            let lower = try ordering(nested, low) { $0 != .less }
            let upper = try ordering(nested, high) { $0 != .greater }
            return truthValue(try truth(lower).and(truth(upper)))
        case .inList(let nested, let values):
            return try listMembership(nested, values: values, negated: false)
        case .notInList(let nested, let values):
            return try listMembership(nested, values: values, negated: true)
        case .caseWhen(let pairs, let fallback):
            for pair in pairs where try predicate(pair.condition) {
                return try evaluate(pair.result)
            }
            return try fallback.map(evaluate) ?? .null
        case .coalesce(let values):
            for expression in values {
                let candidate = try evaluate(expression)
                if !candidate.isNull { return candidate }
            }
            return .null
        case .nullIf(let lhs, let rhs):
            let left = try evaluate(lhs)
            let comparison = try equalityValues(left, evaluate(rhs))
            return comparison == true ? .null : left
        case .cast(let nested, let target):
            return try cast(evaluate(nested), to: target)
        case .function(let function):
            return try evaluate(function)
        case .inSubquery, .aggregate, .triple, .isTriple, .subject, .predicate,
             .object, .subquery, .exists:
            throw DatabaseExpressionEvaluationError.unsupportedExpression(
                String(describing: expression)
            )
        }
    }

    private func value(_ literal: Literal) throws -> DatabaseValue {
        switch literal {
        case .null: return .null
        case .bool(let value): return .bool(value)
        case .int(let value): return .int64(value)
        case .uint(let value): return .uint64(value)
        case .decimal(let coefficient, let scale):
            return .decimal(coefficient: coefficient, scale: scale)
        case .double(let value): return .double(value)
        case .string(let value): return .string(value)
        case .date(let value): return .date(value)
        case .timestamp(let value): return .timestamp(value)
        case .binary(let value): return .bytes(value)
        case .uuid(let value): return .uuid(value)
        case .array(let values): return .array(try values.map(value))
        case .iri(let value): return .rdfTerm(.iri(value))
        case .blankNode(let value): return .rdfTerm(.blankNode(value))
        case .rdfTerm(let value): return .rdfTerm(value)
        case .typedLiteral(let value, let datatype):
            do {
                return .rdfTerm(
                    .literal(try DatabaseRDFLiteral(
                        lexicalForm: value,
                        datatype: datatype
                    ))
                )
            } catch {
                throw DatabaseExpressionEvaluationError.invalidRDFLiteral(
                    datatype: datatype
                )
            }
        case .langLiteral(let value, let language):
            let tag: DatabaseRDFLanguageTag
            do {
                tag = try DatabaseRDFLanguageTag(language)
            } catch {
                throw DatabaseExpressionEvaluationError.invalidRDFLiteral(
                    datatype: DatabaseRDFIRI.rdfLanguageString.rawValue
                )
            }
            return .rdfTerm(
                .literal(
                    DatabaseRDFLiteral(
                        lexicalForm: value,
                        language: tag
                    )
                )
            )
        case .dirLangLiteral(let value, let language, let direction):
            let tag: DatabaseRDFLanguageTag
            do {
                tag = try DatabaseRDFLanguageTag(language)
            } catch {
                throw DatabaseExpressionEvaluationError.invalidRDFLiteral(
                    datatype: DatabaseRDFIRI.rdfDirectionalLanguageString
                        .rawValue
                )
            }
            guard let baseDirection = DatabaseRDFDirection(
                rawValue: direction
            ) else {
                throw DatabaseExpressionEvaluationError.invalidRDFLiteral(
                    datatype: DatabaseRDFIRI.rdfDirectionalLanguageString
                        .rawValue
                )
            }
            return .rdfTerm(
                .literal(
                    DatabaseRDFLiteral(
                        lexicalForm: value,
                        language: tag,
                        direction: baseDirection
                    )
                )
            )
        }
    }

    private func equality(
        _ lhs: Expression,
        _ rhs: Expression,
        expectedEqual: Bool
    ) throws -> DatabaseValue {
        guard let equal = try equalityValues(evaluate(lhs), evaluate(rhs)) else {
            return .null
        }
        return .bool(equal == expectedEqual)
    }

    private func equalityValues(
        _ lhs: DatabaseValue,
        _ rhs: @autoclosure () throws -> DatabaseValue
    ) throws -> Bool? {
        let rhs = try rhs()
        if lhs.isNull || rhs.isNull { return nil }
        if let ordering = exactNumericOrdering(lhs, rhs) { return ordering == .equal }
        switch (lhs, rhs) {
        case (.double(let left), .double(let right)):
            return left == right
        default:
            return lhs == rhs
        }
    }

    private func ordering(
        _ lhs: Expression,
        _ rhs: Expression,
        predicate: (Ordering) -> Bool
    ) throws -> DatabaseValue {
        let left = try evaluate(lhs)
        let right = try evaluate(rhs)
        if left.isNull || right.isNull { return .null }
        return .bool(predicate(try compare(left, right)))
    }

    private func compare(
        _ lhs: DatabaseValue,
        _ rhs: DatabaseValue
    ) throws -> Ordering {
        if let value = exactNumericOrdering(lhs, rhs) { return value }
        switch (lhs, rhs) {
        case (.double(let left), .double(let right)):
            return ordering(left, right)
        case (.string(let left), .string(let right)):
            return ordering(left, right)
        case (.bool(let left), .bool(let right)):
            return ordering(left ? 1 : 0, right ? 1 : 0)
        case (.bytes(let left), .bytes(let right)):
            return lexicographicOrdering(left, right)
        case (.date(let left), .date(let right)):
            return ordering(left, right)
        case (.timestamp(let left), .timestamp(let right)):
            return ordering(left, right)
        case (.uuid(let left), .uuid(let right)):
            return ordering(left, right)
        default:
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "ordering")
        }
    }

    private func exactNumericOrdering(
        _ lhs: DatabaseValue,
        _ rhs: DatabaseValue
    ) -> Ordering? {
        guard let left = numericLiteral(lhs),
              let right = numericLiteral(rhs),
              let comparison = left.compareExactNumeric(to: right) else {
            return nil
        }
        if comparison < 0 { return .less }
        if comparison > 0 { return .greater }
        return .equal
    }

    private func numericLiteral(_ value: DatabaseValue) -> Literal? {
        switch value {
        case .int64(let scalar): return .int(scalar)
        case .uint64(let scalar): return .uint(scalar)
        case .decimal(let coefficient, let scale):
            return .decimal(coefficient: coefficient, scale: scale)
        default: return nil
        }
    }

    private func arithmetic(
        _ lhs: Expression,
        _ rhs: Expression,
        operation: ArithmeticOperation
    ) throws -> DatabaseValue {
        let left = try evaluate(lhs)
        let right = try evaluate(rhs)
        if left.isNull || right.isNull { return .null }

        switch (left, right) {
        case let (left, right)
            where left.isExactDecimal || right.isExactDecimal:
            guard let lhs = DatabaseExactDecimal(left),
                  let rhs = DatabaseExactDecimal(right) else {
                throw DatabaseExpressionEvaluationError.numericOverflow
            }
            let result = try performExactDecimalOperation {
                () throws(DatabaseExactDecimalError) -> DatabaseExactDecimal in
                switch operation {
                case .add: try lhs.adding(rhs)
                case .subtract: try lhs.subtracting(rhs)
                case .multiply: try lhs.multiplying(by: rhs)
                case .divide: try lhs.dividing(by: rhs)
                case .modulo: try lhs.remainder(dividingBy: rhs)
                }
            }
            return result.databaseValue
        case (.int64(let lhs), .int64(let rhs)):
            return .int64(try operation.apply(lhs, rhs))
        case (.uint64(let lhs), .uint64(let rhs)):
            return .uint64(try operation.apply(lhs, rhs))
        case (.double(let lhs), .double(let rhs)):
            let result = try operation.apply(lhs, rhs)
            guard result.isFinite else {
                throw DatabaseExpressionEvaluationError.numericOverflow
            }
            return .double(result)
        default:
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: operation.name)
        }
    }

    private func negate(_ value: DatabaseValue) throws -> DatabaseValue {
        switch value {
        case .null:
            return .null
        case .int64(let scalar):
            guard scalar != Int64.min else {
                throw DatabaseExpressionEvaluationError.numericOverflow
            }
            return .int64(-scalar)
        case .double(let scalar):
            return .double(-scalar)
        case .decimal:
            guard let value = DatabaseExactDecimal(value) else {
                throw DatabaseExpressionEvaluationError.numericOverflow
            }
            return try performExactDecimalOperation {
                () throws(DatabaseExactDecimalError) -> DatabaseExactDecimal in
                try value.negated()
            }.databaseValue
        default:
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "negation")
        }
    }

    private func listMembership(
        _ nested: Expression,
        values: [Expression],
        negated: Bool
    ) throws -> DatabaseValue {
        let candidate = try evaluate(nested)
        if candidate.isNull { return .null }
        var sawUnknown = false
        for expression in values {
            guard let equal = try equalityValues(candidate, evaluate(expression)) else {
                sawUnknown = true
                continue
            }
            if equal { return .bool(!negated) }
        }
        if sawUnknown { return .null }
        return .bool(negated)
    }

    private func evaluate(_ function: FunctionCall) throws -> DatabaseValue {
        let name = function.name.uppercased()
        switch name {
        case "LOWER", "UPPER":
            guard function.arguments.count == 1 else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: name)
            }
            let argument = try evaluate(function.arguments[0])
            if argument.isNull { return .null }
            guard case .string(let value) = argument else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: name)
            }
            return .string(name == "LOWER" ? value.lowercased() : value.uppercased())
        case "LENGTH":
            guard function.arguments.count == 1 else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: name)
            }
            let argument = try evaluate(function.arguments[0])
            if argument.isNull { return .null }
            switch argument {
            case .string(let value): return .int64(Int64(value.count))
            case .bytes(let value): return .int64(Int64(value.count))
            case .array(let value): return .int64(Int64(value.count))
            default:
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: name)
            }
        case "ABS":
            guard function.arguments.count == 1 else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: name)
            }
            let argument = try evaluate(function.arguments[0])
            switch argument {
            case .null: return .null
            case .int64(let value):
                guard value != Int64.min else {
                    throw DatabaseExpressionEvaluationError.numericOverflow
                }
                return .int64(value < 0 ? -value : value)
            case .uint64: return argument
            case .double(let value): return .double(value.magnitude)
            case .decimal:
                guard let value = DatabaseExactDecimal(argument) else {
                    throw DatabaseExpressionEvaluationError.numericOverflow
                }
                return try performExactDecimalOperation {
                    () throws(DatabaseExactDecimalError) -> DatabaseExactDecimal in
                    try value.magnitude()
                }.databaseValue
            default:
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: name)
            }
        default:
            throw DatabaseExpressionEvaluationError.unsupportedFunction(function.name)
        }
    }

    private func cast(
        _ value: DatabaseValue,
        to target: DataType
    ) throws -> DatabaseValue {
        if value.isNull { return .null }
        switch target {
        case .boolean:
            guard case .bool = value else { throw invalidCast(target) }
            return value
        case .smallint, .integer, .bigint:
            if case .int64 = value { return value }
            if case .uint64(let scalar) = value, let signed = Int64(exactly: scalar) {
                return .int64(signed)
            }
            throw invalidCast(target)
        case .real, .doublePrecision:
            guard case .double = value else { throw invalidCast(target) }
            return value
        case .char, .varchar, .text:
            guard case .string = value else { throw invalidCast(target) }
            return value
        case .date:
            guard case .date = value else { throw invalidCast(target) }
            return value
        case .timestamp:
            guard case .timestamp = value else { throw invalidCast(target) }
            return value
        case .binary, .varbinary, .blob:
            guard case .bytes = value else { throw invalidCast(target) }
            return value
        case .uuid:
            if case .uuid = value { return value }
            if case .string(let string) = value,
               let uuid = DatabaseUUID(canonicalString: string) {
                return .uuid(uuid)
            }
            throw invalidCast(target)
        case .array:
            guard case .array = value else { throw invalidCast(target) }
            return value
        case .decimal:
            if case .decimal = value { return value }
            if let exact = DatabaseExactDecimal(value) {
                return exact.databaseValue
            }
            if case .string(let lexicalForm) = value,
               case .decimal(let coefficient, let scale) = Literal.parseDecimal(lexicalForm) {
                return .decimal(coefficient: coefficient, scale: scale)
            }
            throw invalidCast(target)
        case .time, .interval, .json, .jsonb, .custom:
            throw invalidCast(target)
        }
    }

    private func invalidCast(_ type: DataType) -> DatabaseExpressionEvaluationError {
        .invalidCast(String(describing: type))
    }

    private func truth(_ value: DatabaseValue) throws -> TruthValue {
        switch value {
        case .null: return .unknown
        case .bool(true): return .true
        case .bool(false): return .false
        default:
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "boolean evaluation")
        }
    }

    private func truthValue(_ value: TruthValue) -> DatabaseValue {
        switch value {
        case .true: return .bool(true)
        case .false: return .bool(false)
        case .unknown: return .null
        }
    }

    private func ordering<Value: Comparable>(_ lhs: Value, _ rhs: Value) -> Ordering {
        if lhs < rhs { return .less }
        if lhs > rhs { return .greater }
        return .equal
    }

    private func lexicographicOrdering(
        _ lhs: DatabaseBytes,
        _ rhs: DatabaseBytes
    ) -> Ordering {
        lhs.withUnsafeBytes { lhsBytes in
            rhs.withUnsafeBytes { rhsBytes in
                for offset in 0..<min(lhsBytes.count, rhsBytes.count) {
                    if lhsBytes[offset] < rhsBytes[offset] { return .less }
                    if lhsBytes[offset] > rhsBytes[offset] { return .greater }
                }
                return ordering(lhsBytes.count, rhsBytes.count)
            }
        }
    }

    private static func matchesLike(_ value: String, pattern: String) -> Bool {
        let value = Array(value)
        let pattern = Array(pattern)
        var previous = Array(repeating: false, count: value.count + 1)
        previous[0] = true

        for token in pattern {
            var current = Array(repeating: false, count: value.count + 1)
            if token == "%" {
                current[0] = previous[0]
                if !value.isEmpty {
                    for index in 1...value.count {
                        current[index] = previous[index] || current[index - 1]
                    }
                }
            } else {
                if !value.isEmpty {
                    for index in 1...value.count {
                        current[index] = previous[index - 1]
                            && (token == "_" || token == value[index - 1])
                    }
                }
            }
            previous = current
        }
        return previous[value.count]
    }

    private func performExactDecimalOperation<T>(
        _ operation: () throws(DatabaseExactDecimalError) -> T
    ) throws -> T {
        do {
            return try operation()
        } catch let error {
            switch error {
            case .numericOverflow:
                throw DatabaseExpressionEvaluationError.numericOverflow
            case .divisionByZero:
                throw DatabaseExpressionEvaluationError.divisionByZero
            case .inexactResult:
                throw DatabaseExpressionEvaluationError.inexactDecimalResult
            }
        }
    }

    private enum Ordering {
        case less
        case equal
        case greater
    }

    private enum TruthValue {
        case `true`
        case `false`
        case unknown

        func and(_ other: Self) -> Self {
            if self == .false || other == .false { return .false }
            if self == .unknown || other == .unknown { return .unknown }
            return .true
        }

        func or(_ other: Self) -> Self {
            if self == .true || other == .true { return .true }
            if self == .unknown || other == .unknown { return .unknown }
            return .false
        }

        var not: Self {
            switch self {
            case .true: return .false
            case .false: return .true
            case .unknown: return .unknown
            }
        }
    }

    private enum ArithmeticOperation {
        case add
        case subtract
        case multiply
        case divide
        case modulo

        var name: String {
            switch self {
            case .add: return "addition"
            case .subtract: return "subtraction"
            case .multiply: return "multiplication"
            case .divide: return "division"
            case .modulo: return "modulo"
            }
        }

        func apply(_ lhs: Int64, _ rhs: Int64) throws -> Int64 {
            let result: (partialValue: Int64, overflow: Bool)
            switch self {
            case .add: result = lhs.addingReportingOverflow(rhs)
            case .subtract: result = lhs.subtractingReportingOverflow(rhs)
            case .multiply: result = lhs.multipliedReportingOverflow(by: rhs)
            case .divide:
                guard rhs != 0 else { throw DatabaseExpressionEvaluationError.divisionByZero }
                guard !(lhs == Int64.min && rhs == -1) else {
                    throw DatabaseExpressionEvaluationError.numericOverflow
                }
                return lhs / rhs
            case .modulo:
                guard rhs != 0 else { throw DatabaseExpressionEvaluationError.divisionByZero }
                guard !(lhs == Int64.min && rhs == -1) else { return 0 }
                return lhs % rhs
            }
            guard !result.overflow else {
                throw DatabaseExpressionEvaluationError.numericOverflow
            }
            return result.partialValue
        }

        func apply(_ lhs: UInt64, _ rhs: UInt64) throws -> UInt64 {
            let result: (partialValue: UInt64, overflow: Bool)
            switch self {
            case .add: result = lhs.addingReportingOverflow(rhs)
            case .subtract: result = lhs.subtractingReportingOverflow(rhs)
            case .multiply: result = lhs.multipliedReportingOverflow(by: rhs)
            case .divide:
                guard rhs != 0 else { throw DatabaseExpressionEvaluationError.divisionByZero }
                return lhs / rhs
            case .modulo:
                guard rhs != 0 else { throw DatabaseExpressionEvaluationError.divisionByZero }
                return lhs % rhs
            }
            guard !result.overflow else {
                throw DatabaseExpressionEvaluationError.numericOverflow
            }
            return result.partialValue
        }

        func apply(_ lhs: Double, _ rhs: Double) throws -> Double {
            switch self {
            case .add: return lhs + rhs
            case .subtract: return lhs - rhs
            case .multiply: return lhs * rhs
            case .divide:
                guard rhs != 0 else { throw DatabaseExpressionEvaluationError.divisionByZero }
                return lhs / rhs
            case .modulo:
                guard rhs != 0 else { throw DatabaseExpressionEvaluationError.divisionByZero }
                return lhs.truncatingRemainder(dividingBy: rhs)
            }
        }
    }
}

private extension DatabaseValue {
    var isNull: Bool {
        if case .null = self { return true }
        return false
    }

    var isExactDecimal: Bool {
        if case .decimal = self { return true }
        return false
    }
}
