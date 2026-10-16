public enum DatabaseExpressionEvaluationError: Error, Sendable, Equatable, CustomStringConvertible {
    case missingColumn(String)
    case ambiguousColumn(String)
    case unboundParameter
    case typeMismatch(operation: String)
    case divisionByZero
    case inexactDecimalResult
    case numericOverflow
    case numericUnderflow
    case nonFiniteValue
    case numericDomain(operation: String)
    case unsupportedExpression(String)
    case unsupportedFunction(String)
    case invalidFunctionArgument(function: String, reason: String)
    case invalidPattern(offset: UInt64, reason: String)
    case unsupportedPattern(offset: UInt64, reason: String)
    case invalidCast(String)
    case invalidRDFLiteral(datatype: String)

    public var description: String {
        switch self {
        case .missingColumn(let name):
            return "Expression references unknown column '\(name)'"
        case .ambiguousColumn(let name):
            return "Expression references ambiguous column '\(name)'"
        case .unboundParameter:
            return "Expression contains a parameter that was not bound"
        case .typeMismatch(let operation):
            return "Expression operands are incompatible with \(operation)"
        case .divisionByZero:
            return "Expression divides by zero"
        case .inexactDecimalResult:
            return "Expression produces a decimal that cannot be represented exactly"
        case .numericOverflow:
            return "Expression numeric result is outside the canonical value range"
        case .numericUnderflow:
            return "Expression nonzero numeric result underflows its selected type"
        case .nonFiniteValue:
            return "Expression contains a non-finite numeric value"
        case .numericDomain(let operation):
            return "Expression numeric operand is outside the domain of \(operation)"
        case .unsupportedExpression(let expression):
            return "Expression is not supported by the database evaluator: \(expression)"
        case .unsupportedFunction(let function):
            return "Scalar function '\(function)' is not supported by the database evaluator"
        case .invalidFunctionArgument(let function, let reason):
            return "Invalid argument for scalar function '\(function)': \(reason)"
        case .invalidPattern(let offset, let reason):
            return "Invalid pattern at offset \(offset): \(reason)"
        case .unsupportedPattern(let offset, let reason):
            return "Unsupported pattern at offset \(offset): \(reason)"
        case .invalidCast(let type):
            return "Expression cannot be cast to \(type)"
        case .invalidRDFLiteral(let datatype):
            return "Expression contains an invalid RDF literal for \(datatype)"
        }
    }
}
