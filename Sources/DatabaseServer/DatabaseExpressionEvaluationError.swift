public enum DatabaseExpressionEvaluationError: Error, Sendable, Equatable, CustomStringConvertible {
    case missingColumn(String)
    case unboundParameter
    case typeMismatch(operation: String)
    case divisionByZero
    case inexactDecimalResult
    case numericOverflow
    case unsupportedExpression(String)
    case unsupportedFunction(String)
    case invalidCast(String)
    case invalidRDFLiteral(datatype: String)

    public var description: String {
        switch self {
        case .missingColumn(let name):
            return "Expression references unknown column '\(name)'"
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
        case .unsupportedExpression(let expression):
            return "Expression is not supported in a record mutation: \(expression)"
        case .unsupportedFunction(let function):
            return "Scalar function '\(function)' is not supported in a record mutation"
        case .invalidCast(let type):
            return "Expression cannot be cast to \(type)"
        case .invalidRDFLiteral(let datatype):
            return "Expression contains an invalid RDF literal for \(datatype)"
        }
    }
}
