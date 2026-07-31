import DatabaseKit

public enum SPARQLExpressionCompilationError: Error, Sendable, Equatable {
    case unsupportedExpression(String)
    case unboundParameter
    case aggregateNotAllowed
    case invalidFunctionArity(
        function: String,
        actual: Int,
        expected: String
    )
    case distinctScalarFunction(String)
    case invalidExistsSource
    case invalidFunctionIdentifier(String)
    case structural(QueryStructuralValidationError)
    case resourceLimitExceeded(
        resource: SPARQLExpressionCompilationResource,
        actual: UInt64,
        maximum: UInt64
    )
}

extension SPARQLExpressionCompilationError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .unsupportedExpression(let expression):
            return "Unsupported SPARQL expression: \(expression)"
        case .unboundParameter:
            return "SPARQL expression contains an unbound parameter"
        case .aggregateNotAllowed:
            return "Aggregate expression is not valid at this algebra position"
        case .invalidFunctionArity(let function, let actual, let expected):
            return "Invalid arity for \(function): actual=\(actual), expected=\(expected)"
        case .distinctScalarFunction(let function):
            return "DISTINCT is not valid for scalar function \(function)"
        case .invalidExistsSource:
            return "EXISTS contains an unsupported query source or modifier"
        case .invalidFunctionIdentifier(let identifier):
            return "Invalid SPARQL function identifier: \(identifier)"
        case .structural(let error):
            return "Invalid SPARQL expression structure: \(error)"
        case .resourceLimitExceeded(let resource, let actual, let maximum):
            return "SPARQL expression compilation limit exceeded for \(resource.rawValue): actual=\(actual), maximum=\(maximum)"
        }
    }
}
