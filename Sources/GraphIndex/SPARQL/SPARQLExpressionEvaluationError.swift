public enum SPARQLExpressionEvaluationError: Error, Sendable, Equatable {
    case unboundVariable(String)
    case typeError(String)
    case invalidFunctionArguments(String)
    case invalidRegularExpression(String)
    case unsupportedExpression(String)
    case resourceLimitExceeded(
        stage: String,
        required: UInt64?,
        maximum: UInt64?
    )
    case runtimeInvariant(String)

    var isSPARQLEvaluationError: Bool {
        switch self {
        case .unboundVariable, .typeError, .invalidFunctionArguments,
             .invalidRegularExpression:
            return true
        case .unsupportedExpression, .resourceLimitExceeded, .runtimeInvariant:
            return false
        }
    }
}

extension SPARQLExpressionEvaluationError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .unboundVariable(let variable):
            return "SPARQL variable is unbound: \(variable)"
        case .typeError(let detail):
            return "SPARQL expression type error: \(detail)"
        case .invalidFunctionArguments(let function):
            return "Invalid arguments for SPARQL function: \(function)"
        case .invalidRegularExpression(let pattern):
            return "Invalid SPARQL regular expression: \(pattern)"
        case .unsupportedExpression(let expression):
            return "Unsupported SPARQL expression: \(expression)"
        case .resourceLimitExceeded(let stage, let required, let maximum):
            var detail = "SPARQL expression resource limit exceeded at \(stage)"
            if let required { detail += "; required=\(required)" }
            if let maximum { detail += "; maximum=\(maximum)" }
            return detail
        case .runtimeInvariant(let detail):
            return "SPARQL expression runtime invariant failed: \(detail)"
        }
    }
}
