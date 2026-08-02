public enum SPARQLFunctionRegistryError: Error, Sendable, Equatable {
    case duplicateFunction(String)
    case unknownFunction(String)
    case nonCanonicalResult(String)
    case evaluation(SPARQLExpressionEvaluationError)
}
