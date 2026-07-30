/// Dataset-dependent expression evaluation keeps SPARQL expression errors in
/// the value channel. Storage and transaction failures remain ordinary thrown
/// errors, preserving their original concrete type without runtime inspection.
enum SPARQLExpressionEvaluationOutcome<Value: Sendable>: Sendable {
    case value(Value)
    case expressionError(SPARQLExpressionEvaluationError)
}
