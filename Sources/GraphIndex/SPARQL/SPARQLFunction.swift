import DatabaseKit
import DatabaseTypes

/// An explicitly injected SPARQL extension function. Implementations must
/// return a canonical RDF term and must not depend on global mutable state.
public protocol SPARQLFunction: Sendable {
    var identifier: RDFIRI { get }

    /// Upper bound for the canonical retained FieldValue returned by
    /// `evaluate`. Query execution reserves this amount before invoking the
    /// function and rejects a result that exceeds the declared contract.
    var maximumResultByteCount: UInt64 { get }

    func evaluate(
        arguments: [FieldValue]
    ) throws(SPARQLExpressionEvaluationError) -> FieldValue
}
