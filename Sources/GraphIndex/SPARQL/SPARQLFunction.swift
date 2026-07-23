import Core
import DatabaseValue

/// An explicitly injected SPARQL extension function. Implementations must
/// return a canonical RDF term and must not depend on global mutable state.
public protocol SPARQLFunction: Sendable {
    var identifier: DatabaseRDFIRI { get }
    func evaluate(arguments: [FieldValue]) throws -> FieldValue
}
