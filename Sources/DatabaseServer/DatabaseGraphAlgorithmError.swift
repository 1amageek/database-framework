import DatabaseWire
import DatabaseValue

public enum DatabaseGraphAlgorithmError: Error, Sendable, CustomStringConvertible {
    case sourceIndexNotFound(String)
    case sourceIndexHasNoUniqueOwner(String)
    case unsupportedSourceIndex(index: String, kind: String)
    case expectedPropertyGraphIdentifier(DatabaseGraphTerm)
    case expectedRDFTerm(DatabaseGraphTerm)
    case invalidRDFPredicate(DatabaseGraphTerm)
    case invalidRDFGraphName(DatabaseGraphTerm)
    case rdfSourceDoesNotCoverDefaultGraph(index: String)
    case rdfSourceDoesNotCoverNamedGraph(index: String, graph: DatabaseRDFTerm)
    case weightPropertyNotStored(index: String, property: String)
    case edgeWeightMissing(property: String)
    case invalidEdgeWeight(property: String, value: DatabaseValue)
    case ambiguousEdgeWeight(property: String)
    case invalidInvocation(String)
    case numericLimitOutOfRange(field: String, value: UInt64)
    case inconsistentAlgorithmResult(String)
    case unsupportedAlgorithmLimit(String)
    case invalidContinuation
    case continuationDoesNotMatchRequest
    case continuationSnapshotChanged

    public var description: String {
        switch self {
        case .sourceIndexNotFound(let index):
            return "Graph source index '\(index)' was not found"
        case .sourceIndexHasNoUniqueOwner(let index):
            return "Graph source index '\(index)' must belong to exactly one schema entity"
        case .unsupportedSourceIndex(let index, let kind):
            return "Index '\(index)' has unsupported graph kind '\(kind)'"
        case .expectedPropertyGraphIdentifier(let value):
            return "Property graph operations require identifier terms, received \(value)"
        case .expectedRDFTerm(let value):
            return "RDF graph operations require RDF terms, received \(value)"
        case .invalidRDFPredicate(let value):
            return "RDF edge labels must be IRI terms, received \(value)"
        case .invalidRDFGraphName(let value):
            return "RDF named graph selectors require an IRI or blank-node term, received \(value)"
        case .rdfSourceDoesNotCoverDefaultGraph(let index):
            return "RDF source index '\(index)' does not cover the default graph"
        case .rdfSourceDoesNotCoverNamedGraph(let index, let graph):
            return "RDF source index '\(index)' does not cover named graph \(graph)"
        case .weightPropertyNotStored(let index, let property):
            return "Weight property '\(property)' is not stored by graph index '\(index)'"
        case .edgeWeightMissing(let property):
            return "Indexed edge does not contain weight property '\(property)'"
        case .invalidEdgeWeight(let property, let value):
            return "Weight property '\(property)' is not a finite numeric value: \(value)"
        case .ambiguousEdgeWeight(let property):
            return "Indexed edge has more than one value for weight property '\(property)'"
        case .invalidInvocation(let message):
            return "Invalid graph algorithm invocation: \(message)"
        case .numericLimitOutOfRange(let field, let value):
            return "Graph algorithm limit '\(field)' cannot be represented by this runtime: \(value)"
        case .inconsistentAlgorithmResult(let message):
            return "Graph algorithm produced an inconsistent result: \(message)"
        case .unsupportedAlgorithmLimit(let value):
            return "Graph algorithm produced an unsupported limit reason: \(value)"
        case .invalidContinuation:
            return "Graph algorithm continuation is malformed"
        case .continuationDoesNotMatchRequest:
            return "Graph algorithm continuation belongs to a different request"
        case .continuationSnapshotChanged:
            return "Graph algorithm result changed since the continuation was issued"
        }
    }
}
