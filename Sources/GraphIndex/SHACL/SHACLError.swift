import DatabaseKit
import DatabaseTypes

/// Typed failures that prevent SHACL validation from producing a report.
public enum SHACLError: Error, Sendable, Equatable, CustomStringConvertible {
    case shapesGraphNotFound(String)
    case shapeNotFound(RDFTerm)
    case ontologyIdentifierRequired
    case ontologyNotFound(String)
    case graphIndexNotFound(String)
    case invalidPattern(regex: String, reason: String)
    case invalidConstraint(String)
    case resourceLimitExceeded(
        resource: String,
        limit: Int,
        actual: Int
    )
    case runtimeFailure(stage: String, reason: String)
    case resultBindingMissing(variable: String)
    case resultBindingTypeMismatch(variable: String)

    public var description: String {
        switch self {
        case .shapesGraphNotFound(let iri):
            return "SHACL shapes graph not found: \(iri)"
        case .shapeNotFound(let identifier):
            return "SHACL shape not found: \(identifier)"
        case .ontologyIdentifierRequired:
            return "SHACL OWL entailment requires an ontology identifier"
        case .ontologyNotFound(let iri):
            return "Ontology not found (required for OWL entailment): \(iri)"
        case .graphIndexNotFound(let typeName):
            return "No property-graph index is declared on type \(typeName)"
        case .invalidPattern(let regex, let reason):
            return "Invalid SHACL pattern '\(regex)': \(reason)"
        case .invalidConstraint(let reason):
            return "Invalid SHACL constraint: \(reason)"
        case .resourceLimitExceeded(let resource, let limit, let actual):
            return "SHACL resource limit exceeded for \(resource): \(actual) > \(limit)"
        case .runtimeFailure(let stage, let reason):
            return "SHACL runtime failure at \(stage): \(reason)"
        case .resultBindingMissing(let variable):
            return "SHACL query result is missing required binding \(variable)"
        case .resultBindingTypeMismatch(let variable):
            return "SHACL query result binding \(variable) is not an RDF term"
        }
    }
}
