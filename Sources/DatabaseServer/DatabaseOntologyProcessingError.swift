import OntologyIndex

public enum DatabaseOntologyProcessingError: Error, Sendable, Equatable, CustomStringConvertible {
    case invalidDocument(String)
    case invalidReasoningTriple(ReasoningTripleError)
    case materialization(OWL2RLMaterializationError)
    case ontologyIdentifierMismatch(expected: String, actual: String)
    case importsMismatch(expected: [String], actual: [String])
    case importedOntologyNotFound(String)
    case importCycle([String])
    case ontologyNotFound(String)
    case ontologyInUse(ontology: String, dependent: String)
    case resourceNotFound(String)
    case invalidContinuation
    case workLimitExceeded(requested: UInt64, maximum: UInt64)

    public var description: String {
        switch self {
        case .invalidDocument(let reason):
            return "The ontology document is invalid: \(reason)"
        case .invalidReasoningTriple(let error):
            return "The ontology contains an invalid reasoning triple: \(error)"
        case .materialization(let error):
            return "Ontology materialization failed: \(error)"
        case .ontologyIdentifierMismatch(let expected, let actual):
            return "Ontology identifier mismatch: expected \(expected), got \(actual)"
        case .importsMismatch(let expected, let actual):
            return "Ontology imports mismatch: expected \(expected), got \(actual)"
        case .importedOntologyNotFound(let ontology):
            return "Imported ontology was not found: \(ontology)"
        case .importCycle(let path):
            return "Ontology import cycle detected: \(path.joined(separator: " -> "))"
        case .ontologyNotFound(let ontology):
            return "Ontology was not found: \(ontology)"
        case .ontologyInUse(let ontology, let dependent):
            return "Ontology \(ontology) is imported by \(dependent)"
        case .resourceNotFound(let resource):
            return "Ontology resource was not found: \(resource)"
        case .invalidContinuation:
            return "The ontology continuation is invalid"
        case .workLimitExceeded(let requested, let maximum):
            return "Ontology work limit exceeded: requested \(requested), maximum \(maximum)"
        }
    }
}
