public enum DatabaseSHACLValidationError: Error, Sendable, Equatable,
    CustomStringConvertible {
    case shapesGraphNotFound(String)
    case invalidShapesGraph(String)
    case resolvedScopeMismatch
    case resolvedGraphScopeMismatch
    case resolvedEntailmentMismatch
    case missingOWLReasoner(String)
    case invalidSnapshotFingerprint
    case invalidContinuation
    case workLimitExceeded(requested: UInt64, maximum: UInt64)

    public var description: String {
        switch self {
        case .shapesGraphNotFound(let graph):
            return "SHACL shapes graph was not found: \(graph)"
        case .invalidShapesGraph(let reason):
            return "SHACL shapes graph is invalid: \(reason)"
        case .resolvedScopeMismatch:
            return "SHACL data source resolver returned a different data scope"
        case .resolvedGraphScopeMismatch:
            return "SHACL data source resolver returned a different RDF graph scope"
        case .resolvedEntailmentMismatch:
            return "SHACL data source resolver returned a different entailment mode"
        case .missingOWLReasoner(let ontology):
            return "SHACL OWL entailment requires a reasoner for \(ontology)"
        case .invalidSnapshotFingerprint:
            return "SHACL data source returned an invalid snapshot fingerprint"
        case .invalidContinuation:
            return "SHACL validation continuation is invalid"
        case .workLimitExceeded(let requested, let maximum):
            return "SHACL work limit exceeded: requested \(requested), maximum \(maximum)"
        }
    }
}
