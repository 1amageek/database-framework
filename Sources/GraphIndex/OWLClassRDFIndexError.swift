public enum OWLClassRDFIndexError: Error, Sendable, Equatable, CustomStringConvertible {
    case rootDoesNotConform(typeName: String)
    case rootMismatch(expected: String, actual: String)
    case projectionConfigurationMismatch(typeName: String)

    public var description: String {
        switch self {
        case .rootDoesNotConform(let typeName):
            return "OWL RDF index root '\(typeName)' does not conform to OWLClassEntity"
        case .rootMismatch(let expected, let actual):
            return "OWL RDF index root mismatch: expected '\(expected)', got '\(actual)'"
        case .projectionConfigurationMismatch(let typeName):
            return "OWL RDF projection metadata does not match compiled type '\(typeName)'"
        }
    }
}
