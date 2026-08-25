public enum RDFDatasetReadResolutionError:
    Error,
    Sendable,
    Equatable,
    CustomStringConvertible {
    case missing(entityName: String)
    case ambiguous(candidates: [String])

    public var description: String {
        switch self {
        case .missing(let entityName):
            return "No RDF dataset index found for entity '\(entityName)'"
        case .ambiguous(let candidates):
            return "RDF dataset source is ambiguous: \(candidates.joined(separator: ", "))"
        }
    }
}
