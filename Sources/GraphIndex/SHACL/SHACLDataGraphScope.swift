import Graph

/// Selects the RDF graph that SHACL queries evaluate as the data graph.
public enum SHACLDataGraphScope: Sendable, Hashable {
    case defaultGraph
    case named(RDFGraphName)

    func apply(to pattern: ExecutionPattern) -> ExecutionPattern {
        switch self {
        case .defaultGraph:
            return pattern
        case .named(let graph):
            return .graph(.named(graph), pattern)
        }
    }
}
