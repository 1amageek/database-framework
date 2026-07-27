import DatabaseKit

/// Selects the named graph that becomes active while evaluating a graph pattern.
public enum ExecutionGraphSelector: Sendable, Hashable {
    case named(RDFGraphName)
    case variable(String)

    public var variables: Set<String> {
        switch self {
        case .named:
            return []
        case .variable(let name):
            return [name]
        }
    }
}

extension ExecutionGraphSelector: CustomStringConvertible {
    public var description: String {
        switch self {
        case .named(let graph):
            return String(describing: graph.term)
        case .variable(let name):
            return name
        }
    }
}
