public enum GraphScanScope: Sendable, Hashable {
    case all
    case defaultGraph
    case named(GraphIdentity)

    public func contains(_ graph: GraphIdentity?) -> Bool {
        switch self {
        case .all:
            return true
        case .defaultGraph:
            return graph == nil
        case .named(let name):
            return graph == name
        }
    }
}
