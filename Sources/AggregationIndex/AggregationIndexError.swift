public enum AggregationIndexError: Error, Sendable {
    case invalidConfiguration(String)
    case invalidStructure(String)
    case invalidArgument(String)
    case noData(String)
}
