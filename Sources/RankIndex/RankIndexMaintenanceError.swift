/// Failures that prevent deterministic maintenance of a rank index.
public enum RankIndexMaintenanceError: Error, Sendable, Equatable {
    case invalidPercentile(Double)
    case invalidScoreFieldCount(actual: Int)
    case unorderedFloatingPoint(indexName: String)
}
