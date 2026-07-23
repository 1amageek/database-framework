/// Scalar aggregation performed by an aggregation query.
public enum AggregationType: Sendable, Hashable {
    case count
    case sum(field: String)
    case min(field: String)
    case max(field: String)
    case avg(field: String)
    case distinct(field: String)
    case percentile(field: String, percentile: Double)
}
