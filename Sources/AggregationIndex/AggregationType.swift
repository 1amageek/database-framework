import DatabaseKit

/// Scalar aggregation performed by an aggregation query.
public enum AggregationType: Sendable, Hashable {
    case count
    case sum(field: FieldIdentity)
    case min(field: FieldIdentity)
    case max(field: FieldIdentity)
    case avg(field: FieldIdentity)
    case distinct(field: FieldIdentity)
    case percentile(field: FieldIdentity, percentile: Double)
}
