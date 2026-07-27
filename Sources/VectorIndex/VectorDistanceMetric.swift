/// Defines the distance function used by vector similarity searches.
public enum VectorDistanceMetric: String, Sendable {
    case cosine
    case euclidean
    case dotProduct
}
