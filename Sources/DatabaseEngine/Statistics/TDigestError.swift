/// Typed failures for t-digest mutation, query, merge, and persistence.
public enum TDigestError: Error, Sendable, Equatable {
    case invalidCompression(Double)
    case invalidValue(Double)
    case invalidWeight(Int64)
    case weightOverflow
    case invalidQuantile(Double)
    case emptyDigest
    case compressionMismatch(expected: Double, actual: Double)
    case invalidHeader
    case invalidByteCount(expected: Int, actual: Int)
    case centroidLimitExceeded(Int)
    case invalidTotalWeight(Int64)
    case invalidBounds
    case invalidCentroid(index: Int)
    case unsortedCentroids
    case weightMismatch(expected: Int64, actual: Int64)
}
