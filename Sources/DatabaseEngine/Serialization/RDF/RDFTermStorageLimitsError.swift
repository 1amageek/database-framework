/// Invalid resource limits supplied to the RDF storage representation.
public enum RDFTermStorageLimitsError: Error, Sendable, Equatable {
    case negativeMaximumBytes(Int)
    case negativeMaximumDepth(Int)
    case nonPositiveMaximumObjectCount(Int)
}
