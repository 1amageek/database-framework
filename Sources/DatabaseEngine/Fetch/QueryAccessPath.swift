import DatabaseKit

/// The physical storage path selected for a typed model query.
public enum QueryAccessPath: Sendable, Equatable, Hashable {
    case fullScan
    case orderedIndex(
        name: String,
        indexType: IndexType,
        indexedFields: [String]
    )
}
