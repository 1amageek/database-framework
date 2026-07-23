/// The physical storage path selected for a typed model query.
public enum QueryAccessPath: Sendable, Equatable, Hashable {
    case fullScan
    case scalarIndex(
        name: String,
        kind: String,
        indexedFields: [String]
    )
}
