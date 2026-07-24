import Relationship

public enum RelationshipSnapshotError: Error, Sendable, Equatable {
    case relationNotLoaded(owner: String, field: String)
    case cardinalityMismatch(
        owner: String,
        field: String,
        expected: RelationshipCardinality
    )
    case relatedTypeMismatch(
        owner: String,
        field: String,
        expected: String,
        actual: String
    )
}
