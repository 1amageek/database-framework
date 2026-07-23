/// A primary persisted value and its declared polymorphic projection disagree.
public enum PolymorphicProjectionError: Error, Sendable, Equatable {
    case missingProjection(entity: String, group: String)
    case unexpectedProjection(entity: String, group: String)
}
