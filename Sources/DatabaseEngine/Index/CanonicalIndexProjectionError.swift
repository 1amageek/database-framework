/// Typed failures for canonical index projection creation and decoding.
public enum CanonicalIndexProjectionError: Error, Sendable, Equatable {
    case missingCompiledSchema(entity: String)
    case invalidSchema(entity: String, reason: String)
    case unknownField(entity: String, index: String, field: String)
    case missingProjection(index: String)
    case incompleteProjection(entity: String, missingFields: [String])
    case projectionFieldMismatch(
        entity: String,
        missingFields: [String],
        unexpectedFields: [String]
    )
}
