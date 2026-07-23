import DatabaseWire

public enum PropertyGraphDefinitionCatalogError:
    Error,
    Sendable,
    Equatable
{
    public enum StoredDefinitionViolation: Sendable, Equatable {
        case valueTooLarge(actual: Int, maximum: Int)
        case decodingFailed(DatabaseWireError)
        case unexpectedStatement
        case graphNameMismatch(actual: String)
        case containsCreationCondition
        case canonicalizationFailed(DatabaseWireError)
        case nonCanonicalEncoding
    }

    case emptyGraphName
    case graphAlreadyExists(String)
    case graphNotFound(String)
    case keyTooLarge(actual: Int, maximum: Int)
    case definitionTooLarge(actual: Int, maximum: Int)
    case definitionCannotBeRepresented(DatabaseWireError)
    case invalidStoredDefinition(
        graphName: String,
        violation: StoredDefinitionViolation
    )
}
