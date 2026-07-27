import DatabaseEngine

public enum PropertyGraphDefinitionCatalogError:
    Error,
    Sendable,
    Equatable
{
    public enum StoredDefinitionViolation: Sendable, Equatable {
        case valueTooLarge(actual: Int, maximum: Int)
        case decodingFailed(StorageFrameError)
        case graphNameMismatch(actual: String)
        case containsCreationCondition
        case canonicalizationFailed(StorageFrameError)
        case nonCanonicalEncoding
    }

    case emptyGraphName
    case graphAlreadyExists(String)
    case graphNotFound(String)
    case keyTooLarge(actual: Int, maximum: Int)
    case definitionTooLarge(actual: Int, maximum: Int)
    case definitionCannotBeRepresented(StorageFrameError)
    case invalidStoredDefinition(
        graphName: String,
        violation: StoredDefinitionViolation
    )
}
