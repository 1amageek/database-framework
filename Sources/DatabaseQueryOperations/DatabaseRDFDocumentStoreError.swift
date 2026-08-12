import DatabaseOperationCore
#if DATABASE_QUERY_OPERATIONS_GRAPH_INDEXES
public enum DatabaseRDFDocumentStoreError: Error, Sendable, CustomStringConvertible {
    case emptyIdentifier
    case invalidPage(offset: Int, limit: Int)
    case documentNotFound(String)
    case invalidContinuation
    case revisionConflict(expected: UInt64, actual: UInt64)
    case revisionOverflow(String)
    case corruptedMetadata(String)
    case corruptedItemCount(expected: Int, actual: Int)

    public var description: String {
        switch self {
        case .emptyIdentifier:
            return "RDF document identifiers must not be empty"
        case .invalidPage(let offset, let limit):
            return "Invalid RDF document page offset \(offset), limit \(limit)"
        case .documentNotFound(let identifier):
            return "RDF document was not found: \(identifier)"
        case .invalidContinuation:
            return "RDF document continuation is invalid"
        case .revisionConflict(let expected, let actual):
            return "RDF document revision conflict: expected \(expected), actual \(actual)"
        case .revisionOverflow(let identifier):
            return "RDF document revision overflow: \(identifier)"
        case .corruptedMetadata(let identifier):
            return "RDF document metadata is corrupted: \(identifier)"
        case .corruptedItemCount(let expected, let actual):
            return "RDF document item count mismatch: expected \(expected), actual \(actual)"
        }
    }
}

#endif
