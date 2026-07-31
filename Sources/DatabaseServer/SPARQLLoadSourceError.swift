#if DATABASE_SERVER_GRAPH_INDEXES
public enum SPARQLLoadSourceError:
    Error,
    Sendable,
    Equatable,
    CustomStringConvertible {
    case notConfigured
    case sourceNotFound(String)
    case accessDenied(String)
    case unsupportedMediaType(String)
    case invalidDocument(String)
    case transportFailure(String)
    case documentTooLarge(actual: UInt64, maximum: Int)
    case tripleLimitExceeded(actual: Int, maximum: Int)
    case internalFailure(String)

    public var isSilentSuppressible: Bool {
        switch self {
        case .sourceNotFound, .unsupportedMediaType, .invalidDocument,
             .transportFailure:
            return true
        case .notConfigured, .accessDenied, .documentTooLarge,
             .tripleLimitExceeded, .internalFailure:
            return false
        }
    }

    public var description: String {
        switch self {
        case .notConfigured:
            return "SPARQL LOAD source is not configured"
        case .sourceNotFound(let source):
            return "SPARQL LOAD source was not found: \(source)"
        case .accessDenied(let source):
            return "SPARQL LOAD source access was denied: \(source)"
        case .unsupportedMediaType(let mediaType):
            return "SPARQL LOAD media type is unsupported: \(mediaType)"
        case .invalidDocument(let reason):
            return "SPARQL LOAD document is invalid: \(reason)"
        case .transportFailure(let reason):
            return "SPARQL LOAD transport failed: \(reason)"
        case .documentTooLarge(let actual, let maximum):
            return "SPARQL LOAD document has \(actual) bytes; maximum is \(maximum)"
        case .tripleLimitExceeded(let actual, let maximum):
            return "SPARQL LOAD document has \(actual) triples; maximum is \(maximum)"
        case .internalFailure(let reason):
            return "SPARQL LOAD source failed internally: \(reason)"
        }
    }
}

#endif
