import DatabaseKit

/// Positioned by the parser at its source token; independent of parser state.
enum SQLConstantError: Error, Sendable, Equatable, CustomStringConvertible {
    case invalidNumeric(String)
    case numericOutOfRange
    case invalidConstant(String)
    case unsupportedConstant(String)
    case resourceLimit(String)
    case value(QueryValueError)

    init(serialization error: SQLSerializationError) {
        switch error {
        case .unsupportedLanguage(let reason), .unsupportedSource(let reason), .unsupportedMetadata(let reason),
             .unsupportedType(let reason), .unsupportedLiteral(let reason): self = .unsupportedConstant(reason)
        case .invalidStructure(let reason), .constant(let reason): self = .invalidConstant(reason)
        case .structuralLimit(let reason): self = .resourceLimit(reason)
        case .outputLimit(let actual, let maximum): self = .resourceLimit("SQL output byte limit exceeded: \(actual) > \(maximum)")
        }
    }

    var description: String {
        switch self {
        case .invalidNumeric(let reason), .invalidConstant(let reason),
             .unsupportedConstant(let reason), .resourceLimit(let reason): return reason
        case .numericOutOfRange: return "SQL numeric value is out of range"
        case .value(let error):
            switch error {
            case .invalidNumeric(_, let reason), .invalidUUID(_, let reason), .invalidBinary(_, let reason): return reason
            case .invalidNumericDisplayScale(let scale): return "Invalid numeric display scale: \(scale)"
            case .numericOutOfRange: return "Query numeric value is out of range"
            case .invalidJSON(let offset, let reason): return "Invalid JSON at byte \(offset): " + reason
            case .invalidUTF8(let offset): return "Invalid UTF-8 at byte \(offset)"
            case .nonCanonicalJSONB: return "Noncanonical JSONB value"
            case .invalidDimensionCount(let count): return "Invalid array dimension count: \(count)"
            case .invalidDimension(let lower, let length): return "Invalid array dimension: lower \(lower), length \(length)"
            case .dimensionProductOverflow: return "Array dimension product overflow"
            case .elementCountMismatch(let expected, let actual): return "Array cell count mismatch: \(actual), expected \(expected)"
            case .elementTypeMismatch(let index): return "Array cell type mismatch at index \(index)"
            case .incompatibleType: return "Query value is incompatible with its declared type"
            case .invalidColumnNumber(let number): return "Invalid query column number: \(number)"
            case .invalidPersistenceDescriptor: return "Invalid query persistence descriptor"
            case .invalidPersistencePayload(let reason): return "Invalid query persistence payload: " + reason
            case .intervalOutOfRange: return "Query interval is out of range"
            case .timeOutOfRange: return "Query time is out of range"
            case .unknownRetainedByteCount: return "Query value retained byte count is unknown"
            case .resourceLimit(let resource, let actual, let maximum): return "Query " + resource + " limit exceeded: \(actual) > \(maximum)"
            case .arithmeticOverflow: return "Query value resource arithmetic overflow"
            }
        }
    }
}
