@_spi(DatabaseExecution) import DatabaseWire

/// Preserves a bounded response preparation failure before publication.
public enum QueryResponseEncodingError: Error, Sendable {
    case resourceLimit(DatabaseWireError)

    package static func isResourceLimit(_ error: DatabaseWireError) -> Bool {
        switch error {
        case .frameTooLarge, .stringTooLarge, .byteStringTooLarge,
             .collectionTooLarge, .nestingTooDeep, .objectBudgetExceeded:
            return true
        case .invalidQueryValue(.resourceLimit),
             .invalidQueryStructure(.resourceLimitExceeded):
            return true
        default: return false
        }
    }
}
