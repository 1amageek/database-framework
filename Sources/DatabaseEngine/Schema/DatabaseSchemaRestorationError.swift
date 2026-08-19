public enum DatabaseSchemaRestorationError: Error, Sendable, Equatable,
    CustomStringConvertible {
    case missingVersion
    case missingFingerprint
    case fingerprintMismatch
    case invalidGeneration
    case missingIndexPhysicalFingerprint
    case indexPhysicalFingerprintMismatch
    case missingExecutionRuntimeFingerprint
    case executionRuntimeFingerprintMismatch

    public var description: String {
        switch self {
        case .missingVersion:
            return "Persisted schema catalog has no schema version"
        case .missingFingerprint:
            return "Persisted schema catalog has no canonical fingerprint"
        case .fingerprintMismatch:
            return "Persisted schema catalog does not match its canonical fingerprint"
        case .invalidGeneration:
            return "Persisted schema generation is missing or invalid"
        case .missingIndexPhysicalFingerprint:
            return "Persisted schema generation has no index physical fingerprint"
        case .indexPhysicalFingerprintMismatch:
            return "Configured physical index layout does not match the persisted schema generation"
        case .missingExecutionRuntimeFingerprint:
            return "Persisted schema generation has no execution runtime fingerprint"
        case .executionRuntimeFingerprintMismatch:
            return "Configured execution runtime does not match the persisted schema generation"
        }
    }
}
