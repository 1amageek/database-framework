public enum DatabaseSchemaRestorationError: Error, Sendable, Equatable,
    CustomStringConvertible {
    case missingVersion
    case missingFingerprint
    case fingerprintMismatch
    case invalidGeneration

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
        }
    }
}
