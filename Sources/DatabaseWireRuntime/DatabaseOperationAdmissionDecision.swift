/// The state-independent outcome of admitting a database operation.
public enum DatabaseOperationAdmissionDecision: Sendable, Hashable {
    case allow
    case deny(DatabaseOperationAdmissionDenial)
}
