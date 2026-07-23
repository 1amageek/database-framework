/// Admits every operation that has a valid wire envelope.
public struct UnrestrictedDatabaseOperationAdmissionPolicy:
    DatabaseOperationAdmissionPolicy {
    public init() {}

    public func decision(
        for request: DatabaseOperationAdmissionRequest
    ) -> DatabaseOperationAdmissionDecision {
        _ = request
        return .allow
    }
}
