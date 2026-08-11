/// Type-erased state-independent operation admission policy.
public final class AnyDatabaseOperationAdmissionPolicy:
    DatabaseOperationAdmissionPolicy,
    Sendable {
    private let decide: @Sendable (
        DatabaseOperationAdmissionRequest
    ) -> DatabaseOperationAdmissionDecision

    public init<Policy: DatabaseOperationAdmissionPolicy>(
        _ policy: Policy
    ) {
        self.decide = { request in
            policy.decision(for: request)
        }
    }

    public func decision(
        for request: DatabaseOperationAdmissionRequest
    ) -> DatabaseOperationAdmissionDecision {
        decide(request)
    }
}
