/// Applies state-independent operation admission before middleware or handlers.
///
/// Database-backed authorization belongs in the operation transaction so the
/// authorized state cannot change between authorization and execution.
public protocol DatabaseOperationAdmissionPolicy: Sendable {
    func decision(
        for request: DatabaseOperationAdmissionRequest
    ) -> DatabaseOperationAdmissionDecision
}
