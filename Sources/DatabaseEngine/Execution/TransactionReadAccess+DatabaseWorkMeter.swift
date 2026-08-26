import DatabaseTypes
import StorageKit

extension TransactionReadAccess {
    /// Reads one value through a request-scoped atomic byte allowance.
    ///
    /// Admission happens before the backend await and reserves the issued
    /// maximum against both retained and other pending point reads. On
    /// success the allowance is transferred to exact retained ownership; all
    /// other paths release it exactly once.
    package func readPointValue(
        for key: ByteString,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter,
        at stage: DatabaseWorkStage
    ) async throws -> ByteString? {
        try Task.checkCancellation()
        let allowance = try workMeter.admitPointRead(at: stage)
        let value: ByteString?
        do {
            value = try await getValue(
                for: key,
                snapshot: snapshot,
                maximumByteCount: allowance.issuedByteCount
            )
        } catch let error as StorageError {
            allowance.release()
            guard error.isPointReadValueTooLarge,
                  let violation = error.byteLimitViolation,
                  violation.maximumByteCount
                      == UInt64(allowance.issuedByteCount),
                  violation.observedByteCount > violation.maximumByteCount
            else {
                throw error
            }
            throw DatabaseWorkLimitError.maximumIntermediateBytes(
                stage: stage,
                consumed: allowance.consumedByteCount,
                requested: violation.observedByteCount,
                maximum: workMeter.budget.maximumIntermediateBytes
            )
        } catch {
            allowance.release()
            throw error
        }

        do {
            try Task.checkCancellation()
            try workMeter.checkpoint(at: stage)
            guard let value else {
                allowance.release()
                return nil
            }
            let reservation = try allowance.complete(
                returnedByteCount: value.count
            )
            do {
                return try DatabaseRetainedByteString.make(
                    value,
                    reservation: reservation,
                    at: stage
                )
            } catch {
                reservation.release()
                throw error
            }
        } catch {
            allowance.release()
            throw error
        }
    }
}
