import DatabaseKit

/// Keeps generated assignment payloads admitted until the change destination is admitted.
package struct DatabasePreparedStatementMutationChange: Sendable {
    package let change: EntityMutationChange
    package let workMeter: DatabaseWorkMeter
    private let payloads: DatabaseSharedRetainedArray<DatabaseQueryPayloadLease>?

    package init(
        change: EntityMutationChange,
        payloads: DatabaseSharedRetainedArray<DatabaseQueryPayloadLease>? = nil,
        workMeter: DatabaseWorkMeter
    ) throws {
        if let payloads {
            guard payloads.workMeter === workMeter else {
                throw DatabaseIntermediateReservationError.workMeterMismatch
            }
            for payload in payloads {
                if payload.workMeter !== workMeter {
                    throw DatabaseIntermediateReservationError.workMeterMismatch
                }
            }
        }
        self.change = change
        self.payloads = payloads
        self.workMeter = workMeter
    }
}
