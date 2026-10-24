import DatabaseTypes

/// Linear production token for one destination ExecutionQueryRow and its exact request
/// admission. Consuming the token yields the reference owner that must travel
/// with every retained alias of the row.
package struct DatabaseQueryScopedQueryRow: ~Copyable, Sendable {
    private let owner: DatabaseQueryScopedQueryRowOwner

    private init(owner: DatabaseQueryScopedQueryRowOwner) {
        self.owner = owner
    }

    package static func producing(
        exactFootprint: DatabaseIntermediateFootprint,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage,
        retaining sourcePayloadLeases: [DatabaseQueryPayloadLease] = [],
        measuring measure: ((borrowing ExecutionQueryRow) throws -> DatabaseIntermediateFootprint)? = nil,
        _ makeRow: () throws -> ExecutionQueryRow
    ) throws -> Self {
        guard sourcePayloadLeases.allSatisfy({ $0.workMeter === workMeter }) else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        let admittedFootprint = sourcePayloadLeases.isEmpty ? exactFootprint
            : try exactFootprint.adding(DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: sourcePayloadLeases.count, element: DatabaseQueryPayloadLease.self))
        let reservation = try workMeter.reserveIntermediate(
            rows: admittedFootprint.rows,
            bytes: admittedFootprint.bytes,
            at: stage
        )
        do {
            let row = try makeRow()
            let observed: DatabaseIntermediateFootprint
            if let measure { observed = try measure(row) }
            else {
                observed = try CanonicalRelationalFootprintMeter.footprint(
                    of: row, workMeter: workMeter, stage: stage)
            }
            guard observed == exactFootprint else {
                throw DatabaseQueryScopedQueryRowError
                    .payloadFootprintMismatch(
                        expectedRows: exactFootprint.rows,
                        expectedBytes: exactFootprint.bytes,
                        observedRows: observed.rows,
                        observedBytes: observed.bytes
                    )
            }
            return Self(
                owner: DatabaseQueryScopedQueryRowOwner(
                    row: row,
                    footprint: admittedFootprint,
                    reservation: reservation,
                    sourcePayloadLeases: sourcePayloadLeases
                )
            )
        } catch {
            reservation.release()
            throw error
        }
    }

    package borrowing func withRow<Result, Failure: Error>(
        _ body: (borrowing ExecutionQueryRow) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        try owner.withRow(body)
    }

    package consuming func moveToRetainedOwner()
        -> DatabaseQueryScopedQueryRowOwner
    {
        owner
    }
}
