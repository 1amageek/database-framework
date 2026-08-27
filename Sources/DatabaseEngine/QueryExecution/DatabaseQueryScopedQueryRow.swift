import DatabaseTypes

/// Linear production token for one destination QueryRow and its exact request
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
        _ makeRow: () throws -> QueryRow
    ) throws -> Self {
        let reservation = try workMeter.reserveIntermediate(
            rows: exactFootprint.rows,
            bytes: exactFootprint.bytes,
            at: stage
        )
        do {
            let row = try makeRow()
            let observed = try CanonicalRelationalFootprintMeter.footprint(
                of: row,
                workMeter: workMeter,
                stage: stage
            )
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
                    footprint: exactFootprint,
                    reservation: reservation
                )
            )
        } catch {
            reservation.release()
            throw error
        }
    }

    package borrowing func withRow<Result, Failure: Error>(
        _ body: (borrowing QueryRow) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        try owner.withRow(body)
    }

    package consuming func moveToRetainedOwner()
        -> DatabaseQueryScopedQueryRowOwner
    {
        owner
    }
}
