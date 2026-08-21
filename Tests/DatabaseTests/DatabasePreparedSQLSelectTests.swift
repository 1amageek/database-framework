import DatabaseKit
@_spi(DatabaseExecution) import DatabaseEngine
import TestSupport
import Testing

@_spi(DatabaseExecution) @testable import Database

@Suite("Prepared SQL SELECT ownership")
struct DatabasePreparedSQLSelectTests {
    @Test("prepared query retains dynamic literals and their reservation")
    func retainsDynamicLiteralOwnership() async throws {
        let workMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateRows: 16,
                maximumIntermediateBytes: 1_048_576
            ),
            monotonicClock: TestProcessMonotonicClock()
        )

        try await exercisePreparedQuery(workMeter: workMeter)

        #expect(workMeter.retainedIntermediateRows == 0)
        #expect(workMeter.retainedIntermediateBytes == 0)
    }

    @Test("prepared storage rejects reservations from another request")
    func rejectsReservationFromAnotherRequest() throws {
        let ownerMeter = makeMeter()
        let foreignMeter = makeMeter()
        do {
            let retainedStorage = try DatabasePreparedSQLSelectStorage(
                workMeter: ownerMeter
            )
            let foreignReservation = try foreignMeter.reserveIntermediate(
                rows: 1,
                bytes: 128,
                at: .expressionEvaluation
            )

            #expect(throws: DatabasePreparedSQLSelectError.workMeterMismatch) {
                try retainedStorage.retain(
                    elements: [.literal(.string("foreign"))],
                    reservation: foreignReservation
                )
            }
            foreignReservation.release()
            #expect(foreignMeter.retainedIntermediateRows == 0)
            #expect(foreignMeter.retainedIntermediateBytes == 0)
        }
        #expect(ownerMeter.retainedIntermediateRows == 0)
        #expect(ownerMeter.retainedIntermediateBytes == 0)
    }

    private func exercisePreparedQuery(
        workMeter: DatabaseWorkMeter
    ) async throws {
        let retainedStorage = try DatabasePreparedSQLSelectStorage(
            workMeter: workMeter
        )
        let literalReservation = try workMeter.reserveIntermediate(
            rows: 1,
            bytes: 128,
            at: .expressionEvaluation
        )
        let retainedExpressions: [Expression] = [
            .literal(.string("retained"))
        ]
        let queryExpressions = retainedExpressions
        try retainedStorage.retain(
            elements: retainedExpressions,
            reservation: literalReservation
        )
        let prepared = DatabasePreparedSQLSelect(
            query: SelectQuery(
                projection: .all,
                source: .table(TableRef("Example")),
                filter: .inList(
                    .column(ColumnRef("id")),
                    values: queryExpressions
                )
            ),
            workMeter: workMeter,
            retainedStorage: retainedStorage
        )

        #expect(workMeter.retainedIntermediateRows == 1)
        #expect(workMeter.retainedIntermediateBytes >= 128)
        withExtendedLifetime(prepared) {}
    }

    private func makeMeter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateRows: 16,
                maximumIntermediateBytes: 1_048_576
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }
}
