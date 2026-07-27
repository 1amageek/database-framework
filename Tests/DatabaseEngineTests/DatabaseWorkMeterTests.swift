import DatabaseWire
import Synchronization
import Testing
@testable import DatabaseEngine

@Suite("Database work meter")
struct DatabaseWorkMeterTests {
    @Test("claims reach exact row and work limits")
    func reachesExactLimits() throws {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 2,
                maximumWorkUnits: 3,
                timeoutMilliseconds: 30_000
            )
        )

        try meter.consume(1, at: .indexScan)
        try meter.consume(2, at: .resultMaterialization)
        try meter.recordOutputRows(2)

        #expect(meter.consumedRows == 2)
        #expect(meter.consumedWorkUnits == 3)
    }

    @Test("row accounting does not consume work")
    func rowAccountingIsIndependent() throws {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 2,
                maximumWorkUnits: 0,
                timeoutMilliseconds: 30_000
            )
        )

        try meter.recordOutputRows(1)
        #expect(meter.consumedRows == 1)
        #expect(meter.consumedWorkUnits == 0)
    }

    @Test("row and work exhaustion retain distinct typed errors")
    func reportsExhaustedDimension() throws {
        let rowMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 0,
                maximumWorkUnits: 2,
                timeoutMilliseconds: 30_000
            )
        )
        #expect(throws: DatabaseWorkLimitError.self) {
            try rowMeter.recordOutputRows(1)
        }

        let workMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 2,
                maximumWorkUnits: 0,
                timeoutMilliseconds: 30_000
            )
        )
        #expect(throws: DatabaseWorkLimitError.self) {
            try workMeter.consume(at: .joinCandidate)
        }
    }

    @Test("concurrent callers cannot exceed the exact limit")
    func concurrentClaimsAreAtomic() async {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 100,
                maximumWorkUnits: 50,
                timeoutMilliseconds: 30_000
            )
        )
        let results = await withTaskGroup(
            of: ClaimResult.self,
            returning: [ClaimResult].self
        ) { group in
            for _ in 0..<100 {
                group.addTask {
                    do {
                        try meter.consume(at: .bindingCandidate)
                        return .admitted
                    } catch is DatabaseWorkLimitError {
                        return .rejected
                    } catch {
                        return .unexpected(String(describing: error))
                    }
                }
            }
            var values: [ClaimResult] = []
            for await value in group {
                values.append(value)
            }
            return values
        }

        #expect(results.filter { $0 == .admitted }.count == 50)
        #expect(results.filter { $0 == .rejected }.count == 50)
        #expect(!results.contains { result in
            if case .unexpected = result { return true }
            return false
        })
        #expect(meter.consumedWorkUnits == 50)
    }

    @Test("storage sentinel is positive and overflow safe")
    func storageSentinelIsSafe() throws {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1,
                maximumWorkUnits: UInt64.max,
                timeoutMilliseconds: 30_000
            )
        )

        #expect(try meter.storageReadLimitWithSentinel() == Int.max)
    }

    @Test("an expired deadline fails deterministically")
    func expiredDeadlineFails() {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1,
                maximumWorkUnits: 1,
                timeoutMilliseconds: 0
            )
        )

        #expect(throws: DatabaseWorkLimitError.self) {
            try meter.checkpoint(at: .resultMaterialization)
        }
    }

    @Test("deadline rejection is atomic with counter claims")
    func expiredClaimsDoNotMutateCounters() throws {
        let clock = ManualWorkMeterClock()
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 4,
                maximumWorkUnits: 4,
                maximumIntermediateRows: 4,
                maximumIntermediateBytes: 16,
                timeoutMilliseconds: 1
            ),
            now: { clock.now }
        )
        clock.advance(by: .milliseconds(1))

        #expect {
            try meter.consume(at: .bindingCandidate)
        } throws: { error in
            error as? DatabaseWorkLimitError
                == .deadline(stage: .bindingCandidate)
        }
        #expect {
            try meter.recordOutputRows(1, at: .resultMaterialization)
        } throws: { error in
            error as? DatabaseWorkLimitError
                == .deadline(stage: .resultMaterialization)
        }
        #expect {
            try meter.reserveIntermediate(
                rows: 1,
                bytes: 1,
                at: .projection
            )
        } throws: { error in
            error as? DatabaseWorkLimitError == .deadline(stage: .projection)
        }

        #expect(meter.consumedRows == 0)
        #expect(meter.consumedWorkUnits == 0)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("deadline rejection is atomic with storage sentinel reads")
    func expiredStorageSentinelFails() {
        let clock = ManualWorkMeterClock()
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1,
                maximumWorkUnits: 1,
                timeoutMilliseconds: 1
            ),
            now: { clock.now }
        )
        clock.advance(by: .milliseconds(1))

        #expect {
            try meter.storageReadLimitWithSentinel(at: .indexScan)
        } throws: { error in
            error as? DatabaseWorkLimitError == .deadline(stage: .indexScan)
        }
    }

    @Test("intermediate reservations share one exact request budget")
    func intermediateReservationsShareRequestBudget() throws {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 10,
                maximumWorkUnits: 10,
                maximumIntermediateRows: 3,
                maximumIntermediateBytes: 12,
                timeoutMilliseconds: 30_000
            )
        )
        let first = try meter.reserveIntermediate(
            rows: 2,
            bytes: 8,
            at: .subqueryCache
        )

        #expect(meter.retainedIntermediateRows == 2)
        #expect(meter.retainedIntermediateBytes == 8)
        #expect {
            try meter.reserveIntermediate(
                rows: 2,
                bytes: 1,
                at: .sortInput
            )
        } throws: { error in
            error as? DatabaseWorkLimitError == .maximumIntermediateRows(
                stage: .sortInput,
                consumed: 2,
                requested: 2,
                maximum: 3
            )
        }
        #expect {
            try meter.reserveIntermediate(
                rows: 1,
                bytes: 5,
                at: .joinCandidate
            )
        } throws: { error in
            error as? DatabaseWorkLimitError == .maximumIntermediateBytes(
                stage: .joinCandidate,
                consumed: 8,
                requested: 5,
                maximum: 12
            )
        }

        first.release()
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)

        let second = try meter.reserveIntermediate(
            rows: 3,
            bytes: 12,
            at: .resultMaterialization
        )
        #expect(meter.peakIntermediateRows == 3)
        #expect(meter.peakIntermediateBytes == 12)
        second.release()
    }

    @Test("reservation lifetime releases retained resources exactly once")
    func reservationLifetimeReleasesExactlyOnce() throws {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1,
                maximumWorkUnits: 1,
                maximumIntermediateRows: 1,
                maximumIntermediateBytes: 4,
                timeoutMilliseconds: 30_000
            )
        )

        do {
            let reservation = try meter.reserveIntermediate(
                rows: 1,
                bytes: 4,
                at: .subqueryCache
            )
            #expect(meter.retainedIntermediateRows == 1)
            reservation.release()
            reservation.release()
            #expect(meter.retainedIntermediateRows == 0)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("one reservation grows atomically with its retained owner")
    func reservationGrowthIsAtomic() throws {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1,
                maximumWorkUnits: 1,
                maximumIntermediateRows: 3,
                maximumIntermediateBytes: 12,
                timeoutMilliseconds: 30_000
            )
        )
        let reservation = try meter.reserveIntermediate(
            rows: 1,
            bytes: 4,
            at: .joinCandidate
        )

        try reservation.reserveAdditional(
            rows: 2,
            bytes: 8,
            at: .joinCandidate
        )

        #expect(meter.retainedIntermediateRows == 3)
        #expect(meter.retainedIntermediateBytes == 12)
        #expect(meter.peakIntermediateRows == 3)
        #expect(meter.peakIntermediateBytes == 12)

        reservation.release()
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("failed growth preserves the original reservation")
    func failedReservationGrowthIsTransactional() throws {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1,
                maximumWorkUnits: 1,
                maximumIntermediateRows: 2,
                maximumIntermediateBytes: 8,
                timeoutMilliseconds: 30_000
            )
        )
        let reservation = try meter.reserveIntermediate(
            rows: 1,
            bytes: 4,
            at: .joinCandidate
        )

        #expect {
            try reservation.reserveAdditional(
                rows: 2,
                bytes: 1,
                at: .joinCandidate
            )
        } throws: { error in
            error as? DatabaseWorkLimitError == .maximumIntermediateRows(
                stage: .joinCandidate,
                consumed: 1,
                requested: 2,
                maximum: 2
            )
        }
        #expect(meter.retainedIntermediateRows == 1)
        #expect(meter.retainedIntermediateBytes == 4)

        reservation.release()
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("released reservations reject later growth")
    func releasedReservationRejectsGrowth() throws {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1,
                maximumWorkUnits: 1,
                maximumIntermediateRows: 1,
                maximumIntermediateBytes: 1,
                timeoutMilliseconds: 30_000
            )
        )
        let reservation = try meter.reserveIntermediate(
            rows: 1,
            bytes: 1,
            at: .validation
        )
        reservation.release()

        #expect(throws: DatabaseIntermediateReservationError.alreadyReleased) {
            try reservation.reserveAdditional(
                rows: 1,
                at: .validation
            )
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("partial release rolls back only the failed owner increment")
    func partialReleaseIsExact() throws {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1,
                maximumWorkUnits: 1,
                maximumIntermediateRows: 4,
                maximumIntermediateBytes: 16,
                timeoutMilliseconds: 30_000
            )
        )
        let reservation = try meter.reserveIntermediate(
            rows: 1,
            bytes: 4,
            at: .projection
        )
        try reservation.reserveAdditional(
            rows: 2,
            bytes: 8,
            at: .projection
        )

        try reservation.releasePartial(rows: 2, bytes: 8)
        #expect(meter.retainedIntermediateRows == 1)
        #expect(meter.retainedIntermediateBytes == 4)

        #expect {
            try reservation.releasePartial(rows: 2, bytes: 1)
        } throws: { error in
            error as? DatabaseIntermediateReservationError
                == .releaseExceedsReservation(
                    retainedRows: 1,
                    retainedBytes: 4,
                    requestedRows: 2,
                    requestedBytes: 1
                )
        }
        #expect(meter.retainedIntermediateRows == 1)
        #expect(meter.retainedIntermediateBytes == 4)

        reservation.release()
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private enum ClaimResult: Equatable {
        case admitted
        case rejected
        case unexpected(String)
    }
}

private final class ManualWorkMeterClock: Sendable {
    private let instant = Mutex(ContinuousClock().now)

    var now: ContinuousClock.Instant {
        instant.withLock { $0 }
    }

    func advance(by duration: Duration) {
        instant.withLock { instant in
            instant = instant.advanced(by: duration)
        }
    }
}
