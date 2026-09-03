import DatabaseWire
import DatabaseKit
import StorageKit
import Synchronization
import Testing
import TestSupport
@testable import DatabaseEngine

@Suite("Database work meter")
struct DatabaseWorkMeterTests {
    private func makeWorkMeter(
        budget: ExecutionBudget,
        monotonicClock: any StorageMonotonicClock = ManualWorkMeterClock()
    ) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: budget,
            monotonicClock: monotonicClock
        )
    }

    @Test("claims reach exact row and work limits")
    func reachesExactLimits() throws {
        let meter = makeWorkMeter(
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
        let meter = makeWorkMeter(
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
        let rowMeter = makeWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 0,
                maximumWorkUnits: 2,
                timeoutMilliseconds: 30_000
            )
        )
        #expect(throws: DatabaseWorkLimitError.self) {
            try rowMeter.recordOutputRows(1)
        }

        let workMeter = makeWorkMeter(
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
        let meter = makeWorkMeter(
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

    @Test("storage sentinel is positive, overflow safe, and memory bounded")
    func storageSentinelIsSafe() throws {
        let budget = ExecutionBudget(
            maximumRows: 1,
            maximumWorkUnits: UInt64.max,
            timeoutMilliseconds: 30_000
        )
        let meter = makeWorkMeter(
            budget: budget
        )

        #expect(
            try meter.storageReadLimitWithSentinel()
                == Int(budget.maximumIntermediateRows) + 1
        )
    }

    @Test("relational footprint measurement remains independent of the memory budget")
    func relationalFootprintUsesTypedMemoryLimit() throws {
        let meter = makeWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1,
                maximumWorkUnits: 16,
                maximumIntermediateRows: 1,
                maximumIntermediateBytes: 0,
                timeoutMilliseconds: 30_000
            )
        )
        let footprint = try CanonicalRelationalFootprintMeter.footprint(
            of: QueryRow(fields: ["value": .string("payload")]),
            workMeter: meter
        )

        #expect(footprint.bytes > 0)
        #expect {
            try meter.reserveIntermediate(
                rows: footprint.rows,
                bytes: footprint.bytes,
                at: .projection
            )
        } throws: { error in
            guard case DatabaseWorkLimitError.maximumIntermediateBytes = error
            else { return false }
            return true
        }
    }

    @Test("an expired deadline fails deterministically")
    func expiredDeadlineFails() {
        let meter = makeWorkMeter(
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
        let meter = makeWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 4,
                maximumWorkUnits: 4,
                maximumIntermediateRows: 4,
                maximumIntermediateBytes: 16,
                timeoutMilliseconds: 1
            ),
            monotonicClock: clock
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
        let meter = makeWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1,
                maximumWorkUnits: 1,
                timeoutMilliseconds: 1
            ),
            monotonicClock: clock
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
        let meter = makeWorkMeter(
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
        let meter = makeWorkMeter(
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
        let meter = makeWorkMeter(
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
        let meter = makeWorkMeter(
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
        let meter = makeWorkMeter(
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
        let meter = makeWorkMeter(
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

    @Test("concurrent point reads admit before backend dispatch")
    func concurrentPointReadsAdmitBeforeBackendDispatch() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        defer { await storage.waitUntilShutdown() }
        let key = ByteString(utf8: "point-read-race")
        let nonEmptyKey = ByteString(utf8: "point-read-race-nonempty")
        let value = ByteString()
        let nonEmptyValue = ByteString([1, 2, 3, 4])
        try await storage.withTransaction { transaction in
            try transaction.setValue(value, for: key)
            try transaction.setValue(nonEmptyValue, for: nonEmptyKey)
        }
        let meter = makeWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 4,
                maximumWorkUnits: 4,
                maximumIntermediateRows: 4,
                maximumIntermediateBytes: 8,
                timeoutMilliseconds: 30_000
            )
        )
        let firstBarrier = storage.control.suspendNextBoundedValueRead(for: key)
        let secondBarrier = storage.control.suspendNextBoundedValueRead(
            for: nonEmptyKey
        )
        let first = Task {
            try await storage.withTransaction { transaction in
                try await readPointValue(
                    using: transaction,
                    for: key,
                    snapshot: true,
                    workMeter: meter,
                    at: .indexScan
                )
            }
        }
        let firstMonitor = try await firstBarrier.waitUntilEntered(
            beforeCompletionOf: first
        )
        #expect {
            try meter.reserveIntermediate(bytes: 1, at: .projection)
        } throws: { error in
            error as? DatabaseWorkLimitError
                == .maximumIntermediateBytes(
                    stage: .projection,
                    consumed: 8,
                    requested: 1,
                    maximum: 8
                )
        }
        let second = Task {
            try await storage.withTransaction { transaction in
                try await readPointValue(
                    using: transaction,
                    for: nonEmptyKey,
                    snapshot: true,
                    workMeter: meter,
                    at: .indexScan
                )
            }
        }
        let secondMonitor = try await secondBarrier.waitUntilEntered(
            beforeCompletionOf: second
        )

        #expect(storage.control.boundedValueReadMaximums.count == 2)
        #expect(
            storage.control.boundedValueReadMaximums.reduce(0, +) <= 8
        )
        #expect(
            storage.control.boundedValueReadMaximums.sorted() == [0, 8]
        )

        firstBarrier.release()
        secondBarrier.release()
        switch await first.result {
        case .success(let value):
            #expect(value == ByteString())
        case .failure(let error):
            Issue.record("First empty point read failed: \(error)")
        }
        switch await second.result {
        case .success(let value):
            Issue.record("Second non-empty point read succeeded: \(value)")
        case .failure(let error):
            #expect(
                error as? DatabaseWorkLimitError
                    == .maximumIntermediateBytes(
                        stage: .indexScan,
                        consumed: 8,
                        requested: 4,
                        maximum: 8
                    )
            )
        }
        await firstMonitor.value
        await secondMonitor.value
        #expect(meter.pendingPointReadBytes == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("successful point reads retain only the returned value bytes")
    func successfulPointReadTransfersReturnedBytes() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        defer { await storage.waitUntilShutdown() }
        let key = ByteString(utf8: "point-read-success")
        let expected = ByteString([1, 2, 3, 4])
        try await storage.withTransaction { transaction in
            try transaction.setValue(expected, for: key)
        }
        let meter = makeWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateBytes: 8
            )
        )

        var value: ByteString? = try await storage.withTransaction {
            transaction in
            try await readPointValue(
                using: transaction,
                for: key,
                snapshot: true,
                workMeter: meter,
                at: .indexScan
            )
        }

        let expectedAddress = try #require(expected.withUnsafeBytes { buffer in
            buffer.baseAddress.map { UInt(bitPattern: $0) }
        })
        let actualAddress = try #require(value?.withUnsafeBytes { buffer in
            buffer.baseAddress.map { UInt(bitPattern: $0) }
        })

        #expect(value == expected)
        #expect(value?.isStorageSelfContained == true)
        #expect(value?.retainedByteCount == value?.count)
        #expect(actualAddress == expectedAddress)
        #expect(storage.control.boundedValueReadMaximums == [8])
        #expect(meter.pendingPointReadBytes == 0)
        #expect(meter.retainedIntermediateBytes == 4)
        var alias = value
        value = nil
        #expect(alias == expected)
        #expect(meter.retainedIntermediateBytes == 4)
        alias = nil
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("missing point reads release their pending allowance")
    func missingPointReadReleasesAllowance() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        defer { await storage.waitUntilShutdown() }
        let meter = makeWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateBytes: 8
            )
        )

        let value: ByteString? = try await storage.withTransaction {
            transaction in
            try await readPointValue(
                using: transaction,
                for: ByteString(utf8: "point-read-missing"),
                snapshot: true,
                workMeter: meter,
                at: .indexScan
            )
        }

        #expect(value == nil)
        #expect(storage.control.boundedValueReadMaximums == [8])
        #expect(meter.pendingPointReadBytes == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("matching storage over-limit maps without poisoning the transaction")
    func matchingPointReadOverLimitPreservesTransaction() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        defer { await storage.waitUntilShutdown() }
        let key = ByteString(utf8: "point-read-over-limit")
        let expected = ByteString([1, 2, 3, 4])
        let transaction = try storage.createTransaction()
        try transaction.setValue(expected, for: key)
        let meter = makeWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateBytes: 2
            )
        )

        var failure: (any Error)?
        do {
            _ = try await readPointValue(
                using: transaction,
                for: key,
                snapshot: true,
                workMeter: meter,
                at: .indexScan
            )
        } catch {
            failure = error
        }

        #expect(
            failure as? DatabaseWorkLimitError
                == .maximumIntermediateBytes(
                    stage: .indexScan,
                    consumed: 0,
                    requested: 4,
                    maximum: 2
                )
        )
        #expect(transaction.storageFailure == nil)
        #expect(meter.pendingPointReadBytes == 0)
        #expect(meter.retainedIntermediateBytes == 0)
        #expect(try await transaction.getValue(for: key, snapshot: true) == expected)
        try await transaction.cancel()
    }

    @Test("cancellation releases the allowance and leaves the transaction usable")
    func cancelledPointReadReleasesAllowance() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        defer { await storage.waitUntilShutdown() }
        let key = ByteString(utf8: "point-read-cancel")
        let expected = ByteString([5, 6, 7, 8])
        let setup = try storage.createTransaction()
        try setup.setValue(expected, for: key)
        try await setup.commit()

        let transaction = try storage.createTransaction()
        let meter = makeWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateBytes: 8
            )
        )
        let barrier = storage.control.suspendNextValueRead(for: key)
        let task = Task {
            try await readPointValue(
                using: transaction,
                for: key,
                snapshot: true,
                workMeter: meter,
                at: .indexScan
            )
        }
        let monitor = try await barrier.waitUntilEntered(
            beforeCompletionOf: task
        )
        task.cancel()
        barrier.release()

        switch await task.result {
        case .success:
            Issue.record("Cancelled point read completed successfully")
        case .failure(let error):
            #expect(error is CancellationError)
        }
        await monitor.value
        #expect(meter.pendingPointReadBytes == 0)
        #expect(meter.retainedIntermediateBytes == 0)
        #expect(transaction.storageFailure == nil)
        #expect(try await transaction.getValue(for: key, snapshot: true) == expected)
        try await transaction.cancel()
    }

    @Test("mismatched storage over-limit metadata remains a storage error")
    func mismatchedPointReadOverLimitPreservesStorageError() async throws {
        let errors = [
            StorageError.pointReadValueTooLarge(
                observedByteCount: 5,
                maximumByteCount: 999
            ),
            StorageError.pointReadValueTooLarge(
                observedByteCount: 4,
                maximumByteCount: 8
            ),
            StorageError(
                code: .backendFailure,
                operation: .read,
                backend: .inMemory,
                message: "scripted read failure"
            )
        ]

        for expected in errors {
            let transaction = ScriptedPointReadTransaction(
                outcome: .failure(expected)
            )
            let meter = makeWorkMeter(
                budget: ExecutionBudget(
                    maximumIntermediateBytes: 8
                )
            )

            var failure: (any Error)?
            do {
                _ = try await readPointValue(
                    using: transaction,
                    for: ByteString(utf8: "point-read-mismatch"),
                    snapshot: true,
                    workMeter: meter,
                    at: .indexScan
                )
            } catch {
                failure = error
            }

            #expect(failure as? StorageError == expected)
            #expect(meter.pendingPointReadBytes == 0)
            #expect(meter.retainedIntermediateBytes == 0)
        }
    }

    @Test("a backend over-return is an explicit contract failure")
    func backendOverReturnFailsExplicitly() async throws {
        let transaction = ScriptedPointReadTransaction(
            outcome: .success(ByteString([1, 2, 3, 4, 5, 6, 7, 8, 9]))
        )
        let meter = makeWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateBytes: 8
            )
        )

        var failure: (any Error)?
        do {
            _ = try await readPointValue(
                using: transaction,
                for: ByteString(utf8: "point-read-over-return"),
                snapshot: true,
                workMeter: meter,
                at: .indexScan
            )
        } catch {
            failure = error
        }

        guard let error = failure as? StorageError else {
            Issue.record("Expected a StorageError contract failure")
            return
        }
        #expect(error.code == .backendContractViolation)
        #expect(error.operation == .read)
        #expect(
            error.byteLimitViolation == StorageByteLimitViolation(
                resource: .value,
                observedByteCount: 9,
                maximumByteCount: 8,
                measurement: .exact
            )
        )
        #expect(error.underlyingDescription?.contains("observedByteCount=9") == true)
        #expect(error.underlyingDescription?.contains("maximumByteCount=8") == true)
        #expect(!(failure is DatabaseWorkLimitError))
        #expect(meter.pendingPointReadBytes == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("point-read completion atomically shrinks the issued allowance")
    func pointReadCompletionTransfersExactReturnedBytes() throws {
        let meter = makeWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateBytes: 8
            )
        )
        let allowance = try meter.admitPointRead(at: .indexScan)
        #expect(allowance.issuedByteCount == 8)
        #expect(meter.pendingPointReadBytes == 8)

        let reservation = try allowance.complete(returnedByteCount: 4)
        #expect(meter.pendingPointReadBytes == 0)
        #expect(meter.retainedIntermediateBytes == 4)
        reservation.release()
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("point-read admission clamps the backend maximum to Int")
    func pointReadAdmissionClampsToIntMaximum() throws {
        let meter = makeWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateBytes: UInt64.max
            )
        )
        let allowance = try meter.admitPointRead(at: .indexScan)
        #expect(allowance.issuedByteCount == Int.max)
        #expect(meter.pendingPointReadBytes == UInt64(Int.max))
        allowance.release()
        #expect(meter.pendingPointReadBytes == 0)
    }

    @Test("whole-child absorption rejects a foreign meter before mutation")
    func wholeChildAbsorptionRejectsForeignMeterBeforeMutation() throws {
        let destinationMeter = makeWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateRows: 4,
                maximumIntermediateBytes: 16
            )
        )
        let foreignMeter = makeWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateRows: 4,
                maximumIntermediateBytes: 16
            )
        )
        let destination = try destinationMeter.reserveIntermediate(
            rows: 1,
            bytes: 4,
            at: .projection
        )
        let foreign = try foreignMeter.reserveIntermediate(
            rows: 1,
            bytes: 2,
            at: .projection
        )

        #expect {
            try destination.absorbAll(from: foreign)
        } throws: { error in
            error as? DatabaseIntermediateReservationError
                == .workMeterMismatch
        }
        #expect(destinationMeter.retainedIntermediateRows == 1)
        #expect(destinationMeter.retainedIntermediateBytes == 4)
        #expect(foreignMeter.retainedIntermediateRows == 1)
        #expect(foreignMeter.retainedIntermediateBytes == 2)

        #expect {
            try destination.absorbAll(from: destination)
        } throws: { error in
            error as? DatabaseIntermediateReservationError
                == .transferToSelf
        }
        #expect(destinationMeter.retainedIntermediateRows == 1)
        #expect(destinationMeter.retainedIntermediateBytes == 4)

        let releasedChild = try destination.reserveChild(
            rows: 1,
            bytes: 1,
            at: .projection
        )
        releasedChild.release()
        #expect {
            try destination.absorbAll(from: releasedChild)
        } throws: { error in
            error as? DatabaseIntermediateReservationError
                == .alreadyReleased
        }
        #expect(destinationMeter.retainedIntermediateRows == 1)
        #expect(destinationMeter.retainedIntermediateBytes == 4)
        #expect(foreignMeter.retainedIntermediateRows == 1)
        #expect(foreignMeter.retainedIntermediateBytes == 2)

        destination.release()
        #expect(destinationMeter.retainedIntermediateRows == 0)
        #expect(destinationMeter.retainedIntermediateBytes == 0)
        foreign.release()
        #expect(foreignMeter.retainedIntermediateRows == 0)
        #expect(foreignMeter.retainedIntermediateBytes == 0)
    }

    @Test("whole-child absorption transfers ownership without changing totals")
    func completeChildAbsorptionTransfersOwnership() throws {
        let meter = makeWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateRows: 3,
                maximumIntermediateBytes: 12
            )
        )
        let retainedOwner = try meter.reserveIntermediate(
            rows: 1,
            bytes: 4,
            at: .indexScan
        )
        let decodedOwner = try retainedOwner.reserveChild(
            rows: 1,
            bytes: 2,
            at: .indexScan
        )
        try decodedOwner.reserveAdditional(
            rows: 1,
            bytes: 6,
            at: .indexScan
        )

        #expect(meter.retainedIntermediateRows == 3)
        #expect(meter.retainedIntermediateBytes == 12)

        try retainedOwner.absorbAll(from: decodedOwner)

        #expect(meter.retainedIntermediateRows == 3)
        #expect(meter.retainedIntermediateBytes == 12)
        #expect(meter.peakIntermediateRows == 3)
        #expect(meter.peakIntermediateBytes == 12)

        #expect {
            try decodedOwner.reserveAdditional(
                bytes: 1,
                at: .indexScan
            )
        } throws: { error in
            error as? DatabaseIntermediateReservationError
                == .alreadyReleased
        }
        #expect {
            try decodedOwner.releasePartial(bytes: 1)
        } throws: { error in
            error as? DatabaseIntermediateReservationError
                == .alreadyReleased
        }
        #expect {
            try retainedOwner.absorbAll(from: decodedOwner)
        } throws: { error in
            error as? DatabaseIntermediateReservationError
                == .alreadyReleased
        }
        #expect(meter.retainedIntermediateRows == 3)
        #expect(meter.retainedIntermediateBytes == 12)

        decodedOwner.release()
        #expect(meter.retainedIntermediateRows == 3)
        #expect(meter.retainedIntermediateBytes == 12)

        retainedOwner.release()
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private enum ClaimResult: Equatable {
        case admitted
        case rejected
        case unexpected(String)
    }
}

private final class ManualWorkMeterClock: StorageMonotonicClock, Sendable {
    private let instant = Mutex(
        StorageInstant(durationSinceReference: .zero)
    )

    var now: StorageInstant {
        instant.withLock { $0 }
    }

    func advance(by duration: Duration) {
        instant.withLock { instant in
            instant = instant.advanced(by: duration)
        }
    }

    func sleep(
        until deadline: StorageInstant
    ) async throws(StorageClockError) {
        instant.withLock { instant in
            if instant < deadline {
                instant = deadline
            }
        }
    }
}

private final class ScriptedPointReadTransaction:
    TransactionReadAccess,
    Sendable
{
    enum Outcome: Sendable {
        case failure(StorageError)
        case success(ByteString?)
    }

    let transactionDomain = StorageTransactionDomain()
    private let outcome: Outcome

    init(outcome: Outcome) {
        self.outcome = outcome
    }

    func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        try result()
    }

    func getValue(
        for key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int
    ) async throws -> ByteString? {
        try result()
    }

    func getValue(for key: ByteString) async throws -> ByteString? {
        try result()
    }

    func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        nil
    }

    func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        KeyValueCursor(validatingScope: {})
    }

    private func result() throws -> ByteString? {
        switch outcome {
        case .failure(let error):
            throw error
        case .success(let value):
            return value
        }
    }
}
