import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing

@testable import DatabaseEngine
@testable import BitmapIndex

private final class FailingBoundedReadAccess:
    TransactionReadAccess,
    Sendable {
    private let base: any TransactionReadAccess
    private let failureCall: Int
    private let callCount = Mutex(0)
    private let failure: StorageError

    init(
        base: any TransactionReadAccess,
        failureCall: Int,
        failure: StorageError
    ) {
        self.base = base
        self.failureCall = failureCall
        self.failure = failure
    }

    var transactionDomain: StorageTransactionDomain {
        base.transactionDomain
    }

    func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        try await base.getValue(for: key, snapshot: snapshot)
    }

    func getValue(
        for key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int
    ) async throws -> ByteString? {
        let call = callCount.withLock { count in
            count += 1
            return count
        }
        guard call != failureCall else {
            throw failure
        }
        return try await base.getValue(
            for: key,
            snapshot: snapshot,
            maximumByteCount: maximumByteCount
        )
    }

    func getValue(for key: ByteString) async throws -> ByteString? {
        try await base.getValue(for: key)
    }

    func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        try await base.getKey(selector: selector, snapshot: snapshot)
    }

    func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        base.rangeCursor(
            from: begin,
            to: end,
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode
        )
    }
}

@Suite("Bitmap retained read contract", .serialized)
struct BitmapReadResourceContractTests {
    private func dataKey(
        _ data: Subspace,
        _ value: String
    ) throws -> ByteString {
        data.pack(Tuple(try FieldValue.string(value).toTupleElement()))
    }

    private func makeMeter(
        maximumIntermediateBytes: UInt64 = 64 * 1_024,
        maximumWorkUnits: UInt64 = 128
    ) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 128,
                maximumWorkUnits: maximumWorkUnits,
                maximumIntermediateRows: 128,
                maximumIntermediateBytes: maximumIntermediateBytes,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }

    @Test("bounded reads preserve set operations and release promoted owners")
    func boundedSetOperationsAndOutputPromotion() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        defer { await storage.waitUntilShutdown() }
        let transaction = try storage.createTransaction()
        let root = Subspace("bitmap-retained-contract", "set-operations")
        let reader = BitmapIndexReader(subspace: root)
        let data = root.subspace("data")
        try transaction.setValue(
            try RoaringBitmap([1, 2, 7]).serializedBytes(),
            for: try dataKey(data, "active")
        )
        try transaction.setValue(
            try RoaringBitmap([2, 3, 7]).serializedBytes(),
            for: try dataKey(data, "pending")
        )

        let meter = makeMeter()
        do {
            let union = try await reader.union(
                of: [[try FieldValue.string("active").toTupleElement()],
                     [try FieldValue.string("pending").toTupleElement()]],
                transaction: transaction,
                workMeter: meter
            )
            let output = union.promoteToOutput()
            #expect(output.toArray() == [1, 2, 3, 7])

            let intersection = try await reader.intersection(
                of: [[try FieldValue.string("active").toTupleElement()],
                     [try FieldValue.string("pending").toTupleElement()]],
                transaction: transaction,
                workMeter: meter
            )
            let intersectionOutput = intersection.promoteToOutput()
            #expect(intersectionOutput.toArray() == [2, 7])
        }

        #expect(!storage.control.boundedValueReadMaximums.isEmpty)
        #expect(
            storage.control.boundedValueReadMaximums.allSatisfy {
                $0 > 0 && $0 <= 64 * 1_024
            }
        )
        #expect(meter.pendingPointReadBytes == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("retained primary keys preserve limit and skip missing mappings")
    func retainedPrimaryKeysPreserveLimitAndMissing() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        defer { await storage.waitUntilShutdown() }
        let transaction = try storage.createTransaction()
        let root = Subspace("bitmap-retained-contract", "primary-keys")
        let reader = BitmapIndexReader(subspace: root)
        let data = root.subspace("data")
        let ids = root.subspace("ids")
        try transaction.setValue(
            try RoaringBitmap([1, 2, 3]).serializedBytes(),
            for: try dataKey(data, "active")
        )
        try transaction.setValue(
            Tuple("item-1").pack(),
            for: ids.pack(Tuple(Int(1)))
        )
        try transaction.setValue(
            Tuple("item-2").pack(),
            for: ids.pack(Tuple(Int(2)))
        )

        let meter = makeMeter()
        do {
            let bitmap = try await reader.bitmap(
                for: [try FieldValue.string("active").toTupleElement()],
                transaction: transaction,
                workMeter: meter
            )
            let keys = try await reader.primaryKeys(
                for: bitmap,
                transaction: transaction,
                limit: 2,
                workMeter: meter
            )
            #expect(keys.count == 2)
            var values: [String] = []
            for position in 0..<keys.count {
                try keys.withRetainedPrimaryKey(at: position) { key in
                    if case .string(let value) = try key.value(at: 0) {
                        values.append(value)
                    }
                }
            }
            #expect(values == ["item-1", "item-2"])
        }
        #expect(meter.pendingPointReadBytes == 0)
        #expect(meter.retainedIntermediateBytes == 0)

        let missingMeter = makeMeter()
        do {
            let bitmap = try await reader.bitmap(
                for: [try FieldValue.string("active").toTupleElement()],
                transaction: transaction,
                workMeter: missingMeter
            )
            let keys = try await reader.primaryKeys(
                for: bitmap,
                transaction: transaction,
                workMeter: missingMeter
            )
            #expect(keys.count == 2)
        }
        #expect(missingMeter.pendingPointReadBytes == 0)
        #expect(missingMeter.retainedIntermediateBytes == 0)
    }

    @Test("malformed, oversized, and missing payloads release all claims")
    func malformedOversizedAndMissingReleaseClaims() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        defer { await storage.waitUntilShutdown() }
        let transaction = try storage.createTransaction()
        let root = Subspace("bitmap-retained-contract", "failures")
        let reader = BitmapIndexReader(subspace: root)
        let data = root.subspace("data")
        try transaction.setValue(
            ByteString([0, 1, 2]),
            for: try dataKey(data, "malformed")
        )
        try transaction.setValue(
            try RoaringBitmap([1, 2, 3, 4, 5]).serializedBytes(),
            for: try dataKey(data, "oversized")
        )

        let malformedMeter = makeMeter()
        await #expect {
            _ = try await reader.bitmap(
                for: [try FieldValue.string("malformed").toTupleElement()],
                transaction: transaction,
                workMeter: malformedMeter
            )
        } throws: { error in
            error is RoaringBitmapFormatError
        }
        #expect(malformedMeter.pendingPointReadBytes == 0)
        #expect(malformedMeter.retainedIntermediateBytes == 0)

        let oversizedMeter = makeMeter(maximumIntermediateBytes: 4)
        await #expect {
            _ = try await reader.bitmap(
                for: [try FieldValue.string("oversized").toTupleElement()],
                transaction: transaction,
                workMeter: oversizedMeter
            )
        } throws: { error in
            error is DatabaseWorkLimitError
        }
        #expect(oversizedMeter.pendingPointReadBytes == 0)
        #expect(oversizedMeter.retainedIntermediateBytes == 0)

        let missingMeter = makeMeter()
        let missing = try await reader.bitmap(
            for: [try FieldValue.string("missing").toTupleElement()],
            transaction: transaction,
            workMeter: missingMeter
        )
        #expect(missing.promoteToOutput().isEmpty)
        #expect(missingMeter.pendingPointReadBytes == 0)
        #expect(missingMeter.retainedIntermediateBytes == 0)
    }

    @Test("a later malformed set operand releases the earlier bitmap")
    func laterFailureReleasesEarlierOwner() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        defer { await storage.waitUntilShutdown() }
        let transaction = try storage.createTransaction()
        let root = Subspace("bitmap-retained-contract", "later-failure")
        let reader = BitmapIndexReader(subspace: root)
        let data = root.subspace("data")
        try transaction.setValue(
            try RoaringBitmap([1, 2]).serializedBytes(),
            for: try dataKey(data, "valid")
        )
        try transaction.setValue(
            ByteString([1, 2, 3]),
            for: try dataKey(data, "invalid")
        )
        let meter = makeMeter()
        await #expect {
            _ = try await reader.union(
                of: [[try FieldValue.string("valid").toTupleElement()],
                     [try FieldValue.string("invalid").toTupleElement()]],
                transaction: transaction,
                workMeter: meter
            )
        } throws: { error in
            error is RoaringBitmapFormatError
        }
        #expect(meter.pendingPointReadBytes == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("a later backend failure releases the earlier bitmap")
    func laterBackendFailureReleasesEarlierOwner() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        defer { await storage.waitUntilShutdown() }
        let transaction = try storage.createTransaction()
        let root = Subspace("bitmap-retained-contract", "backend-failure")
        let reader = BitmapIndexReader(subspace: root)
        let data = root.subspace("data")
        try transaction.setValue(
            try RoaringBitmap([1, 2]).serializedBytes(),
            for: try dataKey(data, "first")
        )
        try transaction.setValue(
            try RoaringBitmap([2, 3]).serializedBytes(),
            for: try dataKey(data, "second")
        )
        let failure = StorageError.invalidOperation(
            "Injected bounded bitmap read failure"
        )
        let access = FailingBoundedReadAccess(
            base: transaction,
            failureCall: 2,
            failure: failure
        )
        let meter = makeMeter()
        await #expect {
            _ = try await reader.union(
                of: [[try FieldValue.string("first").toTupleElement()],
                     [try FieldValue.string("second").toTupleElement()]],
                transaction: access,
                workMeter: meter
            )
        } throws: { error in
            error as? StorageError == failure
        }
        #expect(meter.pendingPointReadBytes == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("cancellation during a bounded read releases the allowance")
    func cancellationReleasesAllowance() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        defer { await storage.waitUntilShutdown() }
        let transaction = try storage.createTransaction()
        let root = Subspace("bitmap-retained-contract", "cancellation")
        let reader = BitmapIndexReader(subspace: root)
        let key = try dataKey(root.subspace("data"), "active")
        try transaction.setValue(
            try RoaringBitmap([1]).serializedBytes(),
            for: key
        )
        let meter = makeMeter()
        let barrier = storage.control.suspendNextBoundedValueRead(for: key)
        let task = Task {
            _ = try await reader.bitmap(
                for: [try FieldValue.string("active").toTupleElement()],
                transaction: transaction,
                workMeter: meter
            )
        }
        let monitor = try await barrier.waitUntilEntered(beforeCompletionOf: task)
        task.cancel()
        barrier.release()
        switch await task.result {
        case .success:
            Issue.record("Cancellation unexpectedly completed the bitmap read")
        case .failure(let error):
            #expect(error is CancellationError)
        }
        await monitor.value
        #expect(meter.pendingPointReadBytes == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("foreign meter is rejected before primary-key storage reads")
    func foreignMeterRejectedBeforeReads() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        defer { await storage.waitUntilShutdown() }
        let transaction = try storage.createTransaction()
        let root = Subspace("bitmap-retained-contract", "foreign-meter")
        let reader = BitmapIndexReader(subspace: root)
        let key = try dataKey(root.subspace("data"), "active")
        try transaction.setValue(
            try RoaringBitmap([1]).serializedBytes(),
            for: key
        )
        let sourceMeter = makeMeter()
        let foreignMeter = makeMeter()
        var readsBefore = 0
        do {
            let bitmap = try await reader.bitmap(
                for: [try FieldValue.string("active").toTupleElement()],
                transaction: transaction,
                workMeter: sourceMeter
            )
            readsBefore = storage.control.boundedValueReadMaximums.count
            await #expect {
                try await reader.primaryKeys(
                    for: bitmap,
                    transaction: transaction,
                    workMeter: foreignMeter
                )
            } throws: { error in
                error as? DatabaseIntermediateReservationError
                    == .workMeterMismatch
            }
        }
        #expect(storage.control.boundedValueReadMaximums.count == readsBefore)
        #expect(sourceMeter.pendingPointReadBytes == 0)
        #expect(sourceMeter.retainedIntermediateBytes == 0)
        #expect(foreignMeter.pendingPointReadBytes == 0)
        #expect(foreignMeter.retainedIntermediateBytes == 0)
    }
}
