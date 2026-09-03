import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing
@testable import RankIndex

@Suite("Rank scanner retained result")
struct RankScannerRetainedResultTests {
    @Test("Top, range, bottom, and nth scans preserve order and rank")
    func scansPreserveOrderAndRank() async throws {
        let (engine, scoresSubspace) = try await makeFixture()

        let topMeter = makeMeter()
        var top: RankScanResult? = try await engine.withTransaction { transaction in
            try await RankScanner(
                scoresSubspace: scoresSubspace,
                transaction: transaction,
                workMeter: topMeter
            ).top(k: 3)
        }
        let topEntries = try entries(from: try #require(top))
        #expect(topEntries.map(\.score) == [400, 300, 200])
        #expect(topEntries.map(\.rank) == [0, 1, 2])
        #expect(
            try primaryKey(
                at: 2,
                in: try #require(top),
                equals: Tuple("tenant-b", "mid-b")
            )
        )
        top = nil
        #expect(topMeter.retainedIntermediateRows == 0)
        #expect(topMeter.retainedIntermediateBytes == 0)

        let rangeMeter = makeMeter()
        var range: RankScanResult? = try await engine.withTransaction { transaction in
            try await RankScanner(
                scoresSubspace: scoresSubspace,
                transaction: transaction,
                workMeter: rangeMeter
            ).rangeDescending(from: 1, to: 4)
        }
        let rangeEntries = try entries(from: try #require(range))
        #expect(rangeEntries.map(\.score) == [300, 200, 200])
        #expect(rangeEntries.map(\.rank) == [1, 2, 3])
        range = nil
        #expect(rangeMeter.retainedIntermediateRows == 0)

        let bottomMeter = makeMeter()
        var bottom: RankScanResult? = try await engine.withTransaction { transaction in
            try await RankScanner(
                scoresSubspace: scoresSubspace,
                transaction: transaction,
                workMeter: bottomMeter
            ).bottom(k: 2, startRank: 4)
        }
        let bottomEntries = try entries(from: try #require(bottom))
        #expect(bottomEntries.map(\.score) == [100, 200])
        #expect(bottomEntries.map(\.rank) == [4, 3])
        bottom = nil
        #expect(bottomMeter.retainedIntermediateRows == 0)

        let nthMeter = makeMeter()
        var nth: RankScanResult? = try await engine.withTransaction { transaction in
            try await RankScanner(
                scoresSubspace: scoresSubspace,
                transaction: transaction,
                workMeter: nthMeter
            ).nthFromTop(2)
        }
        let nthEntries = try entries(from: try #require(nth))
        #expect(nthEntries.count == 1)
        #expect(nthEntries.first?.score == 200)
        #expect(nthEntries.first?.rank == 2)
        nth = nil
        #expect(nthMeter.retainedIntermediateRows == 0)
    }

    @Test("Malformed storage entry releases the retained scan owner")
    func malformedEntryReleasesOwner() async throws {
        let engine = InMemoryEngine()
        let scoresSubspace = Subspace(
            prefix: Tuple("rank-retained-malformed").pack()
        ).subspace("scores")
        let malformedKey = scoresSubspace.pack(Tuple(Int64(10)))
        try await engine.withTransaction { transaction in
            try transaction.setValue(ByteString([]), for: malformedKey)
        }

        let meter = makeMeter()
        await #expect(
            throws: RankScannerError.malformedEntry(elementCount: 1)
        ) {
            try await engine.withTransaction { transaction in
                try await RankScanner(
                    scoresSubspace: scoresSubspace,
                    transaction: transaction,
                    workMeter: meter
                ).top(k: 1)
            }
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Composite key storage is admitted before tuple allocation")
    func compositeKeyBudgetRejectsBeforeUnadmittedAllocation() async throws {
        let scoresSubspace = Subspace(
            prefix: Tuple("rank-retained-budget").pack()
        ).subspace("scores")
        var tupleElements: [any TupleElement] = [Int64(10)]
        for _ in 0..<1_024 {
            tupleElements.append("x")
        }
        let packedKey = scoresSubspace.pack(Tuple(tupleElements))
        let releaseRecorder = ByteOwnerReleaseRecorder()
        var sourceOwner: BackendOwnedKeyOwner? = BackendOwnedKeyOwner(
            bytes: Array(packedKey),
            releaseRecorder: releaseRecorder
        )
        var sourceKey: ByteString? = ByteString(
            retaining: sourceOwner!
        )
        var transaction: SingleRowReadTransaction? =
            SingleRowReadTransaction(
                row: (sourceKey!, ByteString([]))
            )
        sourceKey = nil

        let maximumBytes = UInt64(packedKey.count) + 4_096
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateRows: 8,
                maximumIntermediateBytes: maximumBytes
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
        do {
            _ = try await RankScanner(
                scoresSubspace: scoresSubspace,
                transaction: transaction!,
                workMeter: meter
            ).top(k: 1)
            Issue.record("Expected tuple allocation admission to fail")
        } catch let error as DatabaseWorkLimitError {
            guard case .maximumIntermediateBytes(
                .indexScan,
                let consumed,
                let requested,
                maximumBytes
            ) = error else {
                Issue.record("Unexpected work-limit error: \(error)")
                return
            }
            #expect(requested > 0)
            #expect(consumed <= maximumBytes)
            #expect(requested > maximumBytes - consumed)
            #expect(meter.peakIntermediateBytes <= maximumBytes)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        transaction = nil
        sourceOwner = nil
        #expect(releaseRecorder.isReleased)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Cancelled scans release the retained scan owner")
    func cancelledScanReleasesOwner() async throws {
        let scoresSubspace = Subspace(
            prefix: Tuple("rank-retained-cancel").pack()
        ).subspace("scores")
        let secondAdvance = StorageOperationBarrier()
        let transaction = CancellationReadTransaction(
            rows: [
                (
                    scoresSubspace.pack(Tuple(300, "first")),
                    ByteString([])
                ),
                (
                    scoresSubspace.pack(Tuple(200, "second")),
                    ByteString([])
                ),
            ],
            secondAdvance: secondAdvance
        )

        let meter = makeMeter()
        let task = Task {
            try await RankScanner(
                scoresSubspace: scoresSubspace,
                transaction: transaction,
                workMeter: meter
            ).top(k: 3)
        }
        let secondMonitor = try await secondAdvance.waitUntilEntered(
            beforeCompletionOf: task
        )
        #expect(meter.retainedIntermediateRows == 1)
        task.cancel()
        secondAdvance.release()
        _ = await secondMonitor.value

        await #expect(throws: CancellationError.self) {
            try await task.value
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private final class CancellationReadTransaction:
        TransactionReadAccess,
        Sendable
    {
        let transactionDomain = StorageTransactionDomain()
        private let rows: [(ByteString, ByteString)]
        private let secondAdvance: StorageOperationBarrier

        init(
            rows: [(ByteString, ByteString)],
            secondAdvance: StorageOperationBarrier
        ) {
            self.rows = rows
            self.secondAdvance = secondAdvance
        }

        func getValue(
            for key: ByteString,
            snapshot: Bool
        ) async throws -> ByteString? {
            nil
        }

        func getValue(
            for key: ByteString,
            snapshot: Bool,
            maximumByteCount: Int
        ) async throws -> ByteString? {
            nil
        }

        func getValue(for key: ByteString) async throws -> ByteString? {
            nil
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
            KeyValueCursor(
                consuming: CancellationRangeResult(
                    rows: rows,
                    secondAdvance: secondAdvance
                )
            )
        }
    }

    private struct CancellationRangeResult: TransactionRangeResult, Sendable {
        let rows: [(ByteString, ByteString)]
        let secondAdvance: StorageOperationBarrier

        func makeCursor() -> Cursor {
            Cursor(rows: rows, secondAdvance: secondAdvance)
        }

        struct Cursor: TransactionRangeCursor, Sendable {
            typealias Element = (ByteString, ByteString)

            let rows: [(ByteString, ByteString)]
            let secondAdvance: StorageOperationBarrier
            var index = 0

            mutating func next() async throws -> Element? {
                guard index < rows.count else {
                    return nil
                }
                let row = rows[index]
                index += 1
                if index == 2 {
                    await secondAdvance.enterAndWait()
                }
                return row
            }

            mutating func finish(
                isolation actor: isolated (any Actor)?
            ) async throws {
                index = rows.count
            }
        }
    }

    @Test("Count reads use the bounded point-read contract")
    func countReadIsBounded() async throws {
        let index = try ResolvedIndex(
            for: CountRankEntity.self,
            name: "CountRankEntity_rank_score",
            definition: .rank(
                score: FieldIdentity(name: "score", number: 2)
            ),
            rootExpression: FieldKeyExpression(fieldName: "score"),
            itemTypes: [CountRankEntity.persistableType]
        )
        let maintainer = RankIndexMaintainer<CountRankEntity, Int64>(
            index: index,
            subspace: Subspace(prefix: Tuple("rank-count").pack()),
            idExpression: FieldKeyExpression(fieldName: "id")
        )
        let transaction = RecordingReadTransaction(
            value: ByteConversion.int64ToBytes(7)
        )

        let count = try await maintainer.getCount(
            transaction: transaction,
            workMeter: DatabaseWorkMeter(
                budget: ExecutionBudget(),
                monotonicClock: TestProcessMonotonicClock()
            )
        )

        #expect(count == 7)
        #expect(transaction.maximumByteCounts == [16 * 1_024 * 1_024])
    }

    private struct ObservedEntry: Sendable {
        let score: Int64
        let rank: Int
    }

    private func entries(
        from result: RankScanResult
    ) throws -> [ObservedEntry] {
        var output: [ObservedEntry] = []
        output.reserveCapacity(result.count)
        for index in 0..<result.count {
            try result.withAnnotation(at: index) { annotation in
                output.append(
                    ObservedEntry(
                        score: try TupleDecoder.decode(
                            annotation.scoreElement,
                            as: Int64.self
                        ),
                        rank: annotation.rank
                    )
                )
            }
        }
        return output
    }

    private func primaryKey(
        at position: Int,
        in result: RankScanResult,
        equals expected: Tuple
    ) throws -> Bool {
        var matches = false
        result.withRetainedPrimaryKey(at: position) { primaryKey in
            matches = primaryKey == expected
        }
        return matches
    }

    private func makeMeter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateRows: 32,
                maximumIntermediateBytes: 1_024 * 1_024
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }

    private func makeFixture() async throws -> (
        engine: InMemoryEngine,
        scoresSubspace: Subspace
    ) {
        let engine = InMemoryEngine()
        let scoresSubspace = Subspace(
            prefix: Tuple("rank-retained-order").pack()
        ).subspace("scores")
        let values: [(Int64, String, String)] = [
            (100, "tenant-a", "low"),
            (200, "tenant-a", "mid-a"),
            (200, "tenant-b", "mid-b"),
            (300, "tenant-a", "high"),
            (400, "tenant-b", "top"),
        ]
        try await engine.withTransaction { transaction in
            for (score, tenant, identifier) in values {
                try transaction.setValue(
                    ByteString([]),
                    for: scoresSubspace.pack(
                        Tuple(score, tenant, identifier)
                    )
                )
            }
        }
        return (engine, scoresSubspace)
    }
}

@Persistable
private struct CountRankEntity {
    let id: String
    let score: Int64
}

private final class RecordingReadTransaction: TransactionReadAccess, Sendable {
    private struct State: Sendable {
        var maximumByteCounts: [Int] = []
    }

    let transactionDomain = StorageTransactionDomain()
    private let state = Mutex(State())
    private let value: ByteString

    init(value: ByteString) {
        self.value = value
    }

    var maximumByteCounts: [Int] {
        state.withLock { $0.maximumByteCounts }
    }

    func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        value
    }

    func getValue(for key: ByteString) async throws -> ByteString? {
        value
    }

    func getValue(
        for key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int
    ) async throws -> ByteString? {
        state.withLock { $0.maximumByteCounts.append(maximumByteCount) }
        return value
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
}

private final class ByteOwnerReleaseRecorder: Sendable {
    private let state = Mutex(false)

    var isReleased: Bool {
        state.withLock { $0 }
    }

    func markReleased() {
        state.withLock { $0 = true }
    }
}

private final class BackendOwnedKeyOwner: ByteStringOwner, Sendable {
    private let bytes: [UInt8]
    private let releaseRecorder: ByteOwnerReleaseRecorder

    let count: Int
    let retainedByteCount: Int? = nil
    let isStorageSelfContained = false

    init(
        bytes: [UInt8],
        releaseRecorder: ByteOwnerReleaseRecorder
    ) {
        self.bytes = bytes
        self.count = bytes.count
        self.releaseRecorder = releaseRecorder
    }

    deinit {
        releaseRecorder.markReleased()
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try bytes.withUnsafeBytes(body)
    }
}

private final class SingleRowReadTransaction: TransactionReadAccess, Sendable {
    let transactionDomain = StorageTransactionDomain()
    private let row: (ByteString, ByteString)

    init(row: (ByteString, ByteString)) {
        self.row = row
    }

    func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        nil
    }

    func getValue(
        for key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int
    ) async throws -> ByteString? {
        nil
    }

    func getValue(for key: ByteString) async throws -> ByteString? {
        nil
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
        KeyValueCursor(
            consuming: SingleRowRangeResult(row: row)
        )
    }
}

private struct SingleRowRangeResult: TransactionRangeResult, Sendable {
    let row: (ByteString, ByteString)

    func makeCursor() -> Cursor {
        Cursor(row: row)
    }

    struct Cursor: TransactionRangeCursor, Sendable {
        typealias Element = (ByteString, ByteString)

        let row: Element
        var emitted = false

        mutating func next() async throws -> Element? {
            guard !emitted else { return nil }
            emitted = true
            return row
        }

        mutating func finish(
            isolation actor: isolated (any Actor)?
        ) async throws {
            emitted = true
        }
    }
}
