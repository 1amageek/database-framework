import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing
@testable import DatabaseEngine

@Suite("Item storage retained ownership")
struct ItemStorageRetainedOwnershipTests {
    @Test("Exact byte assembly publishes one bounded self-contained owner")
    func exactByteAssemblyPublishesBoundedOwner() {
        let first = ByteString([0x01, 0x02])
        let second = ByteString([0x03])
        let third = ByteString([0x04, 0x05, 0x06])
        let assembled: ByteString
        do {
            let owner = ExactByteAssemblyOwner(count: 6)
            owner.append(first)
            owner.append(second)
            owner.append(third)
            assembled = owner.finish()
        }

        #expect(assembled == ByteString([0x01, 0x02, 0x03, 0x04, 0x05, 0x06]))
        #expect(assembled.retainedByteCount == 6)
    }

    @Test("Unmeasurable backend owners are detached under the request budget")
    func unmeasurableBackendOwnerIsDetached() async throws {
        let engine = InMemoryEngine()
        let itemKey = ByteString([0x01, 0x02])
        let blobsSubspace = Subspace("retained-item-blobs")
        let payload = ByteString(Array("owned-payload".utf8))

        try await engine.withTransaction { transaction in
            let storage = ItemStorageWriter(
                transaction: transaction,
                blobsSubspace: blobsSubspace,
                configuration: .v1
            )
            try await storage.write(payload, for: itemKey)
        }

        let envelopeByteCount = try await engine.withTransaction {
            transaction in
            let stored = try await transaction.getValue(for: itemKey)
            return try #require(stored).count
        }
        let releaseProbe = UnmeasurableOwnerReleaseProbe()
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(maximumIntermediateBytes: 1_024),
            monotonicClock: TestProcessMonotonicClock()
        )

        try await engine.withTransaction { transaction in
            let reader = ItemStorageReader(
                transaction: UnmeasurablePointReadAccess(
                    underlying: transaction,
                    targetKey: itemKey,
                    releaseProbe: releaseProbe
                ),
                blobsSubspace: blobsSubspace,
                configuration: .v1
            )
            let read = try await reader.readRetained(
                for: itemKey,
                workMeter: meter,
                stage: .storageRow
            )
            let retained = try #require(read)

            #expect(releaseProbe.releaseCount == 1)
            #expect(meter.retainedIntermediateBytes == envelopeByteCount)
            #expect(meter.peakIntermediateBytes == envelopeByteCount * 2)
            retained.withValue { decoded in
                #expect(decoded == payload)
            }
        }

        #expect(releaseProbe.releaseCount == 1)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Item scans await range cleanup after envelope decode failure")
    func itemScanAwaitsCleanupAfterDecodeFailure() async throws {
        let engine = InMemoryEngine()
        let itemsSubspace = Subspace("item-scan-cleanup")
        let blobsSubspace = Subspace("item-scan-cleanup-blobs")
        let key = itemsSubspace.pack(Tuple("invalid-envelope"))
        try await engine.withTransaction { transaction in
            try transaction.setValue(ByteString(utf8: "invalid"), for: key)
        }
        let probe = RangeFinishProbe()

        await #expect(throws: ItemStorageError.notEnvelopeFormat) {
            try await engine.withTransaction { transaction in
                let reader = ItemStorageReader(
                    transaction: RangeFinishTrackingReadAccess(
                        underlying: transaction,
                        probe: probe
                    ),
                    blobsSubspace: blobsSubspace,
                    configuration: .v1
                )
                var iterator = reader.scan(
                    begin: itemsSubspace.range().begin,
                    end: itemsSubspace.range().end
                ).makeAsyncIterator()
                _ = try await iterator.next()
            }
        }
        #expect(probe.finishCount == 1)
    }

    @Test("Retained item scans await range cleanup after decode failure")
    func retainedItemScanAwaitsCleanupAfterDecodeFailure() async throws {
        let engine = InMemoryEngine()
        let itemsSubspace = Subspace("retained-item-scan-cleanup")
        let blobsSubspace = Subspace("retained-item-scan-cleanup-blobs")
        let key = itemsSubspace.pack(Tuple("invalid-envelope"))
        try await engine.withTransaction { transaction in
            try transaction.setValue(ByteString(utf8: "invalid"), for: key)
        }
        let probe = RangeFinishProbe()
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: TestProcessMonotonicClock()
        )

        await #expect(throws: ItemStorageError.notEnvelopeFormat) {
            try await engine.withTransaction { transaction in
                let reader = ItemStorageReader(
                    transaction: RangeFinishTrackingReadAccess(
                        underlying: transaction,
                        probe: probe
                    ),
                    blobsSubspace: blobsSubspace,
                    configuration: .v1
                )
                var iterator = reader.scanRetained(
                    begin: itemsSubspace.range().begin,
                    end: itemsSubspace.range().end,
                    workMeter: meter,
                    stage: .storageRow
                ).makeAsyncIterator()
                _ = try await iterator.next()
            }
        }
        #expect(probe.finishCount == 1)
        #expect(meter.retainedIntermediateBytes == 0)
    }
}

private struct UnmeasurablePointReadAccess: TransactionReadAccess {
    let underlying: any TransactionReadAccess
    let targetKey: ByteString
    let releaseProbe: UnmeasurableOwnerReleaseProbe

    var capabilities: TransactionCapabilities { underlying.capabilities }

    func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        guard let value = try await underlying.getValue(
            for: key,
            snapshot: snapshot
        ) else {
            return nil
        }
        guard key == targetKey else { return value }
        return ByteString(
            retaining: UnmeasurableByteOwner(
                bytes: value.copyBytes(),
                releaseProbe: releaseProbe
            )
        )
    }

    func getValue(for key: ByteString) async throws -> ByteString? {
        try await getValue(for: key, snapshot: false)
    }

    func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        try await underlying.getKey(selector: selector, snapshot: snapshot)
    }

    func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        underlying.rangeCursor(
            from: begin,
            to: end,
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode
        )
    }
}

private final class UnmeasurableByteOwner: ByteStringOwner, Sendable {
    let bytes: [UInt8]
    let releaseProbe: UnmeasurableOwnerReleaseProbe

    init(
        bytes: consuming [UInt8],
        releaseProbe: UnmeasurableOwnerReleaseProbe
    ) {
        self.bytes = bytes
        self.releaseProbe = releaseProbe
    }

    deinit {
        releaseProbe.recordRelease()
    }

    var count: Int { bytes.count }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try bytes.withUnsafeBytes(body)
    }
}

private final class UnmeasurableOwnerReleaseProbe: Sendable {
    private let state = Mutex(0)

    var releaseCount: Int { state.withLock { $0 } }

    func recordRelease() {
        state.withLock { $0 += 1 }
    }
}

private struct RangeFinishTrackingReadAccess: TransactionReadAccess {
    let underlying: any TransactionReadAccess
    let probe: RangeFinishProbe

    var capabilities: TransactionCapabilities { underlying.capabilities }

    func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        try await underlying.getValue(for: key, snapshot: snapshot)
    }

    func getValue(for key: ByteString) async throws -> ByteString? {
        try await underlying.getValue(for: key)
    }

    func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        try await underlying.getKey(selector: selector, snapshot: snapshot)
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
            consuming: RangeFinishTrackingResult(
                cursor: underlying.rangeCursor(
                    from: begin,
                    to: end,
                    limit: limit,
                    reverse: reverse,
                    snapshot: snapshot,
                    streamingMode: streamingMode
                ),
                probe: probe
            )
        )
    }
}

private struct RangeFinishTrackingResult: TransactionRangeResult {
    let cursor: KeyValueCursor
    let probe: RangeFinishProbe

    func makeCursor() -> RangeFinishTrackingCursor {
        RangeFinishTrackingCursor(cursor: cursor, probe: probe)
    }
}

private struct RangeFinishTrackingCursor: TransactionRangeCursor {
    private var cursor: KeyValueCursor
    private let probe: RangeFinishProbe

    init(cursor: KeyValueCursor, probe: RangeFinishProbe) {
        self.cursor = cursor
        self.probe = probe
    }

    mutating func next() async throws -> KeyValueCursor.Element? {
        try await cursor.next()
    }

    mutating func finish(
        isolation actor: isolated (any Actor)?
    ) async throws {
        defer { probe.recordFinish() }
        try await cursor.finish()
    }
}

private final class RangeFinishProbe: Sendable {
    private let state = Mutex(0)

    var finishCount: Int { state.withLock { $0 } }

    func recordFinish() {
        state.withLock { $0 += 1 }
    }
}
