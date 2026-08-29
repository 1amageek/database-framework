import DatabaseTypes
import StorageKit

@_spi(DatabaseExecution)
public struct DatabaseMutationStateStore: Sendable {
    private let container: DBContainer

    public var boundContainer: DBContainer { container }

    public var containerIdentity: ObjectIdentifier {
        ObjectIdentifier(container)
    }

    public init(container: DBContainer) {
        self.container = container
    }

    public func validateIdempotencyKey(
        _ key: String?,
        maximumKeyBytes: Int
    ) throws -> String {
        guard maximumKeyBytes > 0 else {
            throw DatabaseMutationStateError.invalidLimits
        }
        guard let key, !key.isEmpty else {
            throw DatabaseMutationStateError.idempotencyKeyRequired
        }
        let count = key.utf8.count
        guard count <= maximumKeyBytes else {
            throw DatabaseMutationStateError.idempotencyKeyTooLarge(
                actual: count,
                maximum: maximumKeyBytes
            )
        }
        return key
    }

    /// Operation state is Framework runtime metadata, so it derives from the
    /// Framework root of the control domain's Tenant Partition.
    public func controlBinding() -> DatabaseMutationStateBinding {
        DatabaseMutationStateBinding(
            root: container.controlStorage().systemRoot
                .subspace("operation-state")
        )
    }

    /// Operation state owned by one bound Tenant Partition rather than by the
    /// control domain.
    public func binding(
        for storage: DatabaseExecutionStorage
    ) -> DatabaseMutationStateBinding {
        DatabaseMutationStateBinding(
            root: storage.systemRoot.subspace("operation-state")
        )
    }

    public func nextLogicalVersion(
        in binding: DatabaseMutationStateBinding,
        transaction: any TransactionAccess
    ) async throws -> UInt64 {
        let current = try await logicalVersion(
            in: binding.root,
            transaction: transaction,
            snapshot: false
        )
        guard current < UInt64.max else {
            throw DatabaseMutationStateError.logicalVersionOverflow
        }
        let next = current + 1
        try transaction.setValue(
            Self.bigEndianBytes(next),
            for: logicalVersionKey(in: binding.root)
        )
        return next
    }

    public func currentLogicalVersion(
        in binding: DatabaseMutationStateBinding,
        transaction: any TransactionAccess
    ) async throws -> UInt64 {
        try await logicalVersion(
            in: binding.root,
            transaction: transaction,
            snapshot: true
        )
    }

    public func replay(
        for key: String,
        discriminator: ByteString,
        requestFingerprint: ByteString,
        in binding: DatabaseMutationStateBinding,
        transaction: any TransactionAccess,
        limits: DatabaseMutationStateLimits
    ) async throws -> DatabaseMutationReplayRecord? {
        guard let record = try await record(
            for: key,
            in: binding,
            transaction: transaction,
            limits: limits
        ) else {
            return nil
        }
        guard record.discriminator == discriminator,
              record.requestFingerprint == requestFingerprint else {
            throw DatabaseMutationStateError.idempotencyKeyConflict
        }
        return record
    }

    public func store(
        _ record: DatabaseMutationReplayRecord,
        for key: String,
        in binding: DatabaseMutationStateBinding,
        transaction: any TransactionAccess,
        limits: DatabaseMutationStateLimits
    ) throws {
        try validate(key: key, limits: limits)
        let storage = idempotencySubspace(in: binding.root).subspace(key)
        let chunks = storage.subspace("chunks")
        let manifest = try manifest(for: record, limits: limits)
        let metadata = try manifest.encode(limits: limits)
        guard metadata.count <= Int(DatabaseMutationStateManifest.chunkByteCount),
              let chunkCount = Int(exactly: manifest.chunkCount) else {
            throw DatabaseMutationStateError.corruptedState
        }
        let chunkRange = chunks.range()
        try transaction.clearRange(
            beginKey: chunkRange.begin,
            endKey: chunkRange.end
        )
        for index in 0..<chunkCount {
            let lowerBound = index * Int(
                DatabaseMutationStateManifest.chunkByteCount
            )
            let upperBound = min(
                lowerBound + Int(DatabaseMutationStateManifest.chunkByteCount),
                record.outcome.count
            )
            guard let chunkIndex = UInt32(exactly: index),
                  lowerBound < upperBound else {
                throw DatabaseMutationStateError.corruptedState
            }
            let startIndex = record.outcome.startIndex
            let chunk = record.outcome[
                (startIndex + lowerBound)..<(startIndex + upperBound)
            ]
            try transaction.setValue(
                chunk,
                for: Self.chunkKey(in: chunks, index: chunkIndex)
            )
        }
        try transaction.setValue(
            metadata,
            for: storage.pack(Tuple("metadata"))
        )
    }

    public func record(
        for key: String,
        in binding: DatabaseMutationStateBinding,
        transaction: any TransactionAccess,
        limits: DatabaseMutationStateLimits
    ) async throws -> DatabaseMutationReplayRecord? {
        try validate(key: key, limits: limits)
        let entry = idempotencySubspace(in: binding.root).subspace(key)
        let metadata = try await transaction.getValue(
            for: entry.pack(Tuple("metadata")),
            snapshot: false
        )
        let chunks = entry.subspace("chunks")
        guard let metadata else {
            let range = chunks.range()
            let orphanedChunks = try await TransactionRangeCollection.collect(
                using: transaction,
                from: .firstGreaterOrEqual(range.begin),
                to: .firstGreaterOrEqual(range.end),
                limit: 1,
                reverse: false,
                snapshot: false,
                streamingMode: .exact
            )
            guard orphanedChunks.isEmpty else {
                throw DatabaseMutationStateError.corruptedState
            }
            return nil
        }
        guard metadata.count
                <= Int(DatabaseMutationStateManifest.chunkByteCount) else {
            throw DatabaseMutationStateError.corruptedState
        }
        let manifest = try DatabaseMutationStateManifest.decode(
            metadata,
            limits: limits
        )
        guard let expectedChunkCount = Int(exactly: manifest.chunkCount),
              let totalOutcomeBytes = Int(
                exactly: manifest.totalOutcomeBytes
              ) else {
            throw DatabaseMutationStateError.corruptedState
        }
        let (rangeLimit, overflow) = expectedChunkCount.addingReportingOverflow(1)
        guard !overflow else {
            throw DatabaseMutationStateError.corruptedState
        }
        let range = chunks.range()
        let storedChunks = try await TransactionRangeCollection.collect(
            using: transaction,
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: rangeLimit,
            reverse: false,
            snapshot: false,
            streamingMode: .exact
        )
        guard storedChunks.count == expectedChunkCount else {
            throw DatabaseMutationStateError.corruptedState
        }

        var copiedByteCount = 0
        for (index, storedChunk) in storedChunks.enumerated() {
            guard let chunkIndex = UInt32(exactly: index),
                  storedChunk.0 == Self.chunkKey(
                      in: chunks,
                      index: chunkIndex
                  ),
                  let expectedByteCount = manifest.expectedByteCount(
                      forChunkAt: chunkIndex
                  ),
                  storedChunk.1.count == expectedByteCount else {
                throw DatabaseMutationStateError.corruptedState
            }
            let (next, didOverflow) = copiedByteCount.addingReportingOverflow(
                expectedByteCount
            )
            guard !didOverflow, next <= totalOutcomeBytes else {
                throw DatabaseMutationStateError.corruptedState
            }
            copiedByteCount = next
        }
        guard copiedByteCount == totalOutcomeBytes else {
            throw DatabaseMutationStateError.corruptedState
        }

        let outcome = ByteString.copying(count: totalOutcomeBytes) {
            destination in
            var destinationOffset = 0
            for (_, chunk) in storedChunks {
                chunk.withUnsafeBytes { source in
                    let chunkDestination = UnsafeMutableRawBufferPointer(
                        start: destination.baseAddress?.advanced(
                            by: destinationOffset
                        ),
                        count: source.count
                    )
                    chunkDestination.copyMemory(from: source)
                    destinationOffset += source.count
                }
            }
        }
        return DatabaseMutationReplayRecord(
            discriminator: manifest.discriminator,
            requestFingerprint: manifest.requestFingerprint,
            outcomeFingerprint: manifest.outcomeFingerprint,
            outcome: outcome
        )
    }

    private func logicalVersion(
        in root: Subspace,
        transaction: any TransactionAccess,
        snapshot: Bool
    ) async throws -> UInt64 {
        guard let bytes = try await transaction.getValue(
            for: logicalVersionKey(in: root),
            snapshot: snapshot
        ) else {
            return 0
        }
        guard bytes.count == MemoryLayout<UInt64>.size else {
            throw DatabaseMutationStateError.corruptedState
        }
        return bytes.withUnsafeBytes { storage in
            UInt64(storage[0]) << 56
                | UInt64(storage[1]) << 48
                | UInt64(storage[2]) << 40
                | UInt64(storage[3]) << 32
                | UInt64(storage[4]) << 24
                | UInt64(storage[5]) << 16
                | UInt64(storage[6]) << 8
                | UInt64(storage[7])
        }
    }

    private func manifest(
        for record: DatabaseMutationReplayRecord,
        limits: DatabaseMutationStateLimits
    ) throws -> DatabaseMutationStateManifest {
        guard record.outcome.count <= limits.maximumOutcomeBytes else {
            throw DatabaseMutationStateError.outcomeTooLarge(
                actual: record.outcome.count,
                maximum: limits.maximumOutcomeBytes
            )
        }
        guard let totalOutcomeBytes = UInt64(exactly: record.outcome.count),
              let chunkCount = DatabaseMutationStateManifest
                .expectedChunkCount(totalOutcomeBytes: totalOutcomeBytes) else {
            throw DatabaseMutationStateError.corruptedState
        }
        let manifest = DatabaseMutationStateManifest(
            discriminator: record.discriminator,
            requestFingerprint: record.requestFingerprint,
            outcomeFingerprint: record.outcomeFingerprint,
            totalOutcomeBytes: totalOutcomeBytes,
            chunkCount: chunkCount
        )
        try manifest.validate(limits: limits)
        return manifest
    }

    private func validate(
        key: String,
        limits: DatabaseMutationStateLimits
    ) throws {
        guard !key.isEmpty else {
            throw DatabaseMutationStateError.idempotencyKeyRequired
        }
        let count = key.utf8.count
        guard count <= limits.maximumKeyBytes else {
            throw DatabaseMutationStateError.idempotencyKeyTooLarge(
                actual: count,
                maximum: limits.maximumKeyBytes
            )
        }
    }

    private static func chunkKey(
        in chunks: Subspace,
        index: UInt32
    ) -> ByteString {
        chunks.pack(Tuple(UInt64(index)))
    }

    private func logicalVersionKey(in root: Subspace) -> ByteString {
        root.pack(Tuple("logical-commit-version"))
    }

    private func idempotencySubspace(in root: Subspace) -> Subspace {
        root.subspace("idempotency")
    }

    private static func bigEndianBytes(_ value: UInt64) -> ByteString {
        ByteString.copying(count: MemoryLayout<UInt64>.size) { output in
            for offset in 0..<MemoryLayout<UInt64>.size {
                output[offset] = UInt8(
                    truncatingIfNeeded: value >> UInt64((7 - offset) * 8)
                )
            }
        }
    }
}
