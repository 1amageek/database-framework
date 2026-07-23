import DatabaseEngine
import DatabaseValue
import DatabaseWire
import StorageKit

public struct DatabaseMutationStateStore: Sendable {
    private let container: DBContainer
    private let logicalVersionKey: Bytes
    private let idempotencySubspace: Subspace

    public init(container: DBContainer) async throws {
        let root = try await container.engine.createOrOpenDirectory(
            path: ["database-framework", "wire-runtime"]
        )
        self.container = container
        self.logicalVersionKey = root.pack(Tuple("logical-commit-version"))
        self.idempotencySubspace = root.subspace("idempotency")
    }

    func validate(container: DBContainer) throws {
        guard self.container === container else {
            throw DatabaseMutationError.stateStoreContainerMismatch
        }
    }

    func nextLogicalVersion(
        transaction: any Transaction
    ) async throws -> UInt64 {
        let current = try await logicalVersion(
            transaction: transaction,
            snapshot: false
        )
        guard current < UInt64.max else {
            throw DatabaseMutationError.logicalVersionOverflow
        }
        let next = current + 1
        try transaction.setValue(Self.bigEndianBytes(next), for: logicalVersionKey)
        return next
    }

    func currentLogicalVersion(
        transaction: any Transaction
    ) async throws -> UInt64 {
        try await logicalVersion(transaction: transaction, snapshot: true)
    }

    private func logicalVersion(
        transaction: any Transaction,
        snapshot: Bool
    ) async throws -> UInt64 {
        guard let bytes = try await transaction.getValue(
            for: logicalVersionKey,
            snapshot: snapshot
        ) else {
            return 0
        }
        guard bytes.count == MemoryLayout<UInt64>.size else {
            throw DatabaseMutationError.idempotencyRecordCorrupted
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

    func idempotencyRecord(
        for key: String,
        transaction: any Transaction,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseIdempotencyRecord? {
        let record = idempotencySubspace.subspace(key)
        let metadata = try await transaction.getValue(
            for: record.pack(Tuple("metadata"))
        )
        let chunks = record.subspace("chunks")
        guard let metadata else {
            let range = chunks.range()
            let orphanedChunks = try await transaction.collectRange(
                begin: range.begin,
                end: range.end,
                limit: 1,
                snapshot: false,
                streamingMode: .exact
            )
            guard orphanedChunks.isEmpty else {
                throw DatabaseMutationError.idempotencyRecordCorrupted
            }
            return nil
        }
        guard metadata.count <= Int(DatabaseIdempotencyManifest.chunkByteCount)
        else {
            throw DatabaseMutationError.idempotencyRecordCorrupted
        }
        let manifest: DatabaseIdempotencyManifest
        do {
            manifest = try DatabaseIdempotencyManifest.decode(
                DatabaseBytes(retaining: metadata),
                limits: limits
            )
        } catch {
            throw DatabaseMutationError.idempotencyRecordCorrupted
        }
        guard let expectedChunkCount = Int(exactly: manifest.chunkCount),
              let totalResponseBytes = Int(exactly: manifest.totalResponseBytes)
        else {
            throw DatabaseMutationError.idempotencyRecordCorrupted
        }
        let (rangeLimit, rangeLimitOverflow) = expectedChunkCount
            .addingReportingOverflow(1)
        guard !rangeLimitOverflow else {
            throw DatabaseMutationError.idempotencyRecordCorrupted
        }
        let range = chunks.range()
        let storedChunks = try await transaction.collectRange(
            begin: range.begin,
            end: range.end,
            limit: rangeLimit,
            snapshot: false,
            streamingMode: .exact
        )
        guard storedChunks.count == expectedChunkCount else {
            throw DatabaseMutationError.idempotencyRecordCorrupted
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
                throw DatabaseMutationError.idempotencyRecordCorrupted
            }
            let (nextCopiedByteCount, overflow) = copiedByteCount
                .addingReportingOverflow(expectedByteCount)
            guard !overflow, nextCopiedByteCount <= totalResponseBytes else {
                throw DatabaseMutationError.idempotencyRecordCorrupted
            }
            copiedByteCount = nextCopiedByteCount
        }
        guard copiedByteCount == totalResponseBytes else {
            throw DatabaseMutationError.idempotencyRecordCorrupted
        }

        let responsePayload = DatabaseBytes.copying(
            count: totalResponseBytes
        ) { destination in
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
        return try DatabaseIdempotencyRecord.reconstruct(
            manifest: manifest,
            responsePayload: responsePayload,
            limits: limits
        )
    }

    func store(
        _ record: DatabaseIdempotencyRecord,
        for key: String,
        transaction: any Transaction,
        limits: DatabaseWireLimits
    ) throws {
        let storage = idempotencySubspace.subspace(key)
        let chunks = storage.subspace("chunks")
        let manifest = try record.manifest(limits: limits)
        let metadata = try manifest.encode(limits: limits)
        guard metadata.count <= Int(DatabaseIdempotencyManifest.chunkByteCount),
              let chunkCount = Int(exactly: manifest.chunkCount) else {
            throw DatabaseMutationError.idempotencyRecordCorrupted
        }
        let chunkRange = chunks.range()
        try transaction.clearRange(
            beginKey: chunkRange.begin,
            endKey: chunkRange.end
        )
        for index in 0..<chunkCount {
            let lowerBound = index * Int(
                DatabaseIdempotencyManifest.chunkByteCount
            )
            let upperBound = min(
                lowerBound + Int(DatabaseIdempotencyManifest.chunkByteCount),
                record.responsePayload.count
            )
            guard let chunkIndex = UInt32(exactly: index),
                  lowerBound < upperBound else {
                throw DatabaseMutationError.idempotencyRecordCorrupted
            }
            let chunk = record.responsePayload.slice(lowerBound..<upperBound)
            try transaction.setValue(
                Bytes(retaining: chunk),
                for: Self.chunkKey(in: chunks, index: chunkIndex)
            )
        }
        try transaction.setValue(
            Bytes(retaining: metadata),
            for: storage.pack(Tuple("metadata"))
        )
    }

    private static func chunkKey(
        in chunks: Subspace,
        index: UInt32
    ) -> Bytes {
        chunks.pack(Tuple(UInt64(index)))
    }

    private static func bigEndianBytes(_ value: UInt64) -> Bytes {
        Bytes.copying(count: MemoryLayout<UInt64>.size) { output in
            for offset in 0..<MemoryLayout<UInt64>.size {
                output[offset] = UInt8(
                    truncatingIfNeeded: value >> UInt64((7 - offset) * 8)
                )
            }
        }
    }
}
