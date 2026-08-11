import DatabaseTypes
import DatabaseEngine
import StorageKit

/// Reads the model-independent physical history maintained by a version index.
public struct VersionIndexReader: Sendable {
    private static let timestampByteCount = MemoryLayout<Int64>.size
        + MemoryLayout<UInt32>.size

    public let subspace: Subspace

    public init(subspace: Subspace) {
        self.subspace = subspace
    }

    public func history(
        primaryKey: [any TupleElement],
        limit: Int? = nil,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws -> [(version: Version, data: ByteString)] {
        if let limit {
            guard limit >= 0 else {
                throw VersionIndexError.invalidHistoryLimit(limit)
            }
            guard limit > 0 else { return [] }
        }
        let primaryKeyTuple = Tuple(primaryKey)
        let beginKey = subspace.pack(primaryKeyTuple)
        let endKey = beginKey.appending(0xFF)
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(beginKey),
            to: .firstGreaterOrEqual(endKey),
            limit: limit ?? 0,
            reverse: limit != nil,
            snapshot: true,
            streamingMode: .wantAll
        )

        var versions: [(version: Version, data: ByteString)] = []
        let retention = try workMeter?.reserveIntermediate(
            bytes: UInt64(
                MemoryLayout<[(version: Version, data: ByteString)]>.stride
            ),
            at: .indexScan
        )
        defer { retention?.release() }
        do {
            while let (key, value) = try await cursor.next() {
                try workMeter?.consume(at: .indexScan)
                let expectedKeyByteCount = beginKey.count + 10
                guard key.count == expectedKeyByteCount else {
                    throw VersionIndexError.malformedVersionKey(
                        expectedByteCount: expectedKeyByteCount,
                        actualByteCount: key.count
                    )
                }
                guard value.count >= Self.timestampByteCount else {
                    throw VersionIndexError.malformedVersionValue(
                        byteCount: value.count
                    )
                }
                let version = Version(
                    bytes: key[(key.count - 10)..<key.count]
                )
                let data = value.count > Self.timestampByteCount
                    ? value[Self.timestampByteCount..<value.count]
                    : ByteString()
                try retention?.reserveAdditional(
                    rows: 1,
                    bytes: try DatabaseIntermediateFootprint(
                        bytes: UInt64(key.count)
                    ).adding(
                        DatabaseIntermediateFootprint(
                            bytes: UInt64(value.count)
                        )
                    ).adding(
                        DatabaseIntermediateFootprint(bytes: 48)
                    ).bytes,
                    at: .indexScan
                )
                versions.append((version: version, data: data))
            }
        } catch {
            let iterationError = error
            do {
                try await cursor.finish()
            } catch {
                throw StorageRangeCleanupError(
                    iterationError: iterationError,
                    cleanupError: error
                )
            }
            throw iterationError
        }
        try await cursor.finish()
        if limit == nil {
            versions.reverse()
        }
        return versions
    }

    public func latest(
        primaryKey: [any TupleElement],
        transaction: any TransactionAccess
    ) async throws -> ByteString? {
        let beginKey = subspace.pack(Tuple(primaryKey))
        let endKey = beginKey.appending(0xFF)
        guard let key = try await transaction.getKey(
            selector: .lastLessThan(endKey),
            snapshot: true
        ), key.starts(with: beginKey),
              let value = try await transaction.getValue(
                for: key,
                snapshot: true
              ) else {
            return nil
        }
        let expectedKeyByteCount = beginKey.count + 10
        guard key.count == expectedKeyByteCount else {
            throw VersionIndexError.malformedVersionKey(
                expectedByteCount: expectedKeyByteCount,
                actualByteCount: key.count
            )
        }
        guard value.count >= Self.timestampByteCount else {
            throw VersionIndexError.malformedVersionValue(
                byteCount: value.count
            )
        }
        guard value.count > Self.timestampByteCount else {
            return ByteString()
        }
        return value[Self.timestampByteCount..<value.count]
    }
}
