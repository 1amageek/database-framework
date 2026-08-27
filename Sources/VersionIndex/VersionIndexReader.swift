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

    /// Returns history as an owned array for callers that explicitly request
    /// the public materialized output. The canonical read executor uses
    /// `retainedHistory` instead, so this boundary is never an intermediate
    /// execution representation.
    public func history(
        primaryKey: [any TupleElement],
        limit: Int? = nil,
        transaction: any TransactionReadAccess,
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

    /// Reads history into request-owned storage for the execution path.
    ///
    /// Every cursor result is validated and admitted before its retained byte
    /// owners or history entry are created. The range is read newest-first for
    /// both bounded and unbounded requests, which is equivalent to the public
    /// `history` ordering contract without a second reversal buffer.
    func retainedHistory(
        primaryKey: [any TupleElement],
        limit: Int? = nil,
        transaction: any TransactionReadAccess,
        snapshot: Bool = true,
        workMeter: DatabaseWorkMeter
    ) async throws -> VersionRetainedHistory {
        if let limit {
            guard limit >= 0 else {
                throw VersionIndexError.invalidHistoryLimit(limit)
            }
        }

        var builder = try DatabaseRetainedArrayBuilder<
            VersionRetainedHistory.Entry
        >(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try DatabaseRetainedArrayLayout.forElement(
                VersionRetainedHistory.Entry.self
            ),
            expectedCount: limit ?? 0
        )

        guard limit != 0 else {
            return VersionRetainedHistory(
                storage: try builder.finish().moveToSharedOwnership(
                    at: .indexScan
                )
            )
        }

        let primaryKeyTuple = Tuple(primaryKey)
        let beginKey = subspace.pack(primaryKeyTuple)
        let endKey = beginKey.appending(0xFF)
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(beginKey),
            to: .firstGreaterOrEqual(endKey),
            limit: limit ?? 0,
            reverse: true,
            snapshot: snapshot,
            streamingMode: .wantAll
        )

        do {
            while let (key, value) = try await cursor.next() {
                try workMeter.consume(at: .indexScan)
                try DatabaseByteProcessingMeter.consume(
                    byteCount: key.count,
                    workMeter: workMeter,
                    stage: .indexScan
                )
                try DatabaseByteProcessingMeter.consume(
                    byteCount: value.count,
                    workMeter: workMeter,
                    stage: .indexScan
                )

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

                let data = value.count > Self.timestampByteCount
                    ? value[Self.timestampByteCount..<value.count]
                    : ByteString()
                let payloadByteCount = try DatabaseIntermediateFootprint(
                    bytes: 10
                ).adding(
                    DatabaseIntermediateFootprint(bytes: UInt64(data.count))
                ).bytes
                let payloadReservation = try workMeter.reserveIntermediate(
                    bytes: try DatabaseIntermediateFootprint(
                        bytes: payloadByteCount
                    ).adding(
                        DatabaseIntermediateFootprint(bytes: 64)
                    ).bytes,
                    at: .indexScan
                )
                do {
                    let versionBytes = try DatabaseRetainedByteString.make(
                        key[(key.count - 10)..<key.count],
                        reservation: payloadReservation,
                        at: .indexScan
                    )
                    let retainedData = try DatabaseRetainedByteString.make(
                        data,
                        reservation: payloadReservation,
                        at: .indexScan
                    )
                    let entry = VersionRetainedHistory.Entry(
                        version: Version(bytes: versionBytes),
                        data: retainedData
                    )
                    try builder.append(
                        footprint: DatabaseIntermediateFootprint(
                            rows: 1,
                            // The Array layout already admits each Entry
                            // slot. The separate payload reservation owns
                            // the retained key/value bytes and byte-owner
                            // metadata, so charging Entry stride here would
                            // account for the same storage twice.
                            bytes: 0
                        ),
                        at: .indexScan,
                        make: { entry }
                    )
                } catch {
                    payloadReservation.release()
                    throw error
                }
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
        return VersionRetainedHistory(
            storage: try builder.finish().moveToSharedOwnership(
                at: .indexScan
            )
        )
    }

    public func latest(
        primaryKey: [any TupleElement],
        transaction: any TransactionReadAccess
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
