import DatabaseTypes
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
        transaction: any TransactionAccess
    ) async throws -> [(version: Version, data: ByteString)] {
        let primaryKeyTuple = Tuple(primaryKey)
        let beginKey = subspace.pack(primaryKeyTuple)
        let endKey = beginKey.appending(0xFF)
        let entries = try await TransactionRangeCollection.collect(
            using: transaction,
            from: .firstGreaterOrEqual(beginKey),
            to: .firstGreaterOrEqual(endKey),
            limit: limit ?? 0,
            reverse: limit != nil,
            snapshot: true,
            streamingMode: .wantAll
        )

        var versions: [(version: Version, data: ByteString)] = []
        versions.reserveCapacity(entries.count)
        for (key, value) in entries {
            guard key.count >= 10 else { continue }
            let version = Version(bytes: key[(key.count - 10)..<key.count])
            let data: ByteString
            if value.count > Self.timestampByteCount {
                data = value[Self.timestampByteCount..<value.count]
            } else {
                data = []
            }
            versions.append((version: version, data: data))
        }
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
        guard value.count > Self.timestampByteCount else {
            return []
        }
        return value[Self.timestampByteCount..<value.count]
    }
}
