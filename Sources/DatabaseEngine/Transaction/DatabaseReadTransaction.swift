import StorageKit

/// Read-only capability facade. Its dynamic type does not conform to
/// `Transaction`, so callers cannot recover mutation or commit APIs by cast.
public struct DatabaseReadTransaction: Sendable {
    private let transaction: any Transaction

    package init(transaction: any Transaction) {
        self.transaction = transaction
    }

    public func getValue(
        for key: Bytes,
        snapshot: Bool = false
    ) async throws -> Bytes? {
        try await transaction.getValue(for: key, snapshot: snapshot)
    }

    public func getKey(
        selector: KeySelector,
        snapshot: Bool = false
    ) async throws -> Bytes? {
        try await transaction.getKey(selector: selector, snapshot: snapshot)
    }

    public func collectRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int = 0,
        reverse: Bool = false,
        snapshot: Bool = false,
        streamingMode: StreamingMode = .wantAll
    ) async throws -> [(Bytes, Bytes)] {
        try await transaction.collectRange(
            from: begin,
            to: end,
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode
        )
    }

    public func forEachInRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int = 0,
        reverse: Bool = false,
        snapshot: Bool = false,
        streamingMode: StreamingMode = .wantAll,
        body: (Bytes, Bytes) async throws -> Void
    ) async throws {
        try await transaction.forEachInRange(
            from: begin,
            to: end,
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode,
            body: body
        )
    }

    public func getEstimatedRangeSizeBytes(
        beginKey: Bytes,
        endKey: Bytes
    ) async throws -> Int {
        try await transaction.getEstimatedRangeSizeBytes(
            beginKey: beginKey,
            endKey: endKey
        )
    }

    public func getRangeSplitPoints(
        beginKey: Bytes,
        endKey: Bytes,
        chunkSize: Int
    ) async throws -> [Bytes] {
        try await transaction.getRangeSplitPoints(
            beginKey: beginKey,
            endKey: endKey,
            chunkSize: chunkSize
        )
    }

    public func getReadVersion() async throws -> Int64 {
        try await transaction.getReadVersion()
    }
}
