import StorageKit
import DatabaseTypes

/// Transaction-scoped storage only; schema publication owns commit and lifetime.
struct SQLCatalogStore: Sendable {
    private let key: ByteString

    init(root: Subspace) {
        self.key = root.subspace("schema").pack(Tuple("sql_catalog"))
    }

    func load(transaction: any TransactionAccess,
        limits: StorageFrameLimits = .default
    ) async throws -> SQLCatalogStoredState? {
        guard let bytes = try await transaction.getValue(for: key, snapshot: false,
            maximumByteCount: limits.maximumFrameBytes) else { return nil }
        return try SQLCatalogCodec.decode(bytes, limits: limits)
    }

    func persist(_ snapshot: SQLCatalogSnapshot, transaction: any TransactionAccess,
        limits: StorageFrameLimits = .default
    ) throws {
        try transaction.setValue(SQLCatalogCodec.encode(snapshot, limits: limits), for: key)
    }

    func persist(_ state: SQLCatalogStoredState, transaction: any TransactionAccess,
        limits: StorageFrameLimits = .default
    ) throws {
        try transaction.setValue(SQLCatalogCodec.encode(state, limits: limits), for: key)
    }
}
