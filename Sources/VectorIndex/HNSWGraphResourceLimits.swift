/// Memory limits applied while materializing a persisted HNSW graph.
///
/// Persisted metadata is an input boundary. The graph byte count is validated
/// before allocating its contiguous snapshot buffer.
public struct HNSWGraphResourceLimits: Sendable, Hashable {
    public static let `default` = HNSWGraphResourceLimits(
        maximumSnapshotByteCount: 6 * 1_024 * 1_024,
        maximumPrimaryKeyCount: 32_768,
        maximumPrimaryKeyByteCount: 4 * 1_024 * 1_024,
        maximumRetainedByteCount: 48 * 1_024 * 1_024,
        maximumTransactionMutationByteCount: 6 * 1_024 * 1_024
    )

    public let maximumSnapshotByteCount: Int
    public let maximumPrimaryKeyCount: Int
    public let maximumPrimaryKeyByteCount: Int
    public let maximumRetainedByteCount: Int
    public let maximumTransactionMutationByteCount: Int

    public init(
        maximumSnapshotByteCount: Int,
        maximumPrimaryKeyCount: Int = 32_768,
        maximumPrimaryKeyByteCount: Int = 4 * 1_024 * 1_024,
        maximumRetainedByteCount: Int = 48 * 1_024 * 1_024,
        maximumTransactionMutationByteCount: Int = 6 * 1_024 * 1_024
    ) {
        self.maximumSnapshotByteCount = maximumSnapshotByteCount
        self.maximumPrimaryKeyCount = maximumPrimaryKeyCount
        self.maximumPrimaryKeyByteCount = maximumPrimaryKeyByteCount
        self.maximumRetainedByteCount = maximumRetainedByteCount
        self.maximumTransactionMutationByteCount = maximumTransactionMutationByteCount
    }
}
