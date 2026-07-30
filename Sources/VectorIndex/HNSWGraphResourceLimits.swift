/// Memory limits applied while materializing a persisted HNSW graph.
///
/// Persisted metadata is an input boundary. The graph byte count is validated
/// before allocating its contiguous snapshot buffer.
public struct HNSWGraphResourceLimits: Sendable, Hashable {
    public static let `default` = HNSWGraphResourceLimits(
        maximumSnapshotByteCount: 64 * 1024 * 1024
    )

    public let maximumSnapshotByteCount: Int

    public init(maximumSnapshotByteCount: Int) {
        self.maximumSnapshotByteCount = maximumSnapshotByteCount
    }
}
