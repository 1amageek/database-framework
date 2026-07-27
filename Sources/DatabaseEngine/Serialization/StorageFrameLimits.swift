/// Resource limits applied to engine-owned metadata and continuation frames.
///
/// These limits are independent of the client/server wire protocol. A storage
/// format may evolve without changing the externally observable transport
/// contract.
public struct StorageFrameLimits: Sendable, Hashable {
    public let maximumFrameBytes: Int
    public let maximumStringBytes: Int
    public let maximumByteStringBytes: Int
    public let maximumCollectionCount: Int
    public let maximumNestingDepth: Int

    public init(
        maximumFrameBytes: Int,
        maximumStringBytes: Int,
        maximumByteStringBytes: Int,
        maximumCollectionCount: Int,
        maximumNestingDepth: Int
    ) throws(StorageFrameError) {
        guard maximumFrameBytes >= 0,
              maximumStringBytes >= 0,
              maximumByteStringBytes >= 0,
              maximumCollectionCount >= 0,
              maximumNestingDepth >= 0 else {
            throw .negativeLimit
        }
        self.maximumFrameBytes = maximumFrameBytes
        self.maximumStringBytes = maximumStringBytes
        self.maximumByteStringBytes = maximumByteStringBytes
        self.maximumCollectionCount = maximumCollectionCount
        self.maximumNestingDepth = maximumNestingDepth
    }

    public static let `default` = StorageFrameLimits(
        validatedMaximumFrameBytes: 4 * 1_024 * 1_024,
        maximumStringBytes: 1 * 1_024 * 1_024,
        maximumByteStringBytes: 4 * 1_024 * 1_024,
        maximumCollectionCount: 100_000,
        maximumNestingDepth: 64
    )

    private init(
        validatedMaximumFrameBytes maximumFrameBytes: Int,
        maximumStringBytes: Int,
        maximumByteStringBytes: Int,
        maximumCollectionCount: Int,
        maximumNestingDepth: Int
    ) {
        self.maximumFrameBytes = maximumFrameBytes
        self.maximumStringBytes = maximumStringBytes
        self.maximumByteStringBytes = maximumByteStringBytes
        self.maximumCollectionCount = maximumCollectionCount
        self.maximumNestingDepth = maximumNestingDepth
    }
}
