@_spi(DatabaseExecution)
public struct DatabaseMutationStateLimits: Sendable, Hashable {
    public let maximumKeyBytes: Int
    public let maximumDiscriminatorBytes: Int
    public let maximumFingerprintBytes: Int
    public let maximumOutcomeBytes: Int
    public let maximumChunkCount: Int

    public init(
        maximumKeyBytes: Int,
        maximumDiscriminatorBytes: Int = 64,
        maximumFingerprintBytes: Int = 128,
        maximumOutcomeBytes: Int,
        maximumChunkCount: Int
    ) throws(DatabaseMutationStateError) {
        guard maximumKeyBytes > 0,
              maximumDiscriminatorBytes > 0,
              maximumFingerprintBytes > 0,
              maximumOutcomeBytes >= 0,
              maximumChunkCount >= 0 else {
            throw .invalidLimits
        }
        self.maximumKeyBytes = maximumKeyBytes
        self.maximumDiscriminatorBytes = maximumDiscriminatorBytes
        self.maximumFingerprintBytes = maximumFingerprintBytes
        self.maximumOutcomeBytes = maximumOutcomeBytes
        self.maximumChunkCount = maximumChunkCount
    }
}
