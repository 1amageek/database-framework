public struct FieldValueTupleCodecLimits: Sendable, Equatable {
    public let maximumEncodedBytes: Int
    public let maximumCollectionCount: Int
    public let maximumDepth: Int
    public let maximumObjectCount: Int

    public init(
        maximumEncodedBytes: Int = 65_536,
        maximumCollectionCount: Int = 65_536,
        maximumDepth: Int = 32,
        maximumObjectCount: Int = 65_536
    ) {
        precondition(maximumEncodedBytes > 0)
        precondition(maximumCollectionCount >= 0)
        precondition(maximumDepth >= 0)
        precondition(maximumObjectCount > 0)
        self.maximumEncodedBytes = maximumEncodedBytes
        self.maximumCollectionCount = maximumCollectionCount
        self.maximumDepth = maximumDepth
        self.maximumObjectCount = maximumObjectCount
    }

    public static let `default` = Self()
}
