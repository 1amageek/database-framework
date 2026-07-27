/// Resource limits for decoding and encoding identifier storage keys.
public struct PersistableIdentifierKeyLimits: Sendable, Hashable {
    public let maximumCompositeDepth: Int
    public let maximumComponentCount: Int

    public init(
        maximumCompositeDepth: Int,
        maximumComponentCount: Int
    ) throws(PersistableIdentifierKeyError) {
        guard maximumCompositeDepth >= 0,
              maximumComponentCount > 0 else {
            throw .invalidLimits
        }
        self.maximumCompositeDepth = maximumCompositeDepth
        self.maximumComponentCount = maximumComponentCount
    }

    public static let `default` = Self(
        validatedMaximumCompositeDepth: 64,
        maximumComponentCount: 1_024
    )

    private init(
        validatedMaximumCompositeDepth maximumCompositeDepth: Int,
        maximumComponentCount: Int
    ) {
        self.maximumCompositeDepth = maximumCompositeDepth
        self.maximumComponentCount = maximumComponentCount
    }
}
