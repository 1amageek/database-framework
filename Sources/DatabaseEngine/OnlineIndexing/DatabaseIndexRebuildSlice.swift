public struct DatabaseIndexRebuildSlice: Sendable, Hashable {
    public let completedWorkUnits: UInt64
    public let indexedEntityCount: UInt64
    public let isComplete: Bool

    public init(
        completedWorkUnits: UInt64,
        indexedEntityCount: UInt64,
        isComplete: Bool
    ) {
        self.completedWorkUnits = completedWorkUnits
        self.indexedEntityCount = indexedEntityCount
        self.isComplete = isComplete
    }
}
