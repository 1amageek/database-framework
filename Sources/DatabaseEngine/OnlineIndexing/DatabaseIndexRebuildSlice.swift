package struct DatabaseIndexRebuildSlice: Sendable, Hashable {
    package let completedWorkUnits: UInt64
    package let indexedEntityCount: UInt64
    package let isComplete: Bool
}
