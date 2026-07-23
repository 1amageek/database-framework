package struct DatabaseIndexRebuildSlice: Sendable, Hashable {
    package let completedWorkUnits: UInt64
    package let indexedRecordCount: UInt64
    package let isComplete: Bool
}
