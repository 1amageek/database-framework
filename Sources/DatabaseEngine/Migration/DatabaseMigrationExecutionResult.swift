/// Work completed by one bounded migration invocation.
public struct DatabaseMigrationExecutionResult: Sendable, Hashable {
    public let completedStageCount: UInt64
    public let isComplete: Bool

    public init(completedStageCount: UInt64, isComplete: Bool) {
        self.completedStageCount = completedStageCount
        self.isComplete = isComplete
    }
}
