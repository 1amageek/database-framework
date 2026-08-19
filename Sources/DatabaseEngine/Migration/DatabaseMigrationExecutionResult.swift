/// Work completed for the selected data root by one bounded migration
/// invocation. With `MultiBase`, `isComplete` describes that Base; global
/// data admission remains closed until every active Base is complete.
public struct DatabaseMigrationExecutionResult: Sendable, Hashable {
    public let completedStageCount: UInt64
    public let isComplete: Bool

    public init(completedStageCount: UInt64, isComplete: Bool) {
        self.completedStageCount = completedStageCount
        self.isComplete = isComplete
    }
}
