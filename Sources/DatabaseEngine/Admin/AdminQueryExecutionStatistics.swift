import DatabaseTypes

public struct AdminQueryExecutionStatistics: Sendable, Equatable {
    public let plan: AdminQueryPlan
    public let actualRowCount: Int64
    public let executionDuration: TimeSpan
    public let readVersion: UInt64

    public init(
        plan: AdminQueryPlan,
        actualRowCount: Int64,
        executionDuration: TimeSpan,
        readVersion: UInt64
    ) {
        self.plan = plan
        self.actualRowCount = actualRowCount
        self.executionDuration = executionDuration
        self.readVersion = readVersion
    }
}
