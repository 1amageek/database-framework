import DatabaseKit
import StorageKit

/// Request-scoped policy for one semantic SQL block.
public struct SQLBlockExecutionOptions: Sendable {
    public let checkpointLimits: TransactionCheckpointLimits
    public let budget: ExecutionBudget
    public let structuralLimits: QueryStructuralLimits
    public let searchPath: SQLSearchPath
    public let configuration: TransactionConfiguration

    public init(
        checkpointLimits: TransactionCheckpointLimits,
        budget: ExecutionBudget = ExecutionBudget(),
        structuralLimits: QueryStructuralLimits = .default,
        searchPath: SQLSearchPath = .default,
        configuration: TransactionConfiguration = .default
    ) {
        self.checkpointLimits = checkpointLimits
        self.budget = budget
        self.structuralLimits = structuralLimits
        self.searchPath = searchPath
        self.configuration = configuration
    }
}
