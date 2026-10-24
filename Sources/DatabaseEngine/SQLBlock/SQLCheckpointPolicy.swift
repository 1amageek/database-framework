import DatabaseKit
import StorageKit

/// Derives checkpoint ceilings from the admitted request resources.
@_spi(DatabaseExecution)
public enum SQLCheckpointPolicy {
    public static func limits(for budget: ExecutionBudget,
        structuralLimits: QueryStructuralLimits,
        ceiling: TransactionCheckpointLimits? = nil) throws -> TransactionCheckpointLimits {
        guard budget.maximumWorkUnits > 0 else {
            throw DatabaseWorkLimitError.maximumWorkUnits(stage: .bindingCandidate,
                consumed: 0, requested: 1, maximum: 0)
        }
        guard budget.maximumIntermediateBytes > 0 else {
            throw DatabaseWorkLimitError.maximumIntermediateBytes(stage: .resultMaterialization,
                consumed: 0, requested: 1, maximum: 0)
        }
        guard budget.maximumIntermediateRows > 0 else {
            throw DatabaseWorkLimitError.maximumIntermediateRows(stage: .resultMaterialization,
                consumed: 0, requested: 1, maximum: 0)
        }
        func integer(_ value: UInt64) throws -> Int {
            guard let result = Int(exactly: value) else {
                throw SQLBlockExecutionError.executionLimitOutOfRange(value)
            }
            return result
        }
        let work = try integer(budget.maximumWorkUnits)
        let bytes = try integer(budget.maximumIntermediateBytes)
        let rows = try integer(UInt64(budget.maximumIntermediateRows))
        let depth = try integer(min(structuralLimits.maximumCollectionElements, budget.maximumWorkUnits))
        guard depth > 0 else { throw SQLBlockExecutionError.executionLimitOutOfRange(0) }
        return try TransactionCheckpointLimits(
            maximumEntries: min(work, ceiling?.maximumEntries ?? work),
            maximumDepth: min(depth, ceiling?.maximumDepth ?? depth),
            maximumRetainedBytes: min(bytes, ceiling?.maximumRetainedBytes ?? bytes),
            maximumWorkUnits: min(work, ceiling?.maximumWorkUnits ?? work),
            maximumReadPageRows: min(rows, ceiling?.maximumReadPageRows ?? rows))
    }
}
