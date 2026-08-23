import DatabaseEngine
import GraphIndex
import StorageKit
import TestSupport

func executeSPARQLTest(
    executor: SPARQLQueryExecutor,
    pattern: ExecutionPattern,
    limit: Int? = nil,
    offset: Int = 0,
    workMeter: DatabaseWorkMeter,
    database: any StorageEngine = InMemoryEngine(),
    transactionConfiguration: TransactionConfiguration? = nil
) async throws -> ([VariableBinding], ExecutionStatistics) {
    let operation: @Sendable (any TransactionAccess) async throws -> (
        [VariableBinding],
        ExecutionStatistics
    ) = { transaction in
        try await executor.executeInTransaction(
            pattern: pattern,
            transaction: transaction,
            limit: limit,
            offset: offset,
            workMeter: workMeter
        )
    }

    if let transactionConfiguration {
        return try await StorageTransactionExecutor(engine: database)
            .withTransaction(
                configuration: transactionConfiguration,
                clock: TestProcessMonotonicClock(),
                operation
            )
    }
    return try await database.withTransaction(operation)
}
