import Foundation
import Core
import QueryIR
import DatabaseEngine
import DatabaseClientProtocol

public enum GraphTableReadBridge {
    public static func registerReadExecutors() {
        LogicalSourceExecutorRegistry.shared.register(RuntimeGraphTableSourceExecutor())
    }
}

private struct RuntimeGraphTableSourceExecutor: GraphTableSourceExecutor {
    func execute(
        context: FDBContext,
        graphTableSource: GraphTableSource,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> [QueryRow] {
        guard let resolution = GraphReadResolver.resolve(
            graphName: graphTableSource.graphName,
            schema: context.container.schema
        ),
              let type = resolution.entity.persistableType else {
            throw ServiceError(
                code: "UNKNOWN_GRAPH",
                message: GraphReadResolver.errorMessage(
                    graphName: graphTableSource.graphName,
                    schema: context.container.schema
                )
            )
        }

        return try await execute(
            context: context,
            type: type,
            graphTableSource: graphTableSource,
            options: options,
            partitionValues: partitionValues
        )
    }

    private func execute(
        context: FDBContext,
        type: any Persistable.Type,
        graphTableSource: GraphTableSource,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> [QueryRow] {
        try await _execute(
            context: context,
            type: type,
            graphTableSource: graphTableSource,
            options: options,
            partitionValues: partitionValues
        )
    }

    private func _execute<T: Persistable>(
        context: FDBContext,
        type: T.Type,
        graphTableSource: GraphTableSource,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> [QueryRow] {
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let queryContext = try context.indexQueryContext.withPartitionValues(partitionValues, for: type)
        let executor = GraphTableExecutor<T>(
            queryContext: queryContext,
            graphTableSource: graphTableSource,
            transactionConfiguration: execution.transactionConfiguration
        )

        return try await executor.execute().map { row in
            QueryRow(fields: row.fields)
        }
    }
}
