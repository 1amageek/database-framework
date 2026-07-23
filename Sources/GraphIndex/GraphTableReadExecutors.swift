#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import DatabaseValue
import QueryIR
import DatabaseEngine

public enum GraphTableReadExecutors {
    public static var sourceExecutor: any GraphTableSourceExecutor {
        RuntimeGraphTableSourceExecutor()
    }
}

private struct RuntimeGraphTableSourceExecutor: GraphTableSourceExecutor {
    func execute(
        context: FDBContext,
        graphTableSource: GraphTableSource,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField]
    ) async throws -> [QueryRow] {
        guard let resolution = try PropertyGraphReadResolver.resolve(
            graphName: graphTableSource.graphName,
            schema: context.container.schema
        ),
              let type = resolution.entity.persistableType else {
            throw CanonicalReadError.unsupportedSource(
                try PropertyGraphReadResolver.errorMessage(
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
            partitions: partitions
        )
    }

    private func execute(
        context: FDBContext,
        type: any Persistable.Type,
        graphTableSource: GraphTableSource,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField]
    ) async throws -> [QueryRow] {
        try await _execute(
            context: context,
            type: type,
            graphTableSource: graphTableSource,
            options: options,
            partitions: partitions
        )
    }

    private func _execute<T: Persistable>(
        context: FDBContext,
        type: T.Type,
        graphTableSource: GraphTableSource,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField]
    ) async throws -> [QueryRow] {
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let queryContext = try context.indexQueryContext.withPartitions(partitions, for: type)
        let executor = GraphTableExecutor<T>(
            queryContext: queryContext,
            graphTableSource: graphTableSource,
            transactionConfiguration: execution.transactionConfiguration
        )

        return try await executor.execute().map { row in
            QueryRow(fields: row.fields.mapValues(\.asDatabaseValue))
        }
    }
}
