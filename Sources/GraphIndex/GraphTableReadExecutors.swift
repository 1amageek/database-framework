#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseKit
import DatabaseTypes
import DatabaseEngine

public enum GraphTableReadExecutors {
    public static var sourceExecutor: any GraphTableSourceExecutor {
        RuntimeGraphTableSourceExecutor()
    }
}

private struct RuntimeGraphTableSourceExecutor: GraphTableSourceExecutor {
    func execute(
        context: DatabaseContext,
        graphTableSource: GraphTableSource,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> [QueryRow] {
        guard let resolution = try PropertyGraphReadResolver.resolve(
            graphName: graphTableSource.graphName,
            schema: context.container.schema
        ) else {
            throw CanonicalReadError.unsupportedSource(
                try PropertyGraphReadResolver.errorMessage(
                    graphName: graphTableSource.graphName,
                    schema: context.container.schema
                )
            )
        }
        guard let type = context.container.runtimeConfiguration
            .persistableTypes.type(named: resolution.entity.name) else {
            throw CanonicalReadError.unsupportedSource(
                "Property graph entity '\(resolution.entity.name)' has no compiled runtime type"
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
        context: DatabaseContext,
        type: any Persistable.Type,
        graphTableSource: GraphTableSource,
        options: ReadExecutionContext,
        partitions: FieldObject
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
        context: DatabaseContext,
        type: T.Type,
        graphTableSource: GraphTableSource,
        options: ReadExecutionContext,
        partitions: FieldObject
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
            QueryRow(fields: row.fields)
        }
    }
}
