import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

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
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let queryContext = context.indexQueryContext
        return try await queryContext.withTransaction(
            configuration: execution.transactionConfiguration
        ) { transaction in
            guard let index = try await queryContext
                .readableIndex(
                    named: resolution.indexDescriptor.name,
                    kindIdentifier: resolution.indexDescriptor.kindIdentifier,
                    forEntityName: resolution.entity.name,
                    partitions: partitions,
                    transaction: transaction
                ) else {
                return []
            }
            let rows = try await GraphTableExecutor(
                indexDescriptor: resolution.indexDescriptor,
                indexSubspace: index.subspace,
                graphTableSource: graphTableSource
            ).execute(transaction: transaction)
            return rows.map { row in
                QueryRow(fields: row.fields)
            }
        }
    }
}
