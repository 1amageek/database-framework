import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

public enum GraphTableReadExecutors {
    public static var sourceExecutor: any GraphTableSourceExecutor {
        RuntimeGraphTableSourceExecutor()
    }
}

private struct RuntimeGraphTableSourceExecutor: GraphTableSourceExecutor {
    func executeInTransaction(
        context: DatabaseContext,
        graphTableSource: GraphTableSource,
        options: ReadExecutionContext,
        partitions: FieldObject,
        transaction: any TransactionAccess
    ) async throws -> DatabaseRetainedQueryRows {
        try GraphTableExecutor.validate(graphTableSource)
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
        let queryContext = context.indexQueryContext
        guard let index = try await queryContext.readableIndex(
            named: resolution.indexDescriptor.name,
            indexType: resolution.indexDescriptor.type,
            forEntityName: resolution.entity.name,
            partitions: partitions,
            transaction: transaction
        ) else {
            return try DatabaseRetainedQueryRowsBuilder(
                workMeter: options.workMeter,
                stage: .pathExpansion
            ).finish()
        }
        return try await GraphTableExecutor(
            indexDescriptor: resolution.indexDescriptor,
            indexSubspace: index.subspace,
            graphTableSource: graphTableSource
        ).execute(
            transaction: transaction,
            workMeter: options.workMeter
        )
    }
}
