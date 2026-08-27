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
        session: DatabaseReadSession,
        graphTableSource: GraphTableSource,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedQueryRows {
        try GraphTableExecutor.validate(graphTableSource)
        guard let resolution = try PropertyGraphReadResolver.resolve(
            graphName: graphTableSource.graphName,
            schema: session.schema
        ) else {
            throw CanonicalReadError.unsupportedSource(
                try PropertyGraphReadResolver.errorMessage(
                    graphName: graphTableSource.graphName,
                    schema: session.schema
                )
            )
        }
        guard let index = try await session.readableIndex(
            named: resolution.indexDescriptor.name,
            indexType: resolution.indexDescriptor.type,
            forEntityName: resolution.entity.name,
            partitions: partitions
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
            transaction: session.transaction,
            workMeter: options.workMeter
        )
    }
}
