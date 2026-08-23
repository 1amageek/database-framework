@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseKit
import DatabaseTypes
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
        authorization: IndexReadAuthorization,
        options: ReadExecutionContext,
        partitions: FieldObject,
        transaction: any TransactionReadAccess
    ) async throws -> DatabaseRetainedQueryRows {
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
        guard let index = try await queryContext
            .readableIndex(
                named: resolution.indexDescriptor.name,
                indexType: resolution.indexDescriptor.type,
                forEntityName: resolution.entity.name,
                partitions: partitions,
                authorization: authorization,
                transaction: transaction
            ) else {
            let empty = try DatabaseRetainedArrayBuilder<QueryRow>(
                workMeter: options.workMeter,
                stage: .bindingCandidate,
                layout: try CanonicalRelationalFootprintMeter
                    .retainedArrayLayout(for: QueryRow.self)
            )
            let owner = try empty.finish().moveToSharedOwnership(
                at: .bindingCandidate
            )
            return DatabaseRetainedQueryRows(
                owner: owner,
                visibleRange: owner.startIndex..<owner.endIndex
            )
        }
        return try await GraphTableExecutor(
            indexDescriptor: resolution.indexDescriptor,
            indexSubspace: index.subspace,
            graphTableSource: graphTableSource
        ).executeRetainedCanonicalRows(
            transaction: transaction,
            workMeter: options.workMeter
        )
    }
}
