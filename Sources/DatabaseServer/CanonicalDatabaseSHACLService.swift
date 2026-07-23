import DatabaseEngine
import DatabaseValue
import DatabaseWire
import GraphIndex
import StorageKit

public struct CanonicalDatabaseSHACLService: DatabaseSHACLService {
    private let store: DatabaseRDFDocumentStore
    private let processor: any DatabaseSHACLProcessor
    private let coordinator: DatabaseTransactionalOperationCoordinator
    private let wireLimits: DatabaseWireLimits

    public init(
        store: DatabaseRDFDocumentStore,
        processor: any DatabaseSHACLProcessor,
        coordinator: DatabaseTransactionalOperationCoordinator,
        wireLimits: DatabaseWireLimits = .default
    ) {
        self.store = store
        self.processor = processor
        self.coordinator = coordinator
        self.wireLimits = wireLimits
    }

    public func execute(
        _ request: SHACLExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> DatabasePreparedOperationResponse<SHACLExecuteOperation> {
        let workBudget = SHACLValidationWorkBudget(budget: request.budget)
        switch request.invocation {
        case .describeShapes(let graph):
            let page = try await describe(
                graph: graph,
                page: request.page,
                workBudget: workBudget,
                context: context
            )
            try workBudget.workMeter.recordOutputRows(
                UInt32(page.shapes.count)
            )
            return .encoding(.shapes(page))
        case .upsertShapes(let graph, let shapes, let expectedRevision):
            return try await upsert(
                graph: graph,
                shapes: shapes,
                expectedRevision: expectedRevision,
                request: request,
                workBudget: workBudget,
                context: context
            )
        case .deleteShapes(let graph, let expectedRevision):
            return try await delete(
                graph: graph,
                expectedRevision: expectedRevision,
                request: request,
                workBudget: workBudget,
                context: context
            )
        case .validate(
            let shapesGraph,
            let data,
            let focus,
            let entailment
        ):
            let report = try await read(context: context) { transaction in
                try await processor.validate(
                    shapesGraph: shapesGraph,
                    data: data,
                    focus: focus,
                    entailment: entailment,
                    page: request.page,
                    workBudget: workBudget,
                    transaction: transaction
                )
            }
            guard let issueCount = UInt32(exactly: report.issues.count) else {
                throw DatabaseWorkLimitError.maximumRows(
                    stage: .resultMaterialization,
                    consumed: workBudget.workMeter.consumedRows,
                    requested: UInt32.max,
                    maximum: request.budget.maximumRows
                )
            }
            try workBudget.workMeter.recordOutputRows(issueCount)
            return .encoding(.validation(report))
        }
    }

    private func describe(
        graph: String,
        page: QueryExecuteOperation.Page,
        workBudget: SHACLValidationWorkBudget,
        context: DatabaseOperationContext
    ) async throws -> SHACLExecuteOperation.ShapesPage {
        let cursor = try page.continuation.map {
            try DatabaseRDFDocumentPageCursor.decode(
                $0,
                domain: .shacl,
                identifier: graph,
                limits: wireLimits
            )
        }
        guard let offset = Int(exactly: cursor?.offset ?? 0) else {
            throw DatabaseRDFDocumentStoreError.invalidContinuation
        }
        let stored = try await read(context: context) { transaction in
            try await store.page(
                identifier: graph,
                offset: offset,
                limit: Int(page.limit),
                transaction: transaction
            )
        }
        guard let stored else {
            throw DatabaseRDFDocumentStoreError.documentNotFound(graph)
        }
        guard cursor?.revision == nil || cursor?.revision == stored.revision else {
            throw DatabaseRDFDocumentStoreError.invalidContinuation
        }
        try workBudget.consume(UInt64(stored.quads.count), at: .storageRow)
        let continuation = try stored.nextOffset.map {
            try DatabaseRDFDocumentPageCursor(
                domain: .shacl,
                identifier: graph,
                revision: stored.revision,
                offset: $0
            ).encode(limits: wireLimits)
        }
        return SHACLExecuteOperation.ShapesPage(
            graph: graph,
            revision: stored.revision,
            shapes: stored.quads,
            continuation: continuation
        )
    }

    private func upsert(
        graph: String,
        shapes: [DatabaseRDFQuad],
        expectedRevision: UInt64?,
        request: SHACLExecuteOperation.Request,
        workBudget: SHACLValidationWorkBudget,
        context: DatabaseOperationContext
    ) async throws -> DatabasePreparedOperationResponse<SHACLExecuteOperation> {
        try await coordinator.execute(
                SHACLExecuteOperation.self,
                requestPayload: context.requestPayload,
                context: context,
                timeoutMilliseconds: request.budget.timeoutMilliseconds
            ) { transactionContext in
                let transaction = transactionContext.rawTransaction
                let revision = try await store.replace(
                    identifier: graph,
                    auxiliaryIdentifiers: [],
                    quads: shapes,
                    expectedRevision: expectedRevision,
                    transaction: transaction
                )
                try await processor.replace(
                    graph: graph,
                    quads: shapes,
                    workBudget: workBudget,
                    transaction: transaction
                )
                try workBudget.workMeter.recordOutputRows()
                return revision
            } makeResponse: { revision, commitVersion in
                .mutation(
                    DatabaseRevisionMutationResult(
                        commitVersion: commitVersion,
                        revision: revision
                    )
                )
            }
    }

    private func delete(
        graph: String,
        expectedRevision: UInt64?,
        request: SHACLExecuteOperation.Request,
        workBudget: SHACLValidationWorkBudget,
        context: DatabaseOperationContext
    ) async throws -> DatabasePreparedOperationResponse<SHACLExecuteOperation> {
        try await coordinator.execute(
                SHACLExecuteOperation.self,
                requestPayload: context.requestPayload,
                context: context,
                timeoutMilliseconds: request.budget.timeoutMilliseconds
            ) { transactionContext in
                let transaction = transactionContext.rawTransaction
                let revision = try await store.delete(
                    identifier: graph,
                    expectedRevision: expectedRevision,
                    transaction: transaction
                )
                try await processor.delete(
                    graph: graph,
                    workBudget: workBudget,
                    transaction: transaction
                )
                try workBudget.workMeter.recordOutputRows()
                return revision
            } makeResponse: { revision, commitVersion in
                .mutation(
                    DatabaseRevisionMutationResult(
                        commitVersion: commitVersion,
                        revision: revision
                    )
                )
            }
    }

    private func read<Value: Sendable>(
        context: DatabaseOperationContext,
        body: @Sendable @escaping (any Transaction) async throws -> Value
    ) async throws -> Value {
        try await context.container.engine.withTransaction(
            configuration: .readOnly,
            body
        )
    }
}
