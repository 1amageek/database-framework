#if DATABASE_SERVER_GRAPH_INDEXES
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import StorageKit

public struct CanonicalDatabaseOntologyService: DatabaseOntologyService {
    private let store: DatabaseRDFDocumentStore
    private let processor: any DatabaseOntologyProcessor
    private let coordinator: DatabaseTransactionalOperationCoordinator
    private let wireLimits: DatabaseWireLimits

    public init(
        store: DatabaseRDFDocumentStore,
        processor: any DatabaseOntologyProcessor,
        coordinator: DatabaseTransactionalOperationCoordinator,
        wireLimits: DatabaseWireLimits = .default
    ) {
        self.store = store
        self.processor = processor
        self.coordinator = coordinator
        self.wireLimits = wireLimits
    }

    public func execute(
        _ request: OntologyExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> OntologyExecutionResult {
        switch request.invocation {
        case .describe(let ontology):
            return .encoding(
                .document(try await describe(
                    ontology: ontology,
                    page: request.page,
                    context: context
                ))
            )
        case .upsert(let document, let expectedRevision):
            return try await upsert(
                document: document,
                expectedRevision: expectedRevision,
                request: request,
                context: context
            )
        case .delete(let ontology, let expectedRevision):
            return try await delete(
                ontology: ontology,
                expectedRevision: expectedRevision,
                request: request,
                context: context
            )
        case .reason(let ontology, let profile):
            return .encoding(
                .inference(try await read(context: context) { transaction in
                    try await processor.reason(
                        ontology: ontology,
                        profile: profile,
                        page: request.page,
                        budget: request.budget,
                        transaction: transaction
                    )
                })
            )
        case .hierarchy(
            let ontology,
            let resource,
            let resourceKind,
            let direction,
            let maximumDepth
        ):
            return .encoding(
                .hierarchy(try await read(context: context) { transaction in
                    try await processor.hierarchy(
                        ontology: ontology,
                        resource: resource,
                        resourceKind: resourceKind,
                        direction: direction,
                        maximumDepth: maximumDepth,
                        page: request.page,
                        budget: request.budget,
                        transaction: transaction
                    )
                })
            )
        case .validateSchema(let ontology):
            return .encoding(
                .validation(try await read(context: context) { transaction in
                    try await processor.validateSchema(
                        ontology: ontology,
                        page: request.page,
                        budget: request.budget,
                        transaction: transaction
                    )
                })
            )
        }
    }

    private func describe(
        ontology: String,
        page: QueryExecuteOperation.Page,
        context: DatabaseOperationContext
    ) async throws -> OntologyExecuteOperation.DocumentPage {
        let cursor = try page.continuation.map {
            try DatabaseRDFDocumentPageCursor.decode(
                $0,
                domain: .ontology,
                identifier: ontology,
                limits: wireLimits
            )
        }
        guard let offset = Int(exactly: cursor?.offset ?? 0) else {
            throw DatabaseRDFDocumentStoreError.invalidContinuation
        }
        let stored = try await read(context: context) { transaction in
            try await store.page(
                identifier: ontology,
                offset: offset,
                limit: Int(page.limit),
                transaction: transaction
            )
        }
        guard let stored else {
            throw DatabaseRDFDocumentStoreError.documentNotFound(ontology)
        }
        guard cursor?.revision == nil || cursor?.revision == stored.revision else {
            throw DatabaseRDFDocumentStoreError.invalidContinuation
        }
        let continuation = try stored.nextOffset.map {
            try DatabaseRDFDocumentPageCursor(
                domain: .ontology,
                identifier: ontology,
                revision: stored.revision,
                offset: $0
            ).encode(limits: wireLimits)
        }
        return OntologyExecuteOperation.DocumentPage(
            ontology: ontology,
            revision: stored.revision,
            imports: stored.auxiliaryIdentifiers,
            axioms: stored.quads,
            continuation: continuation
        )
    }

    private func upsert(
        document: OntologyExecuteOperation.Document,
        expectedRevision: UInt64?,
        request: OntologyExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> OntologyExecutionResult {
        let coordinated = try await coordinator.execute(
                operation: OntologyExecuteOperation.identifier,
                requestPayload: context.requestPayload,
                context: context,
                timeoutMilliseconds: request.budget.timeoutMilliseconds
            ) { transactionContext in
                let transaction = transactionContext.storageAccess
                let revision = try await store.replace(
                    identifier: document.ontology,
                    auxiliaryIdentifiers: document.imports,
                    quads: document.axioms,
                    expectedRevision: expectedRevision,
                    transaction: transaction
                )
                try await processor.replace(
                    document,
                    budget: request.budget,
                    transaction: transaction
                )
                return revision
            } makeResponse: { revision, commitVersion in
                DatabaseOperationResponseEncoder(
                    OntologyExecuteOperation.self,
                    response: .mutation(
                        RevisionMutationResult(
                            commitVersion: commitVersion,
                            revision: revision
                        )
                    )
                )
            }
        return try OntologyExecutionResult(
            coordinated: coordinated,
            limits: wireLimits
        )
    }

    private func delete(
        ontology: String,
        expectedRevision: UInt64?,
        request: OntologyExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> OntologyExecutionResult {
        let coordinated = try await coordinator.execute(
                operation: OntologyExecuteOperation.identifier,
                requestPayload: context.requestPayload,
                context: context,
                timeoutMilliseconds: request.budget.timeoutMilliseconds
            ) { transactionContext in
                let transaction = transactionContext.storageAccess
                let revision = try await store.delete(
                    identifier: ontology,
                    expectedRevision: expectedRevision,
                    transaction: transaction
                )
                try await processor.delete(
                    ontology: ontology,
                    budget: request.budget,
                    transaction: transaction
                )
                return revision
            } makeResponse: { revision, commitVersion in
                DatabaseOperationResponseEncoder(
                    OntologyExecuteOperation.self,
                    response: .mutation(
                        RevisionMutationResult(
                            commitVersion: commitVersion,
                            revision: revision
                        )
                    )
                )
            }
        return try OntologyExecutionResult(
            coordinated: coordinated,
            limits: wireLimits
        )
    }

    private func read<Value: Sendable>(
        context: DatabaseOperationContext,
        body: @Sendable @escaping (any TransactionAccess) async throws -> Value
    ) async throws -> Value {
        try await context.requireBaseExecutor().withStorageTransaction(
            configuration: .readOnly,
            body
        )
    }
}

#endif
