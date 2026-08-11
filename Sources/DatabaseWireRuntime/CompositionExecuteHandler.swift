#if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
import DatabaseEngine
import DatabaseKit
@_spi(DatabaseWireRuntime) import DatabaseWire

/// Executes named Composition catalog operations in the control domain.
public struct CompositionExecuteHandler: DatabaseOperationEndpointHandler {
    public typealias Operation = CompositionExecuteOperation

    private let coordinator: DatabaseTransactionalOperationCoordinator
    private let timeoutMilliseconds: UInt32

    public init(
        coordinator: DatabaseTransactionalOperationCoordinator,
        runtimeLimits: DatabaseRuntimeLimits = .default
    ) {
        self.coordinator = coordinator
        self.timeoutMilliseconds = runtimeLimits.maximumTimeoutMilliseconds
    }

    public func requirement(
        for request: CompositionExecuteOperation.Request
    ) throws -> DatabaseOperationRequirement {
        switch request.invocation {
        case .list:
            return DatabaseOperationRequirement(
                acceptedTargets: .database,
                access: .read,
                transaction: .read
            )
        case .describe:
            return DatabaseOperationRequirement(
                acceptedTargets: .composition,
                access: .read,
                transaction: .read
            )
        case .create:
            return DatabaseOperationRequirement(
                acceptedTargets: .database,
                access: .administer,
                transaction: .write
            )
        case .replace, .delete:
            return DatabaseOperationRequirement(
                acceptedTargets: .composition,
                access: .administer,
                transaction: .write
            )
        }
    }

    public func invoke(
        request: CompositionExecuteOperation.Request,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult {
        switch request.invocation {
        case .list:
            let records = try await context.requireControlExecutor()
                .visibleCompositions()
            let descriptions = records.map(Self.description)
            return DatabaseOperationResult(
                CompositionExecuteOperation.self,
                response: .compositions(descriptions)
            )

        case .describe:
            guard case .composition(let id) = context.target else {
                throw DatabaseAdministrationError.targetMismatch(context.target)
            }
            let executor = try context.requireCompositionExecutor()
            guard executor.compositionID == id else {
                throw DatabaseAdministrationError.targetMismatch(context.target)
            }
            let record = try await executor.resolve()
            return DatabaseOperationResult(
                CompositionExecuteOperation.self,
                response: .composition(Self.description(record))
            )

        case .create(
            let composition,
            let expectedRevision,
            let idempotencyKey
        ):
            try Self.requireIdempotencyKey(idempotencyKey, context: context)
            let executor = try context.requireControlExecutor()
            return try await coordinator.executeControlMetadata(
                operation: .compositionExecute,
                requestPayload: context.requestPayload,
                context: context,
                timeoutMilliseconds: timeoutMilliseconds
            ) { transaction in
                try await executor.createComposition(
                    composition,
                    expectedRevision: expectedRevision,
                    transaction: transaction
                )
            } makeResponse: { record, _ in
                DatabaseOperationResponseEncoder(
                    CompositionExecuteOperation.self,
                    response: .mutation(
                        CompositionExecuteOperation.MutationResult(
                            revision: record.revision,
                            generation: record.generation
                        )
                    )
                )
            }.result

        case .replace(
            let bases,
            let expectedRevision,
            let idempotencyKey
        ):
            guard case .composition(let id) = context.target else {
                throw DatabaseAdministrationError.targetMismatch(context.target)
            }
            try Self.requireIdempotencyKey(idempotencyKey, context: context)
            let executor = try context.requireControlExecutor()
            return try await coordinator.executeControlMetadata(
                operation: .compositionExecute,
                requestPayload: context.requestPayload,
                context: context,
                timeoutMilliseconds: timeoutMilliseconds
            ) { transaction in
                try await executor.replaceComposition(
                    id: id,
                    bases: bases,
                    expectedRevision: expectedRevision,
                    transaction: transaction
                )
            } makeResponse: { record, _ in
                DatabaseOperationResponseEncoder(
                    CompositionExecuteOperation.self,
                    response: .mutation(
                        CompositionExecuteOperation.MutationResult(
                            revision: record.revision,
                            generation: record.generation
                        )
                    )
                )
            }.result

        case .delete(let expectedRevision, let idempotencyKey):
            guard case .composition(let id) = context.target else {
                throw DatabaseAdministrationError.targetMismatch(context.target)
            }
            try Self.requireIdempotencyKey(idempotencyKey, context: context)
            let executor = try context.requireControlExecutor()
            return try await coordinator.executeControlMetadata(
                operation: .compositionExecute,
                requestPayload: context.requestPayload,
                context: context,
                timeoutMilliseconds: timeoutMilliseconds
            ) { transaction in
                try await executor.deleteComposition(
                    id,
                    expectedRevision: expectedRevision,
                    transaction: transaction
                )
            } makeResponse: { mutation, _ in
                DatabaseOperationResponseEncoder(
                    CompositionExecuteOperation.self,
                    response: .mutation(
                        CompositionExecuteOperation.MutationResult(
                            revision: mutation.revision,
                            generation: mutation.generation
                        )
                    )
                )
            }.result
        }
    }

    private static func requireIdempotencyKey(
        _ key: String,
        context: DatabaseOperationContext
    ) throws {
        guard context.metadata.idempotencyKey == key else {
            throw DatabaseAdministrationError.idempotencyKeyMismatch
        }
    }

    private static func description(
        _ record: DatabaseCompositionRecord
    ) -> CompositionExecuteOperation.Description {
        CompositionExecuteOperation.Description(
            composition: record.composition,
            revision: record.revision,
            generation: record.generation
        )
    }
}

#endif
