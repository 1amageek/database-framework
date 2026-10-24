import Database
import DatabaseAccess
import DatabaseEngine
import DatabaseKit
import DatabaseWire
import StorageKit

/// Claims one acquisition with explicit container ownership and context authority.
actor EmbeddedOpening: DatabaseOpening {
    enum Source: Sendable {
        case borrowed(DatabaseContext)
        case owned(make: @Sendable () async throws -> DBContainer,
            context: @Sendable (DBContainer) async throws -> DatabaseContext)
    }
    private let source: Source
    private let limits: DatabaseWireLimits
    private let structuralLimits: QueryStructuralLimits
    private let checkpointCeiling: TransactionCheckpointLimits?
    private var opened = false

    init(source: Source, limits: DatabaseWireLimits, structuralLimits: QueryStructuralLimits,
        checkpointCeiling: TransactionCheckpointLimits?) {
        self.source = source; self.limits = limits
        self.structuralLimits = structuralLimits; self.checkpointCeiling = checkpointCeiling
    }

    func open(maximumConcurrentExecutions: Int) async throws -> any DatabaseExecuting {
        if Task.isCancelled { throw CancellationError() }
        guard maximumConcurrentExecutions > 0 else { throw AccessConfigurationError.invalidConcurrencyLimit(maximumConcurrentExecutions) }
        guard !opened else { throw AccessConfigurationError.openingAlreadyUsed }
        opened = true
        switch source {
        case .borrowed(let context):
            let connection = try await context.openConnection(maximumConcurrentExecutions: maximumConcurrentExecutions,
                limits: limits, structuralLimits: structuralLimits, checkpointCeiling: checkpointCeiling)
            if Task.isCancelled {
                await Task { await connection.close() }.value
                throw CancellationError()
            }
            return EmbeddedExecution(connection: connection, container: nil)
        case .owned(let make, let select):
            let container = try await make()
            do {
                let context = try await select(container)
                let connection = try await context.openConnection(maximumConcurrentExecutions: maximumConcurrentExecutions,
                    limits: limits, structuralLimits: structuralLimits, checkpointCeiling: checkpointCeiling)
                if Task.isCancelled {
                    await Task { await connection.close() }.value
                    throw CancellationError()
                }
                return EmbeddedExecution(connection: connection, container: container)
            } catch {
                await Task { await container.shutdown() }.value
                throw error
            }
        }
    }
}

public extension DatabaseConfiguration {
    static func borrowingEmbedded(context: DatabaseContext, maximumConcurrentExecutions: Int,
        limits: DatabaseWireLimits = .default, structuralLimits: QueryStructuralLimits = .default,
        checkpointCeiling: TransactionCheckpointLimits? = nil
    ) throws(AccessConfigurationError) -> Self {
        try Self(opening: EmbeddedOpening(source: .borrowed(context), limits: limits,
            structuralLimits: structuralLimits, checkpointCeiling: checkpointCeiling), maximumConcurrentExecutions: maximumConcurrentExecutions)
    }

    static func owningEmbedded(maximumConcurrentExecutions: Int,
        makeContainer: @escaping @Sendable () async throws -> DBContainer,
        context: @escaping @Sendable (DBContainer) async throws -> DatabaseContext,
        limits: DatabaseWireLimits = .default, structuralLimits: QueryStructuralLimits = .default,
        checkpointCeiling: TransactionCheckpointLimits? = nil
    ) throws(AccessConfigurationError) -> Self {
        try Self(opening: EmbeddedOpening(source: .owned(make: makeContainer, context: context), limits: limits,
            structuralLimits: structuralLimits, checkpointCeiling: checkpointCeiling), maximumConcurrentExecutions: maximumConcurrentExecutions)
    }
}
