import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
#if DATABASE_OPERATIONS_MULTIPLE_BASES
import DatabaseEngine
import DatabaseKit
import StorageKit

/// Read-only execution boundary fixed to one persisted Composition.
package final class CompositionReadExecutor: Sendable {
    package let compositionID: Base.Composition.ID
    package let authorization: AuthorizationContext
    private let container: DBContainer
    private let source: CompositionDataSource

    package init(
        compositionID: Base.Composition.ID,
        container: DBContainer,
        authorization: AuthorizationContext,
        source: CompositionDataSource
    ) {
        self.compositionID = compositionID
        self.container = container
        self.authorization = authorization
        self.source = source
    }

    package var monotonicClock: any StorageMonotonicClock {
        container.monotonicClock
    }

    package var schema: Schema { container.schema }
    package var schemaGeneration: UInt64 { container.schemaGeneration }
    package var runtimeConfiguration: DatabaseRuntimeConfiguration {
        container.runtimeConfiguration
    }
    package var containerIdentity: ObjectIdentifier {
        ObjectIdentifier(container)
    }

    package func acquireReadLease() async throws -> DatabaseCompositionLease {
        try await source.acquireReadLease()
    }

    package func resolve() async throws -> DatabaseCompositionRecord {
        try await source.resolve()
    }

    package func withReadSnapshot<Result: Sendable>(
        _ operation: @escaping @Sendable (
            DatabaseCompositionReadSnapshot
        ) async throws -> Result
    ) async throws -> Result {
        try await source.withReadSnapshot(operation)
    }

    package func withMemberContext<Result: Sendable>(
        _ member: DatabaseBaseLease,
        in snapshot: DatabaseCompositionReadSnapshot,
        _ operation: @Sendable @escaping (
            DatabaseContext
        ) async throws -> Result
    ) async throws -> Result {
        guard snapshot.lease.record.composition.id == compositionID,
              snapshot.lease.members.contains(where: { $0 === member }) else {
            throw DatabaseCompositionAccessError.unavailable(compositionID)
        }
        return try await container.withBaseLease(member) {
            let context = container.session(
                authorization: authorization
            ).base(member.baseID).newContext()
            return try await RequestAuthorization.$context.withValue(
                authorization
            ) {
                try await operation(context)
            }
        }
    }
}

#endif
