import DatabaseKit
import DatabaseTypes

/// Immutable request-scoped entity mutation plan reused by storage retries.
@_spi(DatabaseExecution)
public struct DatabasePreparedEntityMutation: Sendable {
    struct Identity: Sendable {
        let key: ResolvedEntityReference.Key
        let identity: EntityReference
    }

    struct Precondition: Sendable {
        let key: ResolvedEntityReference.Key
        let value: EntityMutationPrecondition
    }

    let changes:
        DatabaseSharedRetainedArray<DatabaseEntityMutationExecutor.PreparedChange>
    let identities: DatabaseSharedRetainedArray<Identity>
    let preconditions: DatabaseSharedRetainedArray<Precondition>

    init(
        changes: DatabaseSharedRetainedArray<
            DatabaseEntityMutationExecutor.PreparedChange
        >,
        identities: DatabaseSharedRetainedArray<Identity>,
        preconditions: DatabaseSharedRetainedArray<Precondition>
    ) {
        self.changes = changes
        self.identities = identities
        self.preconditions = preconditions
    }
}
