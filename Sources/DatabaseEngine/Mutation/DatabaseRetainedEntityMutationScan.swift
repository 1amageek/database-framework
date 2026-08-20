import DatabaseKit

package struct DatabaseRetainedEntityMutationScan: ~Copyable, Sendable {
    private var changes: DatabaseRetainedBuffer<EntityMutationChange>
    package let hasMoreSourceRows: Bool

    package init(
        changes: consuming DatabaseRetainedBuffer<EntityMutationChange>,
        hasMoreSourceRows: Bool
    ) {
        self.changes = consume changes
        self.hasMoreSourceRows = hasMoreSourceRows
    }

    package consuming func takeChanges()
        -> DatabaseRetainedBuffer<EntityMutationChange> {
        consume changes
    }
}
