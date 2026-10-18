import DatabaseKit

/// Canonical direct action images retained for authorized SQL RETURNING.
package struct DatabaseSQLMutationOutcome: Sendable {
    package let effect: EntityMutationEffect
    package let oldModel: PersistedModel?
    package let newModel: PersistedModel?
}
