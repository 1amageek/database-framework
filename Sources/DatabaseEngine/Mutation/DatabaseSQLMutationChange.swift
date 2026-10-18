import DatabaseKit

/// One direct SQL action against a previously captured canonical row image.
package struct DatabaseSQLMutationChange: Sendable {
    package let kind: EntityMutationKind
    package let oldModel: PersistedModel?
    package let newModel: PersistedModel?

    package init(kind: EntityMutationKind, oldModel: PersistedModel?, newModel: PersistedModel?) {
        self.kind = kind
        self.oldModel = oldModel
        self.newModel = newModel
    }
}
