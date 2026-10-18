import DatabaseKit

/// One complete physical entity rewrite owned by the caller's transaction.
package struct SQLSchemaRewrite: Sendable {
    package let entityName: String
    package let transform: @Sendable (PersistedModel) async throws -> PersistedModel?

    package init(entityName: String,
        transform: @escaping @Sendable (PersistedModel) async throws -> PersistedModel?
    ) {
        self.entityName = entityName
        self.transform = transform
    }
}
