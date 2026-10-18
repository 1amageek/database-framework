import DatabaseKit

/// Retains the statement's old row separately from the candidate being assembled.
struct SQLDDLRowTransformation: Sendable {
    let apply: @Sendable (PersistedModel, PersistedModel) async throws -> PersistedModel
    init(_ apply: @escaping @Sendable (PersistedModel, PersistedModel) async throws -> PersistedModel) {
        self.apply = apply
    }
}
