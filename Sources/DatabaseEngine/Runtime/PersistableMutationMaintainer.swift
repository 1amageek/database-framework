import DatabaseKit

/// Maintains container-wide invariants derived from persisted model mutations.
public protocol PersistableMutationMaintainer: Sendable {
    /// Stable identifier referenced by compiled runtime-maintained descriptors.
    var identifier: String { get }

    /// Validates all schema metadata required by this maintainer at bootstrap.
    func validate(schema: Schema) throws

    /// Applies the derived mutation in the caller's transaction.
    func update(
        identity: EntityReference,
        oldModel: PersistedModel?,
        newModel: PersistedModel?,
        context: borrowing PersistableMutationContext
    ) async throws

    /// Validates invariants after all primary mutations are visible in the transaction.
    func validateFinalState(
        of models: [PersistedModel],
        context: borrowing PersistableValidationContext
    ) async throws
}
