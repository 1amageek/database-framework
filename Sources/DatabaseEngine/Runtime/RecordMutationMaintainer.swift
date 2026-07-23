import Core
import StorageKit

/// Maintains container-wide invariants derived from record mutations.
public protocol RecordMutationMaintainer: Sendable {
    /// Stable identifier referenced by compiled runtime-maintained descriptors.
    var identifier: String { get }

    /// Validates all schema metadata required by this maintainer at bootstrap.
    func validate(schema: Schema) throws

    /// Applies the derived mutation in the caller's transaction.
    func update(
        oldModel: (any Persistable)?,
        newModel: (any Persistable)?,
        container: DBContainer,
        transaction: any Transaction
    ) async throws

    /// Validates invariants after all primary mutations are visible in the transaction.
    func validateFinalState(
        of models: [any Persistable],
        container: DBContainer,
        transaction: any Transaction
    ) async throws
}
