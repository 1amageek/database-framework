import Core
import StorageKit

/// Executes all container-scoped record mutation maintainers exactly once per record change.
internal struct RecordMutationMaintenanceService: Sendable {
    private let container: DBContainer
    private let maintainers: [any RecordMutationMaintainer]

    init(
        container: DBContainer,
        maintainers: [any RecordMutationMaintainer]
    ) {
        self.container = container
        self.maintainers = maintainers
    }

    func update(
        oldModel: (any Persistable)?,
        newModel: (any Persistable)?,
        transaction: any Transaction
    ) async throws {
        for maintainer in maintainers {
            try await maintainer.update(
                oldModel: oldModel,
                newModel: newModel,
                container: container,
                transaction: transaction
            )
        }
    }

    func validateFinalState(
        of models: [any Persistable],
        transaction: any Transaction
    ) async throws {
        for maintainer in maintainers {
            try await maintainer.validateFinalState(
                of: models,
                container: container,
                transaction: transaction
            )
        }
    }
}
