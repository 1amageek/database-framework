import DatabaseKit

/// Executes all container-scoped persistable mutation maintainers.
internal struct PersistableMutationMaintenanceService: Sendable {
    private let maintainers: [any PersistableMutationMaintainer]

    init(
        maintainers: [any PersistableMutationMaintainer]
    ) {
        self.maintainers = maintainers
    }

    func update(
        oldModel: (any Persistable)?,
        newModel: (any Persistable)?,
        context: borrowing PersistableMutationContext
    ) async throws {
        for maintainer in maintainers {
            try await maintainer.update(
                oldModel: oldModel,
                newModel: newModel,
                context: context
            )
        }
    }

    func validateFinalState(
        of models: [any Persistable],
        context: borrowing PersistableValidationContext
    ) async throws {
        for maintainer in maintainers {
            try await maintainer.validateFinalState(
                of: models,
                context: context
            )
        }
    }
}
