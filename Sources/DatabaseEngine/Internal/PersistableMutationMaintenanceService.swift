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
        identity: EntityReference,
        oldModel: PersistedModel?,
        newModel: PersistedModel?,
        context: borrowing PersistableMutationContext
    ) async throws {
        for maintainer in maintainers {
            try await maintainer.update(
                identity: identity,
                oldModel: oldModel,
                newModel: newModel,
                context: context
            )
        }
    }

    func validateFinalState(
        of models: [PersistedModel],
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
