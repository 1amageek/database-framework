#if DATABASE_MULTI_BASE
import DatabaseKit
import StorageKit

extension DBContainer {
    /// Resumable core used by the persistent Base provisioning job.
    ///
    /// Each phase is independently idempotent. The control record remains in
    /// `provisioning` until the Base-local Grants and derived index state are
    /// durable in the selected data domain.
    package func provisionBase(
        _ id: Base.ID,
        placementID: Base.Placement.ID,
        initialGrants: [Security.Grant],
        expectedRevision: UInt64
    ) async throws -> DatabaseBaseRecord {
        try validateInitialGrants(initialGrants, for: id)
        guard let placement = storageTopology.placement(
            identifiedBy: placementID
        ) else {
            throw DatabaseBaseCatalogError.placementNotFound(placementID)
        }
        guard let domain = storageTopology.domain(
            identifiedBy: placement.domainID
        ) else {
            throw DatabaseBaseCatalogError.storageDomainNotFound(
                placement.domainID
            )
        }

        let provisioning = try await storageTopology.controlDomain
            .transactionExecutor.withTransaction(
                configuration: .batch,
                clock: monotonicClock
            ) { transaction in
                if let existing = try await self.baseCatalog.load(
                    id,
                    transaction: transaction
                ) {
                    guard existing.placementID == placementID else {
                        throw DatabaseBaseCatalogError.baseAlreadyExists(id)
                    }
                    switch existing.lifecycle {
                    case .active:
                        return existing
                    case .provisioning:
                        try await self.requireNoActiveSchemaTransition(
                            transaction: transaction
                        )
                        return existing
                    case .tombstone:
                        throw DatabaseBaseCatalogError
                            .baseIdentifierRetired(id)
                    case .retiring, .retired, .moving, .deleting:
                        throw DatabaseBaseCatalogError.baseAlreadyExists(id)
                    }
                }
                try await self.requireNoActiveSchemaTransition(
                    transaction: transaction
                )
                return try await self.baseCatalog.insertProvisioning(
                    id: id,
                    placement: placement,
                    domain: domain,
                    expectedRevision: expectedRevision,
                    transaction: transaction
                )
            }

        if provisioning.lifecycle == .active {
            return provisioning
        }

        let access = domain.directoryAccess
        let databaseRoot = domain.databaseRoot
        let baseName = id.value
        // The Base Partition, its reserved children, and the Base-local Grants
        // commit together. A Partition observable without its initial
        // administer Grant would admit unauthorized operations.
        let tenant = try await domain.transactionExecutor.withTransaction(
            configuration: .batch,
            clock: monotonicClock
        ) { transaction in
            let tenant = try await DatabaseDirectoryLayout
                .openOrCreateBaseTenant(
                    baseName,
                    in: databaseRoot,
                    access: access,
                    transaction: transaction
                )
            let grantStore = DatabaseGrantStore(
                resource: .base(id),
                root: tenant.systemRoot
            )
            let existing = try await grantStore.direct(
                transaction: transaction
            )
            if existing.revision == 0 {
                try await grantStore.installInitial(
                    initialGrants,
                    transaction: transaction
                )
            } else {
                guard existing.revision == 1,
                      Self.normalizedGrants(existing.grants)
                        == Self.normalizedGrants(initialGrants) else {
                    throw DatabaseGrantAuthorizationError.revisionConflict(
                        expected: 1,
                        actual: existing.revision
                    )
                }
            }
            return tenant
        }

        let provisionalRecord = DatabaseBaseRecord(
            id: provisioning.id,
            placementID: provisioning.placementID,
            domainID: provisioning.domainID,
            placementGeneration: provisioning.placementGeneration,
            revision: provisioning.revision,
            lifecycle: .active
        )
        let provisionalGeneration = DatabaseBaseGeneration(
            record: provisionalRecord,
            domain: domain,
            tenant: tenant
        )
        let provisionalLease = DatabaseBaseLease(
            generation: provisionalGeneration,
            token: DatabaseBaseLeaseToken(finishOperation: {})
        )
        try await withBaseLease(provisionalLease) {
            try await self.bootstrapProvisionedBase()
        }

        let active = try await storageTopology.controlDomain
            .transactionExecutor.withTransaction(
                configuration: .batch,
                clock: monotonicClock
            ) { transaction in
                guard let current = try await self.baseCatalog.load(
                    id,
                    transaction: transaction
                ) else {
                    throw DatabaseBaseCatalogError.baseNotFound(id)
                }
                if current.lifecycle == .active {
                    return current
                }
                guard current.lifecycle == .provisioning else {
                    throw DatabaseBaseCatalogError.invalidLifecycleTransition(
                        baseID: id,
                        from: current.lifecycle.rawValue,
                        to: DatabaseBaseLifecycleState.active.rawValue
                    )
                }
                let nextRevision = try Self.incrementBaseRevision(
                    current.revision,
                    baseID: id
                )
                return try await self.baseCatalog.replace(
                    DatabaseBaseRecord(
                        id: current.id,
                        placementID: current.placementID,
                        domainID: current.domainID,
                        placementGeneration: current.placementGeneration,
                        revision: nextRevision,
                        lifecycle: .active
                    ),
                    expectedRecordRevision: current.revision,
                    transaction: transaction
                )
            }
        try publishBaseGeneration(active, tenant: tenant)
        return active
    }

    private func validateInitialGrants(
        _ grants: [Security.Grant],
        for id: Base.ID
    ) throws {
        guard !grants.isEmpty,
              grants.allSatisfy({ $0.resource == .base(id) }),
              grants.contains(where: { $0.access.contains(.administer) })
        else {
            throw DatabaseGrantAuthorizationError.denied(
                resource: .base(id),
                required: .administer
            )
        }
        for grant in grants {
            guard grant.access.containsOnlyKnownPermissions,
                  !grant.access.isEmpty else {
                throw DatabaseGrantAuthorizationError.invalidAccessBits(
                    grant.access.rawValue
                )
            }
        }
    }

    private static func normalizedGrants(
        _ grants: [Security.Grant]
    ) -> [Security.Subject: Security.Access] {
        var normalized: [Security.Subject: Security.Access] = [:]
        for grant in grants {
            normalized[grant.subject, default: []].formUnion(grant.access)
        }
        return normalized
    }

    private static func incrementBaseRevision(
        _ revision: UInt64,
        baseID: Base.ID
    ) throws -> UInt64 {
        let (next, overflow) = revision.addingReportingOverflow(1)
        guard !overflow else {
            throw DatabaseBaseCatalogError.corruptedRecord(baseID)
        }
        return next
    }
}

#endif
