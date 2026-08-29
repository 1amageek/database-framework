#if DATABASE_MULTI_BASE
import DatabaseKit

extension DBContainer {
    /// Installs missing access through the production database Grant store
    /// after authorizing the test bootstrap administrator.
    @_spi(Testing)
    public func grantDatabaseAccessForTesting(
        _ grant: Security.Grant,
        authorization: AuthorizationContext
    ) async throws {
        guard grant.resource == .database else {
            throw DatabaseGrantAuthorizationError.resourceMismatch(
                expected: .database,
                actual: grant.resource
            )
        }
        try await withControlTransaction(
            requiredAccess: .administer,
            authorization: authorization
        ) { transaction in
            let current = try await self.databaseGrantStore.direct(
                subject: grant.subject,
                transaction: transaction.storageAccess
            )
            let existing = current.grants.reduce(into: Security.Access()) {
                $0.formUnion($1.access)
            }
            let missing = grant.access.subtracting(existing)
            guard !missing.isEmpty else { return }
            _ = try await self.databaseGrantStore.grant(
                Security.Grant(
                    subject: grant.subject,
                    resource: .database,
                    access: missing
                ),
                expectedRevision: current.revision,
                transaction: transaction.storageAccess
            )
        }
    }

    /// Installs missing access through the production Base Grant store while
    /// retaining an explicitly authorized administration lease.
    @_spi(Testing)
    public func grantBaseAccessForTesting(
        _ grant: Security.Grant,
        authorization: AuthorizationContext
    ) async throws {
        guard case .base(let baseID) = grant.resource else {
            throw DatabaseBaseExecutionError.baseTargetRequired
        }
        let lease = try acquireBaseAdministrationLease(baseID)
        try await withBaseLease(lease) {
            try await self.withBaseAdministrationTransaction(
                requiredAccess: .administer,
                authorization: authorization
            ) { transaction in
                let store = DatabaseGrantStore(
                    resource: grant.resource,
                    root: lease.systemRoot
                )
                let current = try await store.direct(
                    subject: grant.subject,
                    transaction: transaction.storageAccess
                )
                let existing = current.grants.reduce(into: Security.Access()) {
                    $0.formUnion($1.access)
                }
                let missing = grant.access.subtracting(existing)
                guard !missing.isEmpty else { return }
                _ = try await store.grant(
                    Security.Grant(
                        subject: grant.subject,
                        resource: grant.resource,
                        access: missing
                    ),
                    expectedRevision: current.revision,
                    transaction: transaction.storageAccess
                )
            }
        }
    }
}
#endif
