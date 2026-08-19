#if DATABASE_MULTI_BASE
import DatabaseKit
import DatabaseTypes
import StorageKit

/// Transaction-scoped persisted Grants for one exact Security resource.
@_spi(DatabaseExecution)
public struct DatabaseGrantStore: Sendable {
    private static let maximumSubjectUTF8ByteCount = 512
    private static let maximumGrantCount = 65_536

    private let resource: Security.Resource
    private let root: Subspace
    private let principals: Subspace
    private let roles: Subspace
    private let revisionKey: ByteString

    public init(resource: Security.Resource, root: Subspace) {
        self.resource = resource
        self.root = root.subspace("security").subspace("grants")
        self.principals = self.root.subspace("principals")
        self.roles = self.root.subspace("roles")
        self.revisionKey = self.root.pack(Tuple("revision"))
    }

    public func require(
        _ required: Security.Access,
        authorization: AuthorizationContext,
        transaction: any TransactionAccess
    ) async throws {
        guard required.containsOnlyKnownPermissions,
              !required.isEmpty else {
            throw DatabaseGrantAuthorizationError.invalidAccessBits(
                required.rawValue
            )
        }
        guard let principal = authorization.principal else {
            throw DatabaseGrantAuthorizationError.unauthenticated
        }
        let effective = try await effective(
            principal: principal,
            transaction: transaction
        )
        guard effective.access.isSuperset(of: required) else {
            throw DatabaseGrantAuthorizationError.denied(
                resource: resource,
                required: required
            )
        }
    }

    public func effective(
        principal: Principal,
        transaction: any TransactionAccess
    ) async throws -> DatabaseEffectiveGrant {
        var access = Security.Access()
        var contributors: [Security.Grant] = []
        if let principalAccess = try await loadAccess(
            subject: .principal(principal.identifier),
            transaction: transaction
        ) {
            access.formUnion(principalAccess)
            contributors.append(
                Security.Grant(
                    subject: .principal(principal.identifier),
                    resource: resource,
                    access: principalAccess
                )
            )
        }
        for role in principal.roles.sorted() {
            if let roleAccess = try await loadAccess(
                subject: .principalRole(role),
                transaction: transaction
            ) {
                access.formUnion(roleAccess)
                contributors.append(
                    Security.Grant(
                        subject: .principalRole(role),
                        resource: resource,
                        access: roleAccess
                    )
                )
            }
        }
        return DatabaseEffectiveGrant(
            access: access,
            contributors: contributors
        )
    }

    public func direct(
        subject: Security.Subject? = nil,
        transaction: any TransactionAccess
    ) async throws -> DatabaseGrantSet {
        let revision = try await loadRevision(transaction: transaction)
        if let subject {
            let access = try await loadAccess(
                subject: subject,
                transaction: transaction
            )
            return DatabaseGrantSet(
                revision: revision,
                grants: access.map {
                    [Security.Grant(
                        subject: subject,
                        resource: resource,
                        access: $0
                    )]
                } ?? []
            )
        }
        var grants = try await scan(
            subjects: principals,
            makeSubject: Security.Subject.principal,
            transaction: transaction
        )
        grants.append(
            contentsOf: try await scan(
                subjects: roles,
                makeSubject: Security.Subject.principalRole,
                transaction: transaction
            )
        )
        grants.sort(by: subjectPrecedes)
        return DatabaseGrantSet(revision: revision, grants: grants)
    }

    public func grant(
        _ grant: Security.Grant,
        expectedRevision: UInt64,
        transaction: any TransactionAccess
    ) async throws -> UInt64 {
        try validate(grant)
        let revision = try await requireRevision(
            expectedRevision,
            transaction: transaction
        )
        let existing = try await loadAccess(
            subject: grant.subject,
            transaction: transaction
        ) ?? []
        let updated = existing.union(grant.access)
        try write(updated, subject: grant.subject, transaction: transaction)
        let nextRevision = try increment(revision)
        try writeRevision(nextRevision, transaction: transaction)
        return nextRevision
    }

    public func revoke(
        _ grant: Security.Grant,
        expectedRevision: UInt64,
        transaction: any TransactionAccess
    ) async throws -> UInt64 {
        try validate(grant)
        let revision = try await requireRevision(
            expectedRevision,
            transaction: transaction
        )
        let existing = try await loadAccess(
            subject: grant.subject,
            transaction: transaction
        ) ?? []
        let updated = existing.subtracting(grant.access)
        if existing.contains(.administer),
           !updated.contains(.administer),
           !(try await hasAdministrator(
                excluding: grant.subject,
                transaction: transaction
           )) {
            throw DatabaseGrantAuthorizationError.lastAdministrator
        }
        if updated.isEmpty {
            try transaction.clear(key: key(for: grant.subject))
        } else {
            try write(updated, subject: grant.subject, transaction: transaction)
        }
        let nextRevision = try increment(revision)
        try writeRevision(nextRevision, transaction: transaction)
        return nextRevision
    }

    public func installInitial(
        _ grants: [Security.Grant],
        transaction: any TransactionAccess
    ) async throws {
        guard try await loadRevision(transaction: transaction) == 0 else {
            throw DatabaseGrantAuthorizationError.revisionConflict(
                expected: 0,
                actual: try await loadRevision(transaction: transaction)
            )
        }
        for grant in grants {
            try validate(grant)
            let existing = try await loadAccess(
                subject: grant.subject,
                transaction: transaction
            ) ?? []
            try write(
                existing.union(grant.access),
                subject: grant.subject,
                transaction: transaction
            )
        }
        try writeRevision(1, transaction: transaction)
    }

    private func scan(
        subjects: Subspace,
        makeSubject: (String) -> Security.Subject,
        transaction: any TransactionAccess
    ) async throws -> [Security.Grant] {
        let range = subjects.range()
        let rows = try await TransactionRangeCollection.collect(
            using: transaction,
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: Self.maximumGrantCount + 1,
            reverse: false,
            snapshot: false,
            streamingMode: .iterator
        )
        guard rows.count <= Self.maximumGrantCount else {
            throw DatabaseGrantAuthorizationError.corruptedGrant
        }
        var grants: [Security.Grant] = []
        grants.reserveCapacity(rows.count)
        for (key, value) in rows {
            let tuple = try subjects.unpack(key)
            guard tuple.count == 1,
                  case .string(let identifier) = try tuple.value(at: 0) else {
                throw DatabaseGrantAuthorizationError.corruptedGrant
            }
            let access = try decodeAccess(value)
            grants.append(
                Security.Grant(
                    subject: makeSubject(identifier),
                    resource: resource,
                    access: access
                )
            )
        }
        return grants
    }

    private func hasAdministrator(
        excluding excludedSubject: Security.Subject,
        transaction: any TransactionAccess
    ) async throws -> Bool {
        let principalGrants = try await scan(
            subjects: principals,
            makeSubject: Security.Subject.principal,
            transaction: transaction
        )
        if principalGrants.contains(where: {
            $0.subject != excludedSubject && $0.access.contains(.administer)
        }) {
            return true
        }
        let roleGrants = try await scan(
            subjects: roles,
            makeSubject: Security.Subject.principalRole,
            transaction: transaction
        )
        return roleGrants.contains(where: {
            $0.subject != excludedSubject && $0.access.contains(.administer)
        })
    }

    private func loadAccess(
        subject: Security.Subject,
        transaction: any TransactionAccess
    ) async throws -> Security.Access? {
        guard let bytes = try await transaction.getValue(
            for: key(for: subject),
            snapshot: false
        ) else {
            return nil
        }
        return try decodeAccess(bytes)
    }

    private func decodeAccess(
        _ bytes: ByteString
    ) throws -> Security.Access {
        guard bytes.count == 1 else {
            throw DatabaseGrantAuthorizationError.corruptedGrant
        }
        let access = Security.Access(rawValue: bytes[bytes.startIndex])
        guard access.containsOnlyKnownPermissions, !access.isEmpty else {
            throw DatabaseGrantAuthorizationError.invalidAccessBits(
                access.rawValue
            )
        }
        return access
    }

    private func loadRevision(
        transaction: any TransactionAccess
    ) async throws -> UInt64 {
        guard let bytes = try await transaction.getValue(
            for: revisionKey,
            snapshot: false
        ) else {
            return 0
        }
        let tuple = try Tuple(packed: bytes)
        guard tuple.count == 1 else {
            throw DatabaseGrantAuthorizationError.corruptedGrant
        }
        switch try tuple.value(at: 0) {
        case .unsignedInteger(let revision):
            return revision
        case .signedInteger(let revision) where revision >= 0:
            return UInt64(revision)
        default:
            throw DatabaseGrantAuthorizationError.corruptedGrant
        }
    }

    private func requireRevision(
        _ expectedRevision: UInt64,
        transaction: any TransactionAccess
    ) async throws -> UInt64 {
        let actualRevision = try await loadRevision(transaction: transaction)
        guard actualRevision == expectedRevision else {
            throw DatabaseGrantAuthorizationError.revisionConflict(
                expected: expectedRevision,
                actual: actualRevision
            )
        }
        return actualRevision
    }

    private func validate(
        _ grant: Security.Grant
    ) throws {
        guard grant.resource == resource else {
            throw DatabaseGrantAuthorizationError.resourceMismatch(
                expected: resource,
                actual: grant.resource
            )
        }
        guard grant.access.containsOnlyKnownPermissions,
              !grant.access.isEmpty else {
            throw DatabaseGrantAuthorizationError.invalidAccessBits(
                grant.access.rawValue
            )
        }
        let identifier: String
        switch grant.subject {
        case .principal(let value), .principalRole(let value):
            identifier = value
        }
        guard !identifier.isEmpty,
              identifier.utf8.count <= Self.maximumSubjectUTF8ByteCount else {
            throw DatabaseGrantAuthorizationError.invalidSubject
        }
    }

    private func key(for subject: Security.Subject) -> ByteString {
        switch subject {
        case .principal(let identifier):
            principals.pack(Tuple(identifier))
        case .principalRole(let identifier):
            roles.pack(Tuple(identifier))
        }
    }

    private func write(
        _ access: Security.Access,
        subject: Security.Subject,
        transaction: any TransactionAccess
    ) throws {
        try transaction.setValue([access.rawValue], for: key(for: subject))
    }

    private func writeRevision(
        _ revision: UInt64,
        transaction: any TransactionAccess
    ) throws {
        try transaction.setValue(Tuple(revision).pack(), for: revisionKey)
    }

    private func increment(_ revision: UInt64) throws -> UInt64 {
        let (next, overflow) = revision.addingReportingOverflow(1)
        guard !overflow else {
            throw DatabaseGrantAuthorizationError.revisionOverflow
        }
        return next
    }

    private func subjectPrecedes(
        _ lhs: Security.Grant,
        _ rhs: Security.Grant
    ) -> Bool {
        switch (lhs.subject, rhs.subject) {
        case (.principal(let lhs), .principal(let rhs)),
             (.principalRole(let lhs), .principalRole(let rhs)):
            return lhs < rhs
        case (.principal, .principalRole):
            return true
        case (.principalRole, .principal):
            return false
        }
    }
}

private extension Security.Access {
    func isSuperset(of other: Security.Access) -> Bool {
        intersection(other) == other
    }

    func subtracting(_ other: Security.Access) -> Security.Access {
        Security.Access(rawValue: rawValue & ~other.rawValue)
    }
}
#endif
