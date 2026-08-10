import DatabaseKit
import StorageKit

/// Retains one immutable Composition definition and every member Base lease
/// for the complete lifetime of a federated read.
package final class DatabaseCompositionLease: Sendable {
    package let record: DatabaseCompositionRecord
    package let members: [DatabaseBaseLease]

    package init(
        record: DatabaseCompositionRecord,
        members: [DatabaseBaseLease]
    ) {
        precondition(
            record.composition.bases == members.map { $0.baseID },
            "Composition member leases must preserve canonical Base order"
        )
        self.record = record
        self.members = members
    }

    package func member(
        identifiedBy id: Base.ID
    ) -> DatabaseBaseLease? {
        members.first { $0.baseID == id }
    }
}

/// One request-local set of simultaneously open domain transactions.
package struct DatabaseCompositionReadSnapshot: Sendable {
    package let lease: DatabaseCompositionLease
    private let transactions: [String: any TransactionAccess]
    private let capturedReadPoints: [DomainReadPoint]

    package init(
        lease: DatabaseCompositionLease,
        transactions: [String: any TransactionAccess],
        readPoints: [DomainReadPoint]
    ) {
        self.lease = lease
        self.transactions = transactions
        self.capturedReadPoints = readPoints
    }

    package func transaction(
        for member: DatabaseBaseLease
    ) throws -> any TransactionAccess {
        guard let transaction = transactions[member.domainID] else {
            throw DatabaseCompositionAccessError.unavailable(
                lease.record.composition.id
            )
        }
        return transaction
    }

    /// Captures exactly one backend read point for every simultaneously open
    /// physical domain. Domain ordering is canonical so the resulting value is
    /// valid for the DatabaseWire federated-consistency contract.
    package func readPoints() async throws -> [DomainReadPoint] {
        capturedReadPoints
    }
}
