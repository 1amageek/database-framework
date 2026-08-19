#if DATABASE_MULTI_BASE
import DatabaseKit
import StorageKit

/// Retains one immutable Composition definition and every member Base lease
/// for the complete lifetime of a federated read.
@_spi(DatabaseExecution)
public final class DatabaseCompositionLease: Sendable {
    public let selection: CompositionSelection
    public let resolution: CompositionResolution
    public let namedRecord: DatabaseCompositionRecord?
    public let members: [DatabaseBaseLease]

    package init(
        selection: CompositionSelection,
        resolution: CompositionResolution,
        namedRecord: DatabaseCompositionRecord?,
        members: [DatabaseBaseLease]
    ) {
        precondition(
            resolution.bases == members.map { $0.baseID },
            "Composition member leases must preserve canonical Base order"
        )
        self.selection = selection
        self.resolution = resolution
        self.namedRecord = namedRecord
        self.members = members
    }

    public func member(
        identifiedBy id: Base.ID
    ) -> DatabaseBaseLease? {
        members.first { $0.baseID == id }
    }

    /// Immutable placement generation for every member admitted by this
    /// execution. Remote adapters may bind durable result snapshots to these
    /// values without inventing a generation for a derived Composition.
    public var basePlacementGenerations: [Base.ID: UInt64] {
        Dictionary(
            uniqueKeysWithValues: members.map {
                ($0.baseID, $0.placementGeneration)
            }
        )
    }
}

/// One request-local set of simultaneously open domain transactions.
@_spi(DatabaseExecution)
public struct DatabaseCompositionReadSnapshot: Sendable {
    public let lease: DatabaseCompositionLease
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

    public func transaction(
        for member: DatabaseBaseLease
    ) throws -> any TransactionAccess {
        guard let transaction = transactions[member.domainID] else {
            throw DatabaseCompositionAccessError.unavailable(
                lease.selection
            )
        }
        return transaction
    }

    /// Captures exactly one backend read point for every simultaneously open
    /// physical domain. Domain ordering is canonical so the resulting value is
    /// stable for the complete federated read.
    public func readPoints() async throws -> [DomainReadPoint] {
        capturedReadPoints
    }
}

#endif
