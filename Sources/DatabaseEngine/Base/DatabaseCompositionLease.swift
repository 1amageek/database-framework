#if DATABASE_MULTI_BASE
import DatabaseKit
import StorageKit
import Synchronization

/// Retains one immutable Composition definition and every member Base lease
/// while the package is establishing or executing one federated operation.
package final class DatabaseCompositionLease: Sendable {
    package let selection: CompositionSelection
    package let resolution: CompositionResolution
    package let namedRecord: DatabaseCompositionRecord?
    package let members: [DatabaseBaseLease]

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

    package func member(
        identifiedBy id: Base.ID
    ) -> DatabaseBaseLease? {
        members.first { $0.baseID == id }
    }

    package var basePlacementGenerations: [Base.ID: UInt64] {
        Dictionary(
            uniqueKeysWithValues: members.map {
                ($0.baseID, $0.placementGeneration)
            }
        )
    }
}

/// An immutable, non-owning identifier for one member admitted to a snapshot.
/// The token never retains the Base generation lease or its transaction.
@_spi(DatabaseExecution)
public final class DatabaseCompositionReadMember: Sendable {
    public let baseID: Base.ID
    public let placementGeneration: UInt64

    package init(
        baseID: Base.ID,
        placementGeneration: UInt64
    ) {
        self.baseID = baseID
        self.placementGeneration = placementGeneration
    }
}

/// One request-local set of simultaneously open domain transactions.
///
/// Public metadata remains readable after the callback returns, but transaction
/// and Base-lease access is synchronously revoked by `close()`. Consequently,
/// retaining the snapshot cannot extend a transaction or Base generation
/// lifetime beyond `CompositionDataSource.withReadSnapshot`.
@_spi(DatabaseExecution)
public final class DatabaseCompositionReadSnapshot: Sendable {
    private struct MemberState: Sendable {
        let lease: DatabaseBaseLease
        let transaction: DataRootTransactionAccess
    }

    private struct State: Sendable {
        var members: [ObjectIdentifier: MemberState]?
    }

    public let selection: CompositionSelection
    public let resolution: CompositionResolution
    public let basePlacementGenerations: [Base.ID: UInt64]
    public let members: [DatabaseCompositionReadMember]
    package let sourceIdentity: DatabaseCompositionSourceIdentity
    package let authorization: AuthorizationContext
    private let readScope: DatabaseReadScopeGate
    private let state: Mutex<State>
    private let capturedReadPoints: [DomainReadPoint]

    package var operationScope: DatabaseReadScopeGate { readScope }

    package init(
        lease: DatabaseCompositionLease,
        sourceIdentity: DatabaseCompositionSourceIdentity,
        authorization: AuthorizationContext,
        transactions: [String: any TransactionAccess],
        readPoints: [DomainReadPoint]
    ) throws {
        let readScope = DatabaseReadScopeGate()
        var memberStates: [ObjectIdentifier: MemberState] = [:]
        memberStates.reserveCapacity(lease.members.count)
        var members: [DatabaseCompositionReadMember] = []
        members.reserveCapacity(lease.members.count)
        for leaseMember in lease.members {
            guard let transaction = transactions[leaseMember.domainID] else {
                throw DatabaseCompositionAccessError.unavailable(
                    lease.selection
                )
            }
            let member = DatabaseCompositionReadMember(
                baseID: leaseMember.baseID,
                placementGeneration: leaseMember.placementGeneration
            )
            members.append(member)
            memberStates[ObjectIdentifier(member)] = MemberState(
                lease: leaseMember,
                transaction: DataRootTransactionAccess.admitted(
                    transaction,
                    dataRoot: leaseMember.root,
                    readScope: readScope
                )
            )
        }
        self.selection = lease.selection
        self.resolution = lease.resolution
        self.basePlacementGenerations = lease.basePlacementGenerations
        self.members = members
        self.sourceIdentity = sourceIdentity
        self.authorization = authorization
        self.readScope = readScope
        self.state = Mutex(State(members: memberStates))
        self.capturedReadPoints = readPoints
    }

    public func transaction(
        for member: DatabaseCompositionReadMember
    ) throws -> any TransactionReadAccess {
        try admittedMember(for: member).transaction.readProjection()
    }

    func admittedTransaction(
        for member: DatabaseCompositionReadMember
    ) throws -> DataRootTransactionAccess {
        try admittedMember(for: member).transaction
    }

    func admittedMember(
        for member: DatabaseCompositionReadMember
    ) throws -> (
        lease: DatabaseBaseLease,
        transaction: DataRootTransactionAccess
    ) {
        try state.withLock { state in
            guard let memberState = state.members?[ObjectIdentifier(member)]
            else {
                throw DatabaseCompositionAccessError.unavailable(selection)
            }
            return (memberState.lease, memberState.transaction)
        }
    }

    package func close() async throws {
        let transactions = state.withLock { state in
            state.members?.values.map(\.transaction) ?? []
        }
        do {
            try await readScope.closeAndWait()
            for transaction in transactions { transaction.revoke() }
            state.withLock { $0.members = nil }
        } catch {
            for transaction in transactions { transaction.revoke() }
            state.withLock { $0.members = nil }
            throw error
        }
    }

    /// Captures exactly one backend read point for every simultaneously open
    /// physical domain. Domain ordering is canonical so the resulting value is
    /// stable for the complete federated read.
    public func readPoints() async throws -> [DomainReadPoint] {
        capturedReadPoints
    }
}

#endif
