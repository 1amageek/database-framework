import DatabaseKit
import DatabaseTypes

/// Immutable SQL name policy retained with one admitted read generation.
package final class SQLReadResolution: Sendable {
    let policy: DatabaseReadPolicy
    let snapshot: SQLCatalogSnapshot
    package let searchPath: SQLSearchPath
    let currentUser: String?
    let continuationIdentity: ByteString
    let viewBindings: [SQLViewSourceBinding]?
    let expandingViewIDs: [String]
    var workMeter: DatabaseWorkMeter { reservation.workMeter }
    private let reservation: DatabaseIntermediateReservation

    init(policy: DatabaseReadPolicy, snapshot: SQLCatalogSnapshot, searchPath: SQLSearchPath,
         currentUser: String?, reservation: DatabaseIntermediateReservation,
         viewBindings: [SQLViewSourceBinding]? = nil, expandingViewIDs: [String] = []) {
        self.policy = policy
        self.snapshot = snapshot
        self.searchPath = searchPath
        self.currentUser = currentUser
        self.viewBindings = viewBindings
        self.expandingViewIDs = expandingViewIDs
        var digest = SHA256Accumulator()
        digest.update(snapshot.digest)
        func appendCount(_ count: UInt64, to digest: inout SHA256Accumulator) {
            for shift in stride(from: 56, through: 0, by: -8) { digest.update(UInt8(truncatingIfNeeded: count >> shift)) }
        }
        func appendName(_ name: String, to digest: inout SHA256Accumulator) {
            appendCount(UInt64(name.utf8.count), to: &digest)
            for byte in name.utf8 { digest.update(byte) }
        }
        appendCount(policy.schemaGeneration, to: &digest)
        if let currentUser { digest.update(1); appendName(currentUser, to: &digest) } else { digest.update(0) }
        appendCount(UInt64(searchPath.entries.count), to: &digest)
        for entry in searchPath.entries {
            switch entry {
            case .currentUser: digest.update(0)
            case .schema(let name): digest.update(1); appendName(name, to: &digest)
            }
        }
        self.continuationIdentity = digest.finalize()
        self.reservation = reservation
    }

    func resolve(_ table: TableRef) throws -> SQLCatalogResolvedRelation {
        try resolveSQLViewPhysicalSource(table)
    }

    func validate(_ policy: DatabaseReadPolicy) throws {
        guard self.policy.matchesSQLGeneration(policy), snapshot === policy.sqlCatalog else {
            throw SQLReadResolutionError.generationMismatch
        }
    }
}

public enum SQLReadResolutionError: Error, Sendable, Equatable {
    case generationMismatch
    case missingPhysicalColumn(entity: String, field: String)
}

extension SQLReadResolution {
    static func make(policy: DatabaseReadPolicy, searchPath: SQLSearchPath,
        workMeter: DatabaseWorkMeter) throws -> SQLReadResolution {
        var bytes = UInt64(MemoryLayout<SQLReadResolution>.stride) + 256
        for entry in searchPath.entries {
            bytes = try DatabaseIntermediateFootprint(bytes: bytes).adding(
                DatabaseIntermediateFootprint(bytes: 64 + UInt64({
                    if case .schema(let name) = entry { return name.utf8.count }; return 0
                }()))).bytes
        }
        let currentUser = policy.sqlCurrentUser
        bytes = try DatabaseIntermediateFootprint(bytes: bytes).adding(
            DatabaseIntermediateFootprint(bytes: UInt64(currentUser?.utf8.count ?? 0))).bytes
        let claim = try workMeter.reserveIntermediate(bytes: bytes, at: .bindingCandidate)
        return SQLReadResolution(policy: policy, snapshot: policy.sqlCatalog,
            searchPath: searchPath, currentUser: currentUser, reservation: claim)
    }
}

extension DatabaseContext {
    package func makeSQLReadResolution(searchPath: SQLSearchPath,
        workMeter: DatabaseWorkMeter) throws -> SQLReadResolution {
        try SQLReadResolution.make(policy: readPolicy(), searchPath: searchPath, workMeter: workMeter)
    }
}
