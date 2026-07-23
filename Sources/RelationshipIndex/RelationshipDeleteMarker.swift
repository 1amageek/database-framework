import DatabaseValue
import StorageKit

package enum RelationshipDeleteMarker {
    private static let root = Subspace(
        prefix: Tuple(["_database_framework", "relationship_delete_marker"]).pack()
    )

    package static func isMarked(
        _ identity: PersistableIdentity,
        transaction: any TransactionAccess
    ) async throws -> Bool {
        try await transaction.getValue(
            for: key(identity),
            snapshot: false
        ) != nil
    }

    package static func mark(
        _ identity: PersistableIdentity,
        transaction: any TransactionAccess
    ) throws {
        try transaction.setValue([], for: try key(identity))
    }

    package static func clear(
        _ identity: PersistableIdentity,
        transaction: any TransactionAccess
    ) throws {
        try transaction.clear(key: try key(identity))
    }

    private static func key(_ identity: PersistableIdentity) throws -> Bytes {
        root.pack(Tuple([try RelationshipIdentityCodec.encode(identity)]))
    }
}
