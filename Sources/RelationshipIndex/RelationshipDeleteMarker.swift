import DatabaseTypes
import StorageKit

package enum RelationshipDeleteMarker {
    package static func isMarked(
        _ identity: EntityReference,
        baseRoot: Subspace,
        transaction: any TransactionAccess
    ) async throws -> Bool {
        try await transaction.getValue(
            for: key(identity, baseRoot: baseRoot),
            snapshot: false
        ) != nil
    }

    package static func mark(
        _ identity: EntityReference,
        baseRoot: Subspace,
        transaction: any TransactionAccess
    ) throws {
        try transaction.setValue(
            [],
            for: try key(identity, baseRoot: baseRoot)
        )
    }

    package static func clear(
        _ identity: EntityReference,
        baseRoot: Subspace,
        transaction: any TransactionAccess
    ) throws {
        try transaction.clear(key: try key(identity, baseRoot: baseRoot))
    }

    private static func key(
        _ identity: EntityReference,
        baseRoot: Subspace
    ) throws -> ByteString {
        baseRoot
            .subspace("data")
            .subspace("_database_framework")
            .subspace("relationship_delete_marker")
            .pack(Tuple([try RelationshipIdentityCodec.encode(identity)]))
    }
}
