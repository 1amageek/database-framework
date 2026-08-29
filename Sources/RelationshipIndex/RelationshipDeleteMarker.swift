import DatabaseTypes
import StorageKit

package enum RelationshipDeleteMarker {
    package static func isMarked(
        _ identity: EntityReference,
        dataRoot: Subspace,
        transaction: any TransactionAccess
    ) async throws -> Bool {
        try await transaction.getValue(
            for: key(identity, dataRoot: dataRoot),
            snapshot: false
        ) != nil
    }

    package static func mark(
        _ identity: EntityReference,
        dataRoot: Subspace,
        transaction: any TransactionAccess
    ) throws {
        try transaction.setValue(
            [],
            for: try key(identity, dataRoot: dataRoot)
        )
    }

    package static func clear(
        _ identity: EntityReference,
        dataRoot: Subspace,
        transaction: any TransactionAccess
    ) throws {
        try transaction.clear(key: try key(identity, dataRoot: dataRoot))
    }

    private static func key(
        _ identity: EntityReference,
        dataRoot: Subspace
    ) throws -> ByteString {
        dataRoot
            .subspace("relationship-delete-marker")
            .pack(Tuple([try RelationshipIdentityCodec.encode(identity)]))
    }
}
