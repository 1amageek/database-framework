import DatabaseValue
import DatabaseWire
import StorageKit

enum RelationshipIdentityCodec {
    static func encode(_ identity: PersistableIdentity) throws -> Bytes {
        let encoded = try DatabaseWireWriter.encode {
            (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
            try identity.encode(into: &writer)
        }
        return Bytes(retaining: encoded)
    }

    static func decode(_ bytes: Bytes) throws -> PersistableIdentity {
        var reader = DatabaseWireReader(DatabaseBytes(retaining: bytes))
        let identity = try PersistableIdentity(from: &reader)
        guard reader.remainingCount == 0 else {
            throw RelationshipReferenceError.corruptedCatalogEntry
        }
        return identity
    }
}
