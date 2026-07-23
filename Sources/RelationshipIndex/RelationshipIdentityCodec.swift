import DatabaseValue
import DatabaseWire
import StorageKit

enum RelationshipIdentityCodec {
    static func encode(_ identity: RecordIdentity) throws -> Bytes {
        let encoded = try DatabaseWireWriter.encode {
            (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
            try identity.encode(into: &writer)
        }
        return Bytes(retaining: encoded)
    }

    static func decode(_ bytes: Bytes) throws -> RecordIdentity {
        var reader = DatabaseWireReader(DatabaseBytes(retaining: bytes))
        let identity = try RecordIdentity(from: &reader)
        guard reader.remainingCount == 0 else {
            throw RelationshipReferenceError.corruptedCatalogEntry
        }
        return identity
    }
}
