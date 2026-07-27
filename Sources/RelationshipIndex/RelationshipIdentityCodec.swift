import DatabaseEngine
import DatabaseTypes
import StorageKit

enum RelationshipIdentityCodec {
    static func encode(_ identity: EntityReference) throws -> Bytes {
        let encoded = try StorageFrameEncoder.encode {
            (encoder: inout StorageFrameEncoder) throws(StorageFrameError) in
            try StorageValueEncoder.write(
                .reference(identity),
                into: &encoder
            )
        }
        return Bytes(retaining: encoded)
    }

    static func decode(_ bytes: Bytes) throws -> EntityReference {
        var decoder = try StorageFrameDecoder(ByteString(retaining: bytes))
        guard case .reference(let identity) = try StorageValueDecoder.read(
            from: &decoder
        ), decoder.remainingCount == 0 else {
            throw RelationshipReferenceError.corruptedCatalogEntry
        }
        return identity
    }
}
