import DatabaseEngine
import DatabaseTypes
import StorageKit

enum RelationshipIdentityCodec {
    static func encode(_ identity: EntityReference) throws -> ByteString {
        let encoded = try StorageFrameEncoder.encode {
            (encoder: inout StorageFrameEncoder) throws(StorageFrameError) in
            try StorageValueEncoder.write(
                .reference(identity),
                into: &encoder
            )
        }
        return encoded
    }

    static func decode(_ bytes: ByteString) throws -> EntityReference {
        var decoder = try StorageFrameDecoder(bytes)
        guard case .reference(let identity) = try StorageValueDecoder.read(
            from: &decoder
        ), decoder.remainingCount == 0 else {
            throw RelationshipReferenceError.corruptedCatalogEntry
        }
        return identity
    }
}
