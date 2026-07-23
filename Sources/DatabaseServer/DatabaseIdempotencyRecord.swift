import DatabaseValue
import DatabaseWire

struct DatabaseIdempotencyRecord: Sendable, Hashable {
    let operation: DatabaseOperationIdentifier
    let requestDigest: DatabaseBytes
    let responseDigest: DatabaseBytes
    let responsePayload: DatabaseBytes

    func manifest(limits: DatabaseWireLimits) throws -> DatabaseIdempotencyManifest {
        guard responsePayload.count <= limits.maximumFrameBytes,
              let totalResponseBytes = UInt64(exactly: responsePayload.count),
              let chunkCount = DatabaseIdempotencyManifest.expectedChunkCount(
                  totalResponseBytes: totalResponseBytes
              ) else {
            throw DatabaseMutationError.idempotencyRecordCorrupted
        }
        let manifest = DatabaseIdempotencyManifest(
            operation: operation,
            requestDigest: requestDigest,
            responseDigest: responseDigest,
            totalResponseBytes: totalResponseBytes,
            chunkCount: chunkCount
        )
        try manifest.validate(limits: limits)
        return manifest
    }

    static func reconstruct(
        manifest: DatabaseIdempotencyManifest,
        responsePayload: DatabaseBytes,
        limits: DatabaseWireLimits
    ) throws -> Self {
        try manifest.validate(limits: limits)
        guard UInt64(responsePayload.count) == manifest.totalResponseBytes,
              manifest.responseDigest == DatabaseRequestDigest.compute(
                  operation: manifest.operation,
                  payload: responsePayload
              ) else {
            throw DatabaseMutationError.idempotencyRecordCorrupted
        }
        return Self(
            operation: manifest.operation,
            requestDigest: manifest.requestDigest,
            responseDigest: manifest.responseDigest,
            responsePayload: responsePayload
        )
    }
}
