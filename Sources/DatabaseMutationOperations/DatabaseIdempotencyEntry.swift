import DatabaseOperationCore
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

package struct DatabaseIdempotencyEntry: Sendable, Hashable {
    package let operation: DatabaseOperationIdentifier
    package let requestDigest: ByteString
    package let responseDigest: ByteString
    package let responsePayload: ByteString

    package init(
        operation: DatabaseOperationIdentifier,
        requestDigest: ByteString,
        responseDigest: ByteString,
        responsePayload: ByteString
    ) {
        self.operation = operation
        self.requestDigest = requestDigest
        self.responseDigest = responseDigest
        self.responsePayload = responsePayload
    }

    func manifest(limits: DatabaseWireLimits) throws -> DatabaseIdempotencyManifest {
        guard responsePayload.count <= limits.maximumFrameBytes,
              let totalResponseBytes = UInt64(exactly: responsePayload.count),
              let chunkCount = DatabaseIdempotencyManifest.expectedChunkCount(
                  totalResponseBytes: totalResponseBytes
              ) else {
            throw DatabaseMutationError.idempotencyEntryCorrupted
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
        responsePayload: ByteString,
        limits: DatabaseWireLimits
    ) throws -> Self {
        try manifest.validate(limits: limits)
        guard UInt64(responsePayload.count) == manifest.totalResponseBytes,
              manifest.responseDigest == DatabaseRequestDigest.compute(
                  operation: manifest.operation,
                  payload: responsePayload
              ) else {
            throw DatabaseMutationError.idempotencyEntryCorrupted
        }
        return Self(
            operation: manifest.operation,
            requestDigest: manifest.requestDigest,
            responseDigest: manifest.responseDigest,
            responsePayload: responsePayload
        )
    }
}
