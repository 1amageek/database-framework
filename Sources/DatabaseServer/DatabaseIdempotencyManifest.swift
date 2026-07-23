import DatabaseValue
import DatabaseWire

struct DatabaseIdempotencyManifest: Sendable, Hashable {
    static let formatVersion: UInt16 = 1
    static let chunkByteCount: UInt32 = 90_000

    let operation: DatabaseOperationIdentifier
    let requestDigest: DatabaseBytes
    let responseDigest: DatabaseBytes
    let totalResponseBytes: UInt64
    let chunkCount: UInt32

    func encode(limits: DatabaseWireLimits) throws -> DatabaseBytes {
        try validate(limits: limits)
        return try DatabaseWireWriter.encode(limits: limits) {
            (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
            writer.writeUInt16(Self.formatVersion)
            operation.encode(into: &writer)
            try writer.writeBytes(requestDigest)
            try writer.writeBytes(responseDigest)
            writer.writeUInt64(totalResponseBytes)
            writer.writeUInt32(Self.chunkByteCount)
            writer.writeUInt32(chunkCount)
        }
    }

    static func decode(
        _ bytes: DatabaseBytes,
        limits: DatabaseWireLimits
    ) throws -> Self {
        var reader = DatabaseWireReader(bytes, limits: limits)
        guard try reader.readUInt16() == formatVersion else {
            throw DatabaseMutationError.idempotencyRecordCorrupted
        }
        let operation = try DatabaseOperationIdentifier(from: &reader)
        let requestDigest = try reader.readBytes()
        let responseDigest = try reader.readBytes()
        let totalResponseBytes = try reader.readUInt64()
        let storedChunkByteCount = try reader.readUInt32()
        guard storedChunkByteCount == chunkByteCount else {
            throw DatabaseMutationError.idempotencyRecordCorrupted
        }
        let chunkCount = try reader.readUInt32()
        let manifest = Self(
            operation: operation,
            requestDigest: requestDigest,
            responseDigest: responseDigest,
            totalResponseBytes: totalResponseBytes,
            chunkCount: chunkCount
        )
        try reader.ensureFullyRead()
        try manifest.validate(limits: limits)
        return manifest
    }

    func validate(limits: DatabaseWireLimits) throws {
        guard limits.maximumFrameBytes >= 0,
              limits.maximumCollectionCount >= 0,
              requestDigest.count == DatabaseRequestDigest.byteCount,
              responseDigest.count == DatabaseRequestDigest.byteCount,
              totalResponseBytes <= UInt64(limits.maximumFrameBytes),
              UInt64(chunkCount) <= UInt64(limits.maximumCollectionCount),
              chunkCount == Self.expectedChunkCount(
                  totalResponseBytes: totalResponseBytes
              ) else {
            throw DatabaseMutationError.idempotencyRecordCorrupted
        }
    }

    static func expectedChunkCount(totalResponseBytes: UInt64) -> UInt32? {
        guard totalResponseBytes > 0 else { return 0 }
        let count = ((totalResponseBytes - 1) / UInt64(chunkByteCount)) + 1
        return UInt32(exactly: count)
    }

    func expectedByteCount(forChunkAt index: UInt32) -> Int? {
        guard index < chunkCount else { return nil }
        let offset = UInt64(index) * UInt64(Self.chunkByteCount)
        guard offset < totalResponseBytes else { return nil }
        let remaining = totalResponseBytes - offset
        return Int(exactly: min(remaining, UInt64(Self.chunkByteCount)))
    }
}
