import DatabaseValue
import DatabaseWire

struct DatabasePersistentJobResultManifest: DatabaseWireValue, Sendable, Hashable {
    private static let formatVersion: UInt8 = 1

    let jobID: DatabaseUUID
    let operation: DatabaseJobOperationIdentifier
    let specificationDigest: DatabaseBytes
    let responseDigest: DatabaseJobResultDigest
    let totalBytes: UInt64
    let chunkBytes: UInt32
    let chunkCount: UInt32
    let chunkDigests: [DatabaseBytes]
    let createdAt: DatabaseTimestamp

    func validate() throws {
        guard specificationDigest.count == DatabaseRequestDigest.byteCount,
              chunkBytes > 0,
              chunkDigests.count == Int(chunkCount),
              chunkDigests.allSatisfy({
                  $0.count == DatabaseRequestDigest.byteCount
              }) else {
            throw DatabaseJobRuntimeError.corruptedResult
        }
        let expectedChunks: UInt64 = totalBytes == 0
            ? 0
            : ((totalBytes - 1) / UInt64(chunkBytes)) + 1
        guard expectedChunks == UInt64(chunkCount) else {
            throw DatabaseJobRuntimeError.corruptedResult
        }
    }

    func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeUInt8(Self.formatVersion)
        try jobID.encode(into: &writer)
        try operation.encode(into: &writer)
        try writer.writeBytes(specificationDigest)
        try responseDigest.encode(into: &writer)
        writer.writeUInt64(totalBytes)
        writer.writeUInt32(chunkBytes)
        writer.writeUInt32(chunkCount)
        try writer.writeCount(chunkDigests.count)
        for digest in chunkDigests {
            try writer.writeBytes(digest)
        }
        try createdAt.encode(into: &writer)
    }

    init(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) {
        let version = try reader.readUInt8()
        guard version == Self.formatVersion else {
            throw .unsupportedProtocolVersionValue(UInt16(version))
        }
        let jobID = try DatabaseUUID(from: &reader)
        let operation = try DatabaseJobOperationIdentifier(from: &reader)
        let specificationDigest = try reader.readBytes()
        let responseDigest = try DatabaseJobResultDigest(from: &reader)
        let totalBytes = try reader.readUInt64()
        let chunkBytes = try reader.readUInt32()
        let chunkCount = try reader.readUInt32()
        let digestCount = try reader.readCount()
        var chunkDigests: [DatabaseBytes] = []
        chunkDigests.reserveCapacity(digestCount)
        for _ in 0..<digestCount {
            chunkDigests.append(try reader.readBytes())
        }
        self.init(
            jobID: jobID,
            operation: operation,
            specificationDigest: specificationDigest,
            responseDigest: responseDigest,
            totalBytes: totalBytes,
            chunkBytes: chunkBytes,
            chunkCount: chunkCount,
            chunkDigests: chunkDigests,
            createdAt: try DatabaseTimestamp(from: &reader)
        )
    }

    init(
        jobID: DatabaseUUID,
        operation: DatabaseJobOperationIdentifier,
        specificationDigest: DatabaseBytes,
        responseDigest: DatabaseJobResultDigest,
        totalBytes: UInt64,
        chunkBytes: UInt32,
        chunkCount: UInt32,
        chunkDigests: [DatabaseBytes],
        createdAt: DatabaseTimestamp
    ) {
        self.jobID = jobID
        self.operation = operation
        self.specificationDigest = specificationDigest
        self.responseDigest = responseDigest
        self.totalBytes = totalBytes
        self.chunkBytes = chunkBytes
        self.chunkCount = chunkCount
        self.chunkDigests = chunkDigests
        self.createdAt = createdAt
    }
}
