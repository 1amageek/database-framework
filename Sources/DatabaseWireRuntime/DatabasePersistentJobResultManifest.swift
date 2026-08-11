import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire

struct DatabasePersistentJobResultManifest: DatabaseRuntimePayloadValue, Sendable, Hashable {
    private static let formatVersion: UInt8 = 1

    let jobID: DatabaseTypes.UUID
    let operation: JobOperationIdentifier
    let specificationDigest: ByteString
    let responseDigest: JobResultDigest
    let totalBytes: UInt64
    let chunkBytes: UInt32
    let chunkCount: UInt32
    let chunkDigests: [ByteString]
    let createdAt: Timestamp

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
        let jobID = try DatabaseTypes.UUID(from: &reader)
        let operation = try JobOperationIdentifier(from: &reader)
        let specificationDigest = try reader.readBytes()
        let responseDigest = try JobResultDigest(from: &reader)
        let totalBytes = try reader.readUInt64()
        let chunkBytes = try reader.readUInt32()
        let chunkCount = try reader.readUInt32()
        let digestCount = try reader.readCount()
        var chunkDigests: [ByteString] = []
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
            createdAt: try Timestamp(from: &reader)
        )
    }

    init(
        jobID: DatabaseTypes.UUID,
        operation: JobOperationIdentifier,
        specificationDigest: ByteString,
        responseDigest: JobResultDigest,
        totalBytes: UInt64,
        chunkBytes: UInt32,
        chunkCount: UInt32,
        chunkDigests: [ByteString],
        createdAt: Timestamp
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
