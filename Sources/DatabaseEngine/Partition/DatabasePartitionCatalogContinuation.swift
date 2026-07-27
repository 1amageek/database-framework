import DatabaseTypes

struct DatabasePartitionCatalogContinuation: StorageFrameValue {
    private static let version: UInt8 = 1

    let entity: String?
    let lastKey: ByteString

    func encode(
        to writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        writer.writeUInt8(Self.version)
        try writer.writeOptionalString(entity)
        try writer.writeBytes(lastKey)
    }

    init(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) {
        guard try reader.readUInt8() == Self.version else {
            throw StorageFrameError.unsupportedVersion(
                UInt16(Self.version)
            )
        }
        self.entity = try reader.readOptionalString()
        self.lastKey = try reader.readBytes()
    }

    init(entity: String?, lastKey: ByteString) {
        self.entity = entity
        self.lastKey = lastKey
    }
}
