import DatabaseTypes

public struct AdminCollectionStatistics: Sendable, Equatable {
    public let entityName: String
    public let documentCount: Int64
    public let storageByteCount: Int64
    public let averageDocumentByteCount: Int
    public let lastModified: Timestamp?
    public let keyRangeStart: ByteString
    public let keyRangeEnd: ByteString

    public init(
        entityName: String,
        documentCount: Int64,
        storageByteCount: Int64,
        averageDocumentByteCount: Int,
        lastModified: Timestamp?,
        keyRangeStart: ByteString,
        keyRangeEnd: ByteString
    ) {
        self.entityName = entityName
        self.documentCount = documentCount
        self.storageByteCount = storageByteCount
        self.averageDocumentByteCount = averageDocumentByteCount
        self.lastModified = lastModified
        self.keyRangeStart = keyRangeStart
        self.keyRangeEnd = keyRangeEnd
    }
}
