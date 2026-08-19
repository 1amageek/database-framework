import DatabaseKit
import DatabaseTypes

public struct AdminIndexStatistics: Sendable, Equatable {
    public let indexName: String
    public let indexType: IndexType
    public let entryCount: Int64
    public let storageByteCount: Int64
    public let uniqueKeyCount: Int64?
    public let state: AdminIndexState
    public let lastUsed: Timestamp?
    public let usageCount: Int64?

    public init(
        indexName: String,
        indexType: IndexType,
        entryCount: Int64,
        storageByteCount: Int64,
        uniqueKeyCount: Int64?,
        state: AdminIndexState,
        lastUsed: Timestamp?,
        usageCount: Int64?
    ) {
        self.indexName = indexName
        self.indexType = indexType
        self.entryCount = entryCount
        self.storageByteCount = storageByteCount
        self.uniqueKeyCount = uniqueKeyCount
        self.state = state
        self.lastUsed = lastUsed
        self.usageCount = usageCount
    }
}
