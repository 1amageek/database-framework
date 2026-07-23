import Core
import StorageKit

public struct DatabaseRecordPage<Record: Persistable>: Sendable {
    public let records: [Record]
    public let continuation: Bytes?

    public init(records: [Record], continuation: Bytes?) {
        self.records = records
        self.continuation = continuation
    }
}
