import Core
import StorageKit

public struct RelationshipPage<Owner: Persistable>: Sendable {
    public let records: [Owner]
    public let continuation: Bytes?

    public init(
        records: [Owner],
        continuation: Bytes?
    ) {
        self.records = records
        self.continuation = continuation
    }
}
