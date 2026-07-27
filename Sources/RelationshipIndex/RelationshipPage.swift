import DatabaseKit
import DatabaseTypes
import StorageKit

public struct RelationshipPage<Owner: Persistable>: Sendable {
    public let entities: [Owner]
    public let continuation: ByteString?

    public init(
        entities: [Owner],
        continuation: ByteString?
    ) {
        self.entities = entities
        self.continuation = continuation
    }
}
