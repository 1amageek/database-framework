import Core
import StorageKit

public struct RelationshipPage<Owner: Persistable>: Sendable {
    public let entities: [Owner]
    public let continuation: Bytes?

    public init(
        entities: [Owner],
        continuation: Bytes?
    ) {
        self.entities = entities
        self.continuation = continuation
    }
}
