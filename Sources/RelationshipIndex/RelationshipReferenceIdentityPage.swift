import DatabaseTypes
import StorageKit

public struct RelationshipReferenceIdentityPage: Sendable {
    public let identities: [EntityReference]
    public let continuation: ByteString?

    public init(
        identities: [EntityReference],
        continuation: ByteString?
    ) {
        self.identities = identities
        self.continuation = continuation
    }
}
