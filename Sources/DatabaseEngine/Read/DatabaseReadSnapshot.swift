/// One admitted read transaction and its immutable storage position metadata.
///
/// Transaction configuration and lifecycle authority remain inside
/// DatabaseEngine. Consumers can read only through `transaction` and use the
/// metadata to create or validate continuation contracts.
public struct DatabaseReadSnapshot: Sendable {
    public let session: DatabaseReadSession
    public let position: DatabaseReadPosition
    public let supportsPositionRestoration: Bool

    public var transaction: DatabaseReadTransaction {
        session.transaction
    }

    package init(
        session: DatabaseReadSession,
        position: DatabaseReadPosition,
        supportsPositionRestoration: Bool
    ) {
        self.session = session
        self.position = position
        self.supportsPositionRestoration = supportsPositionRestoration
    }
}
