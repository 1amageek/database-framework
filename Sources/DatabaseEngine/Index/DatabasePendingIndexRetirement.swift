/// Durable physical cleanup work retained by one database data root.
///
/// The record contains only execution-layer ownership: the store scope, the
/// logical index name, and the exact logical and provider generation that may
/// be removed.
/// Host orchestration state may be replaced or cancelled without losing this
/// work.
@_spi(DatabaseExecution)
public struct DatabasePendingIndexRetirement: Sendable, Hashable {
    public let scope: DatabaseIndexStorageScope
    public let identity: DatabaseIndexStorageIdentity

    package init(
        scope: DatabaseIndexStorageScope,
        identity: DatabaseIndexStorageIdentity
    ) {
        self.scope = scope
        self.identity = identity
    }

    public init(_ target: DatabaseIndexTransitionPlan.Target) {
        self.init(
            scope: target.scope,
            identity: target.identity
        )
    }
}
