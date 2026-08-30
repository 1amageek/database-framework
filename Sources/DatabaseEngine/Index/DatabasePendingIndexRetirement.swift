import DatabaseKit

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

    /// Layer of each component of the source directory path, in scope order,
    /// or `nil` for a record staged before the layers were recorded.
    ///
    /// The path a retirement addresses belongs to a declaration the published
    /// schema may no longer have, so the layer of each node cannot be derived
    /// again when the work runs. Recording it with the work keeps the address
    /// verifiable: a node that has since been recreated under another layer
    /// holds different storage, and retiring an index generation there would
    /// clear keys this record never described.
    public let directoryLayers: [DirectoryLayer]?

    package init(
        scope: DatabaseIndexStorageScope,
        identity: DatabaseIndexStorageIdentity,
        directoryLayers: [DirectoryLayer]? = nil
    ) {
        self.scope = scope
        self.identity = identity
        self.directoryLayers = directoryLayers
    }

    public init(_ target: DatabaseIndexTransitionPlan.Target) {
        self.init(
            scope: target.scope,
            identity: target.identity
        )
    }

    /// The same physical generation recorded with its source layers.
    package func recording(
        directoryLayers: [DirectoryLayer]?
    ) -> DatabasePendingIndexRetirement {
        DatabasePendingIndexRetirement(
            scope: scope,
            identity: identity,
            directoryLayers: directoryLayers
        )
    }

    /// Two records that address the same physical generation are the same
    /// work. The recorded layers describe how that address is verified, not
    /// which address it is, so they are deliberately not part of identity:
    /// the durable marker key is built from the scope and the identity alone,
    /// and a record staged twice must collapse to one marker whether or not
    /// the earlier staging recorded layers.
    public static func == (
        lhs: DatabasePendingIndexRetirement,
        rhs: DatabasePendingIndexRetirement
    ) -> Bool {
        lhs.scope == rhs.scope && lhs.identity == rhs.identity
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(scope)
        hasher.combine(identity)
    }
}
