import DatabaseKit
import DatabaseTypes

public enum PersistableMutationKind: Sendable, Equatable {
    case insert
    case update
    case delete
}

/// Net observable effect of a logical transaction on one persisted identity.
public struct PersistableMutationEffect: Sendable {
    public let kind: PersistableMutationKind
    public let identity: EntityReference
    public let model: (any Persistable)?

    package init(
        kind: PersistableMutationKind,
        identity: EntityReference,
        model: (any Persistable)?
    ) {
        self.kind = kind
        self.identity = identity
        self.model = model
    }
}
