import Core
import DatabaseValue

public enum PersistableMutationKind: Sendable, Equatable {
    case insert
    case update
    case delete
}

/// Net observable effect of a logical transaction on one persisted identity.
public struct PersistableMutationEffect: Sendable {
    public let kind: PersistableMutationKind
    public let identity: PersistableIdentity
    public let model: (any Persistable)?

    package init(
        kind: PersistableMutationKind,
        identity: PersistableIdentity,
        model: (any Persistable)?
    ) {
        self.kind = kind
        self.identity = identity
        self.model = model
    }
}
