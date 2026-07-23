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
    public let identity: RecordIdentity
    public let model: (any Persistable)?

    package init(
        kind: PersistableMutationKind,
        identity: RecordIdentity,
        model: (any Persistable)?
    ) {
        self.kind = kind
        self.identity = identity
        self.model = model
    }
}
