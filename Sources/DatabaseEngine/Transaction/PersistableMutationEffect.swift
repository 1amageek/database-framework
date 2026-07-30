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
    public let model: PersistedModel?

    package init(
        kind: PersistableMutationKind,
        identity: EntityReference,
        model: PersistedModel?
    ) {
        self.kind = kind
        self.identity = identity
        self.model = model
    }
}
