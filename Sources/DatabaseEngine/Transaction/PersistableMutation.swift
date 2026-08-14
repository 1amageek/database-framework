import DatabaseKit

/// One primary persisted-model intent applied by `DatabaseTransaction`.
@_spi(DatabaseExecution)
public enum PersistableMutation: Sendable {
    case save(
        identity: EntityReference,
        model: PersistedModel,
        precondition: WritePrecondition
    )
    case delete(
        identity: EntityReference,
        model: PersistedModel,
        precondition: WritePrecondition
    )

    package var identity: EntityReference {
        switch self {
        case .save(let identity, _, _), .delete(let identity, _, _):
            return identity
        }
    }

    package var model: PersistedModel {
        switch self {
        case .save(_, let model, _), .delete(_, let model, _):
            return model
        }
    }
}
