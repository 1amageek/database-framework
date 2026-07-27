import DatabaseKit

/// One primary persisted-model intent applied by `DatabaseTransaction`.
package enum PersistableMutation: Sendable {
    case save(
        model: any Persistable,
        precondition: WritePrecondition
    )
    case delete(
        model: any Persistable,
        precondition: WritePrecondition
    )

    package var model: any Persistable {
        switch self {
        case .save(let model, _), .delete(let model, _):
            return model
        }
    }
}
