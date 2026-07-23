import Core

enum DatabaseRecordState: Sendable {
    case missing
    case present(any Persistable)

    var model: (any Persistable)? {
        switch self {
        case .missing:
            return nil
        case .present(let model):
            return model
        }
    }
}
