import DatabaseQueryOperations
import DatabaseOperationCore
import DatabaseKit

enum DatabaseEntityState: Sendable {
    case missing
    case present(PersistedModel)

    var model: PersistedModel? {
        switch self {
        case .missing:
            return nil
        case .present(let model):
            return model
        }
    }
}
