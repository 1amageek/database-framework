import DatabaseKit

enum DatabaseEntityState: Sendable {
    case missing
    case present(PersistedModel)
}
