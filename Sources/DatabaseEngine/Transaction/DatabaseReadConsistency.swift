/// Consistency level applied to one database read operation.
public enum DatabaseReadConsistency: Sendable, Equatable {
    case serializable
    case snapshot
}
