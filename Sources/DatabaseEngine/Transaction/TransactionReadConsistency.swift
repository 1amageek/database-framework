/// Consistency mode applied while reading through one database transaction.
public enum TransactionReadConsistency: Sendable, Equatable {
    case serializable
    case snapshot
}
