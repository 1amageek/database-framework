/// The invariant rejected at the throw site, independent of its user-chosen name.
public enum SQLConstraintViolationKind: Sendable, Equatable {
    case notNull, check, unique, foreignKey, generatedValue
}
