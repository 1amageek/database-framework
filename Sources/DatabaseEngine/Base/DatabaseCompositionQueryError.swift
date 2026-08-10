/// Failures specific to typed Composition query reduction.
public enum DatabaseCompositionQueryError: Error, Sendable, Equatable {
    /// The sum of member counts cannot be represented by the platform `Int`.
    case countOverflow
}
