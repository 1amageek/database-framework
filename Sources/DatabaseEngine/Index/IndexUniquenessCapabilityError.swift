/// Errors raised when an index kind cannot provide uniqueness semantics.
public enum IndexUniquenessCapabilityError: Error, Sendable, Equatable {
    case unsupported(maintainerType: String)
}
