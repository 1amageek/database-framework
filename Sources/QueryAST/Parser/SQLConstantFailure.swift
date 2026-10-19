/// Classifies why a typed SQL constant, type modifier or option value was rejected.
///
/// The parser owns the meaning of its constant failures; the SQL diagnostic
/// owner maps each class to its SQLSTATE.
public enum SQLConstantFailure: Sendable, Equatable {
    /// The constant text is not a valid representation of its target type.
    case invalidRepresentation
    /// The constant is well formed but outside the target type's range.
    case outOfRange
    /// A type length, precision or scale argument is outside its admitted range.
    case invalidTypeModifier
    /// A statement option names a value that the option does not recognize.
    case invalidOptionValue
    /// The constant form is outside the SQL profile.
    case unsupported
    /// Admitted constant size or work limits were exceeded.
    case resourceLimit
}
