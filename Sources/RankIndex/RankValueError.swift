/// Errors raised when a persisted value cannot participate in rank ordering.
public enum RankValueError: Error, Sendable, Equatable {
    case missingField(String)
    case nonNumericField(fieldName: String, actualType: String)
    case unorderedFloatingPoint(fieldName: String)
    case invalidIdentifier(actualType: String)
    case duplicateIdentifier
}
