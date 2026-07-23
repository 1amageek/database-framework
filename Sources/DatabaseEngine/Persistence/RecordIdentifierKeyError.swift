import DatabaseValue

public enum RecordIdentifierKeyError: Error, Sendable, Equatable {
    case invalidIdentifier(RecordIdentifierValidationError)
    case invalidTupleElementCount(actual: Int)
    case invalidTupleValue(expected: RecordIdentifierType)
}
