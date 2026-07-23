import DatabaseValue

public enum PersistableIdentifierKeyError: Error, Sendable, Equatable {
    case invalidIdentifier(PersistableIdentifierValidationError)
    case invalidTupleElementCount(actual: Int)
    case invalidTupleValue(expected: PersistableIdentifierType)
}
