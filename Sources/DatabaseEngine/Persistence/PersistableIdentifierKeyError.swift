import DatabaseTypes
import DatabaseKit

public enum PersistableIdentifierKeyError: Error, Sendable, Equatable {
    case invalidLimits
    case invalidIdentifier(PersistableIdentifierValidationError)
    case componentCountExceeded(actual: Int, maximum: Int)
    case compositeDepthExceeded(actual: Int, maximum: Int)
    case invalidTupleElementCount(actual: Int)
    case invalidTupleValue(expected: PersistableIdentifierType)
}
