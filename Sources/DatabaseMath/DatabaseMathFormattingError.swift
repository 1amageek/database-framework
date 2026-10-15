/// Failures of the finite floating-point decimal formatting adapter.
public enum DatabaseMathFormattingError: Error, Sendable, Equatable {
    case invalidPrecision
    case nonFiniteValue
    case insufficientCapacity
    case localeUnavailable
    case formattingFailed
    case localeRestorationFailed
    case invalidResult
}
