public enum ByteConversionError: Error, Sendable, Equatable {
    case invalidByteCount(expected: Int, actual: Int)
    case nonFiniteDouble
    case scaledDoubleOutOfRange
}
