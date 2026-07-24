/// Errors produced by HyperLogLog configuration and merge operations.
public enum HyperLogLogError: Error, Sendable, Equatable {
    case invalidPrecision(Int)
    case precisionMismatch(expected: Int, actual: Int)
    case invalidRegisterCount(expected: Int, actual: Int)
    case invalidRegisterValue(index: Int, value: UInt8, maximum: UInt8)
    case cardinalityOutOfRange
}
