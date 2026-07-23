package enum DatabaseIntermediateFootprintError: Error, Sendable, Equatable {
    case rowAdditionOverflow(left: UInt64, right: UInt64)
    case byteAdditionOverflow(left: UInt64, right: UInt64)
    case rowMultiplicationOverflow(value: UInt64, multiplier: UInt64)
    case byteMultiplicationOverflow(value: UInt64, multiplier: UInt64)
}
