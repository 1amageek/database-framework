public enum DatabaseIntermediateReservationError: Error, Sendable, Equatable {
    case alreadyReleased
    case transferToSelf
    case workMeterMismatch
    case releaseExceedsReservation(
        retainedRows: UInt64,
        retainedBytes: UInt64,
        requestedRows: UInt64,
        requestedBytes: UInt64
    )
}
