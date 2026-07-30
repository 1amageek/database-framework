import DatabaseTypes

internal enum DatabaseTimestampMeasurementError: Error, Sendable, Equatable {
    case valueOutOfRange
}

internal enum DatabaseTimestampMeasurement {
    static func elapsed(
        from start: Timestamp,
        to end: Timestamp
    ) throws -> TimeSpan {
        let secondsDifference = end.secondsSinceUnixEpoch
            .subtractingReportingOverflow(start.secondsSinceUnixEpoch)
        guard !secondsDifference.overflow else {
            throw DatabaseTimestampMeasurementError.valueOutOfRange
        }

        var seconds = secondsDifference.partialValue
        var nanoseconds = Int64(end.nanoseconds) - Int64(start.nanoseconds)
        if nanoseconds < 0 {
            let adjustedSeconds = seconds.subtractingReportingOverflow(1)
            guard !adjustedSeconds.overflow else {
                throw DatabaseTimestampMeasurementError.valueOutOfRange
            }
            seconds = adjustedSeconds.partialValue
            nanoseconds += 1_000_000_000
        }

        do {
            return try TimeSpan(
                seconds: seconds,
                nanoseconds: UInt32(nanoseconds)
            )
        } catch {
            throw DatabaseTimestampMeasurementError.valueOutOfRange
        }
    }
}
