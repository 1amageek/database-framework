package enum DatabaseByteProcessingMeter {
    /// One work unit represents one bounded 256-byte CPU-processing quantum.
    private static let bytesPerWorkUnit: UInt64 = 256

    package static func consume(
        byteCount: Int,
        passes: UInt64 = 1,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws {
        precondition(byteCount >= 0)
        guard byteCount > 0, passes > 0 else {
            try workMeter.checkpoint(at: stage)
            return
        }
        let bytes = UInt64(byteCount)
        let unitsPerPass = bytes / bytesPerWorkUnit
            + (bytes % bytesPerWorkUnit == 0 ? 0 : 1)
        let (units, overflow) = unitsPerPass.multipliedReportingOverflow(
            by: passes
        )
        try workMeter.consume(overflow ? UInt64.max : units, at: stage)
    }

    package static func consume(
        byteCount: UInt64,
        passes: UInt64 = 1,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws {
        guard byteCount > 0, passes > 0 else {
            try workMeter.checkpoint(at: stage)
            return
        }
        let unitsPerPass = byteCount / bytesPerWorkUnit
            + (byteCount % bytesPerWorkUnit == 0 ? 0 : 1)
        let (units, overflow) = unitsPerPass.multipliedReportingOverflow(
            by: passes
        )
        try workMeter.consume(overflow ? UInt64.max : units, at: stage)
    }
}
