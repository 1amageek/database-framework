import DatabaseKit
import DatabaseEngine

enum SPARQLGroupStorageError: Error, Sendable, Equatable {
    case capacityOverflow
    case keyStorageOverflow
    case invalidGroupIdentifier(Int)

    static func intermediateByteOverflow(
        workMeter: DatabaseWorkMeter
    ) -> DatabaseWorkLimitError {
        DatabaseWorkLimitError.maximumIntermediateBytes(
            stage: .aggregateInput,
            consumed: workMeter.retainedIntermediateBytes,
            requested: UInt64.max,
            maximum: workMeter.budget.maximumIntermediateBytes
        )
    }
}
