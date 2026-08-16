import DatabaseEngine
import DatabaseKit
import StorageKit

public final class SHACLValidationWorkBudget: Sendable {
    public let workMeter: DatabaseWorkMeter

    public init(
        budget: ExecutionBudget,
        monotonicClock: any StorageMonotonicClock
    ) {
        self.workMeter = DatabaseWorkMeter(
            budget: budget,
            monotonicClock: monotonicClock
        )
    }

    public init(workMeter: DatabaseWorkMeter) {
        self.workMeter = workMeter
    }

    public func consume(
        _ units: UInt64 = 1,
        at stage: DatabaseWorkStage = .resultMaterialization
    ) throws {
        try workMeter.consume(units, at: stage)
    }
}
