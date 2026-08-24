import DatabaseEngine
import Synchronization
import SwiftHNSW

final class HNSWDatabaseWorkObserver: HNSWSearchObserver {
    private let workMeter: DatabaseWorkMeter
    private let workUnitsPerDistance: UInt64
    private let failure = Mutex<(any Error)?>(nil)

    init(
        workMeter: DatabaseWorkMeter,
        dimensions: Int
    ) {
        precondition(dimensions > 0)
        self.workMeter = workMeter
        self.workUnitsPerDistance = UInt64(dimensions)
    }

    func shouldEvaluateDistance() -> Bool {
        do {
            try workMeter.consume(
                workUnitsPerDistance,
                at: .indexScan
            )
            return true
        } catch {
            failure.withLock { failure in
                if failure == nil {
                    failure = error
                }
            }
            return false
        }
    }

    func rethrowFailure() throws {
        if let failure = failure.withLock({ $0 }) {
            throw failure
        }
    }
}
