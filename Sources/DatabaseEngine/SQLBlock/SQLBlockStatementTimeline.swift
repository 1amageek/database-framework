import DatabaseTypes
import Synchronization
import StorageKit

/// Retry-stable statement instants captured on each statement's first entry.
final class SQLBlockStatementTimeline: Sendable {
    private let instants: Mutex<[Timestamp?]>
    private let deadlines: Mutex<[StorageInstant?]>
    private let reservation: DatabaseIntermediateReservation

    init(count: Int, workMeter: DatabaseWorkMeter) throws {
        let footprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
            count: count, element: Timestamp?.self).adding(
                DatabaseIntermediateCollectionMeter.arrayFootprint(count: count, element: StorageInstant?.self))
        self.reservation = try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .bindingCandidate)
        self.instants = Mutex(Array(repeating: nil, count: count))
        self.deadlines = Mutex(Array(repeating: nil, count: count))
    }

    func instant(at index: Int, clock: any WallClock) -> Timestamp {
        if let existing = instants.withLock({ $0[index] }) { return existing }
        let captured = clock.now
        return instants.withLock { values in
            if let existing = values[index] { return existing }
            values[index] = captured
            return captured
        }
    }

    func deadline(at index: Int, milliseconds: UInt64, clock: any StorageMonotonicClock) -> StorageInstant? {
        guard milliseconds != 0 else { return nil }
        if let existing = deadlines.withLock({ $0[index] }) { return existing }
        let captured = clock.now.advanced(by: .milliseconds(Int64(milliseconds)))
        return deadlines.withLock { values in
            if let existing = values[index] { return existing }
            values[index] = captured
            return captured
        }
    }
}
