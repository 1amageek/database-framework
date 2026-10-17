import DatabaseKit
import Synchronization

/// Exact scheduled aggregate input slots, scoped to one window invocation.
final class SQLWindowAggregateInputs: Sendable {
    private struct State: Sendable {
        var values: [DatabaseContext.ProjectedQueryValue?]
        var generations: [Int]
        var generation = 0
        var frameHead: Int?
        var row: Int?
        var nextSlot = 0
    }
    private let state: Mutex<State>
    private let slotCount: Int
    private let reservation: DatabaseIntermediateReservation

    init(rowCount: Int, slotCount: Int, workMeter: DatabaseWorkMeter) throws {
        let total = rowCount.multipliedReportingOverflow(by: slotCount)
        guard slotCount > 0, !total.overflow else { throw Self.mismatch }
        let claim = try workMeter.reserveIntermediate(bytes: UInt64(MemoryLayout<State>.stride) + 64, at: .aggregateInput)
        var values = try SQLWindowMemory.array(count: total.partialValue,
            of: DatabaseContext.ProjectedQueryValue?.self, reservation: claim)
        var generations = try SQLWindowMemory.array(count: total.partialValue, of: Int.self, reservation: claim)
        for _ in 0..<total.partialValue {
            try workMeter.consume(at: .aggregateInput)
            values.append(nil); generations.append(-1)
        }
        self.state = Mutex(State(values: values, generations: generations))
        self.slotCount = slotCount
        self.reservation = claim
    }

    func beginFrame(head: Int) throws {
        try state.withLock { state in
            if state.frameHead != head {
                let next = state.generation.addingReportingOverflow(1)
                guard !next.overflow else { throw Self.mismatch }
                state.generation = next.partialValue
                state.frameHead = head
            }
            state.row = nil
            state.nextSlot = 0
        }
    }

    func next(row: Int) throws -> (index: Int, value: DatabaseContext.ProjectedQueryValue?) {
        try state.withLock { state in
            if state.row != row { state.row = row; state.nextSlot = 0 }
            guard state.nextSlot < slotCount else { throw Self.mismatch }
            let index = row * slotCount + state.nextSlot
            guard state.values.indices.contains(index) else { throw Self.mismatch }
            state.nextSlot += 1
            return (index, state.generations[index] == state.generation ? state.values[index] : nil)
        }
    }

    func store(_ cell: DatabaseContext.ProjectedQueryValue, at index: Int) throws
        -> DatabaseContext.ProjectedQueryValue {
        let claim = try reservation.reserveChild(at: .aggregateInput)
        try SQLWindowMemory.retain(cell, reservation: claim)
        var leases = try SQLWindowMemory.array(count: cell.leases.count + 1,
            of: DatabaseQueryPayloadLease.self, reservation: claim)
        let ownerLayout = try DatabaseRetainedArrayLayout.forElement(DatabaseIntermediateReservation.self)
        try claim.reserveAdditional(bytes: try DatabaseIntermediateFootprint(bytes: ownerLayout.containerByteCount)
            .adding(.init(bytes: ownerLayout.growth(from: 0, toFit: 1).additionalByteCount)).bytes, at: .aggregateInput)
        // The published carrier must retain its cache claim independently of the cache lifetime.
        leases.append(contentsOf: cell.leases)
        leases.append(DatabaseQueryPayloadLease(retaining: claim))
        let retained = DatabaseContext.ProjectedQueryValue(value: cell.value, leases: leases, payloadIsRetained: true)
        state.withLock { state in
            state.values[index] = retained
            state.generations[index] = state.generation
        }
        return retained
    }

    private static var mismatch: DatabaseExpressionEvaluationError {
        .typeMismatch(operation: "window aggregate scheduled input slot")
    }
}
