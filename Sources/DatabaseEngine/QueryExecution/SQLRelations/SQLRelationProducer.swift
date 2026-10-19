import DatabaseKit

/// One retained positional row per advance, on one admitted read operation.
protocol SQLRelationProducer: Sendable {
    var schema: CanonicalRelationSchema { get }
    func next() async throws -> CanonicalRelation?
    func close() async throws
}

@_spi(DatabaseExecution)
public struct SQLRelationCleanupError: Error, Sendable {
    public let operationError: any Error
    public let cleanupError: any Error
}

/// Serializes suspension across actor reentrancy without holding a memory lock.
actor SQLRelationSerialGate {
    private final class Waiter {
        let continuation: CheckedContinuation<Void, Never>
        let reservation: DatabaseIntermediateReservation?
        var next: Waiter?
        init(_ continuation: CheckedContinuation<Void, Never>, reservation: DatabaseIntermediateReservation?) {
            self.continuation = continuation; self.reservation = reservation
        }
    }
    private var held = false
    private var first: Waiter?
    private var last: Waiter?

    func acquire(meter: DatabaseWorkMeter) async throws {
        if !held { held = true; return }
        let reservation = try meter.reserveIntermediate(bytes: 256, at: .bindingCandidate)
        await enqueue(reservation: reservation)
    }

    // Owner cleanup is already admitted with the producer and must also run after cancellation.
    func acquire() async {
        if !held { held = true; return }
        await enqueue(reservation: nil)
    }

    private func enqueue(reservation: DatabaseIntermediateReservation?) async {
        await withCheckedContinuation { continuation in
            let waiter = Waiter(continuation, reservation: reservation)
            if let last { last.next = waiter } else { first = waiter }
            last = waiter
        }
    }

    func release() {
        guard let waiter = first else { held = false; return }
        first = waiter.next
        if first == nil { last = nil }
        waiter.next = nil
        waiter.reservation?.release()
        waiter.continuation.resume()
    }
}

/// Indexed pulls compose existing operator closures without materializing outputs.
actor SQLRelationPullProducer: SQLRelationProducer {
    nonisolated let schema: CanonicalRelationSchema
    private let gate = SQLRelationSerialGate()
    private let meter: DatabaseWorkMeter
    private var operation: (@Sendable (Int) async throws -> CanonicalRelation?)?
    private var cleanup: (@Sendable () async throws -> Void)?
    private var position = 0
    private var terminal = false
    private var failure: (any Error)?

    init(schema: CanonicalRelationSchema, meter: DatabaseWorkMeter,
         operation: @escaping @Sendable (Int) async throws -> CanonicalRelation?,
         cleanup: @escaping @Sendable () async throws -> Void = {}) {
        self.schema = schema
        self.meter = meter
        self.operation = operation
        self.cleanup = cleanup
    }

    func next() async throws -> CanonicalRelation? {
        try await gate.acquire(meter: meter)
        do {
            // The WASM runtime resumes fast-path continuations synchronously; each owned pull bounds that native stack.
            await Task.yield()
            try meter.checkpoint(at: .bindingCandidate)
            if let failure { throw failure }
            guard !terminal, let operation else { await gate.release(); return nil }
            let row = try await operation(position)
            if let row {
                guard row.rows.count == 1, row.schema == schema, row.rows.workMeter === meter else {
                    throw CanonicalReadError.unsupportedSelectQuery("A SQL producer returned an invalid row frame")
                }
                let next = position.addingReportingOverflow(1)
                guard !next.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
                position = next.partialValue
            } else { try await finish() }
            await Task.yield()
            try meter.checkpoint(at: .bindingCandidate)
            await gate.release()
            return row
        } catch {
            let operationError = error
            failure = operationError
            do { try await finish() }
            catch {
                await gate.release()
                throw SQLRelationCleanupError(operationError: operationError, cleanupError: error)
            }
            await gate.release()
            throw operationError
        }
    }

    func close() async throws {
        await gate.acquire()
        do { try await finish(); await gate.release() }
        catch { await gate.release(); throw error }
    }

    private func finish() async throws {
        guard !terminal else { return }
        terminal = true
        operation = nil
        let cleanup = cleanup
        self.cleanup = nil
        await Task.yield()
        do { try await cleanup?() }
        catch { await Task.yield(); throw error }
        await Task.yield()
    }
}
