import DatabaseKit

/// Indexed pulls compose existing operator closures without materializing outputs.
actor SQLRelationPullProducer: SQLRelationProducer {
    nonisolated let schema: CanonicalRelationSchema
    private let gate = SQLRelationSerialGate()
    private let meter: DatabaseWorkMeter
    private var operation: (@Sendable (Int) async throws -> SQLRelationRow?)?
    private var cleanup: (@Sendable () async throws -> Void)?
    private var position = 0
    private var terminal = false
    private var failure: (any Error)?

    init(schema: CanonicalRelationSchema, meter: DatabaseWorkMeter,
         operation: @escaping @Sendable (Int) async throws -> SQLRelationRow?,
         cleanup: @escaping @Sendable () async throws -> Void = {}) {
        self.schema = schema
        self.meter = meter
        self.operation = operation
        self.cleanup = cleanup
    }

    func next() async throws -> SQLRelationRow? {
        try await gate.acquire(meter: meter)
        do {
            // The WASM runtime resumes fast-path continuations synchronously; each owned pull bounds that native stack.
            await Task.yield()
            try meter.checkpoint(at: .bindingCandidate)
            if let failure { throw failure }
            guard !terminal, let operation else { await gate.release(); return nil }
            let row = try await operation(position)
            if let row {
                guard row.schema == schema, row.workMeter === meter else {
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
