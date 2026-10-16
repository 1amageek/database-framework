/// Prepares a blocking operator once, then pulls its selected targets on demand.
actor SQLDeferredRowProducer: SQLRelationProducer {
    nonisolated let schema: CanonicalRelationSchema
    private let gate = SQLRelationSerialGate()
    private let meter: DatabaseWorkMeter
    private var factory: (@Sendable () async throws -> any SQLRelationProducer)?
    private var producer: (any SQLRelationProducer)?
    private var terminal = false
    private var failure: (any Error)?

    init(schema: CanonicalRelationSchema, meter: DatabaseWorkMeter,
         factory: @escaping @Sendable () async throws -> any SQLRelationProducer) {
        self.schema = schema; self.meter = meter; self.factory = factory
    }

    func next() async throws -> CanonicalRelation? {
        try await gate.acquire(meter: meter)
        do {
            await Task.yield()
            try meter.checkpoint(at: .bindingCandidate)
            if let failure { throw failure }
            guard !terminal else { await gate.release(); return nil }
            if producer == nil {
                guard let factory else { throw CanonicalReadError.unsupportedSelectQuery("Missing selected operator producer") }
                producer = try await factory(); self.factory = nil
            }
            let row = try await producer?.next()
            if row == nil { try await finish() }
            await Task.yield()
            try meter.checkpoint(at: .bindingCandidate)
            await gate.release(); return row
        } catch {
            failure = error
            let primary = error
            do { try await finish() }
            catch { await gate.release(); throw SQLRelationCleanupError(operationError: primary, cleanupError: error) }
            await gate.release(); throw primary
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
        factory = nil
        let producer = producer; self.producer = nil
        await Task.yield()
        do { try await producer?.close() }
        catch { await Task.yield(); throw error }
        await Task.yield()
    }
}
