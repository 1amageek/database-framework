import DatabaseKit

/// A definition's single demand-driven prefix, shared only within its invocation.
actor SQLCTECache {
    nonisolated let schema: CanonicalRelationSchema
    private let gate = SQLRelationSerialGate()
    private let meter: DatabaseWorkMeter
    private let reservation: DatabaseIntermediateReservation
    private var factory: (@Sendable () async throws -> any SQLRelationProducer)?
    private var producer: (any SQLRelationProducer)?
    private var prefix: [CanonicalRelation] = []
    private var capacity = 0
    private var exhausted = false
    private var closed = false
    private var failure: (any Error)?

    init(schema: CanonicalRelationSchema, meter: DatabaseWorkMeter,
         factory: @escaping @Sendable () async throws -> any SQLRelationProducer) throws {
        self.schema = schema
        self.meter = meter
        self.reservation = try meter.reserveIntermediate(bytes: 256, at: .bindingCandidate)
        self.factory = factory
    }

    nonisolated func consumer() -> any SQLRelationProducer {
        SQLRelationPullProducer(schema: schema, meter: meter, operation: { [self] index in
            try await row(at: index)
        })
    }

    private func row(at index: Int) async throws -> CanonicalRelation? {
        try await gate.acquire(meter: meter)
        do {
            try meter.checkpoint(at: .bindingCandidate)
            if let failure { throw failure }
            guard !closed else { throw CanonicalReadError.unsupportedSelectQuery("A CTE execution has closed") }
            if index < prefix.count { let row = prefix[index]; await gate.release(); return row }
            guard index == prefix.count else { throw CanonicalReadError.unsupportedSelectQuery("A CTE consumer skipped its prefix frontier") }
            guard !exhausted else { await gate.release(); return nil }
            if producer == nil {
                guard let factory else { throw CanonicalReadError.unsupportedSelectQuery("A CTE producer has no factory") }
                producer = try await factory()
                self.factory = nil
            }
            guard let producer else { throw CanonicalReadError.unsupportedSelectQuery("A CTE producer did not initialize") }
            guard let row = try await producer.next() else {
                exhausted = true
                try await producer.close()
                self.producer = nil
                await gate.release()
                return nil
            }
            guard row.schema == schema, row.rows.count == 1, row.rows.workMeter === meter else {
                throw CanonicalReadError.unsupportedSelectQuery("A CTE producer changed its schema")
            }
            let layout = try DatabaseRetainedArrayLayout.forElement(CanonicalRelation.self)
            let growth = try layout.growth(from: capacity, toFit: prefix.count + 1)
            try reservation.reserveAdditional(bytes: growth.additionalByteCount, at: .bindingCandidate)
            if growth.capacity > capacity {
                prefix.reserveCapacity(growth.capacity)
                let actual = prefix.capacity
                if actual > growth.capacity {
                    let extra = try DatabaseIntermediateFootprint(bytes: layout.elementCapacitySlotByteCount)
                        .multiplied(by: UInt64(actual - growth.capacity))
                    try reservation.reserveAdditional(bytes: extra.bytes, at: .bindingCandidate)
                }
                capacity = actual
            }
            prefix.append(row)
            await gate.release()
            return row
        } catch {
            let operationError = error
            failure = operationError
            do { try await finish() }
            catch { await gate.release(); throw SQLRelationCleanupError(operationError: operationError, cleanupError: error) }
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
        guard !closed else { return }
        closed = true
        factory = nil
        let producer = producer
        self.producer = nil
        prefix.removeAll(keepingCapacity: false)
        reservation.release()
        await Task.yield()
        do { try await producer?.close() }
        catch { await Task.yield(); throw error }
        await Task.yield()
    }
}

/// Immutable lexical handles. A definition's factory captures only earlier handles.
struct SQLCTEEnvironment: Sendable {
    struct Entry: Sendable {
        let name: String
        let cache: SQLCTECache?
        let factory: (@Sendable () async throws -> any SQLRelationProducer)?
    }
    let entries: [Entry]
    init(entries: [Entry] = []) { self.entries = entries }
    func consumer(named name: String) async throws -> (any SQLRelationProducer)? {
        guard let entry = entries.first(where: { $0.name == name }) else { return nil }
        if let cache = entry.cache { return cache.consumer() }
        guard let factory = entry.factory else {
            throw CanonicalReadError.unsupportedSelectQuery("An inline CTE has no producer")
        }
        return try await factory()
    }
    func adding(name: String, cache: SQLCTECache) -> Self {
        Self(entries: [Entry(name: name, cache: cache, factory: nil)] + entries.filter { $0.name != name })
    }
    func adding(name: String, factory: @escaping @Sendable () async throws -> any SQLRelationProducer) -> Self {
        Self(entries: [Entry(name: name, cache: nil, factory: factory)] + entries.filter { $0.name != name })
    }
}
