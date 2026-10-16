import DatabaseKit

struct SQLResolvedPagination: Sendable {
    let offset: UInt64
    let limit: UInt64?
    let withTies: Bool
}

func sqlSingleRow(_ row: CanonicalSourceRow, schema: CanonicalRelationSchema,
                  retaining lease: DatabaseQueryPayloadLease? = nil,
                  meter: DatabaseWorkMeter) throws -> CanonicalRelation {
    var builder = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(workMeter: meter,
        stage: .bindingCandidate, layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self), expectedCount: 1)
    try builder.append(footprint: CanonicalRelationalFootprintMeter.sourceAliasFootprint(of: row,
        workMeter: meter, stage: .bindingCandidate)) {
        lease.map { row.retainingPayload($0) } ?? row
    }
    return CanonicalRelation(schema: schema, rows: try builder.finish().moveToSharedOwnership(at: .bindingCandidate))
}

/// A streaming SELECT keeps filters before offset and output projection after it.
actor SQLSelectRowProducer: SQLRelationProducer {
    nonisolated let schema: CanonicalRelationSchema
    private let gate = SQLRelationSerialGate()
    private let meter: DatabaseWorkMeter
    private var source: (any SQLRelationProducer)?
    private var predicate: (@Sendable (CanonicalSourceRow) async throws -> Bool)?
    private var projection: (@Sendable (CanonicalSourceRow) async throws -> CanonicalRelation)?
    private var caches: [SQLCTECache]
    private var offset: UInt64
    private let limit: UInt64?
    private var emitted: UInt64 = 0
    private var terminal = false
    private var failure: (any Error)?

    init(schema: CanonicalRelationSchema, meter: DatabaseWorkMeter, source: any SQLRelationProducer,
         pagination: SQLResolvedPagination, caches: [SQLCTECache],
         predicate: @escaping @Sendable (CanonicalSourceRow) async throws -> Bool,
         projection: @escaping @Sendable (CanonicalSourceRow) async throws -> CanonicalRelation) {
        self.schema = schema; self.meter = meter; self.source = source
        self.offset = pagination.offset; self.limit = pagination.limit
        self.caches = caches; self.predicate = predicate; self.projection = projection
    }

    func next() async throws -> CanonicalRelation? {
        try await gate.acquire(meter: meter)
        do {
            try meter.checkpoint(at: .bindingCandidate)
            if let failure { throw failure }
            guard !terminal, let source, let predicate, let projection,
                  limit.map({ emitted < $0 }) ?? true else {
                try await finish(); await gate.release(); return nil
            }
            while let next = try await source.next() {
                try meter.checkpoint(at: .bindingCandidate)
                let row = next.rows[0].retainingPayload(next.rows.payloadLease)
                guard try await predicate(row) else { continue }
                if offset > 0 { offset -= 1; continue }
                let output = try await projection(row)
                emitted += 1
                await gate.release()
                return output
            }
            try await finish()
            await gate.release()
            return nil
        } catch {
            let operationError = error
            failure = operationError
            do { try await finish() }
            catch { await gate.release(); throw SQLRelationCleanupError(operationError: operationError, cleanupError: error) }
            await gate.release(); throw operationError
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
        let source = source
        self.source = nil; predicate = nil; projection = nil
        let caches = caches; self.caches = []
        await Task.yield()
        var failure: (any Error)?
        do { try await source?.close() } catch { failure = error }
        for cache in caches {
            do { try await cache.close() }
            catch { if failure == nil { failure = error } }
        }
        await Task.yield()
        if let failure { throw failure }
    }
}

/// Blocking physical inputs retain one owner; their SQL targets remain unexecuted.
actor SQLMaterializedRelationProducer: SQLRelationProducer {
    nonisolated let schema: CanonicalRelationSchema
    private let gate = SQLRelationSerialGate()
    private let meter: DatabaseWorkMeter
    private var factory: (@Sendable () async throws -> CanonicalRelation)?
    private var relation: CanonicalRelation?
    private var index = 0
    private var closed = false
    private var failure: (any Error)?

    init(schema: CanonicalRelationSchema, meter: DatabaseWorkMeter,
         factory: @escaping @Sendable () async throws -> CanonicalRelation) {
        self.schema = schema; self.meter = meter; self.factory = factory
    }

    func next() async throws -> CanonicalRelation? {
        try await gate.acquire(meter: meter)
        do {
            // A blocking factory can recursively pull another selected set operand.
            await Task.yield()
            try meter.checkpoint(at: .bindingCandidate)
            if let failure { throw failure }
            guard !closed else { await gate.release(); return nil }
            if relation == nil {
                guard let factory else { throw CanonicalReadError.unsupportedSelectQuery("Missing blocking SQL producer") }
                relation = try await factory(); self.factory = nil
            }
            let row: CanonicalRelation?
            if let relation, index < relation.rows.count {
                row = try sqlSingleRow(relation.rows[index], schema: schema, retaining: relation.rows.payloadLease, meter: meter)
                index += 1
            } else {
                closed = true; relation = nil; factory = nil
                row = nil
            }
            await Task.yield()
            try meter.checkpoint(at: .bindingCandidate)
            await gate.release(); return row
        } catch { failure = error; closed = true; relation = nil; factory = nil; await gate.release(); throw error }
    }

    func close() async throws {
        await gate.acquire()
        closed = true; relation = nil; factory = nil
        await gate.release()
    }
}
