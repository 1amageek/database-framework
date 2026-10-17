import DatabaseKit

/// Iterative working-table evaluation; self consumers retain only one frontier.
actor SQLRecursiveRowProducer: SQLRelationProducer {
    nonisolated let schema: CanonicalRelationSchema
    private let gate = SQLRelationSerialGate()
    private let meter: DatabaseWorkMeter
    private let userColumnCount: Int
    private let environment: SQLTemporalEnvironment?
    private let limits: QueryValueLimits
    private let reservation: DatabaseIntermediateReservation
    private var generated: SQLRecursiveGeneratedColumns?
    private var seedFactory: (@Sendable () async throws -> any SQLRelationProducer)?
    private var stepFactory: (@Sendable (SQLRecursiveFrontier) async throws -> any SQLRelationProducer)?
    private var child: (any SQLRelationProducer)?
    private var frontier: DatabaseRetainedArrayBuilder<CanonicalRelation>?
    private var visited: CanonicalSQLIdentitySet?
    private var seed = true
    private var closed = false
    private var failure: (any Error)?

    init(schema: CanonicalRelationSchema, userColumnCount: Int, unionAll: Bool,
        generated: SQLRecursiveGeneratedColumns?, temporalEnvironment: SQLTemporalEnvironment?,
        limits: QueryValueLimits, meter: DatabaseWorkMeter,
        seedFactory: @escaping @Sendable () async throws -> any SQLRelationProducer,
        stepFactory: @escaping @Sendable (SQLRecursiveFrontier) async throws -> any SQLRelationProducer) throws {
        self.schema = schema; self.userColumnCount = userColumnCount
        self.generated = generated; self.environment = temporalEnvironment
        self.limits = limits; self.meter = meter
        self.seedFactory = seedFactory; self.stepFactory = stepFactory
        self.reservation = try meter.reserveIntermediate(bytes: 512, at: .bindingCandidate)
        self.frontier = try DatabaseRetainedArrayBuilder(workMeter: meter, stage: .bindingCandidate,
            layout: DatabaseRetainedArrayLayout.forElement(CanonicalRelation.self))
        self.visited = unionAll ? nil : try CanonicalSQLIdentitySet(workMeter: meter, stage: .deduplication)
    }

    func next() async throws -> CanonicalRelation? {
        try await gate.acquire(meter: meter)
        do {
            await Task.yield()
            try meter.checkpoint(at: .bindingCandidate)
            if let failure { throw failure }
            let result = try await advance()
            await Task.yield()
            try meter.checkpoint(at: .bindingCandidate)
            await gate.release()
            return result
        } catch {
            let primary = error
            do { try await finish() }
            catch {
                let combined = SQLRelationCleanupError(operationError: primary, cleanupError: error)
                failure = combined
                await gate.release(); throw combined
            }
            failure = primary
            await gate.release(); throw primary
        }
    }

    private func advance() async throws -> CanonicalRelation? {
        guard !closed else { return nil }
        if child == nil {
            guard seed, let factory = seedFactory else {
                throw CanonicalReadError.unsupportedSelectQuery("A recursive seed has no producer")
            }
            try meter.consume(at: .bindingCandidate)
            child = try await factory(); seedFactory = nil
        }
        while let child {
            try meter.consume(at: .bindingCandidate)
            if let input = try await child.next() {
                guard input.rows.workMeter === meter else { throw DatabaseIntermediateReservationError.workMeterMismatch }
                let row: CanonicalRelation
                if let generated {
                    row = try generated.project(input, seed: seed, userColumnCount: userColumnCount,
                        schema: schema, environment: environment, meter: meter, limits: limits)
                } else {
                    guard input.rows.count == 1, input.rows[0].values.count == userColumnCount else {
                        throw CanonicalReadError.unsupportedSelectQuery("A recursive child changed its selected arity")
                    }
                    row = try sqlSingleRow(CanonicalSourceRow(values: input.rows[0].values, layout: schema.layout,
                        payloadLeases: input.rows[0].payloadLeases), schema: schema,
                        retaining: input.rows.payloadLease, meter: meter)
                }
                guard try accept(row.rows[0]) else { continue }
                if try generated?.canExpand(row.rows[0], userColumnCount: userColumnCount,
                    environment: environment, meter: meter) ?? true {
                    guard frontier != nil else {
                        throw CanonicalReadError.unsupportedSelectQuery("A recursive frontier has closed")
                    }
                    try frontier?.append(footprint: DatabaseIntermediateFootprint(rows: 1)) { row }
                }
                return row
            }
            self.child = nil
            await Task.yield()
            try await child.close()
            await Task.yield()
            guard let builder = frontier.take() else {
                throw CanonicalReadError.unsupportedSelectQuery("A recursive frontier is missing")
            }
            let rows = try builder.finish().moveToSharedOwnership(at: .bindingCandidate)
            guard !rows.isEmpty else { try await finish(); return nil }
            try meter.consume(at: .bindingCandidate)
            frontier = try DatabaseRetainedArrayBuilder(workMeter: meter, stage: .bindingCandidate,
                layout: DatabaseRetainedArrayLayout.forElement(CanonicalRelation.self))
            guard let factory = stepFactory else {
                throw CanonicalReadError.unsupportedSelectQuery("A recursive step has no producer")
            }
            seed = false
            self.child = try await factory(SQLRecursiveFrontier(schema: schema, rows: rows, meter: meter))
        }
        throw CanonicalReadError.unsupportedSelectQuery("A recursive iteration lost its producer")
    }

    private func accept(_ row: CanonicalSourceRow) throws -> Bool {
        guard visited != nil else { return true }
        var keys = try SQLArrayBuffer<CanonicalSQLValueIdentity>(count: row.values.count, meter: meter)
        for slot in row.values.indices {
            guard let type = schema.slotTypes[slot] else {
                throw CanonicalReadError.unsupportedSelectQuery("A recursive output has no selected type")
            }
            try keys.append(CanonicalSQLValueIdentity.prepare(row.values[slot], type: type,
                environment: environment, workMeter: meter, stage: .deduplication))
        }
        let owned = keys.finish()
        return try withExtendedLifetime(owned) { try visited?.insert(owned.values) ?? true }
    }

    func close() async throws {
        await gate.acquire()
        do { try await finish(); await gate.release() }
        catch { await gate.release(); throw error }
    }

    private func finish() async throws {
        guard !closed else { return }
        closed = true
        seedFactory = nil; stepFactory = nil
        let child = child; self.child = nil
        await Task.yield()
        do { try await child?.close() }
        catch {
            frontier = nil; visited = nil; generated = nil; reservation.release()
            await Task.yield(); throw error
        }
        frontier = nil; visited = nil; generated = nil; reservation.release()
        await Task.yield()
    }
}
