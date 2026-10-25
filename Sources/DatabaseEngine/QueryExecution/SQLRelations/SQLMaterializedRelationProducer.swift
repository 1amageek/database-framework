import DatabaseKit

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

    func next() async throws -> SQLRelationRow? {
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
            let row: SQLRelationRow?
            if let relation, index < relation.rows.count {
                row = try SQLRelationRow(retaining: relation.rows, at: index, schema: schema)
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
