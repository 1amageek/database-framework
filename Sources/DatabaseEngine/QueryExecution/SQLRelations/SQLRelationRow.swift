import DatabaseKit

/// One produced row, retaining either a checked original position or its own claim.
struct SQLRelationRow: Sendable, RandomAccessCollection {
    typealias Element = CanonicalSourceRow
    typealias Index = Int
    private enum Storage: Sendable {
        case retained(CanonicalRetainedRows, Int)
        case produced(CanonicalSourceRow, DatabaseQueryPayloadLease)
    }
    let schema: CanonicalRelationSchema
    private let storage: Storage

    init(retaining rows: CanonicalRetainedRows, at index: Int, schema: CanonicalRelationSchema) throws {
        guard rows.indices.contains(index) else {
            throw CanonicalReadError.unsupportedSelectQuery("A retained SQL row position is outside its owner")
        }
        try Self.validate(rows[index], meter: rows.workMeter)
        self.schema = schema
        storage = .retained(rows, index)
    }

    init(schema: CanonicalRelationSchema, footprint: DatabaseIntermediateFootprint,
         meter: DatabaseWorkMeter, stage: DatabaseWorkStage,
         make: () throws -> CanonicalSourceRow) throws {
        let leaseBytes = try DatabaseIntermediateCollectionMeter.arrayFootprint(count: 1,
            element: DatabaseIntermediateReservation.self).bytes
        let claim = try meter.reserveIntermediate(rows: footprint.rows,
            bytes: footprint.adding(.init(bytes: leaseBytes)).bytes, at: stage)
        let row = try make()
        try Self.validate(row, meter: meter)
        self.schema = schema
        storage = .produced(row, DatabaseQueryPayloadLease(retaining: claim))
    }

    static func producing(_ row: CanonicalSourceRow, schema: CanonicalRelationSchema,
                          retaining lease: DatabaseQueryPayloadLease? = nil,
                          meter: DatabaseWorkMeter, stage: DatabaseWorkStage = .bindingCandidate) throws -> Self {
        try Self(schema: schema, footprint: CanonicalRelationalFootprintMeter.sourceAliasFootprint(
            of: row, workMeter: meter, stage: stage), meter: meter, stage: stage) {
            lease.map { row.retainingPayload($0) } ?? row
        }
    }

    private static func validate(_ row: CanonicalSourceRow, meter: DatabaseWorkMeter) throws {
        guard row.layout.reservation.map({ $0.workMeter === meter }) ?? true,
              row.layout.payloadLeases.allSatisfy({ $0.workMeter === meter }),
              row.payloadLeases.allSatisfy({ $0.workMeter === meter }) else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
    }

    var startIndex: Int { 0 }
    var endIndex: Int { 1 }
    func index(after index: Int) -> Int { index + 1 }
    func index(before index: Int) -> Int { index - 1 }
    subscript(position: Int) -> CanonicalSourceRow {
        precondition(position == 0)
        return value
    }

    var value: CanonicalSourceRow {
        switch storage {
        case .retained(let owner, let index): return owner[index]
        case .produced(let row, _): return row
        }
    }
    var payloadLease: DatabaseQueryPayloadLease {
        switch storage {
        case .retained(let owner, _): return owner.payloadLease
        case .produced(_, let lease): return lease
        }
    }
    var workMeter: DatabaseWorkMeter { payloadLease.workMeter }
}
