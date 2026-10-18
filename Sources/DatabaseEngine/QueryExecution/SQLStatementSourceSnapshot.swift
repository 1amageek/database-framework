import DatabaseKit
import Synchronization

/// An exact source occurrence's authorized frame; installation is prewrite only.
final class SQLStatementSourceSnapshot: Sendable {
    let table: TableRef
    let query: SelectQuery
    private let meter: DatabaseWorkMeter
    private let scopeIdentity: ObjectIdentifier
    private let reservation: DatabaseIntermediateReservation
    private let value = Mutex<CanonicalRelation?>(nil)

    init(table: TableRef, query: SelectQuery, scopeIdentity: ObjectIdentifier, meter: DatabaseWorkMeter) throws {
        self.table = table; self.query = query; self.meter = meter
        self.scopeIdentity = scopeIdentity
        reservation = try meter.reserveIntermediate(bytes: 256, at: .bindingCandidate)
    }
    var isCaptured: Bool { value.withLock { $0 != nil } }
    func matches(_ table: TableRef, query: SelectQuery) -> Bool {
        CanonicalSourceName.equal(self.table.schema, table.schema)
            && CanonicalSourceName.equal(self.table.table, table.table)
            && CanonicalSourceName.equal(self.table.alias, table.alias)
            && self.table.partitions == table.partitions && self.query == query
    }
    func install(_ relation: CanonicalRelation) throws {
        guard relation.rows.workMeter === meter else { throw DatabaseIntermediateReservationError.workMeterMismatch }
        try value.withLock { value in
            guard value == nil else { throw SQLStatementReadError.uncapturedSource }
            value = relation
        }
    }
    func relation(scope: SQLStatementReadScope) throws -> CanonicalRelation {
        try scope.validate()
        guard ObjectIdentifier(scope) == scopeIdentity else { throw DatabaseTransactionError.invalidOperationContext }
        guard scope.execution.workMeter === meter else { throw DatabaseIntermediateReservationError.workMeterMismatch }
        return try value.withLock { value in
            guard let value else { throw SQLStatementReadError.uncapturedSource }
            return value
        }
    }
}
