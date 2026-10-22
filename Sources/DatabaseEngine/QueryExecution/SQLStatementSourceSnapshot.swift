import DatabaseKit
import Synchronization

/// An exact source occurrence's authorized frame; installation is prewrite only.
final class SQLStatementSourceSnapshot: Sendable {
    let table: TableRef
    let query: SelectQuery
    let sqlResolution: SQLReadResolution?
    let declarations: SQLStatementReadDeclarations
    private let meter: DatabaseWorkMeter
    private let scopeIdentity: ObjectIdentifier
    private let reservation: DatabaseIntermediateReservation
    private let unqualifiedSourceNames: [String]
    private let value = Mutex<CanonicalRelation?>(nil)

    init(table: TableRef, query: SelectQuery, scopeIdentity: ObjectIdentifier, meter: DatabaseWorkMeter, declarations: SQLStatementReadDeclarations, sqlResolution: SQLReadResolution? = nil) throws {
        self.table = table; self.query = query; self.meter = meter
        self.scopeIdentity = scopeIdentity
        self.sqlResolution = sqlResolution
        reservation = try meter.reserveIntermediate(bytes: 256, at: .bindingCandidate)
        self.declarations = declarations
        unqualifiedSourceNames = try Self.sourceNames(in: query, meter: meter, reservation: reservation)
    }
    var isCaptured: Bool { value.withLock { $0 != nil } }
    func matches(_ table: TableRef, query: SelectQuery,
        sqlResolution: SQLReadResolution? = nil) -> Bool {
        return self.sqlResolution?.viewBindings == sqlResolution?.viewBindings
            && CanonicalSourceName.equal(self.table.schema, table.schema)
            && CanonicalSourceName.equal(self.table.table, table.table)
            && CanonicalSourceName.equal(self.table.alias, table.alias)
            && self.table.partitions == table.partitions && self.query == query
    }
    func refersToUnqualifiedSource(_ name: String) throws -> Bool {
        for source in unqualifiedSourceNames {
            try meter.consume(at: .bindingCandidate)
            if CanonicalSourceName.equal(source, name) { return true }
        }
        return false
    }

    private static func sourceNames(in query: SelectQuery, meter: DatabaseWorkMeter,
        reservation: DatabaseIntermediateReservation) throws -> [String] {
        @AdmittedArray var names: [String] = []
        // Retain existing query strings; the walker excludes only query-local WITH bindings.
        try FusionSelectQueryGraphWalker.forEachQuery(in: query, workMeter: meter, { _ in },
            visitTable: { _, _ in }, visitPolymorphic: { _, _ in }, visitExternalTable: { table, _ in
                if table.schema == nil {
                    try CanonicalBindingStorage.append(table.table, to: &$names, reservation: reservation)
                }
            })
        return names
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
