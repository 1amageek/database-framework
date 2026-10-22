import DatabaseKit

/// A fixed lexical prefix of one append-only statement registry, valid until scope close.
final class SQLStatementReadDeclarations: Sendable {
    private let scope: SQLStatementReadScope
    private let count: Int
    private let reservation: DatabaseIntermediateReservation

    init(scope: SQLStatementReadScope, count: Int, meter: DatabaseWorkMeter) throws {
        reservation = try meter.reserveIntermediate(bytes: 128, at: .bindingCandidate)
        self.scope = scope
        self.count = count
    }

    func requireScope(_ scope: SQLStatementReadScope) throws {
        guard self.scope === scope else { throw DatabaseTransactionError.invalidOperationContext }
    }

    func samePrefix(as other: SQLStatementReadDeclarations) -> Bool {
        scope === other.scope && count == other.count
    }

    func relation(named name: String) -> SQLStatementRelation? {
        scope.relation(named: name, visibleCount: count)
    }

    var columns: [(name: String, columns: [QueryColumn]?)] {
        scope.relationColumns(visibleCount: count)
    }
}
