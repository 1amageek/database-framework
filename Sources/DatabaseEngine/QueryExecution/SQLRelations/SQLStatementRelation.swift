import DatabaseKit
import Synchronization

/// A bound private CTE declaration, never an authorization capability.
final class SQLStatementRelation: Sendable {
    let name: String
    let columns: [QueryColumn]?
    private let meter: DatabaseWorkMeter
    private let scopeIdentity: ObjectIdentifier
    private let reservation: DatabaseIntermediateReservation
    private struct State: Sendable {
        var rows: SQLBlockCollectedRows?
        var cache: SQLCTECache?
    }
    private let state = Mutex(State())

    init(name: String, columns: [QueryColumn]?, aliases: [String]?,
        scopeIdentity: ObjectIdentifier, meter: DatabaseWorkMeter) throws {
        self.name = name; self.meter = meter
        self.scopeIdentity = scopeIdentity
        guard columns == nil || aliases == nil || (aliases?.count ?? 0) <= (columns?.count ?? 0) else {
            throw SQLStatementReadError.invalidDeclaration(name)
        }
        reservation = try meter.reserveIntermediate(bytes: 256 + UInt64(name.utf8.count)
            + UInt64(columns?.count ?? 0) * 256, at: .bindingCandidate)
        self.columns = try columns?.enumerated().map { index, column in
            try QueryColumn(number: column.number, name: aliases.flatMap { index < $0.count ? $0[index] : nil } ?? column.name,
                type: column.type, isNullable: column.isNullable)
        }
    }

    func install(_ rows: SQLBlockCollectedRows) throws {
        guard let columns, rows.rows.columns.count == columns.count,
              rows.rows.workMeter === meter else { throw SQLStatementReadError.invalidDeclaration(name) }
        try state.withLock { state in
            guard state.rows == nil, state.cache == nil else { throw SQLStatementReadError.invalidDeclaration(name) }
            state.rows = rows
        }
    }
    func install(cache: SQLCTECache) throws {
        try state.withLock { state in
            guard state.rows == nil, state.cache == nil else { throw SQLStatementReadError.invalidDeclaration(name) }
            state.cache = cache
        }
    }
    func consumer(scope: SQLStatementReadScope) async throws -> any SQLRelationProducer {
        try scope.validate()
        guard ObjectIdentifier(scope) == scopeIdentity else { throw DatabaseTransactionError.invalidOperationContext }
        guard scope.execution.workMeter === meter else { throw DatabaseIntermediateReservationError.workMeterMismatch }
        let selected = state.withLock { $0 }
        if let cache = selected.cache { return cache.consumer() }
        guard let rows = selected.rows, let columns else { throw SQLStatementReadError.unreadableRelation(name) }
        return try scope.context.statementReturnedProducer(rows, columns: columns, meter: meter)
    }
    func close() async throws {
        let cache = state.withLock { state -> SQLCTECache? in
            let cache = state.cache; state.cache = nil; state.rows = nil
            return cache
        }
        try await cache?.close()
    }
}
