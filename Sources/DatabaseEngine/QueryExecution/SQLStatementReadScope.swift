import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization

/// One statement's admitted physical frames and private lexical relations.
final class SQLStatementReadScope: Sendable {
    let context: DatabaseContext
    let session: DatabaseReadSession
    let execution: ReadExecutionContext
    private let resolution: SQLReadResolution
    let parameters: [QueryParameter]
    var searchPath: SQLSearchPath { resolution.searchPath }
    var temporalEnvironment: SQLTemporalEnvironment? { execution.temporalEnvironment }
    private let reservation: DatabaseIntermediateReservation
    private struct MutationClaim: Sendable {
        let key: ResolvedEntityReference.Key
        let owner: UInt64
        let reservation: DatabaseIntermediateReservation
    }
    private struct State: Sendable {
        var sealed = false
        var closed = false
        @AdmittedArray var snapshots: [SQLStatementSourceSnapshot] = []
        @AdmittedArray var relations: [SQLStatementRelation] = []
        @AdmittedArray var claims: [DatabaseIntermediateReservation] = []
        var mutationOwner: UInt64 = 0
        @AdmittedArray var mutationTargets: [MutationClaim] = []
    }
    private let state = Mutex(State())

    init(context: DatabaseContext, session: DatabaseReadSession, execution: ReadExecutionContext,
         parameters: [QueryParameter] = []) throws {
        self.context = context; self.session = session; self.execution = execution; self.parameters = parameters
        guard let resolution = execution.sqlResolution else { throw DatabaseTransactionError.invalidOperationContext }
        self.resolution = resolution
        reservation = try execution.workMeter.reserveIntermediate(bytes: 512, at: .bindingCandidate)
    }

    func validate() throws {
        guard !state.withLock({ $0.closed }) else { throw DatabaseTransactionError.invalidOperationContext }
        guard let binding = ActiveDatabaseTransactionContext.binding else {
            throw DatabaseTransactionError.invalidOperationContext
        }
        try binding.validate(for: context)
        guard session.transaction.storageAccess.matches(binding.transaction) else {
            throw DatabaseTransactionError.invalidOperationContext
        }
        try resolution.validate(context.readPolicy())
    }

    func physicalSource(_ table: TableRef, query: SelectQuery, sqlResolution: SQLReadResolution? = nil) throws -> SQLStatementSourceSnapshot {
        try validate()
        return try state.withLock { state in
            if let existing = state.snapshots.first(where: { $0.matches(table, query: query, sqlResolution: sqlResolution) }) { return existing }
            guard !state.sealed else { throw SQLStatementReadError.uncapturedSource }
            let value = try SQLStatementSourceSnapshot(table: table, query: query,
                scopeIdentity: ObjectIdentifier(self), meter: execution.workMeter, sqlResolution: sqlResolution)
            try CanonicalBindingStorage.append(value, to: &state.$snapshots, reservation: reservation)
            return value
        }
    }

    func relation(named name: String) -> SQLStatementRelation? {
        state.withLock { $0.relations.last(where: { CanonicalSourceName.equal($0.name, name) }) }
    }

    var relationColumns: [(name: String, columns: [QueryColumn]?)] {
        state.withLock { $0.relations.map { ($0.name, $0.columns) } }
    }

    func register(name: String, columns: [QueryColumn]?, aliases: [String]? = nil) throws -> SQLStatementRelation {
        try validate()
        let relation = try SQLStatementRelation(name: name, columns: columns, aliases: aliases,
            scopeIdentity: ObjectIdentifier(self), meter: execution.workMeter)
        try state.withLock { state in
            guard !state.sealed, !state.relations.contains(where: { CanonicalSourceName.equal($0.name, name) }) else {
                throw SQLStatementReadError.invalidDeclaration(name)
            }
            try CanonicalBindingStorage.append(relation, to: &state.$relations, reservation: reservation)
        }
        return relation
    }

    func capture(_ query: SelectQuery, outerLayout: CanonicalSourceLayout? = nil) async throws {
        try validate()
        guard !state.withLock({ $0.sealed }) else { throw SQLStatementReadError.uncapturedSource }
        _ = try context.bindStatementReadQuery(query, scope: self, outerLayout: outerLayout)
        var index = 0
        while let source = state.withLock({ index < $0.snapshots.count ? $0.snapshots[index] : nil }) {
            index += 1
            if !source.isCaptured {
                let relation = try await context.captureStatementReadSource(source, scope: self)
                try source.install(relation)
            }
        }
    }

    func capture(_ expression: CanonicalBoundExpression, outerLayout: CanonicalSourceLayout? = nil) async throws {
        // Even literal-only preparation must not accumulate synchronous WASM continuations.
        await Task.yield()
        try execution.workMeter.checkpoint(at: .expressionEvaluation)
        do {
            var pending = try SQLArrayBuffer<CanonicalBoundExpression>(meter: execution.workMeter)
            try pending.append(expression)
            var index = 0
            while index < pending.values.count {
                let value = pending.values[index]
                index += 1
                let query: SelectQuery?
                switch value.expression {
                case .subquery(let body), .arraySubquery(let body), .exists(let body), .inSubquery(_, let body): query = body
                case .quantifiedComparison(_, _, _, .subquery(let body)): query = body
                default: query = nil
                }
                if let query { try await capture(query, outerLayout: outerLayout) }
                for child in value.children { try pending.append(child) }
            }
            await Task.yield()
            try execution.workMeter.checkpoint(at: .expressionEvaluation)
        } catch {
            await Task.yield()
            throw error
        }
    }

    func seal() throws {
        try validate()
        try state.withLock { state in
            guard state.snapshots.allSatisfy({ $0.isCaptured }) else { throw SQLStatementReadError.uncapturedSource }
            state.sealed = true
        }
    }

    func retain(_ claim: DatabaseIntermediateReservation) throws {
        guard claim.workMeter === execution.workMeter else { throw DatabaseIntermediateReservationError.workMeterMismatch }
        try state.withLock { state in
            try CanonicalBindingStorage.append(claim, to: &state.$claims, reservation: reservation)
        }
    }

    func allocateMutationOwner() throws -> UInt64 {
        try validate()
        return try state.withLock { state in
            guard !state.sealed else { throw SQLStatementReadError.uncapturedSource }
            let next = state.mutationOwner.addingReportingOverflow(1)
            guard !next.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
            state.mutationOwner = next.partialValue
            return next.partialValue
        }
    }

    func claimMutationTarget(_ identity: EntityReference, owner: UInt64) throws {
        try validate()
        let resolved = try ResolvedEntityReference.resolve(identity, container: context.container)
        var bytes = try DatabaseIntermediateFootprint(bytes: UInt64(resolved.id.packedByteCount))
            .adding(DatabaseIntermediateFootprint(bytes: UInt64(identity.entity.utf8.count) + 128)).bytes
        for component in resolved.partitionPath {
            bytes = try DatabaseIntermediateFootprint(bytes: bytes)
                .adding(DatabaseIntermediateFootprint(bytes: UInt64(component.utf8.count) + 64)).bytes
        }
        let claim = try execution.workMeter.reserveIntermediate(bytes: bytes, at: .mutationPlanning)
        let key = ResolvedEntityReference.Key(entity: identity.entity, id: resolved.id.pack(), partitionPath: resolved.partitionPath)
        try state.withLock { state in
            guard state.sealed, owner > 0, owner <= state.mutationOwner else {
                throw SQLStatementReadError.uncapturedSource
            }
            for existing in state.mutationTargets {
                try execution.workMeter.consume(at: .mutationPlanning)
                if CanonicalSourceName.equal(existing.key.entity, key.entity), existing.key.id == key.id,
                   CanonicalSourceName.equal(existing.key.partitionPath, key.partitionPath) {
                    guard existing.owner == owner else { throw SQLStatementReadError.repeatedTarget(identity.entity) }
                    return
                }
            }
            try CanonicalBindingStorage.append(MutationClaim(key: key, owner: owner, reservation: claim),
                to: &state.$mutationTargets, reservation: reservation)
        }
    }

    func select(_ query: SelectQuery, outer: CanonicalSourceRow? = nil) async throws -> SQLBlockCollectedRows {
        try validate()
        return try await context.selectStatementReadQuery(query, scope: self, outer: outer)
    }

    func relationLayout(_ source: DataSource, outerLayout: CanonicalSourceLayout? = nil) throws -> CanonicalSourceLayout {
        try context.statementSourceLayout(source, scope: self, outerLayout: outerLayout)
    }

    func relation(_ source: DataSource, readQuery: SelectQuery, outer: CanonicalSourceRow? = nil) async throws -> CanonicalRelation {
        try validate()
        return try await context.statementSourceRelation(source, readQuery: readQuery, scope: self, outer: outer)
    }

    func bindExpression(_ expression: Expression, layout: CanonicalSourceLayout) throws -> CanonicalBoundExpression {
        try context.bindStatementExpression(expression, layout: layout, scope: self)
    }

    func bindPredicate(_ expression: Expression, layout: CanonicalSourceLayout) throws -> CanonicalBoundExpression {
        try context.bindStatementExpression(expression, layout: layout, scope: self, predicate: true)
    }

    func bindAssignment(_ expression: Expression, to type: QueryColumnType,
                        layout: CanonicalSourceLayout) throws -> CanonicalBoundExpression {
        try context.bindStatementExpression(expression, layout: layout, scope: self, destination: type)
    }

    func bindProjection(_ items: [ProjectionItem], layout: CanonicalSourceLayout,
        allowingMergeAction: Bool = false) throws -> CanonicalBoundSelect {
        try ActiveSQLStatementReads.$mergeReturning.withValue(allowingMergeAction) {
            try context.bindStatementProjection(items, layout: layout, scope: self)
        }
    }

    func evaluate(_ bound: CanonicalBoundExpression, on row: CanonicalSourceRow) async throws -> DatabaseContext.ProjectedQueryValue {
        try validate()
        return try await context.evaluateStatementExpression(bound, row: row, scope: self)
    }

    func project(_ plan: CanonicalBoundSelect, rows: CanonicalRetainedRows, affectedRows: Int?) async throws -> SQLBlockCollectedRows {
        try validate()
        return try await context.projectStatementRows(plan, rows: rows, affectedRows: affectedRows, scope: self)
    }

    func close() async throws {
        let relations = state.withLock { state -> [SQLStatementRelation] in
            let result = state.relations
            state.closed = true; state.sealed = true
            state.relations.removeAll(); state.snapshots.removeAll(); state.claims.removeAll(); state.mutationTargets.removeAll()
            return result
        }
        var failure: (any Error)?
        for relation in relations { do { try await relation.close() } catch { if failure == nil { failure = error } } }
        if let failure { throw failure }
    }
}

enum ActiveSQLStatementReads {
    @TaskLocal static var scope: SQLStatementReadScope?
    @TaskLocal static var metadataOnly = false
    @TaskLocal static var mergeReturning = false
}

enum SQLStatementReadError: Error, Sendable, Equatable {
    case uncapturedSource
    case unreadableRelation(String)
    case invalidDeclaration(String)
    case repeatedTarget(String)
}
