#if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
import DatabaseEngine
import DatabaseKit
import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire
import StorageKit
import Synchronization

/// Plans and executes only those relational Composition reads whose global
/// semantics can be proven from Base-local partial results.
struct DatabaseCompositionQueryPlanner {
    private struct OriginRow: Sendable {
        let row: DatabaseEngine.QueryRow
        var origin: CompositionOrigin
        let sequence: UInt64
        let fingerprint: ByteString
    }

    private struct MemberCursor: Sendable {
        let member: DatabaseBaseLease
        var continuation: QueryContinuation?
        var rows: [DatabaseEngine.QueryRow]
        var nextRowIndex: Int
        var reachedEnd: Bool
        var reservation: DatabaseIntermediateReservation?

        init(member: DatabaseBaseLease) {
            self.member = member
            self.continuation = nil
            self.rows = []
            self.nextRowIndex = 0
            self.reachedEnd = false
            self.reservation = nil
        }

        mutating func releaseConsumedPage() {
            guard nextRowIndex >= rows.count else { return }
            rows.removeAll(keepingCapacity: false)
            nextRowIndex = 0
            reservation?.release()
            reservation = nil
        }

        mutating func replacePage(
            _ newRows: consuming [DatabaseEngine.QueryRow],
            reservation newReservation: DatabaseIntermediateReservation?
        ) {
            precondition(rows.isEmpty && reservation == nil)
            rows = newRows
            nextRowIndex = 0
            reservation = newReservation
        }
    }

    private enum AggregateKind: Sendable {
        case countAll
        case countValue
        case sum
        case average
        case minimum
        case maximum

        var isMinimum: Bool {
            if case .minimum = self { return true }
            return false
        }
    }

    private enum MergeOrdering: Sendable {
        case query([SortKey])
        case vectorDistance
    }

    private struct AggregateDescriptor: Sendable {
        let outputName: String
        let operandName: String?
        let kind: AggregateKind
    }

    private enum AggregateState: Sendable {
        case count(Int64)
        case numeric(DatabaseNumericAggregateAccumulator)
        case extremum(FieldValue?)
    }

    private struct MergeHeap {
        struct Entry: Sendable {
            let memberIndex: Int
            let row: OriginRow
            let reservation: DatabaseIntermediateReservation
        }

        private(set) var rows: [Entry] = []

        mutating func insert(
            memberIndex: Int,
            row: OriginRow,
            footprint: UInt64,
            workMeter: DatabaseWorkMeter,
            orderedBefore: (OriginRow, OriginRow) throws -> Bool
        ) throws {
            let reservation = try workMeter.reserveIntermediate(
                rows: 1,
                bytes: footprint,
                at: .resultMaterialization
            )
            rows.append(
                Entry(
                    memberIndex: memberIndex,
                    row: row,
                    reservation: reservation
                )
            )
            var child = rows.count - 1
            while child > 0 {
                let parent = (child - 1) / 2
                guard try orderedBefore(rows[child].row, rows[parent].row)
                else { break }
                rows.swapAt(child, parent)
                child = parent
            }
        }

        mutating func removeFirst(
            orderedBefore: (OriginRow, OriginRow) throws -> Bool
        ) throws -> Entry? {
            guard !rows.isEmpty else { return nil }
            if rows.count == 1 { return rows.removeLast() }
            let first = rows[0]
            rows[0] = rows.removeLast()
            var parent = 0
            while true {
                let left = parent * 2 + 1
                guard left < rows.count else { break }
                let right = left + 1
                var child = left
                if right < rows.count,
                   try orderedBefore(rows[right].row, rows[left].row) {
                    child = right
                }
                guard try orderedBefore(rows[child].row, rows[parent].row)
                else { break }
                rows.swapAt(parent, child)
                parent = child
            }
            return first
        }
    }

    private struct EmissionWindow {
        var remainingOffset: Int
        var remainingLimit: Int?

        var isExhausted: Bool {
            remainingLimit == 0
        }

        mutating func acceptsNextRow() -> Bool {
            if remainingOffset > 0 {
                remainingOffset -= 1
                return false
            }
            guard let remainingLimit else { return true }
            guard remainingLimit > 0 else { return false }
            self.remainingLimit = remainingLimit - 1
            return true
        }
    }

    private let structuralLimits: QueryStructuralLimits

    init(structuralLimits: QueryStructuralLimits) {
        self.structuralLimits = structuralLimits
    }

    func execute(
        _ query: SelectQuery,
        request: QueryExecuteOperation.Request,
        context: DatabaseOperationContext,
        workMeter: DatabaseWorkMeter,
        queryFingerprint: ByteString,
        snapshotStore: DatabaseQuerySnapshotStore?
    ) async throws -> QueryRowPage {
        guard request.page.continuation == nil else {
            throw DatabaseQueryExecutionError.invalidContinuation
        }
        guard request.graphPartitions.isEmpty
                || Self.isBaseLocalSPARQLSource(query.source) else {
            throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                "graph partitions require a Base-local SPARQL source"
            )
        }
        try validate(query)
        let source = try context.requireCompositionExecutor()

        return try await source.withReadSnapshot { snapshot in
            guard let pageLimit = Int(exactly: request.page.limit) else {
                throw DatabaseRuntimeConfigurationError
                    .unsupportedOnCurrentPlatform(
                        limit: .maximumRows,
                        actual: UInt64(request.page.limit),
                        maximum: UInt64(Int.max)
                    )
            }
            let builder = DatabaseCompositionQueryResultBuilder(
                compositionID: snapshot.lease.record.composition.id,
                compositionGeneration: snapshot.lease.record.generation,
                schemaGeneration: context.executor.schemaGeneration,
                baseIDs: snapshot.lease.record.composition.bases,
                consistency: .federated(try await snapshot.readPoints()),
                pageLimit: pageLimit,
                queryFingerprint: queryFingerprint,
                authorization: context.authorization,
                maximumIntermediateBytes: request.budget
                    .maximumIntermediateBytes,
                snapshotStore: snapshotStore,
                workMeter: workMeter
            )
            do {
                var window = try Self.emissionWindow(query)
                if !window.isExhausted,
                   let aggregates = try Self.aggregateProjection(
                    query.projection
                   ) {
                    let rows = try await executeAggregates(
                        query,
                        aggregates: aggregates,
                        request: request,
                        source: source,
                        context: context,
                        snapshot: snapshot,
                        workMeter: workMeter
                    )
                    for row in rows where window.acceptsNextRow() {
                        try await builder.append(
                            row.row,
                            origin: row.origin,
                            footprint: try rowFootprint(row.row)
                        )
                    }
                } else if !window.isExhausted,
                          query.distinct
                            || Self.isDistinctProjection(query.projection) {
                    guard let snapshotStore else {
                        throw DatabaseQueryExecutionError
                            .querySnapshotUnavailable(
                                "DISTINCT requires durable Composition workspace storage"
                            )
                    }
                    let reservation = try await snapshotStore.beginWrite(
                        compositionID: snapshot.lease.record.composition.id,
                        compositionGeneration: snapshot.lease.record.generation,
                        schemaGeneration: context.executor.schemaGeneration,
                        queryFingerprint: queryFingerprint,
                        authorization: context.authorization
                    )
                    try await builder.adoptWorkspace(reservation)
                    let distinct = DatabaseCompositionDistinctSpill(
                        snapshotStore: snapshotStore,
                        reservation: reservation,
                        maximumIntermediateBytes: request.budget
                            .maximumIntermediateBytes,
                        workMeter: workMeter
                    )
                    var distinctSequence: UInt64 = 0
                    try await executeRows(
                        query,
                        request: request,
                        source: source,
                        context: context,
                        snapshot: snapshot,
                        workMeter: workMeter
                    ) { row in
                        try await distinct.insert(
                            row.row,
                            origin: row.origin,
                            sequence: distinctSequence
                        )
                        let next = distinctSequence.addingReportingOverflow(1)
                        guard !next.overflow else {
                            throw DatabaseQueryExecutionError
                                .querySnapshotCorrupted
                        }
                        distinctSequence = next.partialValue
                        return true
                    }
                    try await builder.accountWorkspacePayloadBytes(
                        await distinct.payloadByteCount
                    )
                    let distinctWindow = Mutex(window)
                    try await distinct.forEachResult(batchSize: pageLimit) {
                        result in
                        if distinctWindow.withLock({
                            $0.acceptsNextRow()
                        }) {
                            try await builder.append(
                                result.row,
                                origin: result.origin,
                                footprint: try rowFootprint(result.row)
                            )
                        }
                        return distinctWindow.withLock { !$0.isExhausted }
                    }
                } else if !window.isExhausted {
                    try await executeRows(
                        query,
                        request: request,
                        source: source,
                        context: context,
                        snapshot: snapshot,
                        workMeter: workMeter
                    ) { row in
                        if window.acceptsNextRow() {
                            try await builder.append(
                                row.row,
                                origin: row.origin,
                                footprint: try rowFootprint(row.row)
                            )
                        }
                        return !window.isExhausted
                    }
                }
                return try await builder.finish()
            } catch {
                try await builder.abort()
                throw error
            }
        }
    }

    private func validate(_ query: SelectQuery) throws {
        let vectorScan = try Self.vectorIndexScan(query)
        if query.accessPath != nil, vectorScan == nil {
            throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                "this index access requires a kind-specific federated planner"
            )
        }
        guard query.subqueries == nil else {
            throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                "subqueries cannot be proven Base-local"
            )
        }
        guard query.groupBy == nil, query.having == nil else {
            throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                "grouped aggregation is not yet a decomposable Composition plan"
            )
        }
        guard query.reduced == false else {
            throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                "SPARQL REDUCED semantics are not advertised for Composition targets"
            )
        }
        let isSPARQL = Self.isBaseLocalSPARQLSource(query.source)
        if !isSPARQL, query.dataset != .implicit {
            throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                "SPARQL dataset selection requires a Base-local SPARQL source"
            )
        }
        guard vectorScan != nil
                ? Self.isVectorSource(query.source)
                : Self.isBaseLocalRelationalSource(query.source) || isSPARQL
        else {
            throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                "the source cannot be proven to execute entirely inside each Base"
            )
        }
        let aggregateProjection = try Self.aggregateProjection(
            query.projection
        )
        if vectorScan != nil {
            guard aggregateProjection == nil,
                  !query.distinct,
                  !Self.isDistinctProjection(query.projection),
                  query.orderBy == nil || query.orderBy?.isEmpty == true,
                  query.offset == nil || query.offset == 0 else {
                throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                    "vector Composition search does not accept aggregate, DISTINCT, ORDER BY, or OFFSET"
                )
            }
        }
        if let orderBy = query.orderBy {
            for sortKey in orderBy {
                switch sortKey.expression {
                case .column, .variable:
                    break
                default:
                    throw DatabaseQueryExecutionError
                        .compositionPlanUnsupported(
                            "global ordering requires an output column or variable"
                        )
                }
            }
        }
    }

    private func executeRows(
        _ query: SelectQuery,
        request: QueryExecuteOperation.Request,
        source: CompositionReadExecutor,
        context: DatabaseOperationContext,
        snapshot: DatabaseCompositionReadSnapshot,
        workMeter: DatabaseWorkMeter,
        emit: (OriginRow) async throws -> Bool
    ) async throws {
        let localQuery = SelectQuery(
            projection: Self.nonDistinctProjection(query.projection),
            source: query.source,
            accessPath: query.accessPath,
            filter: query.filter,
            groupBy: nil,
            having: nil,
            orderBy: query.orderBy,
            limit: nil,
            offset: nil,
            distinct: false,
            subqueries: nil,
            reduced: false,
            dataset: query.dataset
        )
        let memberCount = max(1, snapshot.lease.members.count)
        let mergeOrdering: MergeOrdering?
        if try Self.vectorIndexScan(query) != nil {
            mergeOrdering = .vectorDistance
        } else if let orderBy = query.orderBy, !orderBy.isEmpty {
            mergeOrdering = .query(orderBy)
        } else {
            mergeOrdering = nil
        }
        guard let maximumBufferedRows = Int(
            exactly: request.budget.maximumIntermediateRows
        ),
              let requestedPageLimit = Int(exactly: request.page.limit) else {
            throw DatabaseRuntimeConfigurationError
                .unsupportedOnCurrentPlatform(
                    limit: .maximumIntermediateRows,
                    actual: UInt64(request.budget.maximumIntermediateRows),
                    maximum: UInt64(Int.max)
                )
        }
        let retainedOutputRows = min(requestedPageLimit, maximumBufferedRows)
        // A Base-local page can temporarily retain its input and projected
        // output. Ordered merge also retains one cursor and one heap head for
        // every other member. Allocate one row of lookahead per local page and
        // leave the durable output page inside the same request-wide budget.
        let concurrentPipelineUnits = mergeOrdering == nil
            ? 2
            : memberCount + 1
        let availablePipelineRows = maximumBufferedRows - retainedOutputRows
        let maximumLocalPageSize = availablePipelineRows
            / concurrentPipelineUnits - 1
        guard maximumLocalPageSize >= 1 else {
            let pipelineMinimum = concurrentPipelineUnits
                .multipliedReportingOverflow(by: 2)
            let minimumRows = retainedOutputRows.addingReportingOverflow(
                pipelineMinimum.overflow
                    ? Int.max
                    : pipelineMinimum.partialValue
            )
            throw DatabaseWorkLimitError.maximumIntermediateRows(
                stage: .resultMaterialization,
                consumed: 0,
                requested: UInt64(
                    minimumRows.overflow ? Int.max : minimumRows.partialValue
                ),
                maximum: UInt64(request.budget.maximumIntermediateRows)
            )
        }
        let requestedRowsPerMember: Int
        if mergeOrdering == nil {
            requestedRowsPerMember = requestedPageLimit
        } else {
            requestedRowsPerMember = requestedPageLimit / memberCount
                + (requestedPageLimit % memberCount == 0 ? 0 : 1)
        }
        let localPageSize = max(
            1,
            min(
                requestedRowsPerMember,
                maximumLocalPageSize
            )
        )
        var cursors = snapshot.lease.members.map(MemberCursor.init(member:))
        var sequence: UInt64 = 0

        func prepared(
            _ row: DatabaseEngine.QueryRow,
            member: DatabaseBaseLease
        ) throws -> OriginRow {
            let qualifiedRow = Self.isBaseLocalSPARQLSource(query.source)
                ? try DatabaseCompositionRDFIdentity.qualifyBlankNodes(
                    in: row,
                    baseID: member.baseID
                )
                : row
            return OriginRow(
                row: qualifiedRow,
                origin: .source(member.baseID),
                sequence: sequence,
                fingerprint: try CanonicalRowFingerprint.compute(
                    qualifiedRow,
                    workMeter: workMeter
                )
            )
        }

        func advanceSequence() throws {
            let next = sequence.addingReportingOverflow(1)
            guard !next.overflow else {
                    throw DatabaseWorkLimitError.maximumIntermediateRows(
                        stage: .resultMaterialization,
                        consumed: sequence,
                    requested: UInt64.max,
                    maximum: UInt64(request.budget.maximumIntermediateRows)
                )
            }
            sequence = next.partialValue
        }

        if let mergeOrdering {
            var heap = MergeHeap()
            for index in cursors.indices {
                if let row = try await nextRow(
                    cursor: &cursors[index],
                    query: localQuery,
                    pageSize: localPageSize,
                    request: request,
                    source: source,
                    context: context,
                    snapshot: snapshot,
                    workMeter: workMeter
                ) {
                    let originRow = try prepared(
                        row,
                        member: cursors[index].member
                    )
                    try heap.insert(
                        memberIndex: index,
                        row: originRow,
                        footprint: try rowFootprint(originRow.row),
                        workMeter: workMeter
                    ) { lhs, rhs in
                        try compare(
                            lhs,
                            rhs,
                            ordering: mergeOrdering,
                            workMeter: workMeter
                        )
                    }
                    try advanceSequence()
                }
            }
            while !heap.rows.isEmpty {
                let memberIndex: Int
                do {
                    guard let head = try heap.removeFirst(orderedBefore: {
                        lhs,
                        rhs in
                        try compare(
                            lhs,
                            rhs,
                            ordering: mergeOrdering,
                            workMeter: workMeter
                        )
                    }) else {
                        throw DatabaseQueryExecutionError
                            .querySnapshotCorrupted
                    }
                    memberIndex = head.memberIndex
                    guard try await emit(head.row) else { return }
                    // `head` and its reservation end together at this scope.
                }
                if let row = try await nextRow(
                    cursor: &cursors[memberIndex],
                    query: localQuery,
                    pageSize: localPageSize,
                    request: request,
                    source: source,
                    context: context,
                    snapshot: snapshot,
                    workMeter: workMeter
                ) {
                    let originRow = try prepared(
                        row,
                        member: cursors[memberIndex].member
                    )
                    try heap.insert(
                        memberIndex: memberIndex,
                        row: originRow,
                        footprint: try rowFootprint(originRow.row),
                        workMeter: workMeter
                    ) { lhs, rhs in
                            try compare(
                                lhs,
                                rhs,
                                ordering: mergeOrdering,
                                workMeter: workMeter
                        )
                    }
                    try advanceSequence()
                }
            }
        } else {
            for index in cursors.indices {
                while let row = try await nextRow(
                    cursor: &cursors[index],
                    query: localQuery,
                    pageSize: localPageSize,
                    request: request,
                    source: source,
                    context: context,
                    snapshot: snapshot,
                    workMeter: workMeter
                ) {
                    guard try await emit(
                        prepared(row, member: cursors[index].member)
                    ) else { return }
                    try advanceSequence()
                }
            }
        }
    }

    private func nextRow(
        cursor: inout MemberCursor,
        query: SelectQuery,
        pageSize: Int,
        request: QueryExecuteOperation.Request,
        source: CompositionReadExecutor,
        context: DatabaseOperationContext,
        snapshot: DatabaseCompositionReadSnapshot,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseEngine.QueryRow? {
        while cursor.nextRowIndex >= cursor.rows.count {
            cursor.releaseConsumedPage()
            guard !cursor.reachedEnd else { return nil }
            let previousContinuation = cursor.continuation
            let transaction = try snapshot.transaction(for: cursor.member)
            let response = try await source.withMemberContext(
                cursor.member,
                in: snapshot
            ) { databaseContext in
                try await databaseContext.query(
                    query,
                    execution: try localExecution(
                        request: request,
                        continuation: previousContinuation,
                        pageSize: pageSize,
                        workMeter: workMeter,
                        context: context
                    ),
                    graphPartitions: request.graphPartitions,
                    transaction: transaction
                )
            }
            guard response.continuation == nil
                    || response.continuation != previousContinuation else {
                throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                    "a Base-local cursor did not make progress"
                )
            }
            var retainedBytes: UInt64 = 0
            for row in response.rows {
                retainedBytes = try Self.adding(
                    retainedBytes,
                    rowFootprint(row)
                )
            }
            let reservation = response.rows.isEmpty
                ? nil
                : try workMeter.reserveIntermediate(
                    rows: UInt64(response.rows.count),
                    bytes: retainedBytes,
                    at: .resultMaterialization
                )
            cursor.replacePage(
                response.rows,
                reservation: reservation
            )
            cursor.continuation = response.continuation
            cursor.reachedEnd = response.continuation == nil
        }
        let row = cursor.rows[cursor.nextRowIndex]
        cursor.nextRowIndex += 1
        return row
    }

    private func executeAggregates(
        _ query: SelectQuery,
        aggregates: (
            descriptors: [AggregateDescriptor],
            localProjection: Projection
        ),
        request: QueryExecuteOperation.Request,
        source: CompositionReadExecutor,
        context: DatabaseOperationContext,
        snapshot: DatabaseCompositionReadSnapshot,
        workMeter: DatabaseWorkMeter
    ) async throws -> [OriginRow] {
        let localQuery = SelectQuery(
            projection: aggregates.localProjection,
            source: query.source,
            accessPath: nil,
            filter: query.filter,
            groupBy: nil,
            having: nil,
            orderBy: nil,
            limit: nil,
            offset: nil,
            distinct: false,
            subqueries: nil,
            reduced: false,
            dataset: .implicit
        )
        var states = aggregates.descriptors.map { descriptor in
            switch descriptor.kind {
            case .countAll, .countValue:
                return AggregateState.count(0)
            case .sum, .average:
                return AggregateState.numeric(
                    DatabaseNumericAggregateAccumulator()
                )
            case .minimum, .maximum:
                return AggregateState.extremum(nil)
            }
        }
        guard let pageSize = Int(
            exactly: request.budget.maximumIntermediateRows
        ), pageSize > 0 else {
            throw DatabaseRuntimeConfigurationError
                .unsupportedOnCurrentPlatform(
                    limit: .maximumIntermediateRows,
                    actual: UInt64(request.budget.maximumIntermediateRows),
                    maximum: UInt64(Int.max)
                )
        }
        for member in snapshot.lease.members {
            var cursor = MemberCursor(member: member)
            while let row = try await nextRow(
                cursor: &cursor,
                query: localQuery,
                pageSize: pageSize,
                request: request,
                source: source,
                context: context,
                snapshot: snapshot,
                workMeter: workMeter
            ) {
                try workMeter.consume(at: .aggregateInput)
                for index in aggregates.descriptors.indices {
                    let descriptor = aggregates.descriptors[index]
                    let value: FieldValue
                    if let operandName = descriptor.operandName {
                        guard let operand = row.fields[operandName] else {
                            throw DatabaseQueryExecutionError
                                .compositionAggregateFailure(
                                    "Base-local aggregate operand is missing"
                                )
                        }
                        value = operand
                    } else {
                        value = .bool(true)
                    }
                    try Self.accumulate(
                        value,
                        descriptor: descriptor,
                        state: &states[index]
                    )
                }
            }
        }

        var fields: [String: FieldValue] = [:]
        fields.reserveCapacity(aggregates.descriptors.count)
        for index in aggregates.descriptors.indices {
            fields[aggregates.descriptors[index].outputName] = try Self.result(
                descriptor: aggregates.descriptors[index],
                state: states[index]
            )
        }
        let row = DatabaseEngine.QueryRow(fields: fields)
        return [
            OriginRow(
                row: row,
                origin: .derived(
                    contributors: snapshot.lease.record.composition.bases
                ),
                sequence: 0,
                fingerprint: try CanonicalRowFingerprint.compute(
                    row,
                    workMeter: workMeter
                )
            ),
        ]
    }

    private static func accumulate(
        _ value: FieldValue,
        descriptor: AggregateDescriptor,
        state: inout AggregateState
    ) throws {
        switch (descriptor.kind, state) {
        case (.countAll, .count(let count)):
            state = .count(try incrementAggregateCount(count))
        case (.countValue, .count(let count)):
            state = value == .null
                ? .count(count)
                : .count(try incrementAggregateCount(count))
        case (.sum, .numeric(var accumulator)),
             (.average, .numeric(var accumulator)):
            do {
                try accumulator.add(value)
            } catch let failure {
                throw DatabaseQueryExecutionError
                    .compositionAggregateFailure(
                        aggregateFailureMessage(failure)
                    )
            }
            state = .numeric(accumulator)
        case (.minimum, .extremum(let current)),
             (.maximum, .extremum(let current)):
            guard value != .null else { return }
            guard let current else {
                state = .extremum(value)
                return
            }
            guard let comparison = value.compare(to: current) else {
                throw DatabaseQueryExecutionError
                    .compositionAggregateFailure(
                        "MIN/MAX values are not mutually comparable"
                    )
            }
            if descriptor.kind.isMinimum
                ? comparison == .lessThan
                : comparison == .greaterThan {
                state = .extremum(value)
            }
        default:
            throw DatabaseQueryExecutionError.compositionAggregateFailure(
                "aggregate state does not match its plan"
            )
        }
    }

    private static func result(
        descriptor: AggregateDescriptor,
        state: AggregateState
    ) throws -> FieldValue {
        switch (descriptor.kind, state) {
        case (.countAll, .count(let value)),
             (.countValue, .count(let value)):
            return .int64(value)
        case (.sum, .numeric(let accumulator)):
            do { return try accumulator.sum() ?? .null }
            catch let failure {
                throw DatabaseQueryExecutionError
                    .compositionAggregateFailure(
                        aggregateFailureMessage(failure)
                    )
            }
        case (.average, .numeric(let accumulator)):
            do { return try accumulator.average() ?? .null }
            catch let failure {
                throw DatabaseQueryExecutionError
                    .compositionAggregateFailure(
                        aggregateFailureMessage(failure)
                    )
            }
        case (.minimum, .extremum(let value)),
             (.maximum, .extremum(let value)):
            return value ?? .null
        default:
            throw DatabaseQueryExecutionError.compositionAggregateFailure(
                "aggregate state does not match its plan"
            )
        }
    }

    private static func incrementAggregateCount(
        _ value: Int64
    ) throws -> Int64 {
        let result = value.addingReportingOverflow(1)
        guard !result.overflow else {
            throw DatabaseQueryExecutionError.compositionAggregateFailure(
                "COUNT exceeds Int64"
            )
        }
        return result.partialValue
    }

    private static func aggregateFailureMessage(
        _ failure: DatabaseNumericAggregateAccumulator.Failure
    ) -> String {
        switch failure {
        case .incompatibleNumericKinds:
            return "Aggregate values use incompatible numeric kinds"
        case .nonNumericValue:
            return "Aggregate operand is not numeric"
        case .nonFiniteValue:
            return "Aggregate operand is not finite"
        case .numericOverflow:
            return "Aggregate numeric result overflowed"
        case .resultNotRepresentable:
            return "Aggregate result cannot be represented"
        }
    }

    private func localExecution(
        request: QueryExecuteOperation.Request,
        continuation: QueryContinuation? = nil,
        pageSize: Int? = nil,
        workMeter: DatabaseWorkMeter,
        context: DatabaseOperationContext
    ) throws -> ReadExecutionContext {
        guard let maximumPageSize = Int(
            exactly: request.budget.maximumIntermediateRows
        ) else {
            throw DatabaseRuntimeConfigurationError
                .unsupportedOnCurrentPlatform(
                    limit: .maximumIntermediateRows,
                    actual: UInt64(request.budget.maximumIntermediateRows),
                    maximum: UInt64(Int.max)
                )
        }
        return ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: pageSize ?? maximumPageSize,
                continuation: continuation,
                budget: request.budget,
                continuationSnapshotIsStable: true
            ),
            monotonicClock: context.executor.monotonicClock,
            workMeter: workMeter,
            queryStructuralLimits: structuralLimits
        )
    }

    private func compare(
        _ lhs: OriginRow,
        _ rhs: OriginRow,
        ordering: MergeOrdering,
        workMeter: DatabaseWorkMeter
    ) throws -> Bool {
        switch ordering {
        case .query(let orderBy):
            return try compare(
                lhs,
                rhs,
                orderBy: orderBy,
                workMeter: workMeter
            )
        case .vectorDistance:
            return try compareVectorDistance(
                lhs,
                rhs,
                workMeter: workMeter
            )
        }
    }

    private func compare(
        _ lhs: OriginRow,
        _ rhs: OriginRow,
        orderBy: [SortKey],
        workMeter: DatabaseWorkMeter
    ) throws -> Bool {
        for key in orderBy {
            try workMeter.consume(2, at: .sortComparison)
            let name = try Self.outputName(for: key.expression)
            guard let left = lhs.row.fields[name],
                  let right = rhs.row.fields[name] else {
                throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                    "ORDER BY '\(name)' is not present in the projected result"
                )
            }
            if left == .null || right == .null {
                if left == .null, right == .null { continue }
                let nullsFirst: Bool
                switch key.nulls {
                case .first:
                    nullsFirst = true
                case .last:
                    nullsFirst = false
                case nil:
                    nullsFirst = key.direction == .ascending
                }
                return left == .null ? nullsFirst : !nullsFirst
            }
            guard let comparison = left.compare(to: right) else {
                throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                    "ORDER BY values are not mutually comparable"
                )
            }
            guard comparison != .equal else { continue }
            return key.direction == .ascending
                ? comparison == .lessThan
                : comparison == .greaterThan
        }
        return tieBreaksBefore(lhs, rhs)
    }

    private func compareVectorDistance(
        _ lhs: OriginRow,
        _ rhs: OriginRow,
        workMeter: DatabaseWorkMeter
    ) throws -> Bool {
        try workMeter.consume(2, at: .sortComparison)
        guard case .float64(let left)? = lhs.row.annotations["distance"],
              case .float64(let right)? = rhs.row.annotations["distance"],
              left.isFinite,
              right.isFinite else {
            throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                "vector Composition members must return a finite distance annotation"
            )
        }
        if left != right { return left < right }
        return tieBreaksBefore(lhs, rhs)
    }

    private func tieBreaksBefore(
        _ lhs: OriginRow,
        _ rhs: OriginRow
    ) -> Bool {
        if lhs.fingerprint != rhs.fingerprint {
            return lhs.fingerprint.lexicographicallyPrecedes(rhs.fingerprint)
        }
        let leftBase = Self.firstContributor(lhs.origin)
        let rightBase = Self.firstContributor(rhs.origin)
        if leftBase != rightBase { return leftBase < rightBase }
        return lhs.sequence < rhs.sequence
    }

    private func rowFootprint(
        _ row: DatabaseEngine.QueryRow
    ) throws -> UInt64 {
        var bytes: UInt64 = 128
        for (key, value) in row.fields.sorted(by: { $0.key < $1.key }) {
            bytes = try Self.adding(bytes, UInt64(key.utf8.count))
            bytes = try Self.adding(
                bytes,
                UInt64(
                    try FieldValueTupleCodec.prepareComposite(
                        value,
                        limits: .default
                    ).encodedByteCount
                )
            )
        }
        for (key, value) in row.annotations.sorted(by: { $0.key < $1.key }) {
            bytes = try Self.adding(bytes, UInt64(key.utf8.count))
            bytes = try Self.adding(
                bytes,
                UInt64(
                    try FieldValueTupleCodec.prepareComposite(
                        value,
                        limits: .default
                    ).encodedByteCount
                )
            )
        }
        if let version = row.version {
            bytes = try Self.adding(bytes, UInt64(version.value.utf8.count))
        }
        return bytes
    }

    private static func adding(_ lhs: UInt64, _ rhs: UInt64) throws -> UInt64 {
        let result = lhs.addingReportingOverflow(rhs)
        guard !result.overflow else {
            throw DatabaseIntermediateFootprintError.byteAdditionOverflow(
                left: lhs,
                right: rhs
            )
        }
        return result.partialValue
    }

    private static func aggregateProjection(
        _ projection: Projection
    ) throws -> (
        descriptors: [AggregateDescriptor],
        localProjection: Projection
    )? {
        let items: [ProjectionItem]
        switch projection {
        case .items(let value), .distinctItems(let value):
            items = value
        case .all, .allFrom:
            return nil
        }
        let containsAggregate = items.contains {
            if case .aggregate = $0.expression { return true }
            return false
        }
        guard containsAggregate else { return nil }
        guard items.allSatisfy({ item in
            if case .aggregate = item.expression { return true }
            return false
        }) else {
            throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                "aggregate and non-aggregate projection items cannot be mixed without GROUP BY"
            )
        }
        var descriptors: [AggregateDescriptor] = []
        descriptors.reserveCapacity(items.count)
        var operands: [ProjectionItem] = []
        operands.reserveCapacity(items.count)
        var outputNames = Set<String>()
        for (index, item) in items.enumerated() {
            guard case .aggregate(let aggregate) = item.expression else {
                throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                    "aggregate projection is malformed"
                )
            }
            let operandName = "__composition_aggregate_operand_\(index)"
            let kind: AggregateKind
            let expression: Expression?
            let defaultName: String
            switch aggregate {
            case .count(let value, distinct: false):
                kind = value == nil ? .countAll : .countValue
                expression = value
                defaultName = "count"
            case .sum(let value, distinct: false):
                kind = .sum
                expression = value
                defaultName = "sum"
            case .avg(let value, distinct: false):
                kind = .average
                expression = value
                defaultName = "avg"
            case .min(let value):
                kind = .minimum
                expression = value
                defaultName = "min"
            case .max(let value):
                kind = .maximum
                expression = value
                defaultName = "max"
            case .count(_, distinct: true), .sum(_, distinct: true),
                 .avg(_, distinct: true), .groupConcat, .sample, .arrayAgg:
                throw DatabaseQueryExecutionError
                    .compositionPlanUnsupported(
                        "this aggregate is not decomposable across Base boundaries"
                    )
            }
            let outputName = item.alias ?? defaultName
            guard outputNames.insert(outputName).inserted else {
                throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                    "aggregate output names must be unique"
                )
            }
            if let expression {
                operands.append(
                    ProjectionItem(expression, alias: operandName)
                )
            }
            descriptors.append(
                AggregateDescriptor(
                    outputName: outputName,
                    operandName: expression == nil ? nil : operandName,
                    kind: kind
                )
            )
        }
        if operands.isEmpty {
            operands.append(
                ProjectionItem(
                    .literal(.bool(true)),
                    alias: "__composition_aggregate_marker"
                )
            )
        }
        return (descriptors, .items(operands))
    }

    private static func isBaseLocalRelationalSource(
        _ source: DataSource
    ) -> Bool {
        switch source {
        case .table:
            return true
        case .logical:
            return false
        case .join(let join):
            return isBaseLocalRelationalSource(join.left)
                && isBaseLocalRelationalSource(join.right)
        case .subquery, .values, .graphTable, .graphPattern, .namedGraph,
             .service, .union, .unionAll, .intersect, .except:
            return false
        }
    }

    private static func isBaseLocalSPARQLSource(
        _ source: DataSource
    ) -> Bool {
        switch source {
        case .graphPattern, .namedGraph:
            return true
        case .table, .logical, .join, .subquery, .values, .graphTable,
             .service, .union, .unionAll, .intersect, .except:
            return false
        }
    }

    private static func isVectorSource(_ source: DataSource) -> Bool {
        switch source {
        case .table:
            return true
        case .logical(let source):
            return source.kindIdentifier == LogicalSourceKind.polymorphic
        case .join, .subquery, .values, .graphTable, .graphPattern,
             .namedGraph, .service, .union, .unionAll, .intersect, .except:
            return false
        }
    }

    /// Validates the complete score-comparability contract used by the
    /// federated vector merge. Every member executes the same immutable schema
    /// generation and the same canonical access-path parameters; the merger
    /// therefore compares only distances produced under one metric contract.
    private static func vectorIndexScan(
        _ query: SelectQuery
    ) throws -> IndexScanSource? {
        guard let accessPath = query.accessPath else { return nil }
        guard case .index(let scan) = accessPath,
              scan.kindIdentifier == "vector" else {
            return nil
        }
        guard !scan.indexName.isEmpty,
              case .int64(let dimensions)? = scan.parameters["dimensions"],
              dimensions > 0,
              let dimensionCount = Int(exactly: dimensions),
              case .vector(let queryVector)? =
                scan.parameters["queryVector"],
              queryVector.elementType == .float32,
              queryVector.count == dimensionCount,
              case .string(let metric)? = scan.parameters["metric"],
              !metric.isEmpty,
              case .int64(let k)? = scan.parameters["k"],
              k > 0,
              let resultLimit = UInt64(exactly: k),
              query.limit == resultLimit else {
            throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                "vector Composition search requires one valid vector index contract and LIMIT equal to k"
            )
        }
        return scan
    }

    private static func nonDistinctProjection(
        _ projection: Projection
    ) -> Projection {
        if case .distinctItems(let items) = projection {
            return .items(items)
        }
        return projection
    }

    private static func isDistinctProjection(_ projection: Projection) -> Bool {
        if case .distinctItems = projection { return true }
        return false
    }

    private static func outputName(for expression: Expression) throws -> String {
        switch expression {
        case .column(let column):
            return column.column
        case .variable(let variable):
            return variable.name
        default:
            throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                "global ordering requires an output column or variable"
            )
        }
    }

    private static func firstContributor(
        _ origin: CompositionOrigin
    ) -> Base.ID {
        switch origin {
        case .source(let baseID):
            return baseID
        case .derived(let contributors):
            return contributors[0]
        }
    }

    private static func runtimeCount(
        _ value: UInt64?,
        name: String
    ) throws -> Int? {
        guard let value else { return nil }
        guard let count = Int(exactly: value) else {
            throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                "\(name) exceeds the current runtime range"
            )
        }
        return count
    }

    private static func emissionWindow(
        _ query: SelectQuery
    ) throws -> EmissionWindow {
        EmissionWindow(
            remainingOffset: try runtimeCount(
                query.offset,
                name: "OFFSET"
            ) ?? 0,
            remainingLimit: try runtimeCount(
                query.limit,
                name: "LIMIT"
            )
        )
    }
}

#endif
