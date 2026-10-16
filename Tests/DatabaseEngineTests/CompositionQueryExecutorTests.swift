#if MultiBase
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Composition typed query execution")
struct CompositionQueryExecutorTests {
    @Persistable
    struct Item {
        var id: String = ""
        var rank: Int64 = 0
    }

    private enum DecisionTestError: Error {
        case rollbackRequested
    }

    private final class CountingCompositionClock: WallClock {
        private let state = Mutex(0)

        var readCount: Int { state.withLock { $0 } }

        var now: Timestamp {
            state.withLock { $0 += 1 }
            return Timestamp(secondsSinceUnixEpoch: 2)
        }
    }

    private actor DecisionOperationProbe {
        private var didStart = false

        func markStarted() {
            didStart = true
        }

        func hasStarted() -> Bool {
            didStart
        }
    }

    private actor PublicationEventProbe {
        private var count = 0

        func receive() { count += 1 }
        func receivedCount() -> Int { count }
    }

    private actor CompositionRowProbe {
        private var values: [CompositionQueryRow] = []
        private var columns: [QueryColumn] = []

        func receive(_ event: CompositionQueryEvent) -> Bool {
            if case .began(let metadata) = event {
                columns = metadata.columns
            }
            if case .row(let value) = event {
                values.append(value)
            }
            return true
        }

        func rows() -> [CompositionQueryRow] {
            values
        }

        func result() -> (columns: [QueryColumn], rows: [CompositionQueryRow]) {
            (columns, values)
        }
    }

    @Test("Composition freezes parameter identity before member paging, aggregates and cross-Base JOIN")
    func parametersRetainIdentityAcrossCompositionRoutes() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        let source = fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(fixture.compositionID)
        let parameters = [QueryParameter(position: 1, name: "value", value: .primitive(.null), type: .sql(.bigint))]
        let empty = try await executeParameterizedQuery(
            SelectQuery(
                projection: [
                    ProjectionItem(.parameter(.position(1)), alias: "position"),
                    ProjectionItem(.parameter(.name("value")), alias: "name"),
                ],
                source: .table(TableRef(Item.persistableType))
            ), source: source, parameters: parameters, fixture: fixture
        )
        #expect(empty.rows.isEmpty)
        #expect(empty.columns.map(\.type) == [.sql(.bigint), .sql(.bigint)])
        #expect(empty.columns.allSatisfy { $0.isNullable })

        try await insert([("shared", 1), ("primary", 3)], baseID: fixture.primaryBaseID, fixture: fixture)
        try await insert([("shared", 2), ("secondary", 4)], baseID: fixture.secondaryBaseID, fixture: fixture)
        let raw = [QueryParameter(position: 1, name: "value", value: .primitive(.int64(2)))]
        let filtered = try await executeParameterizedQuery(
            SelectQuery(
                projection: [
                    ProjectionItem(.column(ColumnRef("rank")), alias: "rank"),
                    ProjectionItem(.parameter(.name("value")), alias: "parameter"),
                ],
                source: .table(TableRef(Item.persistableType)),
                filter: .greaterThanOrEqual(.column(ColumnRef("rank")), .parameter(.position(1))),
                orderBy: [SortKey(.column(ColumnRef("rank")), direction: .ascending)]
            ), source: source, parameters: raw, fixture: fixture
        )
        #expect(filtered.rows.map { $0.row.values[0] } == [.primitive(.int64(2)), .primitive(.int64(3)), .primitive(.int64(4))])
        #expect(filtered.rows.allSatisfy { $0.row.values[1] == .primitive(.int64(2)) })
        let aggregate = try await executeParameterizedQuery(
            SelectQuery(
                projection: [ProjectionItem(.aggregate(.sum(.parameter(.name("value")), distinct: false)), alias: "sum")],
                source: .table(TableRef(Item.persistableType)),
                filter: .greaterThanOrEqual(.column(ColumnRef("rank")), .parameter(.position(1)))
            ), source: source, parameters: raw, fixture: fixture
        )
        #expect(aggregate.rows.count == 1)
        guard case .numeric(let sum) = aggregate.rows[0].row.values[0] else {
            Issue.record("Expected numeric SUM output")
            return
        }
        #expect(sum.value == ExactDecimal(coefficient: 6, scale: 0))

        let join = crossBaseQuery(fixture: fixture, joinType: .inner).replacing(
            filter: .lessThan(.column(ColumnRef(table: "world", column: "rank")), .parameter(.position(1)))
        )
        let joined = try await executeParameterizedQuery(join, source: source, parameters: raw, fixture: fixture)
        #expect(joined.rows.count == 1)
        #expect(joined.rows[0].row.values == [.primitive(.int64(1)), .primitive(.int64(2))])
        let orderedJoin = try await executeParameterizedQuery(join.replacing(orderBy: [SortKey(
            .subtract(.literal(.int(0)), .column(ColumnRef(table: "world", column: "rank"))))])
            .replacing(pagination: QueryPagination(limit: .count(.expression(.parameter(.position(1))), withTies: true))),
            source: source, parameters: raw, fixture: fixture)
        #expect(orderedJoin.rows.map { $0.row.values } == joined.rows.map { $0.row.values })
        let skippedAggregate = try await executeParameterizedQuery(
            SelectQuery(projection: [ProjectionItem(.aggregate(.count(nil, distinct: false)), alias: "total")],
                source: .table(TableRef(Item.persistableType)), pagination: QueryPagination(
                    offset: .expression(.parameter(.position(1))))),
            source: source, parameters: raw, fixture: fixture)
        #expect(skippedAggregate.rows.isEmpty)
        let probe = PublicationEventProbe()
        let execution = ReadExecutionContext(options: .default, monotonicClock: fixture.container.monotonicClock)
        await #expect(throws: DatabaseExpressionEvaluationError.typeMismatch(operation: "ambiguous parameter type")) {
            try await CompositionQueryPlanner(structuralLimits: execution.queryStructuralLimits).execute(
                SelectQuery(
                    projection: [
                        ProjectionItem(.parameter(.position(1)), alias: "earlier"),
                        ProjectionItem(.cast(.parameter(.name("value")), targetType: .bigint), alias: "later"),
                    ], source: .table(TableRef(Item.persistableType))
                ), source: source,
                options: CompositionQueryExecutionOptions(pageSize: 1, readContext: execution,
                    parameters: [QueryParameter(position: 1, name: "value", value: .primitive(.null))])
            ) { _ in await probe.receive(); return true }
        }
        #expect(await probe.receivedCount() == 0)
        #expect(execution.workMeter.retainedIntermediateRows == 0)
        #expect(execution.workMeter.retainedIntermediateBytes == 0)
    }

    private func executeParameterizedQuery(
        _ query: SelectQuery,
        source: CompositionDataSource,
        parameters: [QueryParameter],
        fixture: Fixture
    ) async throws -> (columns: [QueryColumn], rows: [CompositionQueryRow]) {
        let execution = ReadExecutionContext(options: .default, monotonicClock: fixture.container.monotonicClock)
        let probe = CompositionRowProbe()
        try await CompositionQueryPlanner(structuralLimits: execution.queryStructuralLimits).execute(
            query, source: source,
            options: CompositionQueryExecutionOptions(pageSize: 1, readContext: execution, parameters: parameters)
        ) { event in await probe.receive(event) }
        return await probe.result()
    }

    @Test("Selected global keys preserve non-output expressions, SQL peers and DISTINCT ON", .timeLimit(.minutes(2)))
    func selectedGlobalKeysAndPeers() async throws {
        let clock = CountingCompositionClock()
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        try await insert([("one", 1), ("three", 3)], baseID: fixture.primaryBaseID, fixture: fixture)
        try await insert([("two", 2), ("four", 4)], baseID: fixture.secondaryBaseID, fixture: fixture)
        let source = fixture.container.session(authorization: fixture.readerAuthorization).composition(fixture.compositionID)
        let rank = DatabaseKit.Expression.column(ColumnRef("rank"))
        let descending = SelectQuery(projection: [ProjectionItem(.column(ColumnRef("id")), alias: "label")],
            source: .table(TableRef(Item.persistableType)),
            orderBy: [SortKey(.subtract(.literal(.int(0)), rank))])
        let ordered = try await executeParameterizedQuery(descending, source: source, parameters: [], fixture: fixture)
        #expect(ordered.columns.map(\.name) == ["label"])
        #expect(ordered.rows.map { $0.row.values[0] } == ["four", "three", "two", "one"].map { .primitive(.string($0)) })
        #expect(ordered.rows.allSatisfy { $0.row.annotations.isEmpty })

        let parity = DatabaseKit.Expression.modulo(rank, .literal(.int(2)))
        let peers = SelectQuery(projection: [ProjectionItem(rank, alias: "position")],
            source: .table(TableRef(Item.persistableType)), orderBy: [SortKey(parity)],
            pagination: QueryPagination(limit: .count(.expression(.parameter(.position(1))), withTies: true)))
        let tied = try await executeParameterizedQuery(peers, source: source,
            parameters: [QueryParameter(position: 1, value: .primitive(.int64(1)), type: .sql(.bigint))], fixture: fixture)
        #expect(tied.rows.count == 2)
        #expect(Set(tied.rows.map { $0.row.values[0] }) == Set([QueryValue.primitive(.int64(2)), .primitive(.int64(4))]))

        let representatives = peers.replacing(quantifier: .distinctOn([parity]))
            .replacing(orderBy: [SortKey(parity), SortKey(rank, direction: .descending)])
            .replacing(pagination: QueryPagination())
        let distinct = try await executeParameterizedQuery(representatives, source: source, parameters: [], fixture: fixture)
        #expect(distinct.rows.map { $0.row.values[0] } == [.primitive(.int64(4)), .primitive(.int64(3))])
        #expect(distinct.rows.map(\.origin) == [.source(fixture.secondaryBaseID), .source(fixture.primaryBaseID)])

        let byOrdinal = peers.replacing(orderBy: [SortKey(.literal(.int(1)), direction: .descending)])
            .replacing(pagination: QueryPagination(limit: .count(.fixed(2), withTies: false),
                offset: .expression(.parameter(.position(1)))))
        let page = try await executeParameterizedQuery(byOrdinal, source: source,
            parameters: [QueryParameter(position: 1, value: .primitive(.int64(1)), type: .sql(.bigint))], fixture: fixture)
        #expect(page.rows.map { $0.row.values[0] } == [.primitive(.int64(3)), .primitive(.int64(2))])

        let count = DatabaseKit.Expression.cast(.function(FunctionCall(name: "date_part", arguments: [
            .literal(.string("second")), .function(FunctionCall(name: "clock_timestamp", arguments: []))
        ])), targetType: .bigint)
        let captured = source.makeSQLTemporalEnvironment()
        // Count SQL evaluation separately from control-metadata transaction timestamps.
        let environment = SQLTemporalEnvironment(transactionInstant: captured.transactionInstant,
            statementInstant: captured.statementInstant, wallClock: clock, configuration: captured.configuration)
        let execution = ReadExecutionContext(options: .default, monotonicClock: fixture.container.monotonicClock,
            temporalEnvironment: environment)
        let probe = CompositionRowProbe()
        let baseline = clock.readCount
        try await CompositionQueryPlanner(structuralLimits: execution.queryStructuralLimits).execute(
            descending.replacing(pagination: QueryPagination(limit: .count(.expression(count), withTies: false))),
            source: source, options: CompositionQueryExecutionOptions(pageSize: 1, readContext: execution)
        ) { event in await probe.receive(event) }
        #expect(clock.readCount - baseline == 1)
        #expect(await probe.rows().count == 2)
    }

    @Test("Selected temporal identity merges lineage and SQL NULL defaults stay global", .timeLimit(.minutes(2)))
    func selectedTemporalIdentityAndNullOrder() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        try await insert([("one", 1)], baseID: fixture.primaryBaseID, fixture: fixture)
        try await insert([("two", 2)], baseID: fixture.secondaryBaseID, fixture: fixture)
        let source = fixture.container.session(authorization: fixture.readerAuthorization).composition(fixture.compositionID)
        let month = try QueryIntervalValue(period: CalendarPeriod(months: 1, days: 0), time: TimeSpan(seconds: 0))
        let days = try QueryIntervalValue(period: CalendarPeriod(months: 0, days: 30), time: TimeSpan(seconds: 0))
        let rank = DatabaseKit.Expression.column(ColumnRef("rank"))
        let condition = DatabaseKit.Expression.equal(rank, .literal(.int(1)))
        let interval = DatabaseKit.Expression.caseWhen(cases: [CaseWhenPair(condition: condition,
            result: .literal(.queryValue(.interval(month), type: .sql(.interval()))))],
            elseResult: .literal(.queryValue(.interval(days), type: .sql(.interval()))))
        let query = SelectQuery(projection: [ProjectionItem(interval, alias: "duration")],
            source: .table(TableRef(Item.persistableType)), orderBy: [SortKey(.column(ColumnRef("duration")))],
            quantifier: .distinct)
        let result = try await executeParameterizedQuery(query, source: source, parameters: [], fixture: fixture)
        try #require(result.rows.count == 1)
        #expect(result.columns.map(\.type) == [.sql(.interval())])
        #expect(result.rows[0].origin == .derived(contributors: [fixture.primaryBaseID, fixture.secondaryBaseID].sorted()))

        let nullable = DatabaseKit.Expression.caseWhen(cases: [CaseWhenPair(condition: condition,
            result: .cast(.literal(.null), targetType: .bigint))], elseResult: rank)
        let nulls = SelectQuery(projection: [ProjectionItem(rank, alias: "position")],
            source: .table(TableRef(Item.persistableType)), orderBy: [SortKey(nullable)])
        let ascending = try await executeParameterizedQuery(nulls, source: source, parameters: [], fixture: fixture)
        #expect(ascending.rows.map { $0.row.values[0] } == [.primitive(.int64(2)), .primitive(.int64(1))])
        let descending = try await executeParameterizedQuery(nulls.replacing(orderBy: [SortKey(nullable, direction: .descending)]),
            source: source, parameters: [], fixture: fixture)
        #expect(descending.rows.map { $0.row.values[0] } == [.primitive(.int64(1)), .primitive(.int64(2))])

        let joinedSource = DataSource.join(JoinClause(type: .left,
            left: .aliased(.table(TableRef(Item.persistableType)), name: "a"),
            right: .aliased(.table(TableRef(Item.persistableType)), name: "b"),
            condition: .on(.and(.equal(.column(ColumnRef(table: "a", column: "id")),
                .column(ColumnRef(table: "b", column: "id"))),
                .equal(.column(ColumnRef(table: "a", column: "rank")), .literal(.int(1)))))))
        let joined = SelectQuery(projection: [ProjectionItem(.wholeRow("b"), alias: "member"),
            ProjectionItem(.column(ColumnRef(table: "a", column: "rank")), alias: "position")],
            source: joinedSource, orderBy: [SortKey(.column(ColumnRef("position")))])
        let presence = try await executeParameterizedQuery(joined, source: source, parameters: [], fixture: fixture)
        try #require(presence.rows.count == 2)
        if case .composite = presence.rows[0].row.values[0] {} else { Issue.record("Expected present joined whole row") }
        #expect(presence.rows[1].row.values[0].isNull)
        #expect(presence.rows.map(\.origin) == [.source(fixture.primaryBaseID), .source(fixture.secondaryBaseID)])

        let inner = SelectQuery(projection: [ProjectionItem(.column(ColumnRef(table: "b", column: "rank")))],
            source: .aliased(.table(TableRef(Item.persistableType)), name: "b"),
            filter: .equal(.column(ColumnRef(table: "b", column: "id")), .column(ColumnRef(table: "a", column: "id"))))
        let correlated = SelectQuery(projection: [ProjectionItem(.column(ColumnRef(table: "a", column: "rank")), alias: "position"),
            ProjectionItem(.subquery(inner), alias: "local_value")],
            source: .aliased(.table(TableRef(Item.persistableType)), name: "a"), orderBy: [SortKey(.literal(.int(1)))])
        let local = try await executeParameterizedQuery(correlated, source: source, parameters: [], fixture: fixture)
        #expect(local.rows.map { $0.row.values } == [
            [.primitive(.int64(1)), .primitive(.int64(1))],
            [.primitive(.int64(2)), .primitive(.int64(2))],
        ])
    }

    @Test("Selected global retained sort fails without publication and releases its exact claims", .timeLimit(.minutes(2)))
    func selectedGlobalBudgetAndRelease() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        try await insert([("one", 1), ("three", 3)], baseID: fixture.primaryBaseID, fixture: fixture)
        try await insert([("two", 2), ("four", 4)], baseID: fixture.secondaryBaseID, fixture: fixture)
        let source = fixture.container.session(authorization: fixture.readerAuthorization).composition(fixture.compositionID)
        let query = SelectQuery(projection: [ProjectionItem(.column(ColumnRef("id")))],
            source: .table(TableRef(Item.persistableType)),
            orderBy: [SortKey(.column(ColumnRef("rank")))])
        let calibrated = ReadExecutionContext(options: .default, monotonicClock: fixture.container.monotonicClock)
        try await CompositionQueryPlanner(structuralLimits: calibrated.queryStructuralLimits).execute(query,
            source: source, options: CompositionQueryExecutionOptions(pageSize: 1, readContext: calibrated)) { _ in true }
        #expect(calibrated.workMeter.retainedIntermediateBytes == 0)
        #expect(calibrated.workMeter.retainedIntermediateRows == 0)
        let maximum = calibrated.workMeter.peakIntermediateBytes
        #expect(maximum > 0)
        let budget = ExecutionBudget(maximumRows: 100, maximumWorkUnits: 1_000_000,
            maximumIntermediateRows: 10_000, maximumIntermediateBytes: maximum - 1, timeoutMilliseconds: 30_000)
        let limited = ReadExecutionContext(options: ReadExecutionOptions(pageSize: 1, budget: budget),
            monotonicClock: fixture.container.monotonicClock)
        let publication = PublicationEventProbe()
        await #expect(throws: DatabaseWorkLimitError.self) {
            try await CompositionQueryPlanner(structuralLimits: limited.queryStructuralLimits).execute(query,
                source: source, options: CompositionQueryExecutionOptions(pageSize: 1, readContext: limited)) { _ in
                    await publication.receive()
                    return true
                }
        }
        #expect(await publication.receivedCount() == 0)
        #expect(limited.workMeter.retainedIntermediateBytes == 0)
        #expect(limited.workMeter.retainedIntermediateRows == 0)
        let recovered = try await source.execute(query)
        #expect(recovered.rows.count == 4)

        let aggregate = SelectQuery(projection: [ProjectionItem(.aggregate(.count(nil, distinct: false)), alias: "total")],
            source: .table(TableRef(Item.persistableType)))
        for plan in [query, aggregate, crossBaseQuery(fixture: fixture, joinType: .inner)] {
            let zero = ReadExecutionContext(options: .default, monotonicClock: fixture.container.monotonicClock)
            let zeroPublication = PublicationEventProbe()
            try await CompositionQueryPlanner(structuralLimits: zero.queryStructuralLimits).execute(
                plan.replacing(pagination: QueryPagination(limit: .count(.expression(.parameter(.position(1))), withTies: false))),
                source: source, options: CompositionQueryExecutionOptions(pageSize: 1, readContext: zero,
                    parameters: [QueryParameter(position: 1, value: .primitive(.int64(0)), type: .sql(.bigint))])
            ) { _ in await zeroPublication.receive(); return true }
            #expect(await zeroPublication.receivedCount() == 1)
            #expect(zero.workMeter.consumedRows == 0)
            #expect(zero.workMeter.retainedIntermediateBytes == 0)
            #expect(zero.workMeter.retainedIntermediateRows == 0)
        }

        let countQuery = SelectQuery(projection: [ProjectionItem(.literal(.int(1)))], source: .values([[]]))
        let unsupported = query.replacing(pagination: QueryPagination(
            limit: .count(.expression(.subquery(countQuery)), withTies: false)))
        let scope = ReadExecutionContext(options: .default, monotonicClock: fixture.container.monotonicClock)
        let countPublication = PublicationEventProbe()
        await #expect(throws: CanonicalReadError.self) {
            try await CompositionQueryPlanner(structuralLimits: scope.queryStructuralLimits).execute(unsupported,
                source: source, options: CompositionQueryExecutionOptions(pageSize: 1, readContext: scope)) { _ in
                    await countPublication.receive()
                    return true
                }
        }
        #expect(await countPublication.receivedCount() == 0)
        #expect(scope.workMeter.retainedIntermediateBytes == 0)
        #expect(scope.workMeter.retainedIntermediateRows == 0)
    }

    @Test("Selected CHAR, array, numeric and composite keys retain their declared meaning", .timeLimit(.minutes(2)))
    func selectedRichKeyMeaning() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        try await insert([("one", 1)], baseID: fixture.primaryBaseID, fixture: fixture)
        try await insert([("two", 2)], baseID: fixture.secondaryBaseID, fixture: fixture)
        let source = fixture.container.session(authorization: fixture.readerAuthorization).composition(fixture.compositionID)
        let rank = DatabaseKit.Expression.column(ColumnRef("rank"))
        let condition = DatabaseKit.Expression.equal(rank, .literal(.int(1)))
        let char = DataType.char(length: nil)
        let numeric = DataType.decimal(precision: nil, scale: nil)
        let month = try QueryIntervalValue(period: CalendarPeriod(months: 1, days: 0), time: TimeSpan(seconds: 0))
        let days = try QueryIntervalValue(period: CalendarPeriod(months: 0, days: 30), time: TimeSpan(seconds: 0))
        let scaled = try QueryNumericValue(value: ExactDecimal(coefficient: 125, scale: 2), displayScale: 5)
        let plain = try QueryNumericValue(value: ExactDecimal(coefficient: 125, scale: 2), displayScale: 2)
        let expressions: [DatabaseKit.Expression] = [
            .collate(.caseWhen(cases: [CaseWhenPair(condition: condition,
                result: .literal(.queryValue(.primitive(.string("a")), type: .sql(char))))],
                elseResult: .literal(.queryValue(.primitive(.string("a  ")), type: .sql(char)))), collation: .c),
            .caseWhen(cases: [CaseWhenPair(condition: condition,
                result: .literal(.queryValue(.numeric(scaled), type: .sql(numeric))))],
                elseResult: .literal(.queryValue(.numeric(plain), type: .sql(numeric)))),
            .array([.caseWhen(cases: [CaseWhenPair(condition: condition,
                result: .literal(.queryValue(.interval(month), type: .sql(.interval()))))],
                elseResult: .literal(.queryValue(.interval(days), type: .sql(.interval()))))]),
        ]
        for expression in expressions {
            let query = SelectQuery(projection: [ProjectionItem(expression, alias: "value")],
                source: .table(TableRef(Item.persistableType)), orderBy: [SortKey(.literal(.int(1)))], quantifier: .distinct)
            let result = try await executeParameterizedQuery(query, source: source, parameters: [], fixture: fixture)
            #expect(result.rows.count == 1)
            #expect(result.rows[0].origin == .derived(contributors: [fixture.primaryBaseID, fixture.secondaryBaseID].sorted()))
        }
        let composite = SelectQuery(projection: [ProjectionItem(.row([rank, .cast(.literal(.null), targetType: .text)]), alias: "value")],
            source: .table(TableRef(Item.persistableType)), orderBy: [SortKey(.literal(.int(1)), direction: .descending)])
        let ordered = try await executeParameterizedQuery(composite, source: source, parameters: [], fixture: fixture)
        guard case .sql(.composite(let descriptor)) = ordered.columns[0].type,
              case .composite(let first) = ordered.rows[0].row.values[0],
              case .composite(let second) = ordered.rows[1].row.values[0] else {
            Issue.record("Expected retained selected composite descriptors and values")
            return
        }
        #expect(descriptor.columns.map(\.name) == ["f1", "f2"])
        #expect(first.fields[0] == .primitive(.int64(2)))
        #expect(second.fields[0] == .primitive(.int64(1)))
        #expect(first.fields[1].isNull && second.fields[1].isNull)
    }

    @Test("Composition numeric aggregates retain widened state and release after final overflow")
    func selectedNumericAggregateWorkspace() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        try await insert([("positive-a", 1), ("positive-b", 1)], baseID: fixture.primaryBaseID, fixture: fixture)
        try await insert([("negative", -1)], baseID: fixture.secondaryBaseID, fixture: fixture)
        let source = fixture.container.session(authorization: fixture.readerAuthorization).composition(fixture.compositionID)
        let numericType = QueryColumnType.sql(.decimal(precision: nil, scale: nil))
        let maximum = Int128("99999999999999999999999999999999999999")!
        let parameters = [QueryParameter(position: 1, value: .numeric(try QueryNumericValue(
            value: ExactDecimal(coefficient: maximum, scale: 0), displayScale: 0)), type: numericType)]
        let operand = DatabaseKit.Expression.parameter(.position(1))
        let failureContext = ReadExecutionContext(options: .default, monotonicClock: fixture.container.monotonicClock)
        do {
            try await CompositionQueryPlanner(structuralLimits: failureContext.queryStructuralLimits).execute(
                SelectQuery(projection: [ProjectionItem(.aggregate(.sum(operand, distinct: false)), alias: "sum")],
                            source: .table(TableRef(Item.persistableType))), source: source,
                options: CompositionQueryExecutionOptions(pageSize: 1, readContext: failureContext, parameters: parameters)
            ) { _ in true }
            Issue.record("Expected final 39-digit SUM rejection")
        } catch let error as CompositionQueryError {
            guard case .aggregateFailure = error else { throw error }
        }
        #expect(failureContext.workMeter.retainedIntermediateRows == 0)
        #expect(failureContext.workMeter.retainedIntermediateBytes == 0)
        let signed = DatabaseKit.Expression.multiply(operand,
            .cast(.column(ColumnRef("rank")), targetType: .decimal(precision: nil, scale: nil)))
        let valid = try await executeParameterizedQuery(
            SelectQuery(projection: [
                ProjectionItem(.aggregate(.avg(operand, distinct: false)), alias: "average"),
                ProjectionItem(.aggregate(.sum(signed, distinct: false)), alias: "sum"),
            ], source: .table(TableRef(Item.persistableType))), source: source, parameters: parameters, fixture: fixture)
        #expect(valid.rows.count == 1)
        #expect(valid.columns.map(\.type) == [numericType, numericType])
        let expected = QueryValue.numeric(try QueryNumericValue(
            value: ExactDecimal(coefficient: maximum, scale: 0), displayScale: 0))
        #expect(valid.rows[0].row.values == [expected, expected])
        let scaledParameters = [QueryParameter(position: 1, value: .numeric(try QueryNumericValue(
            value: ExactDecimal(coefficient: 125, scale: 2), displayScale: 5)), type: numericType)]
        let scaled = try await executeParameterizedQuery(
            SelectQuery(projection: [ProjectionItem(.aggregate(.sum(operand, distinct: false)), alias: "sum")],
                        source: .table(TableRef(Item.persistableType))), source: source,
            parameters: scaledParameters, fixture: fixture)
        #expect(scaled.rows[0].row.values == [.numeric(try QueryNumericValue(
            value: ExactDecimal(coefficient: 375, scale: 2), displayScale: 5))])
        let primitiveParameters = [QueryParameter(position: 1,
            value: .primitive(.decimal(ExactDecimal(coefficient: 125, scale: 2))), type: numericType)]
        let primitive = try await executeParameterizedQuery(
            SelectQuery(projection: [
                ProjectionItem(.aggregate(.sum(operand, distinct: false)), alias: "sum"),
                ProjectionItem(.aggregate(.avg(operand, distinct: false)), alias: "average"),
            ], source: .table(TableRef(Item.persistableType))), source: source,
            parameters: primitiveParameters, fixture: fixture)
        #expect(primitive.rows[0].row.values == [
            .numeric(try QueryNumericValue(value: ExactDecimal(coefficient: 375, scale: 2), displayScale: 2)),
            .numeric(try QueryNumericValue(value: ExactDecimal(coefficient: 125, scale: 2), displayScale: 20)),
        ])
    }

    private actor CancellationRowProbe {
        private var count = 0
        private var minimumRetainedRows: UInt64?
        private var minimumRetainedBytes: UInt64?

        func receive(
            _ event: CompositionQueryEvent,
            workMeter: DatabaseWorkMeter
        ) async throws -> Bool {
            guard case .row = event else { return true }
            count += 1
            minimumRetainedRows = workMeter.retainedIntermediateRows
            minimumRetainedBytes = workMeter.retainedIntermediateBytes
            await Task.yield()
            minimumRetainedRows = min(
                minimumRetainedRows ?? .max,
                workMeter.retainedIntermediateRows
            )
            minimumRetainedBytes = min(
                minimumRetainedBytes ?? .max,
                workMeter.retainedIntermediateBytes
            )
            throw CancellationError()
        }

        func receivedRowCount() -> Int {
            count
        }

        func retainedClaimDuringCancellation() -> (
            rows: UInt64?,
            bytes: UInt64?
        ) {
            (minimumRetainedRows, minimumRetainedBytes)
        }
    }

    private actor AggregateEmissionProbe {
        private var values: [CompositionQueryRow] = []
        private var minimumRetainedRows: UInt64?
        private var minimumRetainedBytes: UInt64?

        func receive(
            _ event: CompositionQueryEvent,
            workMeter: DatabaseWorkMeter
        ) async -> Bool {
            guard case .row(let value) = event else { return true }
            minimumRetainedRows = workMeter.retainedIntermediateRows
            minimumRetainedBytes = workMeter.retainedIntermediateBytes
            await Task.yield()
            values.append(value)
            minimumRetainedRows = min(
                minimumRetainedRows ?? .max,
                workMeter.retainedIntermediateRows
            )
            minimumRetainedBytes = min(
                minimumRetainedBytes ?? .max,
                workMeter.retainedIntermediateBytes
            )
            return true
        }

        func removeAll() { values.removeAll() }

        func result() -> (
            values: [CompositionQueryRow],
            rows: UInt64?,
            bytes: UInt64?
        ) {
            (values, minimumRetainedRows, minimumRetainedBytes)
        }
    }

    private final class IdentityFingerprintProbe: Sendable {
        private struct State {
            var retainedBytes: UInt64?
            var invocationCount = 0
        }

        private let state = Mutex(State())

        func record(retainedBytes: UInt64) {
            state.withLock {
                $0.retainedBytes = retainedBytes
                $0.invocationCount += 1
            }
        }

        func markInvoked() {
            state.withLock { $0.invocationCount += 1 }
        }

        var retainedBytes: UInt64? {
            state.withLock { $0.retainedBytes }
        }

        var wasInvoked: Bool {
            state.withLock { $0.invocationCount > 0 }
        }
    }

    private struct DistinctEntryLayoutProbe: Sendable {
        let fingerprint: ByteString
        let ownedRow: DatabaseQueryScopedQueryRowOwner
        var contributors: [Base.ID]
    }

    @Test("Composition binds duplicate output columns before fetching empty members")
    func emptyMembersRetainBoundColumns() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        let source = fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(fixture.compositionID)
        let query = SelectQuery(
            projection: [
                ProjectionItem(.cast(.literal(.null), targetType: .bigint), alias: "same"),
                ProjectionItem(.column(ColumnRef("rank")), alias: "same"),
            ],
            source: .table(TableRef(Item.persistableType))
        )
        let empty = try await source.execute(query)
        #expect(empty.rows.isEmpty)
        #expect(empty.columns.map(\.number) == [1, 2])
        #expect(empty.columns.map(\.name) == ["same", "same"])
        #expect(empty.columns[0].type == .sql(.bigint))
        #expect(empty.columns.map(\.isNullable) == [true, false])
        try await insert([("first", 7)], baseID: fixture.primaryBaseID, fixture: fixture)
        let populated = try await source.execute(query)
        #expect(populated.columns == empty.columns)
        #expect(populated.rows.count == 1)
        #expect(populated.rows[0].value.values == [.primitive(.null), .primitive(.int64(7))])
    }

    @Test("Materialized Composition response rejects a row that violates its declared schema")
    func materializedResponseValidatesRows() throws {
        let base = try Base.ID("response-shape")
        let composition = try CompositionResolution.derived([base])
        let columns = try [QueryColumn(number: 1, name: "value", type: .sql(.bigint), isNullable: false)]
        let malformed = QueryRow(validatedValues: [.primitive(.string("wrong"))])
        #expect(throws: QueryRowCodecError.self) {
            try CompositionQueryResponse(columns: columns, rows: [
                CompositionResult(composition: composition, origin: .source(base), value: malformed),
            ])
        }
        let empty = try CompositionQueryResponse(columns: columns, rows: [])
        #expect(empty.columns == columns)
        #expect(empty.rows.isEmpty)
    }

    @Test("Global ordering, windowing, and provenance span domains")
    func globalOrderingWindowAndProvenance() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        try await insert(
            [("shared", 1), ("primary", 3)],
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )
        try await insert(
            [("shared", 2), ("secondary", 4)],
            baseID: fixture.secondaryBaseID,
            fixture: fixture
        )

        let results = try await fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(fixture.compositionID)
            .query(Item.self)
            .orderBy(#field(\Item.rank))
            .offset(1)
            .limit(2)
            .execute()

        #expect(results.map(\.value.rank) == [2, 3])
        #expect(
            results.map(\.origin) == [
                .source(fixture.secondaryBaseID),
                .source(fixture.primaryBaseID),
            ]
        )
        #expect(results.allSatisfy {
            $0.composition.namedID == fixture.compositionID
                && $0.composition.generation == 1
        })

        let count = try await fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(fixture.compositionID)
            .query(Item.self)
            .offset(1)
            .limit(2)
            .count()
        #expect(count == 2)

        try await insert(
            (0..<12).map { index in
                ("paged-primary-\(index)", Int64(10 + index))
            },
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )

        let aggregateQuery = SelectQuery(
            projection: [
                ProjectionItem(
                    .aggregate(
                        .min(.column(ColumnRef(column: "id")))
                    ),
                    alias: "minimum"
                ),
                ProjectionItem(
                    .aggregate(
                        .max(.column(ColumnRef(column: "id")))
                    ),
                    alias: "maximum"
                ),
            ],
            source: .table(TableRef(Item.persistableType))
        )
        let aggregateSource = fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(fixture.compositionID)
        let calibrationContext = ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: 8,
                budget: ExecutionBudget(
                    maximumRows: 100,
                    maximumWorkUnits: 100_000,
                    maximumIntermediateRows: 7,
                    maximumIntermediateBytes: 4 * 1_024 * 1_024,
                    timeoutMilliseconds: 30_000
                )
            ),
            monotonicClock: fixture.container.monotonicClock
        )
        let aggregateProbe = AggregateEmissionProbe()
        try await CompositionQueryPlanner(
            structuralLimits: calibrationContext.queryStructuralLimits
        ).execute(
            aggregateQuery,
            source: aggregateSource,
            options: CompositionQueryExecutionOptions(
                pageSize: 8,
                readContext: calibrationContext
            )
        ) { event in
            await aggregateProbe.receive(
                event,
                workMeter: calibrationContext.workMeter
            )
        }
        var aggregateEmission = await aggregateProbe.result()
        var extrema = aggregateEmission.values
        #expect(extrema.count == 1)
        #expect(
            extrema[0].row.values[0]
                == .primitive(.string("paged-primary-0"))
        )
        #expect(extrema[0].row.values[1] == .primitive(.string("shared")))
        #expect(
            extrema[0].origin == .derived(
                contributors: [
                    fixture.primaryBaseID,
                    fixture.secondaryBaseID,
                ]
            )
        )
        #expect(aggregateEmission.rows == 1)
        #expect((aggregateEmission.bytes ?? 0) > 0)
        let aggregatePeak = calibrationContext.workMeter.peakIntermediateBytes
        #expect(aggregatePeak > 0)
        #expect(calibrationContext.workMeter.peakIntermediateRows == 7)
        #expect(calibrationContext.workMeter.retainedIntermediateRows == 1)
        #expect(calibrationContext.workMeter.retainedIntermediateBytes > 0)
        extrema.removeAll()
        aggregateEmission.values.removeAll()
        await aggregateProbe.removeAll()
        #expect(calibrationContext.workMeter.retainedIntermediateRows == 0)
        #expect(calibrationContext.workMeter.retainedIntermediateBytes == 0)

        let insufficientRowContext = ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: 8,
                budget: ExecutionBudget(
                    maximumRows: 100,
                    maximumWorkUnits: 100_000,
                    maximumIntermediateRows: 6,
                    maximumIntermediateBytes: 4 * 1_024 * 1_024,
                    timeoutMilliseconds: 30_000
                )
            ),
            monotonicClock: fixture.container.monotonicClock
        )
        var insufficientRowFailure: Error?
        do {
            try await CompositionQueryPlanner(
                structuralLimits: insufficientRowContext.queryStructuralLimits
            ).execute(
                aggregateQuery,
                source: aggregateSource,
                options: CompositionQueryExecutionOptions(
                    pageSize: 8,
                    readContext: insufficientRowContext
                )
            ) { _ in true }
            Issue.record("Expected aggregate row admission to reject a maximum of six")
        } catch {
            insufficientRowFailure = error
        }
        guard let insufficientRowLimit = insufficientRowFailure
                as? DatabaseWorkLimitError,
              case .maximumIntermediateRows(
                let insufficientRowStage,
                let insufficientRowConsumed,
                let insufficientRowRequested,
                let insufficientRowMaximum
              ) = insufficientRowLimit else {
            Issue.record(
                "Expected aggregate row-limit failure, got \(String(describing: insufficientRowFailure))"
            )
            return
        }
        #expect(insufficientRowStage == .aggregateInput)
        #expect(insufficientRowConsumed == 0)
        #expect(insufficientRowRequested == 7)
        #expect(insufficientRowMaximum == 6)
        #expect(insufficientRowContext.workMeter.retainedIntermediateRows == 0)
        #expect(insufficientRowContext.workMeter.retainedIntermediateBytes == 0)

        let bufferProbeContext = ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: 8,
                budget: ExecutionBudget(
                    maximumRows: 100,
                    maximumWorkUnits: 100_000,
                    maximumIntermediateRows: 100,
                    maximumIntermediateBytes: 0,
                    timeoutMilliseconds: 30_000
                )
            ),
            monotonicClock: fixture.container.monotonicClock
        )
        var bufferProbeFailure: Error?
        do {
            try await CompositionQueryPlanner(
                structuralLimits: bufferProbeContext.queryStructuralLimits
            ).execute(
                aggregateQuery,
                source: aggregateSource,
                options: CompositionQueryExecutionOptions(
                    pageSize: 8,
                    readContext: bufferProbeContext
                )
            ) { _ in true }
            Issue.record("Expected Ready buffer admission to reject zero bytes")
        } catch {
            bufferProbeFailure = error
        }
        guard let bufferWorkLimit = bufferProbeFailure as? DatabaseWorkLimitError,
              case .maximumIntermediateBytes(
                let bufferStage,
                let bufferConsumed,
                let bufferRequested,
                let bufferMaximum
              ) = bufferWorkLimit else {
            Issue.record(
                "Expected Ready buffer byte-limit failure, got \(String(describing: bufferProbeFailure))"
            )
            return
        }
        #expect(bufferStage == .resultMaterialization)
        #expect(bufferConsumed == 0)
        #expect(bufferMaximum == 0)
        guard bufferRequested > 0 else {
            Issue.record("Ready buffer footprint must be nonzero")
            return
        }
        #expect(bufferProbeContext.workMeter.retainedIntermediateRows == 0)
        #expect(bufferProbeContext.workMeter.retainedIntermediateBytes == 0)

        let oneByteShortContext = ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: 8,
                budget: ExecutionBudget(
                    maximumRows: 100,
                    maximumWorkUnits: 100_000,
                    maximumIntermediateRows: 100,
                    maximumIntermediateBytes: bufferRequested - 1,
                    timeoutMilliseconds: 30_000
                )
            ),
            monotonicClock: fixture.container.monotonicClock
        )
        var oneByteShortFailure: Error?
        do {
            try await CompositionQueryPlanner(
                structuralLimits: oneByteShortContext.queryStructuralLimits
            ).execute(
                aggregateQuery,
                source: aggregateSource,
                options: CompositionQueryExecutionOptions(
                    pageSize: 8,
                    readContext: oneByteShortContext
                )
            ) { _ in true }
            Issue.record("Expected Ready buffer admission one byte short")
        } catch {
            oneByteShortFailure = error
        }
        guard let oneByteShortLimit = oneByteShortFailure
                as? DatabaseWorkLimitError,
              case .maximumIntermediateBytes(
                let shortStage,
                let shortConsumed,
                let shortRequested,
                let shortMaximum
              ) = oneByteShortLimit else {
            Issue.record(
                "Expected one-byte-short Ready buffer failure, got \(String(describing: oneByteShortFailure))"
            )
            return
        }
        #expect(shortStage == .resultMaterialization)
        #expect(shortConsumed == 0)
        #expect(shortRequested == bufferRequested)
        #expect(shortMaximum == bufferRequested - 1)
        #expect(oneByteShortContext.workMeter.retainedIntermediateRows == 0)
        #expect(oneByteShortContext.workMeter.retainedIntermediateBytes == 0)

        let failingAggregateContext = ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: 1,
                budget: ExecutionBudget(
                    maximumRows: 100,
                    maximumWorkUnits: 100_000,
                    maximumIntermediateRows: 8,
                    maximumIntermediateBytes: 4 * 1_024 * 1_024,
                    timeoutMilliseconds: 30_000
                )
            ),
            monotonicClock: fixture.container.monotonicClock
        )
        var failingAggregateError: Error?
        do {
            try await CompositionQueryPlanner(
                structuralLimits: failingAggregateContext
                    .queryStructuralLimits
            ).execute(
                SelectQuery(
                    projection: [
                        ProjectionItem(
                            .aggregate(
                                .sum(
                                    .column(ColumnRef(column: "id")),
                                    distinct: false
                                )
                            ),
                            alias: "invalidSum"
                        )
                    ],
                    source: .table(TableRef(Item.persistableType))
                ),
                source: aggregateSource,
                options: CompositionQueryExecutionOptions(
                    pageSize: 1,
                    readContext: failingAggregateContext
                )
            ) { _ in true }
            Issue.record("Expected nonnumeric SUM to fail")
        } catch {
            failingAggregateError = error
        }
        guard let failingAggregate = failingAggregateError as? CanonicalReadError,
              case .expressionEvaluation(.typeMismatch(operation: "numeric expression"), _) = failingAggregate else {
            Issue.record(
                "Expected bound numeric type failure, got \(String(describing: failingAggregateError))"
            )
            return
        }
        #expect(
            failingAggregateContext.workMeter.retainedIntermediateRows == 0
        )
        #expect(
            failingAggregateContext.workMeter.retainedIntermediateBytes == 0
        )

        // This canonical query retains admitted source relations before global LIMIT.
        // Its result uses the standard budget; exact aggregate bounds are verified above.
        let selectedRows = try await aggregateSource.execute(
            SelectQuery(
                projection: [.wildcard()],
                source: .table(TableRef(Item.persistableType)),
                pagination: QueryPagination(limit: .count(.fixed(2), withTies: false))
            ),
            options: ReadExecutionOptions(
                pageSize: 1,
                budget: ExecutionBudget()
            )
        )
        #expect(selectedRows.rows.count == 2)
    }

    @Test("Composition validates structure before opening a read snapshot")
    func compositionValidatesStructureBeforeReadSnapshot() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        let source = fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(fixture.compositionID)
        let readContext = ReadExecutionContext(
            options: .default,
            monotonicClock: fixture.container.monotonicClock,
            queryStructuralLimits: QueryStructuralLimits(
                maximumTotalNodes: 0
            )
        )
        let planner = CompositionQueryPlanner(
            structuralLimits: readContext.queryStructuralLimits
        )

        await #expect(throws: QueryStructuralValidationError.self) {
            try await planner.execute(
                SelectQuery(
                    projection: [.wildcard()],
                    source: .table(TableRef(Item.persistableType))
                ),
                source: source,
                options: CompositionQueryExecutionOptions(
                    pageSize: 1,
                    readContext: readContext
                )
            ) { _ in
                Issue.record("Invalid query reached event emission")
                return false
            }
        }
        #expect(readContext.workMeter.consumedRows == 0)
        #expect(readContext.workMeter.retainedIntermediateRows == 0)
        #expect(readContext.workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Unadmitted Composition sources reject canonical metadata before opening member reads", .timeLimit(.minutes(2)))
    func unsupportedSourcesRejectBeforeMemberRead() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        fixture.secondaryEngine.requestShutdown()
        await fixture.secondaryEngine.waitUntilShutdown()
        let source = fixture.container.session(authorization: fixture.readerAuthorization).composition(fixture.compositionID)
        let leaf = SelectQuery(projection: [.wildcard()], source: .table(TableRef(Item.persistableType)))
        let queries = [
            leaf.replacing(source: .subquery(leaf)),
            leaf.replacing(source: .values([[.literal(.int(1))]])),
            leaf.replacing(source: .setOperation(operator: .intersect, quantifier: .all, left: leaf, right: leaf)),
            leaf.replacing(subqueries: [NamedSubquery(name: "unused", query: leaf, materialized: .materialized)]),
        ]
        for query in queries {
            let execution = ReadExecutionContext(options: .default, monotonicClock: fixture.container.monotonicClock)
            let publication = PublicationEventProbe()
            await #expect(throws: CompositionQueryError.self) {
                try await CompositionQueryPlanner(structuralLimits: execution.queryStructuralLimits).execute(query,
                    source: source, options: CompositionQueryExecutionOptions(pageSize: 1, readContext: execution)) { _ in
                        await publication.receive()
                        return true
                    }
            }
            #expect(await publication.receivedCount() == 0)
            #expect(execution.workMeter.retainedIntermediateRows == 0)
            #expect(execution.workMeter.retainedIntermediateBytes == 0)
        }
    }

    @Test("Composition publishes retained events only after all producer leases close")
    func eventsFollowAuthoritativeSnapshotClose() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        try await insert([("first", 1)], baseID: fixture.primaryBaseID, fixture: fixture)
        try await insert([("second", 2)], baseID: fixture.secondaryBaseID, fixture: fixture)
        let source = fixture.container.session(authorization: fixture.readerAuthorization)
            .composition(fixture.compositionID)
        let context = ReadExecutionContext(options: .default,
            monotonicClock: fixture.container.monotonicClock)
        let planner = CompositionQueryPlanner(structuralLimits: context.queryStructuralLimits)
        let query = SelectQuery(projection: [.wildcard()],
            source: .table(TableRef(Item.persistableType)))
        let received = PublicationEventProbe()
        try await planner.execute(query, source: source,
            options: CompositionQueryExecutionOptions(pageSize: 1, readContext: context)) { _ in
            #expect(fixture.container.baseDrainState(fixture.primaryBaseID)?.activeLeaseCount == 0)
            #expect(fixture.container.baseDrainState(fixture.secondaryBaseID)?.activeLeaseCount == 0)
            #expect(context.workMeter.retainedIntermediateRows > 0)
            #expect(context.workMeter.retainedIntermediateBytes > 0)
            await received.receive()
            await Task.yield()
            return true
        }
        let receivedCount = await received.receivedCount()
        #expect(receivedCount == 3)
        #expect(context.workMeter.retainedIntermediateRows == 0)
        #expect(context.workMeter.retainedIntermediateBytes == 0)

        let cancelled = ReadExecutionContext(options: .default,
            monotonicClock: fixture.container.monotonicClock)
        await #expect(throws: CancellationError.self) {
            try await planner.execute(query, source: source,
                options: CompositionQueryExecutionOptions(pageSize: 1, readContext: cancelled)) { event in
                #expect(fixture.container.baseDrainState(fixture.primaryBaseID)?.activeLeaseCount == 0)
                #expect(fixture.container.baseDrainState(fixture.secondaryBaseID)?.activeLeaseCount == 0)
                if case .row = event { throw CancellationError() }
                return true
            }
        }
        #expect(cancelled.workMeter.retainedIntermediateRows == 0)
        #expect(cancelled.workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Equal logical IDs remain Base-qualified")
    func equalLogicalIDsRemainBaseQualified() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        try await insert(
            [("shared", 1)],
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )
        try await insert(
            [("shared", 2)],
            baseID: fixture.secondaryBaseID,
            fixture: fixture
        )

        let results = try await fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(fixture.compositionID)
            .query(Item.self)
            .orderBy(#field(\Item.rank))
            .execute()

        #expect(results.map(\.value.id) == ["shared", "shared"])
        #expect(Set(results.map(\.origin)).count == 2)
    }

    @Test("Every member is authorized before results are exposed")
    func everyMemberMustBeAuthorized() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: false)
        defer { await fixture.container.shutdown() }
        try await insert(
            [("visible-only-if-composition-were-partial", 1)],
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )

        await #expect(throws: DatabaseCompositionAccessError.self) {
            try await fixture.container.session(
                authorization: fixture.readerAuthorization
            ).composition(fixture.compositionID)
                .query(Item.self)
                .execute()
        }
    }

    @Test("Metadata lease preserves non-authorization storage failures")
    func metadataLeasePreservesStorageFailure() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        fixture.secondaryEngine.requestShutdown()
        await fixture.secondaryEngine.waitUntilShutdown()
        let source = fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(fixture.compositionID)

        do {
            _ = try await source.acquireReadMetadata()
            Issue.record("Expected the stopped storage domain to fail")
        } catch is DatabaseCompositionAccessError {
            Issue.record("Storage failure was incorrectly hidden as access denial")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
            #expect(error.operation == .beginTransaction)
        }
    }

    @Test("Derived Composition does not require a catalog record")
    func derivedCompositionExecutesWithoutCatalogRecord() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        try await insert(
            [("primary", 1)],
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )
        try await insert(
            [("secondary", 2)],
            baseID: fixture.secondaryBaseID,
            fixture: fixture
        )

        let results = try await fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(
            bases: [fixture.secondaryBaseID, fixture.primaryBaseID]
        ).query(Item.self).orderBy(#field(\Item.rank)).execute()

        #expect(results.map(\.value.rank) == [1, 2])
        #expect(results.allSatisfy {
            $0.composition.kind == .derived
                && $0.composition.namedID == nil
                && $0.composition.generation == nil
                && $0.composition.bases == [
                    fixture.primaryBaseID,
                    fixture.secondaryBaseID,
                ].sorted()
        })
    }

    @Test("Decision reads and writes share one storage-domain transaction")
    func sameDomainDecisionTransactionCommitsAndRollsBack() async throws {
        let fixture = try await makeFixture(
            readerCanReadSecondary: true,
            secondarySharesControlDomain: true
        )
        defer { await fixture.container.shutdown() }
        try await insert(
            [("world", 7)],
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )
        let composition = try fixture.container.session(
            authorization: fixture.ownerAuthorization
        ).composition(
            bases: [fixture.primaryBaseID, fixture.secondaryBaseID]
        )

        try await composition.withDecisionTransaction(
            writingTo: fixture.secondaryBaseID
        ) { transaction in
            let world = try await transaction.fetch(
                Query<Item>().where(#field(\Item.id) == "world"),
                from: fixture.primaryBaseID
            )
            #expect(world.map(\.rank) == [7])
            var decision = Item()
            decision.id = "committed"
            decision.rank = world[0].rank + 1
            try await transaction.save(decision)
        }

        do {
            try await composition.withDecisionTransaction(
                writingTo: fixture.secondaryBaseID
            ) { transaction in
                var decision = Item()
                decision.id = "rolled-back"
                decision.rank = 99
                try await transaction.save(decision)
                throw DecisionTestError.rollbackRequested
            }
            Issue.record("The requested rollback unexpectedly committed")
        } catch DecisionTestError.rollbackRequested {
            // Expected typed application failure.
        }

        let written = try await fixture.container.session(
            authorization: fixture.ownerAuthorization
        ).base(fixture.secondaryBaseID).newContext()
            .fetch(Item.self)
            .orderBy(#field(\Item.rank))
            .execute()
        #expect(written.map(\.id) == ["committed"])
    }

    @Test("Decision transaction rejects multiple storage domains before work")
    func decisionTransactionRejectsMultipleDomains() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        let operationProbe = DecisionOperationProbe()
        let composition = try fixture.container.session(
            authorization: fixture.ownerAuthorization
        ).composition(
            bases: [fixture.primaryBaseID, fixture.secondaryBaseID]
        )

        do {
            try await composition.withDecisionTransaction(
                writingTo: fixture.primaryBaseID
            ) { _ in
                await operationProbe.markStarted()
            }
            Issue.record("The cross-domain decision transaction unexpectedly started")
        } catch CompositionDecisionError.multipleStorageDomains {
            // Expected typed failure before the operation begins.
        } catch {
            Issue.record("Unexpected decision transaction error: \(error)")
        }
        let operationStarted = await operationProbe.hasStarted()
        #expect(operationStarted == false)
    }

    @Test("Explicit cross-Base INNER JOIN preserves derived lineage")
    func crossBaseInnerJoinUsesCanonicalBoundedExecutor() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        try await insert(
            [("shared", 1), ("left-only", 3)],
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )
        try await insert(
            [("shared", 2), ("right-only", 4)],
            baseID: fixture.secondaryBaseID,
            fixture: fixture
        )
        let source = try fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(
            bases: [fixture.primaryBaseID, fixture.secondaryBaseID]
        )
        let result = try await executeCompositionQuery(
            crossBaseQuery(fixture: fixture, joinType: .inner),
            source: source
        )
        #expect(result.first?.composition.kind == .derived)
        #expect(result.count == 1)
        #expect(result[0].value.values[0] == .primitive(.int64(1)))
        #expect(result[0].value.values[1] == .primitive(.int64(2)))
        #expect(
            result[0].origin == .derived(
                contributors: [
                    fixture.primaryBaseID,
                    fixture.secondaryBaseID,
                ].sorted()
            )
        )
    }

    @Test("Cross-Base outer JOIN fails without fallback")
    func crossBaseOuterJoinIsExplicitlyUnsupported() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        let source = try fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(
            bases: [fixture.primaryBaseID, fixture.secondaryBaseID]
        )

        do {
            _ = try await executeCompositionQuery(
                crossBaseQuery(fixture: fixture, joinType: .left),
                source: source
            )
            Issue.record("The unsupported outer JOIN unexpectedly executed")
        } catch let error as CompositionQueryError {
            guard case .unsupportedPlan = error else {
                Issue.record("Unexpected Composition failure: \(error)")
                return
            }
        } catch {
            Issue.record("Unexpected outer JOIN failure: \(error)")
        }
    }

    @Test("Cross-Base JOIN input materialization is request-bounded")
    func crossBaseJoinHonorsIntermediateRowBudget() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        try await insert(
            [("one", 1), ("two", 2)],
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )
        try await insert(
            [("one", 3), ("two", 4)],
            baseID: fixture.secondaryBaseID,
            fixture: fixture
        )
        let source = try fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(
            bases: [fixture.primaryBaseID, fixture.secondaryBaseID]
        )

        await #expect(throws: DatabaseWorkLimitError.self) {
            _ = try await executeCompositionQuery(
                crossBaseQuery(fixture: fixture, joinType: .inner),
                source: source,
                maximumIntermediateRows: 2
            )
        }
    }

    @Test("DISTINCT compares exact identity after a digest collision")
    func distinctDigestCollisionUsesExactIdentity() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }

        func meter(maximumBytes: UInt64 = 1 * 1_024 * 1_024)
            -> DatabaseWorkMeter {
            DatabaseWorkMeter(
                budget: ExecutionBudget(
                    maximumRows: 100,
                    maximumWorkUnits: 100_000,
                    maximumIntermediateRows: 16,
                    maximumIntermediateBytes: maximumBytes,
                    timeoutMilliseconds: 30_000
                ),
                monotonicClock: TestProcessMonotonicClock()
            )
        }

        func owned(
            _ row: QueryRow,
            workMeter: DatabaseWorkMeter
        ) throws -> DatabaseQueryScopedQueryRow {
            let footprint = try CanonicalRelationalFootprintMeter.footprint(
                of: row,
                workMeter: workMeter,
                stage: .resultMaterialization
            )
            return try DatabaseQueryScopedQueryRow.producing(
                exactFootprint: footprint,
                workMeter: workMeter,
                stage: .resultMaterialization
            ) { row }
        }

        let identityRow = try priorityRow(1)
        let identityCalibrationMeter = meter()
        let frameConstructionProbe = IdentityFingerprintProbe()
        let identityCalibrationWorkspace = CompositionDistinctWorkspace.create(
            maximumIntermediateBytes: 1 * 1_024 * 1_024,
            workMeter: identityCalibrationMeter,
            identityFingerprint: { _ in
                frameConstructionProbe.record(
                    retainedBytes: identityCalibrationMeter.retainedIntermediateBytes
                )
                return ByteString(repeating: 0x41, count: 32)
            }
        )
        try await identityCalibrationWorkspace.insert(
            try owned(identityRow, workMeter: identityCalibrationMeter),
            origin: .source(fixture.primaryBaseID),
            sequence: 0
        )
        let identityAdmissionPeak = try #require(
            frameConstructionProbe.retainedBytes
        )
        #expect(identityAdmissionPeak > 0)
        await identityCalibrationWorkspace.removeAll()
        #expect(identityCalibrationMeter.retainedIntermediateRows == 0)
        #expect(identityCalibrationMeter.retainedIntermediateBytes == 0)

        let identityConstrainedMeter = meter(
            maximumBytes: identityAdmissionPeak - 1
        )
        let constrainedFingerprintProbe = IdentityFingerprintProbe()
        let identityConstrainedWorkspace = CompositionDistinctWorkspace.create(
            maximumIntermediateBytes: 1 * 1_024 * 1_024,
            workMeter: identityConstrainedMeter,
            identityFingerprint: { _ in
                constrainedFingerprintProbe.markInvoked()
                return ByteString(repeating: 0x41, count: 32)
            }
        )
        await #expect(throws: DatabaseWorkLimitError.self) {
            try await identityConstrainedWorkspace.insert(
                try owned(identityRow, workMeter: identityConstrainedMeter),
                origin: .source(fixture.primaryBaseID),
                sequence: 0
            )
        }
        #expect(!constrainedFingerprintProbe.wasInvoked)
        await identityConstrainedWorkspace.removeAll()
        #expect(identityConstrainedMeter.retainedIntermediateRows == 0)
        #expect(identityConstrainedMeter.retainedIntermediateBytes == 0)

        let workMeter = meter()
        let entryCapacityProbe = IdentityFingerprintProbe()
        let workspace = CompositionDistinctWorkspace.create(
            maximumIntermediateBytes: 1 * 1_024 * 1_024,
            workMeter: workMeter,
            identityFingerprint: { _ in
                entryCapacityProbe.record(
                    retainedBytes: workMeter.retainedIntermediateBytes
                )
                return ByteString(repeating: 0x42, count: 32)
            }
        )
        let output = Mutex<[CompositionDistinctWorkspace.Result]>([])

        do {
            try await workspace.insert(
                try owned(try priorityRow(1,
                    annotations: ["representative": .string("first")],
                    version: PersistableVersionToken("version-1")
                ), workMeter: workMeter),
                origin: .source(fixture.primaryBaseID),
                sequence: 0
            )
            let retainedRowsAfterNewEntry = workMeter.retainedIntermediateRows
            let retainedBytesAfterNewEntry = workMeter.retainedIntermediateBytes
            #expect(retainedRowsAfterNewEntry == 1)
            #expect(retainedBytesAfterNewEntry > 0)
            try await workspace.insert(
                try owned(try priorityRow(1,
                    annotations: ["representative": .string("duplicate")]
                ), workMeter: workMeter),
                origin: .source(fixture.primaryBaseID),
                sequence: 1
            )
            #expect(
                workMeter.retainedIntermediateRows == retainedRowsAfterNewEntry
            )
            #expect(
                workMeter.retainedIntermediateBytes == retainedBytesAfterNewEntry
            )
            try await workspace.insert(
                try owned(
                    try priorityRow(2),
                    workMeter: workMeter
                ),
                origin: .source(fixture.secondaryBaseID),
                sequence: 2
            )
            let retainedBeforeEntryReplacement = try #require(
                entryCapacityProbe.retainedBytes
            )
            let contributorLayout = try DatabaseRetainedArrayLayout
                .forElement(Base.ID.self)
            let contributorGrowth = try contributorLayout.growth(
                from: 0,
                toFit: 1
            )
            let contributorBytes = contributorLayout.containerByteCount
                + contributorGrowth.additionalByteCount
                + UInt64(fixture.secondaryBaseID.value.utf8.count)
            let entryLayout = try DatabaseRetainedArrayLayout.forElement(
                DistinctEntryLayoutProbe.self
            )
            let entryReplacement = try entryLayout.growth(
                from: 0,
                toFit: 2
            )
            let expectedIncrease = contributorBytes.addingReportingOverflow(
                entryReplacement.additionalByteCount
            )
            let expectedPeak = retainedBeforeEntryReplacement
                .addingReportingOverflow(expectedIncrease.partialValue)
            #expect(!expectedIncrease.overflow)
            #expect(!expectedPeak.overflow)
            #expect(
                workMeter.peakIntermediateBytes >= expectedPeak.partialValue
            )
            try await workspace.insert(
                try owned(try priorityRow(1,
                    annotations: ["representative": .string("later")],
                    version: PersistableVersionToken("version-2")
                ), workMeter: workMeter),
                origin: .source(fixture.secondaryBaseID),
                sequence: 3
            )
            try await workspace.forEachResult(batchSize: 1) { result in
                output.withLock { $0.append(result) }
                return true
            }
            let retainedWithResults = workMeter.retainedIntermediateBytes
            await workspace.removeAll()
            #expect(workMeter.retainedIntermediateBytes == retainedWithResults)
        } catch {
            let operationError = error
            await workspace.removeAll()
            throw operationError
        }

        var results = output.withLock { $0 }
        // The duplicate candidate remains admitted until exact identity
        // comparison completes against both published entries.
        #expect(workMeter.peakIntermediateRows == 3)
        #expect(workMeter.peakIntermediateBytes > 0)
        #expect(workMeter.retainedIntermediateRows == 2)
        #expect(workMeter.retainedIntermediateBytes > 0)
        #expect(results.count == 2)
        #expect(results[0].row.values[0] == .primitive(.int64(1)))
        #expect(
            results[0].origin == .derived(
                contributors: [
                    fixture.primaryBaseID,
                    fixture.secondaryBaseID,
                ].sorted()
            )
        )
        #expect(
            results[0].row.annotations["representative"] == .string("first")
        )
        #expect(results[0].row.version?.value == "version-1")
        #expect(results[1].row.values[0] == .primitive(.int64(2)))
        results.removeAll()
        output.withLock { $0.removeAll() }
        #expect(workMeter.retainedIntermediateRows == 0)
        #expect(workMeter.retainedIntermediateBytes == 0)
    }

    @Test("DISTINCT contributor replacement is admitted before publication")
    func distinctContributorReplacementAdmissionIsAtomic() async throws {
        let firstBaseID = try Base.ID("contributor-a")
        let secondBaseID = try Base.ID("contributor-b")
        let representativeRow = try priorityRow(1,
            annotations: ["representative": .string("first")],
            version: PersistableVersionToken("version-1")
        )
        let duplicateRow = try priorityRow(1,
            annotations: ["representative": .string("later")],
            version: PersistableVersionToken("version-2")
        )

        func meter(maximumBytes: UInt64) -> DatabaseWorkMeter {
            DatabaseWorkMeter(
                budget: ExecutionBudget(
                    maximumRows: 100,
                    maximumWorkUnits: 100_000,
                    maximumIntermediateRows: 16,
                    maximumIntermediateBytes: maximumBytes,
                    timeoutMilliseconds: 30_000
                ),
                monotonicClock: TestProcessMonotonicClock()
            )
        }

        func owned(
            _ row: QueryRow,
            workMeter: DatabaseWorkMeter
        ) throws -> DatabaseQueryScopedQueryRow {
            let footprint = try CanonicalRelationalFootprintMeter.footprint(
                of: row,
                workMeter: workMeter,
                stage: .resultMaterialization
            )
            return try DatabaseQueryScopedQueryRow.producing(
                exactFootprint: footprint,
                workMeter: workMeter,
                stage: .resultMaterialization
            ) { row }
        }

        let calibrationMeter = meter(maximumBytes: 1 * 1_024 * 1_024)
        let calibrationWorkspace = CompositionDistinctWorkspace.create(
            maximumIntermediateBytes: 1 * 1_024 * 1_024,
            workMeter: calibrationMeter
        )
        try await calibrationWorkspace.insert(
            try owned(representativeRow, workMeter: calibrationMeter),
            origin: .source(firstBaseID),
            sequence: 0
        )
        try await calibrationWorkspace.insert(
            try owned(duplicateRow, workMeter: calibrationMeter),
            origin: .source(secondBaseID),
            sequence: 1
        )
        let successfulPeak = calibrationMeter.peakIntermediateBytes
        let calibrationOutput = Mutex<
            [CompositionDistinctWorkspace.Result]
        >([])
        try await calibrationWorkspace.forEachResult(batchSize: 1) { result in
            calibrationOutput.withLock { $0.append(result) }
            return true
        }
        var calibrationResults = calibrationOutput.withLock { $0 }
        #expect(calibrationResults.count == 1)
        #expect(
            calibrationResults.first?.origin == .derived(
                contributors: [firstBaseID, secondBaseID]
            )
        )
        #expect(
            calibrationResults.first?.row.annotations["representative"]
                == .string("first")
        )
        #expect(calibrationResults.first?.row.version?.value == "version-1")
        let retainedWithCalibrationResults = calibrationMeter.retainedIntermediateBytes
        await calibrationWorkspace.removeAll()
        #expect(calibrationMeter.retainedIntermediateBytes == retainedWithCalibrationResults)
        #expect(successfulPeak > 0)
        #expect(calibrationMeter.retainedIntermediateRows == 1)
        #expect(calibrationMeter.retainedIntermediateBytes > 0)
        calibrationResults.removeAll()
        calibrationOutput.withLock { $0.removeAll() }
        #expect(calibrationMeter.retainedIntermediateRows == 0)
        #expect(calibrationMeter.retainedIntermediateBytes == 0)

        let constrainedMeter = meter(maximumBytes: successfulPeak - 1)
        let constrainedWorkspace = CompositionDistinctWorkspace.create(
            maximumIntermediateBytes: 1 * 1_024 * 1_024,
            workMeter: constrainedMeter
        )
        try await constrainedWorkspace.insert(
            try owned(representativeRow, workMeter: constrainedMeter),
            origin: .source(firstBaseID),
            sequence: 0
        )
        await #expect(throws: DatabaseWorkLimitError.self) {
            try await constrainedWorkspace.insert(
                try owned(duplicateRow, workMeter: constrainedMeter),
                origin: .source(secondBaseID),
                sequence: 1
            )
        }
        let output = Mutex<[CompositionDistinctWorkspace.Result]>([])
        try await constrainedWorkspace.forEachResult(batchSize: 1) { result in
            output.withLock { $0.append(result) }
            return true
        }
        var results = output.withLock { $0 }
        #expect(results.count == 1)
        #expect(results.first?.origin == .source(firstBaseID))
        #expect(
            results.first?.row.annotations["representative"]
                == .string("first")
        )
        #expect(results.first?.row.version?.value == "version-1")
        let retainedWithConstrainedResults = constrainedMeter.retainedIntermediateBytes
        await constrainedWorkspace.removeAll()
        #expect(constrainedMeter.retainedIntermediateBytes == retainedWithConstrainedResults)
        #expect(constrainedMeter.retainedIntermediateRows == 1)
        #expect(constrainedMeter.retainedIntermediateBytes > 0)
        results.removeAll()
        output.withLock { $0.removeAll() }
        #expect(constrainedMeter.retainedIntermediateRows == 0)
        #expect(constrainedMeter.retainedIntermediateBytes == 0)
    }

    @Test("Cancelled DISTINCT enumeration releases every admitted claim")
    func cancelledDistinctInsertionReleasesClaims() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        try await insert(
            [("shared", 1), ("primary", 3)],
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )
        try await insert(
            [("shared", 2), ("secondary", 4)],
            baseID: fixture.secondaryBaseID,
            fixture: fixture
        )
        let readContext = ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: 1,
                budget: ExecutionBudget(
                    maximumRows: 100,
                    maximumWorkUnits: 100_000,
                    maximumIntermediateRows: 100,
                    maximumIntermediateBytes: 4 * 1_024 * 1_024,
                    timeoutMilliseconds: 30_000
                )
            ),
            monotonicClock: fixture.container.monotonicClock
        )
        let source = fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(fixture.compositionID)
        let probe = CancellationRowProbe()

        await #expect(throws: CancellationError.self) {
            try await CompositionQueryPlanner(
                structuralLimits: readContext.queryStructuralLimits
            ).execute(
                SelectQuery(
                    projection: [.wildcard()],
                    source: .table(TableRef(Item.persistableType)),
                    quantifier: .distinct
                ),
                source: source,
                options: CompositionQueryExecutionOptions(
                    pageSize: 1,
                    readContext: readContext
                )
            ) { event in
                try await probe.receive(
                    event,
                    workMeter: readContext.workMeter
                )
            }
        }
        #expect(await probe.receivedRowCount() == 1)
        let distinctCancellationClaim = await probe
            .retainedClaimDuringCancellation()
        #expect((distinctCancellationClaim.rows ?? 0) > 0)
        #expect((distinctCancellationClaim.bytes ?? 0) > 0)
        #expect(readContext.workMeter.retainedIntermediateRows == 0)
        #expect(readContext.workMeter.retainedIntermediateBytes == 0)

        let aggregateContext = ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: 1,
                budget: ExecutionBudget(
                    maximumRows: 100,
                    maximumWorkUnits: 100_000,
                    maximumIntermediateRows: 8,
                    maximumIntermediateBytes: 4 * 1_024 * 1_024,
                    timeoutMilliseconds: 30_000
                )
            ),
            monotonicClock: fixture.container.monotonicClock
        )
        let aggregateProbe = CancellationRowProbe()
        await #expect(throws: CancellationError.self) {
            try await CompositionQueryPlanner(
                structuralLimits: aggregateContext.queryStructuralLimits
            ).execute(
                SelectQuery(
                    projection: [
                        ProjectionItem(
                            .aggregate(
                                .min(.column(ColumnRef(column: "id")))
                            ),
                            alias: "minimum"
                        ),
                        ProjectionItem(
                            .aggregate(
                                .max(.column(ColumnRef(column: "id")))
                            ),
                            alias: "maximum"
                        ),
                    ],
                    source: .table(TableRef(Item.persistableType))
                ),
                source: source,
                options: CompositionQueryExecutionOptions(
                    pageSize: 1,
                    readContext: aggregateContext
                )
            ) { event in
                try await aggregateProbe.receive(
                    event,
                    workMeter: aggregateContext.workMeter
                )
            }
        }
        #expect(await aggregateProbe.receivedRowCount() == 1)
        let aggregateCancellationClaim = await aggregateProbe
            .retainedClaimDuringCancellation()
        #expect(aggregateCancellationClaim.rows == 1)
        #expect((aggregateCancellationClaim.bytes ?? 0) > 0)
        #expect(aggregateContext.workMeter.retainedIntermediateRows == 0)
        #expect(aggregateContext.workMeter.retainedIntermediateBytes == 0)
    }

    private struct Fixture: Sendable {
        let container: DBContainer
        let primaryBaseID: Base.ID
        let secondaryBaseID: Base.ID
        let compositionID: Base.Composition.ID
        let ownerAuthorization: AuthorizationContext
        let readerAuthorization: AuthorizationContext
        let secondaryEngine: InMemoryEngine
    }

    private func priorityRow(
        _ value: Int64, annotations: [String: FieldValue] = [:],
        version: PersistableVersionToken? = nil
    ) throws -> QueryRow {
        try QueryRow(values: [.primitive(.int64(value))], columns: [
            QueryColumn(number: 1, name: "priority", type: .primitive(.int64), isNullable: false),
        ], annotations: annotations, version: version)
    }

    private func makeFixture(
        readerCanReadSecondary: Bool,
        secondarySharesControlDomain: Bool = false,
        wallClock: any WallClock = FixedTestWallClock()
    ) async throws -> Fixture {
        let controlDomainID = try DatabaseStorageDomain.ID("control")
        let secondaryDomainID = try DatabaseStorageDomain.ID("secondary")
        let primaryPlacementID = try Base.Placement.ID("primary")
        // Section 14 addresses a Base Partition at `bases/<Base.ID>` below the
        // database root of its domain, so one domain has exactly one placement
        // destination. Two Bases share a domain by sharing its placement.
        let secondaryPlacementID = secondarySharesControlDomain
            ? primaryPlacementID
            : try Base.Placement.ID("secondary")
        let primaryBaseID = try Base.ID("company-a")
        let secondaryBaseID = try Base.ID("company-b")
        let compositionID = try Base.Composition.ID("shared")
        let owner = Principal(identifier: "owner")
        let reader = Principal(identifier: "reader")
        let controlEngine = InMemoryEngine()
        let secondaryEngine = secondarySharesControlDomain
            ? controlEngine
            : InMemoryEngine()
        var domains = [
            try DatabaseStorageDomain(
                id: controlDomainID,
                rootPath: ["tests", "composition", "control"],
                storageEngine: controlEngine
            )
        ]
        if !secondarySharesControlDomain {
            domains.append(
                try DatabaseStorageDomain(
                    id: secondaryDomainID,
                    rootPath: ["tests", "composition", "secondary"],
                    storageEngine: secondaryEngine
                )
            )
        }
        var placements = [
            DatabaseStoragePlacement(
                id: primaryPlacementID,
                domainID: controlDomainID
            )
        ]
        if !secondarySharesControlDomain {
            placements.append(
                DatabaseStoragePlacement(
                    id: secondaryPlacementID,
                    domainID: secondaryDomainID
                )
            )
        }
        let topology = try DatabaseStorageTopology(
            controlDomainID: controlDomainID,
            domains: domains,
            placements: placements,
            defaultPlacementID: primaryPlacementID
        )
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try Item.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(
                name: "composition-typed-query",
                storageTopology: topology,
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: wallClock
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(Item.self)
                ]
            ),
            security: .testingDisabled
        )
        let ownerAuthorization: AuthorizationContext = .authenticated(owner)
        let readerAuthorization: AuthorizationContext = .authenticated(reader)
        do {
            _ = try await container.provisionBase(
                primaryBaseID,
                placementID: primaryPlacementID,
                initialGrants: [
                    Security.Grant(
                        subject: .principal(owner.identifier),
                        resource: .base(primaryBaseID),
                        access: .all
                    ),
                    Security.Grant(
                        subject: .principal(reader.identifier),
                        resource: .base(primaryBaseID),
                        access: .read
                    ),
                ],
                expectedRevision: 0
            )
            var secondaryGrants = [
                Security.Grant(
                    subject: .principal(owner.identifier),
                    resource: .base(secondaryBaseID),
                    access: .all
                )
            ]
            if readerCanReadSecondary {
                secondaryGrants.append(
                    Security.Grant(
                        subject: .principal(reader.identifier),
                        resource: .base(secondaryBaseID),
                        access: .read
                    )
                )
            }
            _ = try await container.provisionBase(
                secondaryBaseID,
                placementID: secondaryPlacementID,
                initialGrants: secondaryGrants,
                expectedRevision: 0
            )
            _ = try await container.withControlMetadataTransaction {
                transaction in
                try await container.compositionCatalog.create(
                    try Base.Composition(
                        id: compositionID,
                        bases: [primaryBaseID, secondaryBaseID]
                    ),
                    expectedRevision: 0,
                    transaction: transaction.storageAccess
                )
            }
        } catch {
            await container.shutdown()
            throw error
        }
        return Fixture(
            container: container,
            primaryBaseID: primaryBaseID,
            secondaryBaseID: secondaryBaseID,
            compositionID: compositionID,
            ownerAuthorization: ownerAuthorization,
            readerAuthorization: readerAuthorization,
            secondaryEngine: secondaryEngine
        )
    }

    private func insert(
        _ values: [(String, Int64)],
        baseID: Base.ID,
        fixture: Fixture
    ) async throws {
        let context = fixture.container.session(
            authorization: fixture.ownerAuthorization
        ).base(baseID).newContext()
        for value in values {
            var item = Item()
            item.id = value.0
            item.rank = value.1
            try context.insert(item)
        }
        try await context.save()
    }

    private func crossBaseQuery(
        fixture: Fixture,
        joinType: JoinType
    ) -> SelectQuery {
        SelectQuery(
            projection: [
                ProjectionItem(
                    .column(ColumnRef(table: "world", column: "rank")),
                    alias: "worldRank"
                ),
                ProjectionItem(
                    .column(ColumnRef(table: "tenant", column: "rank")),
                    alias: "tenantRank"
                ),
            ],
            source: .join(
                JoinClause(
                    type: joinType,
                    left: .base(
                        fixture.primaryBaseID,
                        .aliased(.table(TableRef(Item.persistableType)), name: "world")
                    ),
                    right: .base(
                        fixture.secondaryBaseID,
                        .aliased(.table(TableRef(Item.persistableType)), name: "tenant")
                    ),
                    condition: .on(
                        .equal(
                            .column(
                                ColumnRef(table: "world", column: "id")
                            ),
                            .column(
                                ColumnRef(table: "tenant", column: "id")
                            )
                        )
                    )
                )
            )
        )
    }

    private func executeCompositionQuery(
        _ query: SelectQuery,
        source: CompositionDataSource,
        maximumIntermediateRows: UInt32 = 100
    ) async throws -> [CompositionResult<QueryRow>] {
        let readOptions = ReadExecutionOptions(
            pageSize: 8,
            budget: ExecutionBudget(
                maximumRows: 100,
                maximumWorkUnits: 100_000,
                maximumIntermediateRows: maximumIntermediateRows,
                maximumIntermediateBytes: 4 * 1_024 * 1_024,
                timeoutMilliseconds: 30_000
            )
        )
        return try await source.execute(
            query,
            options: readOptions
        ).rows
    }
}
#endif
