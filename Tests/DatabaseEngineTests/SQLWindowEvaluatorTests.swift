import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Selected SQL window evaluator", .timeLimit(.minutes(1)))
struct SQLWindowEvaluatorTests {
    @Test func rankingsUsePeersAndOriginalOrder() async throws {
        let rows = [[2], [1], [1], [3]].map { $0.map(SQLWindowFixture.int) }
        let cases: [(SQLWindowFunction, [QueryValue])] = [
            (.rowNumber, [3, 1, 2, 4].map(SQLWindowFixture.bigint)),
            (.rank, [3, 1, 1, 4].map(SQLWindowFixture.bigint)),
            (.denseRank, [2, 1, 1, 3].map(SQLWindowFixture.bigint)),
            (.percentRank, [2.0 / 3, 0, 0, 1].map(SQLWindowFixture.floating)),
            (.cumeDist, [0.75, 0.5, 0.5, 1].map(SQLWindowFixture.floating))
        ]
        for (function, expected) in cases {
            #expect(try await SQLWindowFixture.run(SQLWindowFixture.plan(function), rows: rows).values.map(\.value) == expected)
        }
    }

    @Test func partitionsRemainIndependent() async throws {
        let rows = [[2, 2], [1, 1], [1, 2], [2, 1]].map { $0.map(SQLWindowFixture.int) }
        let plan = SQLWindowFixture.plan(.rank, partition: [SQLWindowFixture.column(1)])
        #expect(try await SQLWindowFixture.run(plan, rows: rows).values.map(\.value) == [2, 1, 1, 2].map(SQLWindowFixture.bigint))
    }

    @Test func ntileUsesPartitionCountAndCachesSelectedArgument() async throws {
        let plan = SQLWindowFixture.plan(.ntile, arguments: [SQLWindowFixture.column(1)])
        let rows = [[1, 3], [2, 0], [3, 0], [4, 0], [5, 0], [6, 0], [7, 0]].map { $0.map(SQLWindowFixture.int) }
        #expect(try await SQLWindowFixture.run(plan, rows: rows).values.map(\.value) == [1, 1, 1, 2, 2, 3, 3].map(SQLWindowFixture.int))
        let leadingNull = [[SQLWindowFixture.int(1), SQLWindowFixture.null], [SQLWindowFixture.int(2), SQLWindowFixture.int(2)], [SQLWindowFixture.int(3), SQLWindowFixture.int(0)]]
        #expect(try await SQLWindowFixture.run(plan, rows: leadingNull).values.map(\.value) == [SQLWindowFixture.null, SQLWindowFixture.int(1), SQLWindowFixture.int(1)])
    }

    @Test func lagLeadSignedNullOffsetsAndLazyDefaults() async throws {
        let rows = [[10, -1], [20, 0], [30, 1]].map { $0.map(SQLWindowFixture.int) }
        let fallback = SQLWindowFixture.column(2)
        let plan = SQLWindowFixture.plan(.lag, arguments: [SQLWindowFixture.column(0), SQLWindowFixture.column(1), fallback])
        #expect(try await SQLWindowFixture.run(plan, rows: rows, override: { _, expression in
            if expression.columnSlot == 2 { throw SQLWindowFixture.Failure.unselected }
            return nil
        }).values.map(\.value) == [20, 20, 20].map(SQLWindowFixture.int))
        let lead = SQLWindowFixture.plan(.lead, arguments: [SQLWindowFixture.column(0), SQLWindowFixture.constant(SQLWindowFixture.null), fallback], order: [])
        #expect(try await SQLWindowFixture.run(lead, rows: rows, override: { _, expression in
            if expression.columnSlot == 0 || expression.columnSlot == 2 { throw SQLWindowFixture.Failure.unselected }
            return nil
        }).values.allSatisfy { $0.value.isNull })
        let missing = SQLWindowFixture.plan(.lead, arguments: [SQLWindowFixture.column(0), SQLWindowFixture.constant(SQLWindowFixture.int(100)), SQLWindowFixture.column(1)])
        #expect(try await SQLWindowFixture.run(missing, rows: rows).values.map(\.value) == [-1, 0, 1].map(SQLWindowFixture.int))
    }

    @Test func defaultRangeIncludesEveryPeer() async throws {
        let rows = [[1, 10], [1, 20], [2, 30]].map { $0.map(SQLWindowFixture.int) }
        let plan = SQLWindowFixture.plan(.lastValue, arguments: [SQLWindowFixture.column(1)])
        #expect(try await SQLWindowFixture.run(plan, rows: rows).values.map(\.value) == [20, 20, 30].map(SQLWindowFixture.int))
        let unordered = SQLWindowFixture.plan(.lastValue, arguments: [SQLWindowFixture.column(1)], order: [])
        #expect(try await SQLWindowFixture.run(unordered, rows: rows).values.map(\.value) == [30, 30, 30].map(SQLWindowFixture.int))
    }

    @Test func rowFramesClipAndReturnNullWhenEmpty() async throws {
        let rows = [[1], [2], [3]].map { $0.map(SQLWindowFixture.int) }
        let one = SQLWindowFixture.constant(SQLWindowFixture.bigint(1), type: .bigint)
        let frame = SQLWindowFixture.frame(.rows, start: .preceding(one.expression), end: .preceding(one.expression), startOffset: one, endOffset: one)
        let plan = SQLWindowFixture.plan(.firstValue, arguments: [SQLWindowFixture.column(0)], frame: frame)
        #expect(try await SQLWindowFixture.run(plan, rows: rows).values.map(\.value) == [SQLWindowFixture.null, SQLWindowFixture.int(1), SQLWindowFixture.int(2)])
    }

    @Test func groupsFrameMovesByPeers() async throws {
        let rows = [[1, 10], [1, 20], [2, 30], [3, 40], [3, 50]].map { $0.map(SQLWindowFixture.int) }
        let one = SQLWindowFixture.constant(SQLWindowFixture.bigint(1), type: .bigint)
        let frame = SQLWindowFixture.frame(.groups, start: .preceding(one.expression), end: .preceding(one.expression), startOffset: one, endOffset: one)
        let plan = SQLWindowFixture.plan(.lastValue, arguments: [SQLWindowFixture.column(1)], frame: frame)
        #expect(try await SQLWindowFixture.run(plan, rows: rows).values.map(\.value) == [SQLWindowFixture.null, SQLWindowFixture.null, SQLWindowFixture.int(20), SQLWindowFixture.int(30), SQLWindowFixture.int(30)])
    }

    @Test func allFrameExclusionsHaveDistinctMembership() async throws {
        let rows = [[1, 10], [1, 20], [2, 30]].map { $0.map(SQLWindowFixture.int) }
        let cases: [(WindowFrameExclusion, [QueryValue])] = [
            (.noOthers, [10, 10, 10].map(SQLWindowFixture.int)),
            (.currentRow, [20, 10, 10].map(SQLWindowFixture.int)),
            (.group, [SQLWindowFixture.null, SQLWindowFixture.null, SQLWindowFixture.int(10)]),
            (.ties, [10, 20, 10].map(SQLWindowFixture.int))
        ]
        for (exclusion, expected) in cases {
            let frame = SQLWindowFixture.frame(.range, start: .unboundedPreceding, end: .currentRow, exclusion: exclusion)
            #expect(try await SQLWindowFixture.run(SQLWindowFixture.plan(.firstValue, arguments: [SQLWindowFixture.column(1)], frame: frame), rows: rows).values.map(\.value) == expected)
        }
    }

    @Test func numericRangeUsesSelectedOffsetsAndDescendingOrder() async throws {
        let rows = [[1], [3], [4], [8]].map { $0.map(SQLWindowFixture.int) }
        let two = SQLWindowFixture.constant(SQLWindowFixture.int(2))
        let frame = SQLWindowFixture.frame(.range, start: .preceding(two.expression), end: .currentRow, startOffset: two)
        #expect(try await SQLWindowFixture.run(SQLWindowFixture.plan(.firstValue, arguments: [SQLWindowFixture.column(0)], frame: frame), rows: rows).values.map(\.value) == [1, 1, 3, 8].map(SQLWindowFixture.int))
        let descending = [SQLWindowFixture.order(0, direction: .descending)]
        #expect(try await SQLWindowFixture.run(SQLWindowFixture.plan(.firstValue, arguments: [SQLWindowFixture.column(0)], order: descending, frame: frame), rows: rows).values.map(\.value) == [3, 4, 4, 8].map(SQLWindowFixture.int))
        for type in [DataType.real, .doublePrecision, .decimal(precision: nil, scale: nil)] {
            func selected(_ value: Int32) throws -> QueryValue {
                switch type {
                case .real: return .primitive(.float32(Float(value)))
                case .doublePrecision: return .primitive(.float64(Double(value)))
                default: return .numeric(try QueryNumericValue(value: ExactDecimal(coefficient: Int128(value), scale: 0), displayScale: 0))
                }
            }
            let offset = SQLWindowFixture.constant(try selected(2), type: type)
            let frame = SQLWindowFixture.frame(.range, start: .preceding(offset.expression), end: .currentRow, startOffset: offset)
            let plan = SQLWindowFixture.plan(.firstValue, arguments: [SQLWindowFixture.column(1)], order: [SQLWindowFixture.order(0, type: type)], frame: frame)
            let typedRows = try [1, 3, 4, 8].map { [try selected($0), SQLWindowFixture.int($0)] }
            #expect(try await SQLWindowFixture.run(plan, rows: typedRows).values.map(\.value) == [1, 1, 3, 8].map(SQLWindowFixture.int))
        }
    }

    @Test func numericRangeWideningAvoidsIntegerOverflow() async throws {
        let rows = [[SQLWindowFixture.bigint(Int64.max - 1)], [SQLWindowFixture.bigint(Int64.max)]]
        let offset = SQLWindowFixture.constant(SQLWindowFixture.bigint(10), type: .bigint)
        let frame = SQLWindowFixture.frame(.range, start: .currentRow, end: .following(offset.expression), endOffset: offset)
        let plan = SQLWindowFixture.plan(.lastValue, arguments: [SQLWindowFixture.column(0, type: .bigint)], order: [SQLWindowFixture.order(0, type: .bigint)], frame: frame)
        #expect(try await SQLWindowFixture.run(plan, rows: rows).values.map(\.value) == [SQLWindowFixture.bigint(Int64.max), SQLWindowFixture.bigint(Int64.max)])
    }

    @Test func nullOrderingAndRangeOffsetsStayWithinNullPeers() async throws {
        let rows = [[SQLWindowFixture.null, SQLWindowFixture.int(10)], [SQLWindowFixture.int(1), SQLWindowFixture.int(20)], [SQLWindowFixture.null, SQLWindowFixture.int(30)]]
        let one = SQLWindowFixture.constant(SQLWindowFixture.int(1))
        let frame = SQLWindowFixture.frame(.range, start: .preceding(one.expression), end: .currentRow, startOffset: one)
        let plan = SQLWindowFixture.plan(.lastValue, arguments: [SQLWindowFixture.column(1)], order: [SQLWindowFixture.order(0, nulls: .first)], frame: frame)
        #expect(try await SQLWindowFixture.run(plan, rows: rows).values.map(\.value) == [30, 20, 30].map(SQLWindowFixture.int))
    }

    @Test func dateRangeUsesCalendarInterval() async throws {
        let dates = try [(2024, 1, 31), (2024, 2, 29), (2024, 3, 1)].map { year, month, day in
            QueryValue.primitive(.date(try CivilDate(year: Int32(year), month: UInt8(month), day: UInt8(day))))
        }
        let interval = QueryValue.interval(try QueryIntervalValue(period: CalendarPeriod(months: 1), time: TimeSpan(seconds: 0)))
        let offset = SQLWindowFixture.constant(interval, type: .interval())
        let frame = SQLWindowFixture.frame(.range, start: .preceding(offset.expression), end: .currentRow, startOffset: offset)
        let plan = SQLWindowFixture.plan(.firstValue, arguments: [SQLWindowFixture.column(1)], order: [SQLWindowFixture.order(0, type: .date)], frame: frame)
        let rows = dates.enumerated().map { [$0.element, SQLWindowFixture.int(Int32($0.offset + 1))] }
        #expect(try await SQLWindowFixture.run(plan, rows: rows, environment: SQLWindowFixture.environment).values.map(\.value) == [1, 1, 2].map(SQLWindowFixture.int))
    }

    @Test func timeRangeNeverWrapsAtMidnight() async throws {
        let interval = QueryValue.interval(try QueryIntervalValue(period: CalendarPeriod(months: -2, days: 100), time: TimeSpan(seconds: 7200)))
        let offset = SQLWindowFixture.constant(interval, type: .interval())
        let frame = SQLWindowFixture.frame(.range, start: .preceding(offset.expression), end: .currentRow, startOffset: offset)
        let rows = try [0, 3600, 82800].enumerated().map { [QueryValue.time(try QueryTimeValue(microsecondsSinceMidnight: Int64($0.element) * 1_000_000)), SQLWindowFixture.int(Int32($0.offset + 1))] }
        let plan = SQLWindowFixture.plan(.firstValue, arguments: [SQLWindowFixture.column(1)], order: [SQLWindowFixture.order(0, type: .time(withTimeZone: false))], frame: frame)
        #expect(try await SQLWindowFixture.run(plan, rows: rows, environment: SQLWindowFixture.environment).values.map(\.value) == [1, 1, 3].map(SQLWindowFixture.int))
    }

    @Test func nthValueNullInvalidAndExclusion() async throws {
        let rows = [[1], [2], [3]].map { $0.map(SQLWindowFixture.int) }
        let frame = SQLWindowFixture.frame(.rows, start: .unboundedPreceding, end: .unboundedFollowing, exclusion: .currentRow)
        let plan = SQLWindowFixture.plan(.nthValue, arguments: [SQLWindowFixture.column(0), SQLWindowFixture.constant(SQLWindowFixture.int(2))], frame: frame)
        #expect(try await SQLWindowFixture.run(plan, rows: rows).values.map(\.value) == [3, 3, 2].map(SQLWindowFixture.int))
        let null = SQLWindowFixture.plan(.nthValue, arguments: [SQLWindowFixture.column(0), SQLWindowFixture.constant(SQLWindowFixture.null)])
        #expect(try await SQLWindowFixture.run(null, rows: rows).values.allSatisfy { $0.value.isNull })
        let invalid = SQLWindowFixture.plan(.nthValue, arguments: [SQLWindowFixture.column(0), SQLWindowFixture.constant(SQLWindowFixture.int(0))])
        await #expect(throws: DatabaseExpressionEvaluationError.self) { _ = try await SQLWindowFixture.run(invalid, rows: rows) }
        #expect(try await SQLWindowFixture.run(invalid, rows: []).values.isEmpty)
    }

    @Test func aggregateOverUsesSharedKernelAndFilter() async throws {
        let input = SQLWindowFixture.column(1)
        let filter = SQLWindowFixture.column(2, type: .boolean)
        let call = SQLAggregateCall(function: .sum, arguments: [input.expression], filter: filter.expression)
        let aggregate = CanonicalBoundAggregate(call: call, arguments: [input], directArguments: [], orderBy: [], filter: filter, withinGroup: [])
        let bound = CanonicalBoundExpression(expression: .aggregate(.sql(call)), type: .sql(.bigint), isNullable: true, children: [input, filter], aggregate: aggregate)
        let plan = CanonicalBoundWindow(call: WindowCall(function: .aggregate(call), reference: .specification(WindowSpecification())), arguments: [], aggregate: bound, partitionBy: [], orderBy: [SQLWindowFixture.order(0)], frame: nil, identity: 0)
        let rows: [[QueryValue]] = [[.primitive(.int32(1)), .primitive(.int32(10)), .primitive(.bool(true))], [.primitive(.int32(1)), .primitive(.int32(20)), .primitive(.bool(false))], [.primitive(.int32(2)), .primitive(.int32(30)), .primitive(.bool(true))]]
        #expect(try await SQLWindowFixture.run(plan, rows: rows).values.map(\.value) == [10, 10, 40].map(SQLWindowFixture.bigint))
        let counter = Mutex<Int32>(0)
        let plainCall = SQLAggregateCall(function: .sum, arguments: [input.expression])
        let plainSelected = CanonicalBoundAggregate(call: plainCall, arguments: [input], directArguments: [], orderBy: [], filter: nil, withinGroup: [])
        let plainBound = CanonicalBoundExpression(expression: .aggregate(.sql(plainCall)), type: .sql(.bigint), isNullable: true, children: [input], aggregate: plainSelected)
        func ordinary(_ frame: CanonicalBoundWindowFrame) -> CanonicalBoundWindow {
            CanonicalBoundWindow(call: WindowCall(function: .aggregate(plainCall), reference: .specification(WindowSpecification())), arguments: [], aggregate: plainBound, partitionBy: [], orderBy: [SQLWindowFixture.order(0)], frame: frame, identity: 0)
        }
        let sequenceRows = [[1, 0], [2, 0], [3, 0]].map { $0.map(SQLWindowFixture.int) }
        func next(_ row: Int, _ expression: CanonicalBoundExpression) -> QueryValue? {
            guard expression.columnSlot == 1 else { return nil }
            return counter.withLock { value in
                value += 1
                if expression.type == .sql(.doublePrecision) { return SQLWindowFixture.floating(Double(value)) }
                return SQLWindowFixture.int(value)
            }
        }
        let prefix = SQLWindowFixture.frame(.rows, start: .unboundedPreceding, end: .currentRow)
        #expect(try await SQLWindowFixture.run(ordinary(prefix), rows: sequenceRows, override: next).values.map(\.value) == [1, 3, 6].map(SQLWindowFixture.bigint))
        #expect(counter.withLock { $0 } == 3)
        counter.withLock { $0 = 0 }
        let one = SQLWindowFixture.constant(SQLWindowFixture.bigint(1), type: .bigint)
        let moving = SQLWindowFixture.frame(.rows, start: .preceding(one.expression), end: .currentRow, startOffset: one)
        #expect(try await SQLWindowFixture.run(ordinary(moving), rows: sequenceRows, override: next).values.map(\.value) == [1, 3, 7].map(SQLWindowFixture.bigint))
        #expect(counter.withLock { $0 } == 4)
        counter.withLock { $0 = 0 }
        let excluding = SQLWindowFixture.frame(.rows, start: .unboundedPreceding, end: .currentRow, exclusion: .currentRow)
        #expect(try await SQLWindowFixture.run(ordinary(excluding), rows: sequenceRows, override: next).values.map(\.value) == [SQLWindowFixture.null, SQLWindowFixture.bigint(1), SQLWindowFixture.bigint(5)])
        #expect(counter.withLock { $0 } == 3)
        counter.withLock { $0 = 0 }
        let pairInput = SQLWindowFixture.column(1, type: .doublePrecision)
        let pairCall = SQLAggregateCall(function: .regrCount, arguments: [pairInput.expression, pairInput.expression])
        let pairSelected = CanonicalBoundAggregate(call: pairCall, arguments: [pairInput, pairInput], directArguments: [], orderBy: [], filter: nil, withinGroup: [])
        let pairBound = CanonicalBoundExpression(expression: .aggregate(.sql(pairCall)), type: .sql(.bigint), isNullable: false, children: [pairInput, pairInput], aggregate: pairSelected)
        let pairPlan = CanonicalBoundWindow(call: WindowCall(function: .aggregate(pairCall), reference: .specification(WindowSpecification())), arguments: [], aggregate: pairBound, partitionBy: [], orderBy: [SQLWindowFixture.order(0)], frame: prefix, identity: 0)
        #expect(try await SQLWindowFixture.run(pairPlan, rows: sequenceRows, override: next).values.map(\.value) == [1, 2, 3].map(SQLWindowFixture.bigint))
        #expect(counter.withLock { $0 } == 6)
    }

    @Test func emptyInputStillValidatesFrameInitialization() async throws {
        for mode in [WindowFrameMode.rows, .groups, .range] {
            let offset = SQLWindowFixture.constant(SQLWindowFixture.null, type: mode == .range ? .integer : .bigint)
            let frame = SQLWindowFixture.frame(mode, start: .preceding(offset.expression), end: .currentRow, startOffset: offset)
            await #expect(throws: DatabaseExpressionEvaluationError.self) { _ = try await SQLWindowFixture.run(SQLWindowFixture.plan(.rowNumber, frame: frame), rows: []) }
        }
        for mode in [WindowFrameMode.rows, .groups] {
            let offset = SQLWindowFixture.constant(SQLWindowFixture.bigint(-1), type: .bigint)
            let frame = SQLWindowFixture.frame(mode, start: .preceding(offset.expression), end: .currentRow, startOffset: offset)
            await #expect(throws: DatabaseExpressionEvaluationError.self) { _ = try await SQLWindowFixture.run(SQLWindowFixture.plan(.rowNumber, frame: frame), rows: []) }
        }
        let offset = SQLWindowFixture.constant(SQLWindowFixture.int(-1))
        let frame = SQLWindowFixture.frame(.range, start: .preceding(offset.expression), end: .currentRow, startOffset: offset)
        #expect(try await SQLWindowFixture.run(SQLWindowFixture.plan(.firstValue, arguments: [SQLWindowFixture.column(0)], frame: frame), rows: []).values.isEmpty)
    }

    @Test func retainedClaimsReleaseOnSuccessFailureAndBudgetRejection() async throws {
        let rows = [[1], [2], [3]].map { $0.map(SQLWindowFixture.int) }
        let plan = SQLWindowFixture.plan(.rowNumber)
        let meter = SQLArrayFixture.meter()
        do {
            let result = try await SQLWindowFixture.run(plan, rows: rows, meter: meter)
            #expect(result.payloadLease.workMeter === meter)
            #expect(meter.retainedIntermediateBytes > 0)
        }
        #expect(meter.retainedIntermediateBytes == 0)
        do {
            let payloadRows = [[SQLWindowFixture.int(1), QueryValue.primitive(.string(String(repeating: "x", count: 1024)))]]
            let rich = SQLWindowFixture.plan(.firstValue, arguments: [SQLWindowFixture.column(1, type: .text)])
            let result = try await SQLWindowFixture.run(rich, rows: payloadRows, meter: meter)
            #expect(result.values.first?.value == payloadRows[0][1])
            #expect(meter.retainedIntermediateBytes >= 1024)
        }
        #expect(meter.retainedIntermediateBytes == 0)
        await #expect(throws: SQLWindowFixture.Failure.self) {
            _ = try await SQLWindowFixture.run(plan, rows: rows, meter: meter, override: { _, _ in throw SQLWindowFixture.Failure.unselected })
        }
        #expect(meter.retainedIntermediateBytes == 0)
        for limited in [SQLArrayFixture.meter(bytes: 8), SQLArrayFixture.meter(work: 1)] {
            await #expect(throws: DatabaseWorkLimitError.self) { _ = try await SQLWindowFixture.run(plan, rows: rows, meter: limited) }
            #expect(limited.retainedIntermediateBytes == 0)
        }
    }

    @Test func cancellationRejectsWithoutRetainingWorkspace() async throws {
        let meter = SQLArrayFixture.meter()
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try await SQLWindowFixture.run(SQLWindowFixture.plan(.rank), rows: [[SQLWindowFixture.int(1)]], meter: meter)
        }
        await #expect(throws: (any Error).self) { _ = try await task.value }
        #expect(meter.retainedIntermediateBytes == 0)
    }
}

private enum SQLWindowFixture {
    enum Failure: Error { case unselected }
    static var null: QueryValue { .primitive(.null) }
    static func int(_ value: Int32) -> QueryValue { .primitive(.int32(value)) }
    static func bigint(_ value: Int64) -> QueryValue { .primitive(.int64(value)) }
    static func floating(_ value: Double) -> QueryValue { .primitive(.float64(value)) }
    static func column(_ slot: Int, type: DataType = .integer) -> CanonicalBoundExpression {
        .init(expression: .literal(.null), type: .sql(type), isNullable: true, columnSlot: slot)
    }
    static func constant(_ value: QueryValue, type: DataType = .integer) -> CanonicalBoundExpression {
        .init(expression: .literal(.queryValue(value, type: .sql(type))), type: .sql(type), isNullable: value.isNull)
    }
    static func order(_ slot: Int, type: DataType = .integer, direction: SortDirection = .ascending,
        nulls: NullOrdering? = nil) -> CanonicalBoundKey {
        let expression = column(slot, type: type)
        return CanonicalBoundKey(expression: expression, sortKey: SortKey(expression.expression, direction: direction, nulls: nulls))
    }
    static func plan(_ function: SQLWindowFunction, arguments: [CanonicalBoundExpression] = [],
        partition: [CanonicalBoundExpression] = [], order: [CanonicalBoundKey]? = nil,
        frame: CanonicalBoundWindowFrame? = nil) -> CanonicalBoundWindow {
        CanonicalBoundWindow(call: WindowCall(function: .analytic(function, arguments: arguments.map(\.expression)), reference: .specification(WindowSpecification())), arguments: arguments, aggregate: nil, partitionBy: partition, orderBy: order ?? [self.order(0)], frame: frame, identity: 0)
    }
    static func frame(_ mode: WindowFrameMode, start: WindowFrameBound, end: WindowFrameBound,
        startOffset: CanonicalBoundExpression? = nil, endOffset: CanonicalBoundExpression? = nil,
        exclusion: WindowFrameExclusion = .noOthers) -> CanonicalBoundWindowFrame {
        CanonicalBoundWindowFrame(mode: mode, start: start, end: end, startOffset: startOffset, endOffset: endOffset, exclusion: exclusion)
    }
    static func run(_ plan: CanonicalBoundWindow, rows: [[QueryValue]], meter: DatabaseWorkMeter? = nil,
        environment: SQLTemporalEnvironment? = nil,
        override: ((Int, CanonicalBoundExpression) throws -> QueryValue?)? = nil) async throws -> SQLWindowResults {
        let meter = meter ?? SQLArrayFixture.meter()
        func value(_ row: Int, _ expression: CanonicalBoundExpression) throws -> DatabaseContext.ProjectedQueryValue {
            let result: QueryValue
            if let selected = try override?(row, expression) { result = selected }
            else if let slot = expression.columnSlot { result = rows[row][slot] }
            else if case .literal(.queryValue(let selected, _)) = expression.expression { result = selected }
            else { throw Failure.unselected }
            return .init(value: result, leases: [], payloadIsRetained: false)
        }
        return try await SQLWindowEvaluator.evaluate(plan: plan, rowCount: rows.count,
            evaluate: { try value($0, $1) }, direct: { try value(0, $0) }, workMeter: meter,
            temporalEnvironment: environment, limits: SQLArrayFixture.limits)
    }
    private struct Clock: WallClock { var now: Timestamp { Timestamp(secondsSinceUnixEpoch: 0) } }
    static var environment: SQLTemporalEnvironment {
        SQLTemporalEnvironment(transactionInstant: Timestamp(secondsSinceUnixEpoch: 0),
            statementInstant: Timestamp(secondsSinceUnixEpoch: 0), wallClock: Clock())
    }
}
