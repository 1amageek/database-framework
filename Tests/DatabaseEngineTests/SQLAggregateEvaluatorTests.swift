import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Selected SQL aggregate evaluator", .timeLimit(.minutes(1)))
struct SQLAggregateEvaluatorTests {
    private typealias Fixture = SQLAggregateFixture

    @Test func countAndEmptyIdentities() async throws {
        let rows = [[Fixture.int(1)], [Fixture.null], [Fixture.int(2)]]
        #expect(try await Fixture.run(.count, types: [], result: .bigint, rows: [[], [], []]).value == .primitive(.int64(3)))
        #expect(try await Fixture.run(.count, types: [.integer], result: .bigint, rows: rows).value == .primitive(.int64(2)))
        for function: SQLAggregateFunction in [.sum, .avg, .min, .max, .boolAnd, .bitAnd, .arrayAgg, .jsonAgg] {
            let type: DataType = function == .boolAnd ? .boolean : .integer
            let result: DataType = function == .avg ? .decimal(precision: nil, scale: nil) :
                (function == .arrayAgg ? .array(.integer) : function == .jsonAgg ? .json : .bigint)
            #expect(try await Fixture.run(function, types: [type], result: result, rows: []).value.isNull)
        }
    }

    @Test func filterPrecedesArgumentsAndOrderingReusesArguments() async throws {
        let meter = SQLArrayFixture.meter()
        let calls = Mutex([Int](repeating: 0, count: 3))
        let bound = Fixture.bound(.arrayAgg, types: [.integer], result: .array(.integer),
            order: .descending, distinct: true, filter: true)
        let result = try await SQLAggregateEvaluator.evaluate(bound: bound, rowCount: 4,
            evaluate: { row, expression in
                let slot = expression.columnSlot ?? 0
                calls.withLock { $0[slot] += 1 }
                if slot == 2 { return Fixture.cell(.primitive(.bool(row != 0))) }
                if row == 0 { throw DatabaseExpressionEvaluationError.divisionByZero }
                return Fixture.cell(Fixture.int(row == 3 ? 1 : Int32(row)))
            }, direct: { _ in throw DatabaseExpressionEvaluationError.unboundParameter },
            temporalEnvironment: nil, workMeter: meter, limits: SQLArrayFixture.limits)
        #expect(try SQLArrayFixture.cells(result.value) == [Fixture.int(2), Fixture.int(1)])
        #expect(calls.withLock { $0 } == [3, 0, 4])
    }

    @Test func numericSumAndAverageUseSelectedTransitions() async throws {
        let rows = [[Fixture.int(1)], [Fixture.null], [Fixture.int(2)]]
        #expect(try await Fixture.run(.sum, types: [.integer], result: .bigint, rows: rows).value == .primitive(.int64(3)))
        let result = try await Fixture.run(.avg, types: [.integer], result: Fixture.numeric, rows: rows)
        let expectedAverage = try Fixture.number(15, scale: 1, display: 16)
        #expect(result.value == expectedAverage)
    }

    @Test func extremaPreserveCharacterComparisonAndPayload() async throws {
        let rows = [[Fixture.text("b")], [Fixture.text("a ")], [Fixture.text("a")], [Fixture.null]]
        let meter = SQLArrayFixture.meter()
        do {
            let result = try await Fixture.run(.min, types: [.char(length: nil)], result: .char(length: nil), rows: rows, meter: meter)
            #expect(result.value == Fixture.text("a "))
            #expect(result.payloadIsRetained && !result.leases.isEmpty)
            #expect(meter.retainedIntermediateBytes > 0)
        }
        #expect(meter.retainedIntermediateBytes == 0)
        #expect(try await Fixture.run(.max, types: [.char(length: nil)], result: .char(length: nil), rows: rows).value == Fixture.text("b"))
    }

    @Test func booleanAndBitFamiliesRetainSelectedWidth() async throws {
        let booleans = [[Fixture.null], [.primitive(.bool(true))], [.primitive(.bool(false))]]
        for function: SQLAggregateFunction in [.boolAnd, .every, .boolOr] {
            #expect(try await Fixture.run(function, types: [.boolean], result: .boolean, rows: booleans).value == .primitive(.bool(function == .boolOr)))
            #expect(try await Fixture.run(function, types: [.boolean], result: .boolean, rows: [[Fixture.null]]).value.isNull)
        }
        for (type, left, right): (DataType, QueryValue, QueryValue) in [
            (.smallint, .primitive(.int16(6)), .primitive(.int16(3))),
            (.integer, Fixture.int(6), Fixture.int(3)),
            (.bigint, .primitive(.int64(6)), .primitive(.int64(3)))] {
            for (function, expected): (SQLAggregateFunction, Int64) in [(.bitAnd, 2), (.bitOr, 7), (.bitXor, 5)] {
                let value = try await Fixture.run(function, types: [type], result: type, rows: [[left], [Fixture.null], [right]]).value
                #expect(try CanonicalSQLNumericEvaluator.integer(value) == expected)
                #expect(QueryValueValidator.accepts(value, type: .sql(type)))
            }
        }
    }

    @Test func stringAndByteaUsePerRowNullableDelimiters() async throws {
        let rows = [[Fixture.text("a"), Fixture.text("ignored")], [Fixture.null, Fixture.text("!")],
                    [Fixture.text("b"), Fixture.null], [Fixture.text("c"), Fixture.text("/")]]
        #expect(try await Fixture.run(.stringAgg, types: [.text, .text], result: .text, rows: rows).value == Fixture.text("ab/c"))
        let binary = try rows.map { row in try row.map { value -> QueryValue in
            if value.isNull { return value }
            guard case .primitive(.string(let text)) = value else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: "fixture") }
            return .primitive(.bytes(ByteString(Array(text.utf8))))
        } }
        let result = try await Fixture.run(.stringAgg, types: [.blob, .blob], result: .blob, rows: binary)
        #expect(result.value == .primitive(.bytes(ByteString(Array("ab/c".utf8)))))
        #expect(try await Fixture.run(.stringAgg, types: [.text, .text], result: .text, rows: [[Fixture.null, Fixture.text(",")]]).value.isNull)
    }

    @Test func scalarAndNestedArraysPreserveBoundsAndRejectInvalidShape() async throws {
        let scalar = try await Fixture.run(.arrayAgg, types: [.integer], result: .array(.integer), rows: [[Fixture.int(2)], [Fixture.null], [Fixture.int(1)]])
        #expect(try SQLArrayFixture.cells(scalar.value) == [Fixture.int(2), Fixture.null, Fixture.int(1)])
        let first = try SQLArrayFixture.array([1, 2], lower: -2), second = try SQLArrayFixture.array([3, 4], lower: -2)
        let nested = try await Fixture.run(.arrayAgg, types: [.array(.integer)], result: .array(.integer), rows: [[first], [second]])
        #expect(try SQLArrayFixture.shape(nested.value).map(\.lowerBound) == [1, -2])
        #expect(try SQLArrayFixture.shape(nested.value).map(\.length) == [2, 2])
        #expect(try SQLArrayFixture.cells(nested.value) == [1, 2, 3, 4].map(Fixture.int))
        for bad in [Fixture.null, try SQLArrayFixture.array([]), try SQLArrayFixture.array([1], lower: -2)] {
            do { _ = try await Fixture.run(.arrayAgg, types: [.array(.integer)], result: .array(.integer), rows: [[first], [bad]]); Issue.record("Expected invalid array aggregate input") }
            catch is DatabaseExpressionEvaluationError { }
        }
    }

    @Test func jsonAggregatesIncludeNullAndApplyObjectDuplicatePolicy() async throws {
        let rows = [[Fixture.int(1)], [Fixture.null]]
        let lexical = try await Fixture.run(.jsonAgg, types: [.integer], result: .json, rows: rows)
        guard case .json(let json) = lexical.value else { Issue.record("Expected JSON"); return }
        #expect(String(decoding: json.bytes, as: UTF8.self) == "[1, null]")
        let pairs = [[Fixture.text("a"), Fixture.int(1)], [Fixture.text("a"), Fixture.int(2)], [Fixture.text("b"), Fixture.null]]
        let object = try await Fixture.run(.jsonObjectAgg, types: [.text, .integer], result: .json, rows: pairs)
        guard case .json(let text) = object.value else { Issue.record("Expected JSON object"); return }
        #expect(String(decoding: text.bytes, as: UTF8.self) == "{ \"a\" : 1, \"a\" : 2, \"b\" : null }")
        let binary = try await Fixture.run(.jsonbObjectAgg, types: [.text, .integer], result: .jsonb, rows: pairs)
        guard case .jsonb(let value) = binary.value, case .object(let members) = value.root else { Issue.record("Expected JSONB object"); return }
        #expect(members.count == 2 && members[0].key == "a")
        let expectedMember = QueryJSONBNode.number(try QueryNumericValue(value: ExactDecimal(coefficient: 2, scale: 0), displayScale: 0))
        #expect(members[0].value == expectedMember)
        do { _ = try await Fixture.run(.jsonObjectAgg, types: [.text, .integer], result: .json, rows: [[Fixture.null, Fixture.int(1)]]); Issue.record("Expected NULL JSON key error") }
        catch let error as DatabaseExpressionEvaluationError { #expect(error == .collection(.nullJSONObjectKey(argument: 1))) }
    }

    @Test func intervalSumAverageAndInfinity() async throws {
        let a = QueryValue.interval(try SQLTemporalPrecision.interval(months: 1, days: 2, microseconds: 10))
        let b = QueryValue.interval(try SQLTemporalPrecision.interval(months: 2, days: 3, microseconds: 20))
        let expectedSum = QueryValue.interval(try SQLTemporalPrecision.interval(months: 3, days: 5, microseconds: 30))
        let expectedAverage = QueryValue.interval(try SQLTemporalPrecision.interval(months: 1, days: 17, microseconds: 43_200_000_015))
        #expect(try await Fixture.run(.sum, types: [.interval()], result: .interval(), rows: [[a], [b]]).value == expectedSum)
        #expect(try await Fixture.run(.avg, types: [.interval()], result: .interval(), rows: [[a], [b]]).value == expectedAverage)
        for function: SQLAggregateFunction in [.sum, .avg] {
            #expect(try await Fixture.run(function, types: [.interval()], result: .interval(), rows: [[a], [.intervalInfinity(.positive)]]).value == .intervalInfinity(.positive))
            do { _ = try await Fixture.run(function, types: [.interval()], result: .interval(), rows: [[.intervalInfinity(.positive)], [.intervalInfinity(.negative)]]); Issue.record("Expected opposite infinity failure") }
            catch let error as DatabaseExpressionEvaluationError { #expect(error == .temporalValueOutOfRange) }
        }
    }

    @Test func exactStatisticsAndAliasesMatchReferenceScale() async throws {
        let rows = try [1, 2, 3].map { [try Fixture.number(Int128($0))] } + [[Fixture.null]]
        let expected: [(SQLAggregateFunction, String, Int32)] = [
            (.varPop, "0.66666666666666666667", 20), (.varSamp, "1", 20), (.variance, "1", 20),
            (.stddevPop, "0.81649658092772603273", 20), (.stddevSamp, "1", 20), (.stddev, "1", 20)]
        for (function, text, scale) in expected {
            let result = try await Fixture.run(function, types: [Fixture.numeric], result: Fixture.numeric, rows: rows)
            let expectedValue = QueryValue.numeric(try QueryNumericValue(value: QueryNumericValue(sqlLexicalForm: text[...], maximumUTF8Count: UInt64(text.utf8.count)).value, displayScale: scale))
            #expect(result.value == expectedValue)
        }
        let large: Int128 = 100_000_000_000_000_000_000_000_000_000
        let wideRows = [[try Fixture.number(large + 1)], [try Fixture.number(large + 3)]]
        let expectedPopulation = try Fixture.number(1, display: 20)
        let expectedDeviation = QueryValue.numeric(try QueryNumericValue(value: QueryNumericValue(sqlLexicalForm: "1.4142135623730950", maximumUTF8Count: 18).value, displayScale: 16))
        #expect(try await Fixture.run(.varPop, types: [Fixture.numeric], result: Fixture.numeric, rows: wideRows).value == expectedPopulation)
        #expect(try await Fixture.run(.stddevSamp, types: [Fixture.numeric], result: Fixture.numeric, rows: wideRows).value == expectedDeviation)
    }

    @Test func statisticEmptySingletonAndFloatingSignatures() async throws {
        for function: SQLAggregateFunction in [.varPop, .varSamp, .variance, .stddevPop, .stddevSamp, .stddev] {
            #expect(try await Fixture.run(function, types: [.integer], result: Fixture.numeric, rows: []).value.isNull)
            let result = try await Fixture.run(function, types: [.integer], result: Fixture.numeric, rows: [[Fixture.int(7)]])
            #expect(result.value.isNull == !(function == .varPop || function == .stddevPop))
        }
        for type: DataType in [.real, .doublePrecision] {
            let values: [[QueryValue]] = type == .real ? [[.primitive(.float32(1))], [.primitive(.float32(2))], [.primitive(.float32(3))]] : [[.primitive(.float64(1))], [.primitive(.float64(2))], [.primitive(.float64(3))]]
            #expect(try await Fixture.run(.varSamp, types: [type], result: .doublePrecision, rows: values).value == .primitive(.float64(1)))
        }
    }

    @Test func pairwiseStatisticsAndDegenerateLines() async throws {
        let pair = { (y: Double?, x: Double?) in [y.map { QueryValue.primitive(.float64($0)) } ?? Fixture.null, x.map { QueryValue.primitive(.float64($0)) } ?? Fixture.null] }
        let rows = [pair(3, 1), pair(5, 2), pair(7, 3), pair(nil, 9), pair(9, nil)]
        let expected: [(SQLAggregateFunction, Double)] = [(.corr, 1), (.covarPop, 4.0 / 3), (.covarSamp, 2),
            (.regrAvgx, 2), (.regrAvgy, 5), (.regrSxx, 2), (.regrSxy, 4), (.regrSyy, 8),
            (.regrSlope, 2), (.regrIntercept, 1), (.regrR2, 1)]
        for (function, expected) in expected {
            #expect(try await Fixture.run(function, types: [.doublePrecision, .doublePrecision], result: .doublePrecision, rows: rows).value == .primitive(.float64(expected)))
        }
        #expect(try await Fixture.run(.regrCount, types: [.doublePrecision, .doublePrecision], result: .bigint, rows: rows).value == .primitive(.int64(3)))
        for function: SQLAggregateFunction in [.corr, .regrSlope, .regrIntercept, .regrR2] {
            #expect(try await Fixture.run(function, types: [.doublePrecision, .doublePrecision], result: .doublePrecision, rows: [pair(3, 1), pair(4, 1)]).value.isNull)
        }
        #expect(try await Fixture.run(.regrR2, types: [.doublePrecision, .doublePrecision], result: .doublePrecision, rows: [pair(3, 1), pair(3, 2)]).value == .primitive(.float64(1)))
        #expect(try await Fixture.run(.corr, types: [.doublePrecision, .doublePrecision], result: .doublePrecision, rows: [pair(3, 1), pair(3, 2)]).value.isNull)
    }

    @Test func scalarPercentilesModeAndEmptyFractionValidation() async throws {
        let rows = [40, 10, 30, 20].map { [QueryValue.primitive(.float64(Double($0))) ] } + [[Fixture.null]]
        for (function, expected): (SQLAggregateFunction, Double) in [(.percentileCont, 17.5), (.percentileDisc, 10)] {
            let result = try await Fixture.run(function, types: [.doublePrecision], result: .doublePrecision,
                rows: rows, fraction: .primitive(.float64(0.25)))
            #expect(result.value == .primitive(.float64(expected)))
        }
        for direction: SortDirection in [.ascending, .descending] {
            let result = try await Fixture.run(.mode, types: [.integer], result: .integer,
                rows: [1, 2, 1, 2].map { [Fixture.int(Int32($0))] }, direction: direction)
            #expect(result.value == Fixture.int(direction == .ascending ? 1 : 2))
        }
        do { _ = try await Fixture.run(.percentileCont, types: [.doublePrecision], result: .doublePrecision, rows: [], fraction: .primitive(.float64(2))); Issue.record("Expected invalid scalar fraction on empty input") }
        catch let error as DatabaseExpressionEvaluationError { #expect(error == .invalidFunctionArgument(function: "percentile_cont", reason: "fraction must be between zero and one")) }
    }

    @Test func percentileArraysPreserveShapeBoundsAndNulls() async throws {
        let fraction = try SQLArrayFixture.value([.primitive(.float64(0)), .primitive(.float64(0.25)), Fixture.null,
            .primitive(.float64(0.75)), .primitive(.float64(1))], element: .doublePrecision, dimensions: [5], lower: 0)
        let rows = [10.0, 20, 40].map { [QueryValue.primitive(.float64($0))] }
        let result = try await Fixture.run(.percentileCont, types: [.doublePrecision], result: .array(.doublePrecision), rows: rows, fraction: fraction)
        #expect(try SQLArrayFixture.shape(result.value).map(\.lowerBound) == [0])
        #expect(try SQLArrayFixture.cells(result.value) == [.primitive(.float64(10)), .primitive(.float64(15)), Fixture.null, .primitive(.float64(30)), .primitive(.float64(40))])
        let multidimensional = try SQLArrayFixture.value([.primitive(.float64(0)), .primitive(.float64(1)), Fixture.null, .primitive(.float64(0.5))], element: .doublePrecision, dimensions: [2, 2], lower: -3)
        let discrete = try await Fixture.run(.percentileDisc, types: [.doublePrecision], result: .array(.doublePrecision), rows: rows, fraction: multidimensional)
        #expect(try SQLArrayFixture.shape(discrete.value).map(\.length) == [2, 2])
        #expect(try SQLArrayFixture.shape(discrete.value).map(\.lowerBound) == [-3, 1])
        #expect(try SQLArrayFixture.cells(discrete.value) == [.primitive(.float64(10)), .primitive(.float64(40)), Fixture.null, .primitive(.float64(20))])
        #expect(try await Fixture.run(.percentileCont, types: [.doublePrecision], result: .array(.doublePrecision), rows: [], fraction: fraction).value.isNull)
    }

    @Test func richArrayInputsRetainNestedCompositeValues() async throws {
        let type = try QueryCompositeType(columns: [QueryColumn(number: 1, name: "payload", type: .sql(.text), isNullable: true)], limits: SQLArrayFixture.limits)
        let first = QueryValue.composite(try QueryCompositeValue(type: type, fields: [Fixture.text("first")], limits: SQLArrayFixture.limits))
        let second = QueryValue.composite(try QueryCompositeValue(type: type, fields: [Fixture.null], limits: SQLArrayFixture.limits))
        let meter = SQLArrayFixture.meter()
        do {
            let result = try await Fixture.run(.arrayAgg, types: [.composite(type)], result: .array(.composite(type)),
                rows: [[first], [Fixture.null], [second]], meter: meter)
            #expect(try SQLArrayFixture.cells(result.value) == [first, Fixture.null, second])
            #expect(result.payloadIsRetained && meter.retainedIntermediateBytes > 0)
        }
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test func intervalPercentileUsesTemporalInterpolation() async throws {
        let first = QueryValue.interval(try SQLTemporalPrecision.interval(months: 0, days: 0, microseconds: 0))
        let last = QueryValue.interval(try SQLTemporalPrecision.interval(months: 2, days: 0, microseconds: 0))
        let result = try await Fixture.run(.percentileCont, types: [.interval()], result: .interval(),
            rows: [[first], [last]], fraction: .primitive(.float64(0.25)), environment: Fixture.environment())
        let expectedInterval = QueryValue.interval(try SQLTemporalPrecision.interval(months: 0, days: 15, microseconds: 0))
        #expect(result.value == expectedInterval)
    }

    @Test func orderedSetDirectArgumentIsEvaluatedOnceOnEmptyInput() async throws {
        let calls = Mutex(0)
        let bound = Fixture.bound(.percentileCont, types: [.doublePrecision], result: .doublePrecision,
            fraction: .primitive(.float64(0.5)))
        let result = try await SQLAggregateEvaluator.evaluate(bound: bound, rowCount: 0,
            evaluate: { _, _ in throw DatabaseExpressionEvaluationError.unboundParameter },
            direct: { _ in calls.withLock { $0 += 1 }; return Fixture.cell(.primitive(.float64(0.5))) },
            temporalEnvironment: nil, workMeter: SQLArrayFixture.meter(), limits: SQLArrayFixture.limits)
        #expect(result.value.isNull && calls.withLock { $0 } == 1)
    }

    @Test func outputCollectionLimitFailsAndReleasesScratch() async throws {
        let bound = Fixture.bound(.arrayAgg, types: [.integer], result: .array(.integer))
        let meter = SQLArrayFixture.meter()
        let limits = QueryValueLimits(maximumRetainedBytes: 1_000_000, maximumTotalNodes: 100,
            maximumNestingDepth: 10, maximumCollectionElements: 1)
        do {
            _ = try await SQLAggregateEvaluator.evaluate(bound: bound, rowCount: 2,
                evaluate: { row, _ in Fixture.cell(Fixture.int(Int32(row))) },
                direct: { _ in throw DatabaseExpressionEvaluationError.unboundParameter },
                temporalEnvironment: nil, workMeter: meter, limits: limits)
            Issue.record("Expected output collection limit")
        } catch is QueryValueError { }
        #expect(meter.retainedIntermediateBytes == 0 && meter.retainedIntermediateRows == 0)
    }

    @Test func resourceFailureAndCancellationReleaseInputClaims() async throws {
        for meter in [SQLArrayFixture.meter(bytes: 10), SQLArrayFixture.meter(work: 3)] {
            do { _ = try await Fixture.run(.arrayAgg, types: [.text], result: .array(.text), rows: [[Fixture.text("payload")]], meter: meter); Issue.record("Expected metered failure") }
            catch is DatabaseWorkLimitError { }
            #expect(meter.retainedIntermediateBytes == 0)
        }
        let meter = SQLArrayFixture.meter()
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            _ = try await Fixture.run(.arrayAgg, types: [.integer], result: .array(.integer), rows: [[Fixture.int(1)]], meter: meter)
        }
        do { try await task.value; Issue.record("Expected cancellation") }
        catch is CancellationError { }
        #expect(meter.retainedIntermediateBytes == 0)
    }
}

private enum SQLAggregateFixture {
    typealias Cell = DatabaseContext.ProjectedQueryValue
    static let null = QueryValue.primitive(.null)
    static let numeric = DataType.decimal(precision: nil, scale: nil)
    static func int(_ value: Int32) -> QueryValue { .primitive(.int32(value)) }
    static func text(_ value: String) -> QueryValue { .primitive(.string(value)) }
    static func cell(_ value: QueryValue) -> Cell { Cell(value: value, leases: [], payloadIsRetained: false) }
    static func number(_ coefficient: Int128, scale: Int32 = 0, display: Int32? = nil) throws -> QueryValue {
        .numeric(try QueryNumericValue(value: ExactDecimal(coefficient: coefficient, scale: scale), displayScale: display ?? max(0, scale)))
    }
    static func bound(_ function: SQLAggregateFunction, types: [DataType], result: DataType,
        order: SortDirection? = nil, distinct: Bool = false, filter: Bool = false,
        fraction: QueryValue? = nil, direction: SortDirection = .ascending) -> CanonicalBoundExpression {
        let ordered = function == .percentileCont || function == .percentileDisc || function == .mode
        let inputs = types.enumerated().map { index, type in CanonicalBoundExpression(expression: .col("c\(index)"), type: .sql(type), isNullable: true, columnSlot: index) }
        let direct: [CanonicalBoundExpression]
        if let fraction {
            let type: DataType
            if case .array = fraction { type = .array(.doublePrecision) } else { type = .doublePrecision }
            direct = [CanonicalBoundExpression(expression: .literal(.queryValue(fraction, type: .sql(type))), type: .sql(type), isNullable: true)]
        } else { direct = [] }
        let arguments = ordered ? direct : inputs
        let orderKeys: [CanonicalBoundKey] = order.map { order in
            [CanonicalBoundKey(expression: inputs[0], projectionIndex: 0, sortKey: SortKey(inputs[0].expression, direction: order))]
        } ?? []
        let within: [CanonicalBoundKey] = ordered ? [CanonicalBoundKey(expression: inputs[0], sortKey: SortKey(inputs[0].expression, direction: direction))] : []
        let predicate = filter ? CanonicalBoundExpression(expression: .col("keep"), type: .sql(.boolean), isNullable: true, columnSlot: 2) : nil
        let call = SQLAggregateCall(function: function, arguments: arguments.map(\.expression), distinct: distinct,
            orderBy: orderKeys.compactMap(\.sortKey), filter: predicate?.expression, withinGroup: ordered ? within.compactMap(\.sortKey) : nil)
        let selected = CanonicalBoundAggregate(call: call, arguments: arguments, directArguments: ordered ? direct : [],
            orderBy: orderKeys, filter: predicate, withinGroup: within)
        return CanonicalBoundExpression(expression: .aggregate(.sql(call)), type: .sql(result), isNullable: function != .count && function != .regrCount,
            children: arguments + orderKeys.map(\.expression) + (predicate.map { [$0] } ?? []) + within.map(\.expression), aggregate: selected)
    }
    private struct Clock: WallClock { let now: Timestamp }
    static func environment() -> SQLTemporalEnvironment {
        let instant = Timestamp(secondsSinceUnixEpoch: 0)
        return SQLTemporalEnvironment(transactionInstant: instant, statementInstant: instant, wallClock: Clock(now: instant))
    }
    static func run(_ function: SQLAggregateFunction, types: [DataType], result: DataType,
        rows: [[QueryValue]], meter: DatabaseWorkMeter? = nil, fraction: QueryValue? = nil,
        direction: SortDirection = .ascending, environment: SQLTemporalEnvironment? = nil) async throws -> Cell {
        let selected = bound(function, types: types, result: result, fraction: fraction, direction: direction)
        return try await SQLAggregateEvaluator.evaluate(bound: selected, rowCount: rows.count,
            evaluate: { row, expression in
                guard let index = expression.columnSlot, rows[row].indices.contains(index) else { throw DatabaseExpressionEvaluationError.unboundParameter }
                return cell(rows[row][index])
            }, direct: { expression in
                guard case .literal(.queryValue(let value, _)) = expression.expression else { throw DatabaseExpressionEvaluationError.unboundParameter }
                return cell(value)
            }, temporalEnvironment: environment, workMeter: meter ?? SQLArrayFixture.meter(), limits: SQLArrayFixture.limits)
    }
}
