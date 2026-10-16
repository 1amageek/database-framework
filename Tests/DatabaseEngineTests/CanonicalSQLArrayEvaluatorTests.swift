import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Selected SQL array evaluator")
struct CanonicalSQLArrayEvaluatorTests {
    @Test func selectedConstructors() throws {
        let meter = SQLArrayFixture.meter()
        let values = [SQLArrayFixture.int(1), SQLArrayFixture.null]
        let bound = try SQLArrayFixture.bound(.array([]), values, types: [.sql(.integer), .sql(.integer)], result: .array(.integer))
        let result = try SQLArrayFixture.evaluate(bound, values, meter: meter)
        #expect(try SQLArrayFixture.cells(result.value) == values)
        #expect(try SQLArrayFixture.shape(result.value) == [QueryArrayDimension(lowerBound: 1, length: 2)])
        let empty = try SQLArrayFixture.bound(.array([]), [], result: .array(.integer))
        #expect(try SQLArrayFixture.shape(SQLArrayFixture.evaluate(empty, [], meter: meter).value).isEmpty)
        let text = try SQLArrayFixture.bound(.array([]), [SQLArrayFixture.null], types: [.sql(.text)], result: .array(.text))
        #expect(try SQLArrayFixture.cells(SQLArrayFixture.evaluate(text, [SQLArrayFixture.null], meter: meter).value) == [SQLArrayFixture.null])
        withExtendedLifetime(result) { #expect(meter.retainedIntermediateBytes > 0) }
    }

    @Test func rectangularNestedConstructors() throws {
        let row = try SQLArrayFixture.array([1, 2])
        let nested = try SQLArrayFixture.bound(.array([]), [row, row], result: .array(.integer))
        let result = try SQLArrayFixture.evaluate(nested, [row, row])
        #expect(try SQLArrayFixture.cells(result.value) == [1, 2, 1, 2].map(SQLArrayFixture.int))
        #expect(try SQLArrayFixture.shape(result.value).map { $0.length } == [2, 2])
        let shifted = try SQLArrayFixture.array([1, 2], lower: -2)
        let shiftedBound = try SQLArrayFixture.bound(.array([]), [shifted, shifted], result: .array(.integer))
        #expect(try SQLArrayFixture.shape(SQLArrayFixture.evaluate(shiftedBound, [shifted, shifted]).value).map { $0.lowerBound } == [1, -2])
        #expect(throws: DatabaseExpressionEvaluationError.collection(.invalidArrayShape)) {
            _ = try SQLArrayFixture.evaluate(nested, [row, shifted])
        }
        let empty = try SQLArrayFixture.array([])
        #expect(try SQLArrayFixture.shape(SQLArrayFixture.evaluate(nested, [SQLArrayFixture.null, empty]).value).isEmpty)
        let short = try SQLArrayFixture.array([1])
        #expect(throws: DatabaseExpressionEvaluationError.collection(.invalidArrayShape)) {
            _ = try SQLArrayFixture.evaluate(nested, [row, short])
        }
        #expect(throws: DatabaseExpressionEvaluationError.collection(.invalidArrayShape)) {
            _ = try SQLArrayFixture.evaluate(nested, [row, SQLArrayFixture.null])
        }
        let rankSix = try SQLArrayFixture.value([SQLArrayFixture.int(1)], dimensions: [1, 1, 1, 1, 1, 1])
        let tooDeep = try SQLArrayFixture.bound(.array([]), [rankSix], result: .array(.integer))
        #expect(throws: DatabaseExpressionEvaluationError.collection(.invalidArrayShape)) {
            _ = try SQLArrayFixture.evaluate(tooDeep, [rankSix])
        }
    }

    @Test func exactElementAccess() throws {
        let array = try SQLArrayFixture.array([10, 20], lower: -2)
        func read(_ subscriptValue: QueryValue) throws -> QueryValue {
            let inputs = [array, subscriptValue]
            let bound = try SQLArrayFixture.bound(.arrayAccess(.literal(.null), subscripts: [.index(.literal(.null))]), inputs,
                types: [.sql(.array(.integer)), .sql(.integer)], result: .integer)
            return try SQLArrayFixture.evaluate(bound, inputs).value
        }
        #expect(try read(SQLArrayFixture.int(-2)) == SQLArrayFixture.int(10))
        #expect(try read(SQLArrayFixture.int(0)).isNull)
        #expect(try read(SQLArrayFixture.null).isNull)
        let matrix = try SQLArrayFixture.value([1, 2, 3, 4].map(SQLArrayFixture.int), dimensions: [2, 2])
        let bound = try SQLArrayFixture.bound(.arrayAccess(.literal(.null), subscripts: [.index(.literal(.null))]),
            [matrix, SQLArrayFixture.int(1)], result: .integer)
        #expect(try SQLArrayFixture.evaluate(bound, [matrix, SQLArrayFixture.int(1)]).value.isNull)
    }

    @Test func slicesClipRebaseAndPreserveGroups() throws {
        let array = try SQLArrayFixture.array([10, 20, 30], lower: -2)
        let expression = Expression.arrayAccess(.literal(.null), subscripts: [.slice(lower: .literal(.null), upper: .literal(.null))])
        let inputs = [array, SQLArrayFixture.int(-100), SQLArrayFixture.int(-1)]
        let bound = try SQLArrayFixture.bound(expression, inputs, result: .array(.integer))
        let result = try SQLArrayFixture.evaluate(bound, inputs)
        #expect(try SQLArrayFixture.cells(result.value) == [10, 20].map(SQLArrayFixture.int))
        #expect(try SQLArrayFixture.shape(result.value)[0].lowerBound == 1)
        #expect(try SQLArrayFixture.cells(SQLArrayFixture.evaluate(bound, [array, SQLArrayFixture.int(9), SQLArrayFixture.int(4)]).value).isEmpty)
        #expect(try SQLArrayFixture.evaluate(bound, [array, SQLArrayFixture.null, SQLArrayFixture.int(1)]).value.isNull)
        let whole = try SQLArrayFixture.bound(.arrayAccess(.literal(.null), subscripts: [.slice(lower: nil, upper: nil)]), [array], result: .array(.integer))
        #expect(try SQLArrayFixture.cells(SQLArrayFixture.evaluate(whole, [array]).value) == [10, 20, 30].map(SQLArrayFixture.int))
        let matrix = try SQLArrayFixture.value([1, 2, 3, 4].map(SQLArrayFixture.int), dimensions: [2, 2])
        let mixed = try SQLArrayFixture.bound(.arrayAccess(.literal(.null), subscripts: [.index(.literal(.null)), .slice(lower: nil, upper: nil)]),
            [matrix, SQLArrayFixture.int(1)], result: .array(.integer))
        #expect(try SQLArrayFixture.shape(SQLArrayFixture.evaluate(mixed, [matrix, SQLArrayFixture.int(1)]).value).map { $0.length } == [1, 2])
    }

    @Test func concatenationAndNullIdentities() throws {
        let left = try SQLArrayFixture.array([1, 2], lower: -4), right = try SQLArrayFixture.array([3], lower: 9)
        let bound = try SQLArrayFixture.bound(.concatenate(.literal(.null), .literal(.null)), [left, right], result: .array(.integer))
        let result = try SQLArrayFixture.evaluate(bound, [left, right])
        #expect(try SQLArrayFixture.cells(result.value) == [1, 2, 3].map(SQLArrayFixture.int))
        #expect(try SQLArrayFixture.shape(result.value)[0].lowerBound == -4)
        #expect(try SQLArrayFixture.evaluate(bound, [SQLArrayFixture.null, right]).value == right)
        #expect(try SQLArrayFixture.evaluate(bound, [SQLArrayFixture.null, SQLArrayFixture.null]).value.isNull)
        let matrix = try SQLArrayFixture.value([1, 2, 3, 4].map(SQLArrayFixture.int), dimensions: [2, 2])
        let row = try SQLArrayFixture.array([5, 6])
        let rankDifference = try SQLArrayFixture.bound(.concatenate(.literal(.null), .literal(.null)), [matrix, row], result: .array(.integer))
        #expect(try SQLArrayFixture.shape(SQLArrayFixture.evaluate(rankDifference, [matrix, row]).value).map { $0.length } == [3, 2])
        let wrongBounds = try SQLArrayFixture.array([5, 6], lower: 2)
        #expect(throws: DatabaseExpressionEvaluationError.collection(.invalidArrayShape)) {
            _ = try SQLArrayFixture.evaluate(rankDifference, [matrix, wrongBounds])
        }
    }

    @Test func collectionNullsAndMultiplicity() throws {
        let one = try SQLArrayFixture.array([1]), repeated = try SQLArrayFixture.array([1, 1])
        let nulls = try SQLArrayFixture.value([SQLArrayFixture.null], dimensions: [1])
        for (operation, left, right, expected): (CollectionOperator, QueryValue, QueryValue, Bool) in [
            (.contains, one, repeated, true), (.containedBy, repeated, one, true),
            (.overlaps, one, repeated, true), (.contains, nulls, nulls, false), (.overlaps, nulls, nulls, false),
        ] {
            let bound = try SQLArrayFixture.bound(.collectionOperation(operation, left: .literal(.null), right: .literal(.null)),
                [left, right], result: .boolean)
            #expect(try SQLArrayFixture.evaluate(bound, [left, right]).value == .primitive(.bool(expected)))
            #expect(try SQLArrayFixture.evaluate(bound, [left, SQLArrayFixture.null]).value.isNull)
        }
    }

    @Test func comparisonUsesScalarDeclarationsAndShape() throws {
        let meter = SQLArrayFixture.meter()
        let a = try SQLArrayFixture.array([1]), b = try SQLArrayFixture.array([1], lower: 2)
        #expect(try CanonicalSQLArrayEvaluator.compare(a, b, leftType: .sql(.array(.integer)), rightType: .sql(.array(.integer)), temporalEnvironment: nil, workMeter: meter) == .lessThan)
        let nulls = try SQLArrayFixture.value([SQLArrayFixture.null], dimensions: [1])
        #expect(try CanonicalSQLArrayEvaluator.compare(nulls, a, leftType: .sql(.array(.integer)), rightType: .sql(.array(.integer)), temporalEnvironment: nil, workMeter: meter) == .greaterThan)
        let charsA = try SQLArrayFixture.value([.primitive(.string("a "))], element: .char(length: nil), dimensions: [1])
        let charsB = try SQLArrayFixture.value([.primitive(.string("a"))], element: .char(length: nil), dimensions: [1])
        #expect(try CanonicalSQLArrayEvaluator.compare(charsA, charsB, leftType: .sql(.array(.char(length: nil))), rightType: .sql(.array(.char(length: nil))), temporalEnvironment: nil, workMeter: meter) == .equal)
        let numeric = DataType.decimal(precision: nil, scale: nil)
        let n1 = try QueryNumericValue(sqlLexicalForm: "1.0"[...], maximumUTF8Count: 3)
        let n2 = try QueryNumericValue(sqlLexicalForm: "1.00"[...], maximumUTF8Count: 4)
        #expect(try CanonicalSQLArrayEvaluator.compare(SQLArrayFixture.value([.numeric(n1)], element: numeric, dimensions: [1]),
            SQLArrayFixture.value([.numeric(n2)], element: numeric, dimensions: [1]), leftType: .sql(.array(numeric)), rightType: .sql(.array(numeric)), temporalEnvironment: nil, workMeter: meter) == .equal)
    }

    @Test func dimensionFunctions() throws {
        let value = try SQLArrayFixture.array([1, 2], lower: -2)
        for (name, result): (String, QueryValue) in [("array_length", SQLArrayFixture.int(2)), ("array_lower", SQLArrayFixture.int(-2)), ("array_upper", SQLArrayFixture.int(-1))] {
            #expect(try SQLArrayFixture.call(name, [value, SQLArrayFixture.int(1)], result: .integer).value == result)
            #expect(try SQLArrayFixture.call(name, [value, SQLArrayFixture.int(0)], result: .integer).value.isNull)
        }
        #expect(try SQLArrayFixture.call("array_ndims", [value], result: .integer).value == SQLArrayFixture.int(1))
        #expect(try SQLArrayFixture.call("array_dims", [value], result: .text).value == .primitive(.string("[-2:-1]")))
        #expect(try SQLArrayFixture.call("cardinality", [value], result: .integer).value == SQLArrayFixture.int(2))
        let empty = try SQLArrayFixture.array([])
        #expect(try SQLArrayFixture.call("cardinality", [empty], result: .integer).value == SQLArrayFixture.int(0))
        #expect(try SQLArrayFixture.call("array_ndims", [empty], result: .integer).value.isNull)
        #expect(try SQLArrayFixture.call("array_dims", [empty], result: .text).value.isNull)
    }

    @Test func appendAndPrependRetainNullElements() throws {
        let array = try SQLArrayFixture.array([1], lower: -1)
        let appended = try SQLArrayFixture.call("array_append", [array, SQLArrayFixture.null], types: [.sql(.array(.integer)), .sql(.integer)], result: .array(.integer))
        #expect(try SQLArrayFixture.cells(appended.value) == [SQLArrayFixture.int(1), SQLArrayFixture.null])
        #expect(try SQLArrayFixture.shape(appended.value)[0].lowerBound == -1)
        let prepended = try SQLArrayFixture.call("array_prepend", [SQLArrayFixture.null, SQLArrayFixture.null], types: [.sql(.integer), .sql(.array(.integer))], result: .array(.integer))
        #expect(try SQLArrayFixture.cells(prepended.value) == [SQLArrayFixture.null])
        let cat = try SQLArrayFixture.call("array_cat", [array, array], result: .array(.integer))
        #expect(try SQLArrayFixture.cells(cat.value) == [1, 1].map(SQLArrayFixture.int))
        let binary = try SQLArrayFixture.bound(.concatenate(.literal(.null), .literal(.null)), [array, SQLArrayFixture.int(2)], result: .array(.integer))
        #expect(try SQLArrayFixture.cells(SQLArrayFixture.evaluate(binary, [array, SQLArrayFixture.int(2)]).value) == [1, 2].map(SQLArrayFixture.int))
    }

    @Test func nullSafeSearchAndActualSubscripts() throws {
        let array = try SQLArrayFixture.value([SQLArrayFixture.null, SQLArrayFixture.int(2), SQLArrayFixture.null], dimensions: [3], lower: -2)
        let types: [QueryColumnType] = [.sql(.array(.integer)), .sql(.integer)]
        #expect(try SQLArrayFixture.call("array_position", [array, SQLArrayFixture.null], types: types, result: .integer).value == SQLArrayFixture.int(-2))
        let positions = try SQLArrayFixture.call("array_positions", [array, SQLArrayFixture.null], types: types, result: .array(.integer))
        #expect(try SQLArrayFixture.cells(positions.value) == [-2, 0].map(SQLArrayFixture.int))
        #expect(try SQLArrayFixture.shape(positions.value)[0].lowerBound == 1)
        #expect(try SQLArrayFixture.call("array_position", [array, SQLArrayFixture.null, SQLArrayFixture.int(-1)], types: types + [.sql(.integer)], result: .integer).value == SQLArrayFixture.int(0))
        #expect(throws: DatabaseExpressionEvaluationError.collection(.nullArraySubscript(function: "array_position"))) {
            _ = try SQLArrayFixture.call("array_position", [array, SQLArrayFixture.int(2), SQLArrayFixture.null], types: types + [.sql(.integer)], result: .integer)
        }
        #expect(try SQLArrayFixture.call("array_position", [SQLArrayFixture.array([]), SQLArrayFixture.int(2), SQLArrayFixture.null], types: types + [.sql(.integer)], result: .integer).value.isNull)
    }

    @Test func removeAndReplace() throws {
        let array = try SQLArrayFixture.value([SQLArrayFixture.null, SQLArrayFixture.int(2), SQLArrayFixture.null], dimensions: [3], lower: -2)
        let removed = try SQLArrayFixture.call("array_remove", [array, SQLArrayFixture.null], types: [.sql(.array(.integer)), .sql(.integer)], result: .array(.integer))
        #expect(try SQLArrayFixture.cells(removed.value) == [SQLArrayFixture.int(2)])
        #expect(try SQLArrayFixture.shape(removed.value)[0].lowerBound == -2)
        let replaced = try SQLArrayFixture.call("array_replace", [array, SQLArrayFixture.null, SQLArrayFixture.int(9)],
            types: [.sql(.array(.integer)), .sql(.integer), .sql(.integer)], result: .array(.integer))
        #expect(try SQLArrayFixture.cells(replaced.value) == [9, 2, 9].map(SQLArrayFixture.int))
        let allRemoved = try SQLArrayFixture.call("array_remove", [removed.value, SQLArrayFixture.int(2)], result: .array(.integer))
        #expect(try SQLArrayFixture.shape(allRemoved.value).isEmpty)
    }

    @Test func splitAndJoinText() throws {
        let split = try SQLArrayFixture.call("string_to_array", [.primitive(.string("a,,NULL,b")), .primitive(.string(",")), .primitive(.string("NULL"))], result: .array(.text))
        #expect(try SQLArrayFixture.cells(split.value) == [.primitive(.string("a")), .primitive(.string("")), SQLArrayFixture.null, .primitive(.string("b"))])
        #expect(try SQLArrayFixture.call("array_to_string", [split.value, .primitive(.string("|"))], result: .text).value == .primitive(.string("a||b")))
        #expect(try SQLArrayFixture.call("array_to_string", [split.value, .primitive(.string("|")), .primitive(.string("?"))], result: .text).value == .primitive(.string("a||?|b")))
        let scalars = try SQLArrayFixture.call("string_to_array", [.primitive(.string("e\u{301}😀")), SQLArrayFixture.null], types: [.sql(.text), .sql(.text)], result: .array(.text))
        #expect(try SQLArrayFixture.cells(scalars.value).count == 3)
        #expect(try SQLArrayFixture.cells(SQLArrayFixture.call("string_to_array", [.primitive(.string("")), .primitive(.string(","))], result: .array(.text)).value).isEmpty)
    }

    @Test func lexicalCastsAndScalarTypmods() throws {
        let meter = SQLArrayFixture.meter()
        let result = try CanonicalSQLArrayEvaluator.cast(.primitive(.string("[-2:-1]={1,NULL}")), from: .sql(.text), to: .sql(.array(.integer)), context: .explicit, temporalEnvironment: nil, workMeter: meter, limits: SQLArrayFixture.limits)
        #expect(try SQLArrayFixture.cells(result.value) == [SQLArrayFixture.int(1), SQLArrayFixture.null])
        #expect(try SQLArrayFixture.shape(result.value)[0].lowerBound == -2)
        let formatted = try CanonicalSQLArrayEvaluator.cast(result.value, from: .sql(.array(.integer)), to: .sql(.text), context: .explicit, temporalEnvironment: nil, workMeter: meter, limits: SQLArrayFixture.limits)
        #expect(formatted.value == .primitive(.string("[-2:-1]={1,NULL}")))
        let char = try CanonicalSQLArrayEvaluator.cast(.primitive(.string("{a,abcd}")), from: .sql(.text), to: .sql(.array(.char(length: 3))), context: .explicit, temporalEnvironment: nil, workMeter: meter, limits: SQLArrayFixture.limits)
        #expect(try SQLArrayFixture.cells(char.value) == [.primitive(.string("a  ")), .primitive(.string("abc"))])
        #expect(try CanonicalSQLArrayEvaluator.cast(char.value, from: .sql(.array(.char(length: 3))), to: .sql(.text), context: .explicit, temporalEnvironment: nil, workMeter: meter, limits: SQLArrayFixture.limits).value == .primitive(.string("{\"a  \",abc}")))
        let primitive = QueryValue.primitive(.array([.int64(1), .null]))
        let normalized = try CanonicalSQLArrayEvaluator.cast(primitive, from: .primitive(.int64, isArray: true), to: .sql(.array(.bigint)), context: .implicit, temporalEnvironment: nil, workMeter: meter, limits: SQLArrayFixture.limits)
        #expect(try SQLArrayFixture.cells(normalized.value) == [.primitive(.int64(1)), SQLArrayFixture.null])
        let rounded = try CanonicalSQLArrayEvaluator.cast(.primitive(.string("{1.235}")), from: .sql(.text), to: .sql(.array(.decimal(precision: 4, scale: 2))), context: .explicit, temporalEnvironment: nil, workMeter: meter, limits: SQLArrayFixture.limits)
        let expected = try QueryNumericValue(sqlLexicalForm: "1.24"[...], maximumUTF8Count: 4)
        #expect(try SQLArrayFixture.cells(rounded.value) == [.numeric(expected)])
        let strings = try SQLArrayFixture.value([.primitive(.string("NULL")), .primitive(.string("a\\b\"c")), .primitive(.string("")), SQLArrayFixture.null], element: .text, dimensions: [4])
        let text = try CanonicalSQLArrayEvaluator.cast(strings, from: .sql(.array(.text)), to: .sql(.text), context: .explicit, temporalEnvironment: nil, workMeter: meter, limits: SQLArrayFixture.limits)
        #expect(try CanonicalSQLArrayEvaluator.cast(text.value, from: .sql(.text), to: .sql(.array(.text)), context: .explicit, temporalEnvironment: nil, workMeter: meter, limits: SQLArrayFixture.limits).value == strings)
    }

    @Test func lexicalFailuresAndTemporalEnvironment() throws {
        let meter = SQLArrayFixture.meter()
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            _ = try CanonicalSQLArrayEvaluator.cast(.primitive(.string("{{1},{2,3}}")), from: .sql(.text), to: .sql(.array(.integer)), context: .explicit, temporalEnvironment: nil, workMeter: meter, limits: SQLArrayFixture.limits)
        }
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            _ = try CanonicalSQLArrayEvaluator.cast(.primitive(.string("{invalid}")), from: .sql(.text), to: .sql(.array(.integer)), context: .explicit, temporalEnvironment: nil, workMeter: meter, limits: SQLArrayFixture.limits)
        }
        #expect(throws: DatabaseExpressionEvaluationError.missingTemporalEnvironment) {
            _ = try CanonicalSQLArrayEvaluator.cast(.primitive(.string("{2000-01-01}")), from: .sql(.text), to: .sql(.array(.date)), context: .explicit, temporalEnvironment: nil, workMeter: meter, limits: SQLArrayFixture.limits)
        }
        #expect(meter.retainedIntermediateBytes == 0)
        let environment = SQLTemporalEnvironment(transactionInstant: Timestamp(secondsSinceUnixEpoch: 0), statementInstant: Timestamp(secondsSinceUnixEpoch: 0), wallClock: SQLArrayTestWallClock())
        do {
            let dates = try CanonicalSQLArrayEvaluator.cast(.primitive(.string("{2000-01-01,infinity}")), from: .sql(.text), to: .sql(.array(.date)), context: .explicit, temporalEnvironment: environment, workMeter: meter, limits: SQLArrayFixture.limits)
            #expect(try SQLArrayFixture.cells(dates.value)[1] == .dateInfinity(.positive))
            #expect(try CanonicalSQLArrayEvaluator.cast(dates.value, from: .sql(.array(.date)), to: .sql(.text), context: .explicit, temporalEnvironment: environment, workMeter: meter, limits: SQLArrayFixture.limits).value == .primitive(.string("{2000-01-01,infinity}")))
        }
        #expect(meter.retainedIntermediateBytes == 0)
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            _ = try CanonicalSQLArrayEvaluator.cast(SQLArrayFixture.null, from: .sql(.text), to: .sql(.array(.array(.integer))), context: .explicit, temporalEnvironment: nil, workMeter: meter, limits: SQLArrayFixture.limits)
        }
    }

    @Test func lexicalWorkspaceAdmissionAndRecovery() throws {
        let input = "[-2:0]={1,NULL,3}"
        let parsed = try QueryArrayInput(sqlLexicalForm: input, limits: SQLArrayFixture.limits)
        let limits = QueryValueLimits(maximumRetainedBytes: parsed.retainedByteCount, maximumTotalNodes: 100, maximumNestingDepth: 6, maximumCollectionElements: 10)
        func run(_ meter: DatabaseWorkMeter) throws {
            try SQLArrayInputWorkspace.withInput(sqlLexicalForm: input, limits: limits, workMeter: meter) { value in
                let first = try value.elementText(at: 0)
                let second = try value.elementText(at: 1)
                #expect(value.count == 3)
                #expect(first == "1")
                #expect(second == nil)
            }
        }
        let measured = SQLArrayFixture.meter(); try run(measured)
        #expect(measured.retainedIntermediateBytes == 0)
        let exact = SQLArrayFixture.meter(bytes: measured.peakIntermediateBytes); try run(exact)
        #expect(exact.retainedIntermediateBytes == 0)
        let short = SQLArrayFixture.meter(bytes: measured.peakIntermediateBytes - 1)
        #expect(throws: QueryValueError.self) { try run(short) }
        #expect(short.retainedIntermediateBytes == 0)
        let exhausted = SQLArrayFixture.meter(work: 1)
        #expect(throws: DatabaseWorkLimitError.self) { try run(exhausted) }
        #expect(exhausted.retainedIntermediateBytes == 0)
        let recovery = SQLArrayFixture.meter()
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try SQLArrayInputWorkspace.withInput(sqlLexicalForm: "{", limits: limits, workMeter: recovery) { _ in }
        }
        #expect(recovery.retainedIntermediateBytes == 0)
        try run(recovery)
        #expect(recovery.retainedIntermediateBytes == 0)
    }

    @Test func exactBudgetAndFailureCleanup() throws {
        let input = try SQLArrayFixture.array([1, 2, 3, 4])
        func run(_ meter: DatabaseWorkMeter) throws {
            let result = try SQLArrayFixture.call("array_append", [input, SQLArrayFixture.int(5)], result: .array(.integer), meter: meter)
            withExtendedLifetime(result) { #expect(meter.retainedIntermediateBytes > 0) }
        }
        let measured = SQLArrayFixture.meter(); try run(measured)
        #expect(measured.retainedIntermediateBytes == 0)
        let exact = SQLArrayFixture.meter(bytes: measured.peakIntermediateBytes); try run(exact)
        #expect(exact.retainedIntermediateBytes == 0)
        let short = SQLArrayFixture.meter(bytes: measured.peakIntermediateBytes - 1)
        #expect(throws: DatabaseWorkLimitError.self) { try run(short) }
        #expect(short.retainedIntermediateBytes == 0)
        let work = SQLArrayFixture.meter(work: 1)
        #expect(throws: DatabaseWorkLimitError.self) { try run(work) }
        #expect(work.retainedIntermediateBytes == 0)
    }
}

enum SQLArrayFixture {
    static let null = QueryValue.primitive(.null)
    static let limits = QueryValueLimits(maximumRetainedBytes: 1_000_000, maximumTotalNodes: 100_000, maximumNestingDepth: 100, maximumCollectionElements: 100_000)
    static func int(_ value: Int32) -> QueryValue { .primitive(.int32(value)) }
    static func meter(bytes: UInt64 = 10_000_000, work: UInt64 = 10_000_000, clock: SQLArrayTestClock = SQLArrayTestClock()) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 1000, maximumWorkUnits: work, maximumIntermediateRows: 1000,
            maximumIntermediateBytes: bytes, timeoutMilliseconds: 1000), monotonicClock: clock)
    }
    static func array(_ values: [Int32], lower: Int32 = 1) throws -> QueryValue {
        try value(values.map(int), dimensions: values.isEmpty ? [] : [Int32(values.count)], lower: lower)
    }
    static func value(_ values: [QueryValue], element: DataType = .integer, dimensions: [Int32], lower: Int32 = 1) throws -> QueryValue {
        .array(try QueryArrayValue(elementType: element, dimensions: dimensions.enumerated().map {
            try QueryArrayDimension(lowerBound: $0.offset == 0 ? lower : 1, length: $0.element)
        }, elements: values, limits: limits))
    }
    static func cells(_ value: QueryValue) throws -> [QueryValue] {
        guard case .array(let array) = value else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: "array test result") }
        return array.elements
    }
    static func shape(_ value: QueryValue) throws -> [QueryArrayDimension] {
        guard case .array(let array) = value else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: "array test result") }
        return array.dimensions
    }
    static func bound(_ expression: Expression, _ values: [QueryValue], types: [QueryColumnType]? = nil, result: DataType) throws -> CanonicalBoundExpression {
        let children = try values.enumerated().map { index, value -> CanonicalBoundExpression in
            let type = try types?[index] ?? CanonicalQueryTypeBinder.type(of: value)
            return CanonicalBoundExpression(expression: .literal(.queryValue(value, type: type)), type: type, isNullable: value.isNull)
        }
        return CanonicalBoundExpression(expression: expression, type: .sql(result), isNullable: true, children: children)
    }
    static func evaluate(_ bound: CanonicalBoundExpression, _ operands: [QueryValue], meter: DatabaseWorkMeter? = nil) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        try CanonicalSQLArrayEvaluator.evaluate(bound, operands: operands, temporalEnvironment: nil, workMeter: meter ?? self.meter(), limits: limits)
    }
    static func call(_ name: String, _ operands: [QueryValue], types: [QueryColumnType]? = nil, result: DataType,
        meter: DatabaseWorkMeter? = nil) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        let bound = try bound(.function(FunctionCall(name: name, arguments: [])), operands, types: types, result: result)
        return try evaluate(bound, operands, meter: meter)
    }
}

final class SQLArrayTestClock: StorageMonotonicClock, Sendable {
    private let instant = Mutex(StorageInstant(durationSinceReference: .zero))
    var now: StorageInstant { instant.withLock { $0 } }
    func advance() { instant.withLock { $0 = $0.advanced(by: .seconds(2)) } }
    func sleep(until deadline: StorageInstant) async throws(StorageClockError) { instant.withLock { $0 = deadline } }
}

private struct SQLArrayTestWallClock: WallClock {
    var now: Timestamp { Timestamp(secondsSinceUnixEpoch: 0) }
}
