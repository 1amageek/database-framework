import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization
import StorageKitSystemClock
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Selected SQL numeric scalar kernels")
struct CanonicalSQLNumericEvaluatorTests {
    private let decimal = DataType.decimal(precision: nil, scale: nil)

    private func meter(bytes: UInt64 = 1_000_000, work: UInt64 = 1_000_000) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumWorkUnits: work, maximumIntermediateBytes: bytes),
                          monotonicClock: SystemStorageClock())
    }

    private func numeric(_ coefficient: Int128, scale: Int32 = 0, display: Int32? = nil) throws -> QueryValue {
        .numeric(try QueryNumericValue(value: ExactDecimal(coefficient: coefficient, scale: scale),
                                      displayScale: display ?? max(0, scale)))
    }

    private func run(_ expression: Expression, result: DataType, inputs: [DataType], values: [QueryValue],
                     workMeter: DatabaseWorkMeter? = nil) throws -> QueryValue {
        let bound = CanonicalBoundExpression(expression: expression, type: .sql(result), isNullable: true,
            children: inputs.map { CanonicalBoundExpression(expression: .null, type: .sql($0), isNullable: true) })
        return try CanonicalSQLNumericEvaluator.evaluate(bound, operands: values, workMeter: workMeter ?? meter())
    }

    private func call(_ name: String, result: DataType, inputs: [DataType], values: [QueryValue],
                      workMeter: DatabaseWorkMeter? = nil) throws -> QueryValue {
        try run(.function(FunctionCall(name: name, arguments: inputs.map { _ in .null })), result: result,
                inputs: inputs, values: values, workMeter: workMeter)
    }

    private func cast(_ value: QueryValue, from source: DataType, to target: DataType,
                      workMeter: DatabaseWorkMeter? = nil) throws -> QueryValue {
        try run(.cast(.null, targetType: target), result: target, inputs: [source], values: [value], workMeter: workMeter)
    }

    @Test("Integer arithmetic retains each selected width")
    func integerWidths() throws {
        let cases: [(DataType, QueryValue, QueryValue, QueryValue)] = [
            (.smallint, .primitive(.int16(8)), .primitive(.int16(3)), .primitive(.int16(11))),
            (.integer, .primitive(.int32(8)), .primitive(.int32(3)), .primitive(.int32(11))),
            (.bigint, .primitive(.int64(8)), .primitive(.int64(3)), .primitive(.int64(11))),
        ]
        for (type, lhs, rhs, expected) in cases {
            #expect(try run(.add(.null,.null), result:type, inputs:[type,type], values:[lhs,rhs]) == expected)
            let quotient = try run(.divide(.null,.null), result:type, inputs:[type,type], values:[lhs,rhs])
            #expect(try CanonicalSQLNumericEvaluator.integer(quotient) == 2)
            #expect(try call("abs",result:type,inputs:[type],values:[rhs]) == rhs)
        }
        #expect(try run(.multiply(.null,.null),result:.integer,inputs:[.integer,.integer],
                        values:[.primitive(.int32(-7)),.primitive(.int32(6))]) == .primitive(.int32(-42)))
    }

    @Test("Integer minimum ABS, negate and divide overflow; minimum remainder -1 is zero")
    func integerBoundaries() throws {
        let cases: [(DataType, QueryValue, QueryValue, QueryValue)] = [
            (.smallint,.primitive(.int16(.min)),.primitive(.int16(-1)),.primitive(.int16(0))),
            (.integer,.primitive(.int32(.min)),.primitive(.int32(-1)),.primitive(.int32(0))),
            (.bigint,.primitive(.int64(.min)),.primitive(.int64(-1)),.primitive(.int64(0))),
        ]
        for (type, minimum, minusOne, zero) in cases {
            #expect(throws: DatabaseExpressionEvaluationError.numericOverflow) {
                try run(.negate(.null),result:type,inputs:[type],values:[minimum])
            }
            #expect(throws: DatabaseExpressionEvaluationError.numericOverflow) {
                try call("abs",result:type,inputs:[type],values:[minimum])
            }
            #expect(throws: DatabaseExpressionEvaluationError.numericOverflow) {
                try run(.divide(.null,.null),result:type,inputs:[type,type],values:[minimum,minusOne])
            }
            #expect(try run(.modulo(.null,.null),result:type,inputs:[type,type],values:[minimum,minusOne]) == zero)
            #expect(throws: DatabaseExpressionEvaluationError.divisionByZero) {
                try run(.modulo(.null,.null),result:type,inputs:[type,type],values:[minimum,zero])
            }
        }
        #expect(throws: DatabaseExpressionEvaluationError.numericOverflow) {
            try run(.add(.null,.null),result:.smallint,inputs:[.smallint,.smallint],
                    values:[.primitive(.int16(.max)),.primitive(.int16(1))])
        }
    }

    @Test("Division uses normalized base-10000 weights and retains selected decimal scale")
    func decimalDivisionScales() throws {
        let cases: [(QueryValue, QueryValue, QueryValue)] = [
            (try numeric(1),try numeric(3),try numeric(33333333333333333333,scale:20)),
            (try numeric(1),try numeric(10),try numeric(1,scale:1,display:20)),
            (try numeric(5),try numeric(2),try numeric(25,scale:1,display:16)),
            (try numeric(1,scale:8),try numeric(3),try numeric(33333333333333333333,scale:28)),
            (try numeric(1,scale:-20),try numeric(3),try numeric(33333333333333333333)),
        ]
        for (left,right,expected) in cases {
            let workMeter = meter()
            #expect(try run(.divide(.null,.null),result:decimal,inputs:[decimal,decimal],values:[left,right],workMeter:workMeter) == expected)
            #expect(workMeter.retainedIntermediateBytes == 0)
        }
    }

    @Test("Numeric addition, multiplication and MOD preserve packed scales beyond 1000")
    func packedArithmeticScales() throws {
        #expect(try run(.add(.null,.null),result:decimal,inputs:[decimal,decimal],
            values:[try numeric(12,scale:1,display:2),try numeric(34,scale:1,display:3)]) == numeric(46,scale:1,display:3))
        #expect(try run(.multiply(.null,.null),result:decimal,inputs:[decimal,decimal],
            values:[try numeric(12,scale:1,display:2),try numeric(34,scale:1,display:3)]) == numeric(408,scale:2,display:5))
        for expression: Expression in [.add(.null,.null),.subtract(.null,.null),.modulo(.null,.null)] {
            let result = try run(expression,result:decimal,inputs:[decimal,decimal],
                                 values:[try numeric(4,display:16383),try numeric(3,display:1001)])
            guard case .numeric(let number) = result else { Issue.record("Expected numeric result"); continue }
            #expect(number.displayScale == 16383)
        }
        #expect(try run(.multiply(.null,.null),result:decimal,inputs:[decimal,decimal],
            values:[try numeric(2,display:9000),try numeric(3,display:9000)]) == numeric(6,display:16383))
    }

    @Test("Only the final normalized numeric coefficient is admitted at 38 digits")
    func finalWidthAndRelease() throws {
        let maximum = Int128("99999999999999999999999999999999999999")!
        let workMeter = meter()
        #expect(throws: DatabaseExpressionEvaluationError.numericOverflow) {
            try run(.add(.null,.null),result:decimal,inputs:[decimal,decimal],
                    values:[try numeric(maximum),try numeric(maximum)],workMeter:workMeter)
        }
        #expect(workMeter.retainedIntermediateBytes == 0)
        #expect(try run(.multiply(.null,.null),result:decimal,inputs:[decimal,decimal],
            values:[try numeric(maximum),try numeric(1,scale:1)],workMeter:workMeter) == numeric(maximum,scale:1,display:1))
    }

    @Test("Numeric typmods round away from zero before precision admission")
    func typmods() throws {
        let negative = DataType.decimal(precision:2,scale:-3)
        #expect(try cast(numeric(99499),from:decimal,to:negative) == numeric(99,scale:-3))
        #expect(try cast(numeric(-99499),from:decimal,to:negative) == numeric(-99,scale:-3))
        for value in [Int128(99500),Int128(-99500)] {
            #expect(throws: DatabaseExpressionEvaluationError.numericOverflow) {
                try cast(numeric(value),from:decimal,to:negative)
            }
        }
        let fractional = DataType.decimal(precision:3,scale:5)
        #expect(try cast(numeric(9994,scale:6),from:decimal,to:fractional) == numeric(999,scale:5))
        #expect(throws: DatabaseExpressionEvaluationError.numericOverflow) {
            try cast(numeric(9995,scale:6),from:decimal,to:fractional)
        }
        #expect(try cast(numeric(0),from:decimal,to:fractional) == numeric(0,display:5))
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try cast(.primitive(.null),from:decimal,to:.decimal(precision:0,scale:5))
        }
    }

    @Test("Numeric and floating integer casts use different tie rules")
    func integerCastRounding() throws {
        #expect(try cast(numeric(25,scale:1),from:decimal,to:.integer) == .primitive(.int32(3)))
        #expect(try cast(numeric(-25,scale:1),from:decimal,to:.integer) == .primitive(.int32(-3)))
        #expect(try cast(.primitive(.float64(2.5)),from:.doublePrecision,to:.integer) == .primitive(.int32(2)))
        #expect(try cast(.primitive(.float64(-2.5)),from:.doublePrecision,to:.integer) == .primitive(.int32(-2)))
        #expect(throws: DatabaseExpressionEvaluationError.numericOverflow) {
            try cast(.primitive(.int64(32768)),from:.bigint,to:.smallint)
        }
        #expect(try cast(.primitive(.int32(-2)),from:.integer,to:.boolean) == .primitive(.bool(true)))
        #expect(try cast(.primitive(.bool(false)),from:.boolean,to:.bigint) == .primitive(.int64(0)))
        let primitiveIntegers: [(QueryColumnType, QueryValue, Int128)] = [
            (.primitive(.int8),.primitive(.int8(.min)),Int128(Int8.min)),
            (.primitive(.int16),.primitive(.int16(.min)),Int128(Int16.min)),
            (.primitive(.int32),.primitive(.int32(.min)),Int128(Int32.min)),
            (.primitive(.int64),.primitive(.int64(.min)),Int128(Int64.min)),
            (.primitive(.uint8),.primitive(.uint8(.max)),Int128(UInt8.max)),
            (.primitive(.uint16),.primitive(.uint16(.max)),Int128(UInt16.max)),
            (.primitive(.uint32),.primitive(.uint32(.max)),Int128(UInt32.max)),
            (.primitive(.uint64),.primitive(.uint64(.max)),Int128(UInt64.max)),
        ]
        for (source,value,coefficient) in primitiveIntegers {
            #expect(try CanonicalSQLNumericCast.evaluate(value,from:source,to:decimal,workMeter:meter()) == numeric(coefficient))
            let column = try QueryColumn(number:1,name:"number",type:source,isNullable:false)
            let binder = CanonicalQueryTypeBinder(columns:[column],resolveColumn:{ _ in 0 },wildcardSlots:{ _ in [0] })
            let bound = try binder.bind(.aggregate(.sum(.column(ColumnRef("number")),distinct:false)))
            let selected = try #require(bound.children.first)
            let actual = try CanonicalSQLNumericEvaluator.evaluate(selected,operands:[value],workMeter:meter())
            #expect(QueryValueValidator.accepts(actual,type:selected.type))
            #expect(try CanonicalSQLNumericCast.evaluate(actual,from:selected.type,to:decimal,workMeter:meter()) == numeric(coefficient))
        }
        #expect(throws: DatabaseExpressionEvaluationError.numericOverflow) {
            try CanonicalSQLNumericCast.evaluate(.primitive(.uint64(.max)),from:.primitive(.uint64),to:.bigint,workMeter:meter())
        }
        for flag in [false, true] {
            let value = QueryValue.primitive(.bool(flag))
            #expect(try cast(value,from:.boolean,to:.boolean) == value)
            #expect(try CanonicalSQLNumericCast.evaluate(value,from:.primitive(.bool),to:.boolean,workMeter:meter()) == value)
            let column = try QueryColumn(number:1,name:"flag",type:.primitive(.bool),isNullable:false)
            let binder = CanonicalQueryTypeBinder(columns:[column],resolveColumn:{ _ in 0 },wildcardSlots:{ _ in [0] })
            let bound = try binder.bind(.equal(.column(ColumnRef("flag")),.literal(.bool(flag))))
            for child in bound.children {
                let actual: QueryValue
                if case .cast = child.expression {
                    actual = try CanonicalSQLNumericEvaluator.evaluate(child,operands:[value],workMeter:meter())
                } else {
                    actual = try child.normalize(value)
                }
                #expect(actual == value)
            }
            let explicit = try binder.bind(.cast(.column(ColumnRef("flag")),targetType:.boolean))
            #expect(try CanonicalSQLNumericEvaluator.evaluate(explicit,operands:[value],workMeter:meter()) == value)
        }
    }

    @Test("Floating numeric casts use the reference 6 and 15 significant digits")
    func floatingDecimalCasts() throws {
        #expect(try cast(.primitive(.float32(1.2345678)),from:.real,to:decimal) == numeric(123457,scale:5))
        #expect(try cast(.primitive(.float64(1.2345678901234567)),from:.doublePrecision,to:decimal) == numeric(123456789012346,scale:14))
        #expect(try cast(.primitive(.float64(Double.leastNonzeroMagnitude)),from:.doublePrecision,to:decimal)
                == numeric(494065645841247,scale:338))
        #expect(try cast(numeric(1,scale:45),from:decimal,to:.real) == .primitive(.float32(.leastNonzeroMagnitude)))
        #expect(throws: DatabaseExpressionEvaluationError.numericUnderflow) {
            try cast(numeric(1,scale:100),from:decimal,to:.real)
        }
    }

    @Test("Real arithmetic keeps its width and admits representable subnormals")
    func floatingBoundaries() throws {
        #expect(try run(.add(.null,.null),result:.real,inputs:[.real,.real],
            values:[.primitive(.float32(1)),.primitive(.float32(2))]) == .primitive(.float32(3)))
        #expect(try run(.multiply(.null,.null),result:.real,inputs:[.real,.real],
            values:[.primitive(.float32(.leastNonzeroMagnitude)),.primitive(.float32(1))]) == .primitive(.float32(.leastNonzeroMagnitude)))
        #expect(throws: DatabaseExpressionEvaluationError.numericUnderflow) {
            try run(.multiply(.null,.null),result:.real,inputs:[.real,.real],
                    values:[.primitive(.float32(.leastNonzeroMagnitude)),.primitive(.float32(0.5))])
        }
        #expect(throws: DatabaseExpressionEvaluationError.numericUnderflow) {
            try run(.divide(.null,.null),result:.doublePrecision,inputs:[.doublePrecision,.doublePrecision],
                    values:[.primitive(.float64(.leastNonzeroMagnitude)),.primitive(.float64(2))])
        }
        #expect(throws: DatabaseExpressionEvaluationError.numericOverflow) {
            try run(.multiply(.null,.null),result:.real,inputs:[.real,.real],
                    values:[.primitive(.float32(.greatestFiniteMagnitude)),.primitive(.float32(2))])
        }
        #expect(throws: DatabaseExpressionEvaluationError.nonFiniteValue) {
            try call("abs",result:.doublePrecision,inputs:[.doublePrecision],values:[.primitive(.float64(.infinity))])
        }
    }

    @Test("Decimal elementary functions retain values and display scales")
    func decimalElementaryFunctions() throws {
        let inputs: [(String,[QueryValue],QueryValue)] = [
            ("abs",[try numeric(-12,scale:1,display:3)],try numeric(12,scale:1,display:3)),
            ("sign",[try numeric(-12,scale:1)],try numeric(-1)),
            ("ceil",[try numeric(-12,scale:1)],try numeric(-1)),
            ("floor",[try numeric(-12,scale:1)],try numeric(-2)),
            ("round",[try numeric(-25,scale:1)],try numeric(-3)),
            ("trunc",[try numeric(-25,scale:1)],try numeric(-2)),
            ("div",[try numeric(-5),try numeric(2)],try numeric(-2)),
            ("mod",[try numeric(-5,display:3),try numeric(2)],try numeric(-1,display:3)),
            ("gcd",[try numeric(12,scale:1,display:3),try numeric(3,scale:1)],try numeric(3,scale:1,display:3)),
            ("lcm",[try numeric(12,scale:1,display:3),try numeric(3,scale:1)],try numeric(12,scale:1,display:3)),
            ("trim_scale",[try numeric(12,scale:1,display:3)],try numeric(12,scale:1)),
        ]
        for (name,values,expected) in inputs {
            #expect(try call(name,result:decimal,inputs:values.map { _ in decimal },values:values) == expected)
        }
        #expect(try call("round",result:decimal,inputs:[decimal,.integer],values:[try numeric(25,scale:1),.primitive(.int32(1001))]) == numeric(25,scale:1,display:1001))
        #expect(try call("scale",result:.integer,inputs:[decimal],values:[try numeric(12,scale:1,display:3)]) == .primitive(.int32(3)))
        #expect(try call("min_scale",result:.integer,inputs:[decimal],values:[try numeric(12,scale:1,display:3)]) == .primitive(.int32(1)))
    }

    @Test("Integer GCD and LCM handle sign, zero and final width")
    func integerGCDAndLCM() throws {
        #expect(try call("gcd",result:.integer,inputs:[.integer,.integer],values:[.primitive(.int32(-12)),.primitive(.int32(18))]) == .primitive(.int32(6)))
        #expect(try call("lcm",result:.bigint,inputs:[.bigint,.bigint],values:[.primitive(.int64(0)),.primitive(.int64(.min))]) == .primitive(.int64(0)))
        #expect(throws: DatabaseExpressionEvaluationError.numericOverflow) {
            try call("gcd",result:.bigint,inputs:[.bigint,.bigint],values:[.primitive(.int64(.min)),.primitive(.int64(0))])
        }
    }

    @Test("F03 and floating F02 use actual selected double signatures and domains")
    func doubleFunctions() throws {
        let cases: [(String,[Double],Double)] = [
            ("pi",[],Double.pi),("degrees",[Double.pi],180),("radians",[180],Double.pi),
            ("sin",[0],0),("cos",[0],1),("tan",[0],0),("asin",[1],Double.pi/2),
            ("acos",[1],0),("atan",[0],0),("atan2",[0,1],0),("cbrt",[8],2),
            ("sqrt",[4],2),("exp",[0],1),("ln",[1],0),("log",[100],2),("log10",[100],2),
            ("power",[2,3],8),("round",[-2.5],-2),("trunc",[-2.5],-2),
        ]
        for (name,values,expected) in cases {
            #expect(try call(name,result:.doublePrecision,inputs:values.map { _ in .doublePrecision },
                             values:values.map { .primitive(.float64($0)) }) == .primitive(.float64(expected)))
        }
        for (name,value) in [("sqrt",-1.0),("ln",0.0),("asin",2.0),("acos",-2.0)] {
            #expect(throws: DatabaseExpressionEvaluationError.numericDomain(operation:name)) {
                try call(name,result:.doublePrecision,inputs:[.doublePrecision],values:[.primitive(.float64(value))])
            }
        }
    }

    @Test("WIDTH_BUCKET handles reverse bounds, final overflow and threshold duplicates")
    func widthBuckets() throws {
        #expect(try call("width_bucket",result:.integer,inputs:[decimal,decimal,decimal,.integer],
            values:[try numeric(8),try numeric(10),try numeric(0),.primitive(.int32(5))]) == .primitive(.int32(2)))
        #expect(throws: DatabaseExpressionEvaluationError.numericOverflow) {
            try call("width_bucket",result:.integer,inputs:[decimal,decimal,decimal,.integer],
                values:[try numeric(10),try numeric(0),try numeric(10),.primitive(.int32(.max))])
        }
        let array = try QueryArrayValue(elementType:.integer,dimensions:[QueryArrayDimension(lowerBound:1,length:4)],
            elements:[.primitive(.int32(1)),.primitive(.int32(3)),.primitive(.int32(3)),.primitive(.int32(5))],limits:limits)
        #expect(try call("width_bucket",result:.integer,inputs:[.integer,.array(.integer)],
                         values:[.primitive(.int32(3)),.array(array)]) == .primitive(.int32(3)))
        let nullThreshold = try QueryArrayValue(elementType:.integer,
            dimensions:[QueryArrayDimension(lowerBound:1,length:3)],
            elements:[.primitive(.null),.primitive(.int32(3)),.primitive(.int32(5))],limits:limits)
        #expect(throws: DatabaseExpressionEvaluationError.numericDomain(operation:"width_bucket")) {
            try call("width_bucket",result:.integer,inputs:[.integer,.array(.integer)],
                     values:[.primitive(.int32(4)),.array(nullThreshold)])
        }
        let multidimensional = try QueryArrayValue(elementType:.integer,
            dimensions:[QueryArrayDimension(lowerBound:1,length:2),QueryArrayDimension(lowerBound:1,length:2)],
            elements:[.primitive(.int32(1)),.primitive(.int32(2)),.primitive(.int32(3)),.primitive(.int32(4))],limits:limits)
        #expect(throws: DatabaseExpressionEvaluationError.numericDomain(operation:"width_bucket")) {
            try call("width_bucket",result:.integer,inputs:[.integer,.array(.integer)],
                     values:[.primitive(.int32(4)),.array(multidimensional)])
        }
        #expect(try call("width_bucket",result:.integer,inputs:[.integer,.array(.integer)],
                         values:[.primitive(.int32(4)),.primitive(.null)]) == .primitive(.null))
        let empty = try QueryArrayValue(elementType:.integer,dimensions:[],elements:[],limits:limits)
        #expect(try call("width_bucket",result:.integer,inputs:[.integer,.array(.integer)],values:[.primitive(.int32(9)),.array(empty)]) == .primitive(.int32(0)))
    }

    @Test("Scalar scratch admission fails explicitly and releases before the next operation")
    func scratchBoundaries() throws {
        let probe = meter()
        _ = try run(.divide(.null,.null),result:decimal,inputs:[decimal,decimal],values:[try numeric(1),try numeric(3)],workMeter:probe)
        let exact = meter(bytes:probe.peakIntermediateBytes)
        _ = try run(.divide(.null,.null),result:decimal,inputs:[decimal,decimal],values:[try numeric(1),try numeric(3)],workMeter:exact)
        #expect(exact.retainedIntermediateBytes == 0)
        let limited = meter(bytes:probe.peakIntermediateBytes-1)
        #expect(throws: DatabaseWorkLimitError.self) {
            try run(.divide(.null,.null),result:decimal,inputs:[decimal,decimal],values:[try numeric(1),try numeric(3)],workMeter:limited)
        }
        #expect(limited.retainedIntermediateBytes == 0)
        #expect(try call("abs",result:.integer,inputs:[.integer],values:[.primitive(.int32(-1))],workMeter:limited) == .primitive(.int32(1)))
        let castProbe = meter()
        _ = try cast(numeric(1234567890123456789,scale:8),from:decimal,to:.doublePrecision,workMeter:castProbe)
        let castExact = meter(bytes:castProbe.peakIntermediateBytes)
        _ = try cast(numeric(1234567890123456789,scale:8),from:decimal,to:.doublePrecision,workMeter:castExact)
        #expect(castExact.retainedIntermediateBytes == 0)
        let castLimited = meter(bytes:castProbe.peakIntermediateBytes-1)
        #expect(throws: DatabaseWorkLimitError.self) {
            try cast(numeric(1234567890123456789,scale:8),from:decimal,to:.doublePrecision,workMeter:castLimited)
        }
        #expect(castLimited.retainedIntermediateBytes == 0)
        let noWork = meter(work:0)
        #expect(throws: DatabaseWorkLimitError.self) {
            try call("abs",result:.integer,inputs:[.integer],values:[.primitive(.int32(-1))],workMeter:noWork)
        }
    }

    @Test("Cancellation and deadline failure preserve typed causes and release scalar workspace")
    func cancellationAndDeadline() async throws {
        let cancelledMeter = meter()
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try run(.divide(.null,.null),result:decimal,inputs:[decimal,decimal],
                           values:[try numeric(1),try numeric(3)],workMeter:cancelledMeter)
        }
        do { _ = try await task.value; Issue.record("Expected cancellation") }
        catch is CancellationError { }
        #expect(cancelledMeter.retainedIntermediateBytes == 0)
        let deadlineMeter = DatabaseWorkMeter(budget:ExecutionBudget(maximumWorkUnits:1_000_000,
            maximumIntermediateBytes:1_000_000,timeoutMilliseconds:5),monotonicClock:ScalarDeadlineClock())
        #expect(throws: DatabaseWorkLimitError.deadline(stage:.expressionEvaluation)) {
            try run(.divide(.null,.null),result:decimal,inputs:[decimal,decimal],
                    values:[try numeric(1),try numeric(3)],workMeter:deadlineMeter)
        }
        #expect(deadlineMeter.peakIntermediateBytes > 0)
        #expect(deadlineMeter.retainedIntermediateBytes == 0)
    }

    @Test("NULLs propagate and supplied values cannot reselect the bound signature")
    func nullAndSelectedSignature() throws {
        let operands: [QueryValue] = [.primitive(.int32(1)), .primitive(.int32(2))]
        let child = CanonicalBoundExpression(expression: .null, type: .sql(.integer), isNullable: true)
        let selected = CanonicalBoundExpression(expression: .add(.null, .null), type: .sql(.integer),
            isNullable: true, children: [child, child])
        #expect(try CanonicalSQLNumericEvaluator.evaluate(selected, operands: operands.lazy.map { $0 }, workMeter: meter()) == .primitive(.int32(3)))
        let shifted = [.primitive(.int32(0))] + operands
        #expect(throws: DatabaseExpressionEvaluationError.typeMismatch(operation: "selected numeric operand count")) {
            try CanonicalSQLNumericEvaluator.evaluate(selected, operands: shifted[1...], workMeter: meter())
        }
        #expect(try run(.add(.null,.null),result:decimal,inputs:[decimal,decimal],values:[.primitive(.null),try numeric(1)]) == .primitive(.null))
        #expect(throws: DatabaseExpressionEvaluationError.typeMismatch(operation:"selected numeric input")) {
            try run(.add(.null,.null),result:.real,inputs:[.real,.real],values:[.primitive(.int32(1)),.primitive(.int32(2))])
        }
    }

    private var limits: QueryValueLimits {
        QueryValueLimits(maximumRetainedBytes:10000,maximumTotalNodes:100,maximumNestingDepth:10,maximumCollectionElements:100)
    }
}

private final class ScalarDeadlineClock: StorageMonotonicClock, Sendable {
    private let ticks = Mutex(Int64(0))
    var now: StorageInstant {
        ticks.withLock { value in
            defer { value += 1 }
            return StorageInstant(durationSinceReference:.milliseconds(value))
        }
    }
    func sleep(until deadline: StorageInstant) async throws(StorageClockError) { }
}
