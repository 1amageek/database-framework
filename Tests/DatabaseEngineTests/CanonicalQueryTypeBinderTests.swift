import DatabaseTypes
import DatabaseKit
import StorageKitSystemClock
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Canonical SQL expression type binding")
struct CanonicalQueryTypeBinderTests {

    @Test("SQL constructors reject binding while temporal literal NULL casts retain their declared type")
    func deferredSQLSemanticsFailBinding() throws {
        for expression: Expression in [.array([.int(1)]), .row([.null])] {
            let query = SelectQuery(projection: .items([ProjectionItem(expression)]),
                                    source: .values([], columnNames: nil))
            #expect(throws: DatabaseExpressionEvaluationError.unsupportedExpression("SQL ARRAY/ROW constructor")) {
                try binder().bind(query)
            }
        }
        for type: DataType in [.time(withTimeZone: false, precision: 0),
                               .timestamp(withTimeZone: true, precision: 6)] {
            for operand: Expression in [.null] {
                let bound = try binder().bind(.cast(operand, targetType: type))
                #expect(bound.type == .sql(type))
                #expect(bound.isNullable)
                #expect(try bound.normalize(.primitive(.null)) == .primitive(.null))
            }
            #expect(throws: DatabaseExpressionEvaluationError.self) {
                try binder().bind(.cast(.literal(.queryValue(.primitive(.null), type: .sql(.integer))), targetType: type))
            }
            #expect(throws: DatabaseExpressionEvaluationError.unsupportedExpression("temporal precision conversion")) {
                try binder().bind(.cast(.string("12:00"), targetType: type))
            }
            let identity = try binder(parameters: [QueryParameter(position: 1, value: .primitive(.null), type: .sql(type))])
                .bind(.cast(.parameter(.position(1)), targetType: type))
            #expect(identity.type == .sql(type))
            #expect(try identity.normalize(.primitive(.null)) == .primitive(.null))
            let declared = try binder(parameters: [QueryParameter(position: 1, value: .primitive(.null), type: .sql(type))])
                .bind(.parameter(.position(1)))
            #expect(declared.type == .sql(type))
            let fieldType: FieldSchemaType
            if case .time = type { fieldType = .time } else { fieldType = .timestamp }
            let entity = try Schema.Entity(name: "Temporal", identifierType: .string, fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "clock", fieldNumber: 2, type: fieldType, isOptional: true, queryType: .sql(type)),
            ], directoryComponents: [.staticPath("temporal")])
            #expect(try SchemaEntityEntryCodec.decode(SchemaEntityEntryCodec.encode(entity)) == entity)
        }
        for precision in [-1, 7] {
            #expect(throws: DatabaseExpressionEvaluationError.typeMismatch(operation: "unresolved type")) {
                try binder().bind(.cast(.null, targetType: .time(withTimeZone: false, precision: precision)))
            }
        }
        var fingerprints: Set<ByteString> = []
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 100, maximumWorkUnits: 1_000,
            timeoutMilliseconds: 30_000), monotonicClock: SystemStorageClock())
        for precision: Int? in [nil, 0, 3, 6] {
            let column = try QueryColumn(number: 1, name: "clock",
                type: .sql(.timestamp(withTimeZone: true, precision: precision)), isNullable: true)
            let row = try DatabaseEngine.QueryRow(values: [.primitive(.null)], columns: [column])
            fingerprints.insert(try CanonicalRowFingerprint.compute(row, columns: [column], workMeter: meter))
        }
        #expect(fingerprints.count == 4)
    }

    @Test("An empty input preserves duplicate output labels and declared NULL types")
    func emptyInputHasOrderedTypes() throws {
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.cast(.literal(.null), targetType: .bigint), alias: "same"),
                ProjectionItem(.literal(.int(7)), alias: "same"),
                ProjectionItem(.literal(.null), alias: ""),
            ]),
            source: .values([], columnNames: nil)
        )
        let columns = try binder().bind(query)
        #expect(columns.map(\.number) == [1, 2, 3])
        #expect(columns.map(\.name) == ["same", "same", ""])
        #expect(columns.map(\.type) == [.sql(.bigint), .sql(.bigint), .sql(.text)])
        #expect(columns.map(\.isNullable) == [true, false, true])
    }

    @Test("Resolved columns retain their positional slot and admitted nullable type")
    func columnUsesAdmittedSlot() throws {
        let columns = try [
            QueryColumn(number: 1, name: "name", type: .primitive(.string), isNullable: false),
            QueryColumn(number: 2, name: "score", type: .sql(.integer), isNullable: true),
        ]
        let bound = try binder(columns: columns).bind(.column(ColumnRef("score")))
        #expect(bound.columnSlot == 1)
        #expect(bound.type == .sql(.integer))
        #expect(bound.isNullable)
        for type: QueryColumnType in [.primitive(.bool), .sql(.boolean)] {
            let flag = try QueryColumn(number: 1, name: "eligible", type: type, isNullable: true)
            let boolBinder = binder(columns: [flag])
            for expression: Expression in [
                .equal(.column(ColumnRef("eligible")), .literal(.bool(true))),
                .equal(.literal(.bool(true)), .column(ColumnRef("eligible"))),
            ] {
                let equality = try boolBinder.bind(expression)
                #expect(equality.type == .sql(.boolean))
                #expect(equality.isNullable)
                #expect(equality.children[0].type == equality.children[1].type)
                #expect(try DatabaseExpressionEvaluator(fields: [:])
                    .evaluate(equality.materializing(in: [.primitive(.bool(true))])) == .bool(true))
                #expect(try DatabaseExpressionEvaluator(fields: [:])
                    .evaluate(equality.materializing(in: [.primitive(.bool(false))])) == .bool(false))
                #expect(try DatabaseExpressionEvaluator(fields: [:])
                    .evaluate(equality.materializing(in: [.primitive(.null)])) == .null)
            }
            for invalid: Expression in [.literal(.int(1)), .literal(.array([.bool(true)]))] {
                #expect(throws: DatabaseExpressionEvaluationError.typeMismatch(operation: "common result type")) {
                    try boolBinder.bind(.equal(.column(ColumnRef("eligible")), invalid))
                }
            }
        }
    }

    @Test("Invalid references fail during binding despite an empty relation")
    func missingAndAmbiguousNamesFail() throws {
        let columns = try [
            QueryColumn(number: 1, name: "id", type: .sql(.bigint), isNullable: false),
            QueryColumn(number: 2, name: "id", type: .sql(.bigint), isNullable: false),
        ]
        let binder = binder(columns: columns)
        #expect(throws: DatabaseExpressionEvaluationError.ambiguousColumn("id")) {
            try binder.bind(.column(ColumnRef("id")))
        }
        #expect(throws: DatabaseExpressionEvaluationError.missingColumn("absent")) {
            try binder.bind(.column(ColumnRef("absent")))
        }
    }

    @Test("Wildcard projection follows bound slot order and renumbers output ordinals")
    func wildcardUsesDeclaredOrder() throws {
        let columns = try [
            QueryColumn(number: 1, name: "first", type: .sql(.text), isNullable: false),
            QueryColumn(number: 2, name: "second", type: .sql(.bigint), isNullable: true),
        ]
        let binder = CanonicalQueryTypeBinder(
            columns: columns,
            resolveColumn: { _ in 0 },
            wildcardSlots: { _ in [1, 0] }
        )
        let output = try binder.bind(SelectQuery(projection: .all, source: .values([], columnNames: nil)))
        #expect(output.map(\.name) == ["second", "first"])
        #expect(output.map(\.number) == [1, 2])
        #expect(output.map(\.isNullable) == [true, false])
    }

    @Test("Explicit parameter declarations survive NULL and typed empty arrays")
    func explicitParameterTypes() throws {
        let array = try QueryArrayValue(elementType: .integer, dimensions: [], elements: [], limits: limits)
        let binder = binder(parameters: [
            QueryParameter(position: 1, value: .primitive(.null), type: .sql(.bigint)),
            QueryParameter(position: 2, name: "items", value: .array(array), type: .sql(.array(.integer))),
        ])
        let null = try binder.bind(.parameter(.position(1)))
        let empty = try binder.bind(.parameter(.name("items")))
        #expect(null.type == .sql(.bigint))
        #expect(null.isNullable)
        #expect(empty.type == .sql(.array(.integer)))
        #expect(!empty.isNullable)
        #expect(try empty.normalize(.array(array)) == .array(array))
    }

    @Test("Untyped NULL parameters require context and unknown parameters fail")
    func parameterTypeContext() throws {
        let binder = binder(parameters: [QueryParameter(position: 1, value: .primitive(.null))])
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder.bind(.parameter(.position(1))) }
        let expression = try self.binder(parameters: [QueryParameter(position: 1, value: .primitive(.null))])
            .bind(.add(.parameter(.position(1)), .literal(.int(2))))
        #expect(expression.type == .sql(.bigint))
        #expect(expression.children[0].type == .sql(.bigint))
        #expect(throws: DatabaseExpressionEvaluationError.unboundParameter) { try binder.bind(.parameter(.position(2))) }
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 100, maximumWorkUnits: 1_000,
            maximumIntermediateBytes: 1024, timeoutMilliseconds: 30_000), monotonicClock: SystemStorageClock())
        let array = QueryParameter(position: 1, value: .primitive(.array(Array(repeating: .int64(7), count: 10_000))))
        let state = try CanonicalParameterBindingState(parameters: [array], workMeter: meter)
        let baseline = meter.retainedIntermediateBytes
        let bound = try self.binder(parameterState: state, workMeter: meter).bind(.parameter(.position(1)))
        #expect(bound.type == .primitive(.int64, isArray: true))
        #expect(meter.peakIntermediateBytes == baseline)
        #expect(meter.retainedIntermediateBytes == baseline)
        for values: [FieldValue] in [[.null], [.int8(1), .int64(1)], [.array([.int64(1)])]] {
            #expect(throws: DatabaseExpressionEvaluationError.typeMismatch(operation: "untyped primitive array")) {
                try CanonicalQueryTypeBinder.type(of: .primitive(.array(values)))
            }
        }
    }

    @Test("Literal declarations are validated rather than guessed from their payload")
    func incompatibleLiteralTypeFails() throws {
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try binder().bind(.literal(.queryValue(.primitive(.int64(1)), type: .sql(.text))))
        }
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try binder().bind(.cast(.literal(.null), targetType: .custom("unknown")))
        }
    }

    @Test("CASE binds every branch and resolves NULL without evaluating results")
    func caseTypeAndBranchValidation() throws {
        let expression = DatabaseKit.Expression.caseWhen(cases: [
            CaseWhenPair(condition: .literal(.bool(true)), result: .literal(.null)),
            CaseWhenPair(condition: .literal(.bool(false)), result: .literal(.int(9))),
        ], elseResult: .literal(.int(4)))
        let bound = try binder().bind(expression)
        #expect(bound.type == .sql(.bigint))
        #expect(bound.isNullable)
        #expect(try bound.normalize(.primitive(DatabaseExpressionEvaluator(fields: [:]).evaluate(expression))) == .primitive(.null))
        let invalid = DatabaseKit.Expression.caseWhen(cases: [
            CaseWhenPair(condition: .literal(.bool(true)), result: .literal(.int(1))),
        ], elseResult: .literal(.string("unselected")))
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(invalid) }
    }

    @Test("COALESCE derives common type and nullable metadata from every branch")
    func coalesceAndNullIf() throws {
        let coalesce = try binder().bind(.coalesce([.literal(.null), .literal(.int(5))]))
        #expect(coalesce.type == .sql(.bigint))
        #expect(!coalesce.isNullable)
        #expect(coalesce.children[0].type == .sql(.bigint))
        let nullIf = try binder().bind(.nullIf(.literal(.int(5)), .literal(.int(5))))
        #expect(nullIf.type == .sql(.bigint))
        #expect(nullIf.isNullable)
    }

    @Test("Checked normalization makes evaluator numeric widths match CAST declarations")
    func castNormalization() throws {
        for (target, expected) in [(DataType.smallint, FieldValue.int16(7)), (.integer, .int32(7)), (.real, .float32(7))] {
            let expression = DatabaseKit.Expression.cast(.literal(.int(7)), targetType: target)
            let bound = try binder().bind(expression)
            let actual = try DatabaseExpressionEvaluator(fields: [:]).evaluate(expression)
            #expect(try bound.normalize(.primitive(actual)) == .primitive(expected))
        }
        let narrow = try binder().bind(.cast(.literal(.int(32768)), targetType: .smallint))
        #expect(throws: DatabaseExpressionEvaluationError.numericOverflow) {
            try narrow.normalize(.primitive(.int64(32768)))
        }
        let rounded = try binder().bind(.cast(.literal(.double(0.1)), targetType: .real))
        #expect(try rounded.normalize(.primitive(.float64(0.1))) == .primitive(.float32(0.1)))
    }

    @Test("Mixed signed and unsigned arithmetic has one exact result type")
    func exactArithmeticNormalization() throws {
        let expression = DatabaseKit.Expression.add(.literal(.int(1)), .literal(.uint(2)))
        let bound = try binder().bind(expression)
        #expect(bound.type == .sql(.decimal(precision: nil, scale: nil)))
        let actual = try DatabaseExpressionEvaluator(fields: [:]).evaluate(expression)
        let normalized = try bound.normalize(.primitive(actual))
        #expect(QueryValueValidator.accepts(normalized, type: bound.type))
        #expect(try bound.normalize(.primitive(.int64(3))) == .numeric(QueryNumericValue(value: ExactDecimal(coefficient: 3, scale: 0), displayScale: 0)))
    }

    @Test("Aggregate signatures normalize actual reducer representations")
    func aggregateResultSignatures() throws {
        let binder = binder()
        let count = try binder.bind(.aggregate(.count(nil, distinct: false)))
        #expect(count.type == .sql(.bigint))
        #expect(!count.isNullable)
        let sum = try binder.bind(.aggregate(.sum(.literal(.int(1)), distinct: false)))
        let average = try binder.bind(.aggregate(.avg(.literal(.int(1)), distinct: false)))
        var accumulator = DatabaseNumericAggregateAccumulator()
        try accumulator.add(.int64(1))
        try accumulator.add(.int64(2))
        #expect(sum.type == .sql(.decimal(precision: nil, scale: nil)))
        #expect(average.type == .sql(.doublePrecision))
        #expect(sum.isNullable && average.isNullable)
        let sumResult = try accumulator.sum()
        let averageResult = try accumulator.average()
        let sumValue = try #require(sumResult)
        let averageValue = try #require(averageResult)
        #expect(QueryValueValidator.accepts(try sum.normalize(.primitive(sumValue)), type: sum.type))
        #expect(try average.normalize(.primitive(averageValue)) == .primitive(.float64(1.5)))
        #expect(try average.normalize(.primitive(.int64(2))) == .primitive(.float64(2)))
        #expect(throws: DatabaseExpressionEvaluationError.numericOverflow) {
            try average.normalize(.primitive(.int64(9_007_199_254_740_993)))
        }
        let numericType = QueryColumnType.sql(.decimal(precision: nil, scale: nil))
        let amount = try QueryColumn(number: 1, name: "amount", type: numericType, isNullable: false)
        let numericBinder = self.binder(columns: [amount])
        for aggregate: AggregateFunction in [.sum(.column(ColumnRef("amount")), distinct: false),
                                            .avg(.column(ColumnRef("amount")), distinct: false)] {
            let bound = try numericBinder.bind(.aggregate(aggregate))
            #expect(bound.type == numericType)
            #expect(bound.children[0].type == numericType)
            #expect(bound.children[0].columnSlot == 0)
        }
        let rich = QueryValue.numeric(try QueryNumericValue(value: ExactDecimal(coefficient: 125, scale: 2), displayScale: 2))
        #expect(try binder.bind(.aggregate(.sum(.literal(.queryValue(rich)), distinct: false))).type == numericType)
        #expect(throws: DatabaseExpressionEvaluationError.typeMismatch(operation: "numeric expression")) {
            try binder.bind(.aggregate(.sum(.literal(.string("1")), distinct: false)))
        }
    }

    @Test("Scalar subqueries use declared metadata and require one output column")
    func scalarSubqueryBinding() throws {
        let query = SelectQuery(projection: .all, source: .values([], columnNames: nil))
        let output = try QueryColumn(number: 1, name: "value", type: .sql(.jsonb), isNullable: false)
        let binder = CanonicalQueryTypeBinder(columns: [], resolveColumn: { _ in 0 }, wildcardSlots: { _ in [] }, bindSubquery: { _ in [output] })
        let scalar = try binder.bind(.subquery(query))
        #expect(scalar.type == .sql(.jsonb))
        #expect(scalar.isNullable)
        let invalid = CanonicalQueryTypeBinder(columns: [], resolveColumn: { _ in 0 }, wildcardSlots: { _ in [] }, bindSubquery: { _ in [output, output] })
        #expect(throws: DatabaseExpressionEvaluationError.self) { try invalid.bind(.subquery(query)) }
        let exists = try invalid.bind(.exists(query))
        #expect(exists.type == .sql(.boolean))
        #expect(!exists.isNullable)
    }

    @Test("Rich literal projection retains its query type and original value")
    func richLiteralProjection() throws {
        let json = try QueryJSONValue(bytes: ByteString(Array(" {\"a\": 1, \"a\": 2} ".utf8)), limits: limits)
        let bound = try binder().bind(.literal(.queryValue(.json(json))))
        #expect(bound.type == .sql(.json))
        #expect(try bound.normalize(.json(json)) == .json(json))
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try binder().bind(.function(FunctionCall(name: "length", arguments: [.literal(.queryValue(.json(json)))])))
        }
    }

    @Test("Primitive RDF projection keeps primitive semantics")
    func rdfKeepsPrimitiveType() throws {
        let bound = try binder().bind(.literal(.iri("https://example.org/value")))
        #expect(bound.type == .primitive(.rdfTerm))
        let array = try binder().bind(.literal(.array([.int(1), .null, .int(2)])))
        #expect(array.type == .primitive(.int64, isArray: true))
    }

    @Test("Unsupported functions and invalid predicates fail statically")
    func unsupportedOperationsFailBeforeRows() throws {
        #expect(throws: DatabaseExpressionEvaluationError.unsupportedFunction("future_function")) {
            try binder().bind(.function(FunctionCall(name: "future_function", arguments: [])))
        }
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try binder().bind(.function(FunctionCall(name: "lower", arguments: [.literal(.int(1))])))
        }
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try binder().bind(SelectQuery(projection: .all, source: .values([], columnNames: nil), filter: .literal(.int(1))))
        }
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try binder().bind(.coalesce([.literal(.array([.int(1)])), .literal(.int(1))]))
        }
    }

    @Test("Unknown strings choose executable comparison and conditional coercions")
    func unknownStringsExecuteSelectedCoercion() throws {
        let unknown = Expression.literal(.sqlUnknownString("7"))
        let comparison = try binder().bind(.equal(unknown, .literal(.int(7))))
        #expect(try DatabaseExpressionEvaluator(fields: [:]).evaluate(comparison.materializing(in: [])) == .bool(true))
        let primitiveInputs: [(FieldSchemaType, FieldValue)] = [
            (.int8, .int8(7)), (.int16, .int16(7)), (.int32, .int32(7)), (.int64, .int64(7)),
            (.uint8, .uint8(7)), (.uint16, .uint16(7)), (.uint32, .uint32(7)), (.uint64, .uint64(7)),
            (.float32, .float32(7)), (.float64, .float64(7)),
            (.decimal, .decimal(ExactDecimal(coefficient: 7, scale: 0))),
        ]
        for (kind, expected) in primitiveInputs {
            let type = QueryColumnType.primitive(kind)
            let column = try QueryColumn(number: 1, name: "value", type: type, isNullable: false)
            let bound = try binder(columns: [column]).bind(.equal(.column(ColumnRef("value")), unknown))
            #expect(bound.children.map(\.type) == [type, type])
            #expect(try DatabaseExpressionEvaluator(fields: [:])
                .evaluate(bound.children[1].materializing(in: [])) == expected)
            #expect(try DatabaseExpressionEvaluator(fields: [:])
                .evaluate(bound.materializing(in: [.primitive(expected)])) == .bool(true))
        }
        let maximumUnsigned = try binder().bind(.literal(.sqlUnknownString("18446744073709551615")),
            contextualType: .primitive(.uint64))
        #expect(try DatabaseExpressionEvaluator(fields: [:])
            .evaluate(maximumUnsigned.materializing(in: [])) == .uint64(.max))
        #expect(throws: DatabaseExpressionEvaluationError.invalidCast("unsigned integer input")) {
            try binder().bind(.literal(.sqlUnknownString("-1")), contextualType: .primitive(.uint64))
        }
        #expect(throws: DatabaseExpressionEvaluationError.numericOverflow) {
            try binder().bind(.literal(.sqlUnknownString("128")), contextualType: .primitive(.int8))
        }
        for (text, type, message): (String, QueryColumnType, String) in [
            ("invalid", .primitive(.int64), "integer input"),
            ("NaN", .primitive(.float32), "floating-point input"),
            ("invalid", .primitive(.decimal), "numeric input"),
        ] {
            #expect(throws: DatabaseExpressionEvaluationError.invalidCast(message)) {
                try binder().bind(.literal(.sqlUnknownString(text)), contextualType: type)
            }
        }
        for type: QueryColumnType in [.primitive(.int64, isArray: true), .primitive(.bytes), .sql(.array(.integer))] {
            #expect(throws: DatabaseExpressionEvaluationError.unsupportedExpression("SQL unknown string input for unsupported target type")) {
                try binder().bind(unknown, contextualType: type)
            }
        }
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try binder().bind(.equal(.literal(.string("7")), .literal(.int(7))))
        }
        let conditional = try binder().bind(.caseWhen(cases: [
            CaseWhenPair(condition: .literal(.bool(true)), result: unknown),
        ], elseResult: .literal(.int(8))))
        #expect(conditional.type == .sql(.bigint))
        #expect(try DatabaseExpressionEvaluator(fields: [:]).evaluate(conditional.materializing(in: [])) == .int64(7))
        let coalesce = try binder().bind(.coalesce([.literal(.null), unknown]))
        #expect(coalesce.type == .sql(.text))
        #expect(try DatabaseExpressionEvaluator(fields: [:]).evaluate(coalesce.materializing(in: [])) == .string("7"))
        let widths = try binder().bind(.coalesce([
            .literal(.queryValue(.primitive(.int16(7)), type: .sql(.smallint))),
            .literal(.queryValue(.primitive(.int32(8)), type: .sql(.integer))),
        ]))
        #expect(widths.type == .sql(.integer))
        #expect(try DatabaseExpressionEvaluator(fields: [:]).evaluate(widths.materializing(in: [])) == .int32(7))
        for text in ["0xF", "1_0"] {
            let cast = try binder().bind(.cast(.literal(.sqlUnknownString(text)), targetType: .integer))
            #expect(try DatabaseExpressionEvaluator(fields: [:]).evaluate(cast.materializing(in: []))
                == .int32(text == "0xF" ? 15 : 10))
        }
        let rich = QueryValue.numeric(try QueryNumericValue(value: ExactDecimal(coefficient: 7, scale: 0), displayScale: 0))
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try binder().bind(.add(.literal(.queryValue(rich)), .literal(.int(1))))
        }
        let scratchBytes = UInt64(8 * (" 7 ".utf8.count + 1 + MemoryLayout<String>.stride))
        func meter(_ bytes: UInt64) -> DatabaseWorkMeter {
            DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 100, maximumWorkUnits: 1_000,
                maximumIntermediateBytes: bytes, timeoutMilliseconds: 30_000), monotonicClock: SystemStorageClock())
        }
        let exact = meter(scratchBytes)
        let converted = try binder(workMeter: exact).bind(.literal(.sqlUnknownString(" 7 ")),
            contextualType: .primitive(.int64))
        #expect(try DatabaseExpressionEvaluator(fields: [:]).evaluate(converted.materializing(in: [])) == .int64(7))
        #expect(exact.peakIntermediateBytes == scratchBytes)
        #expect(exact.retainedIntermediateBytes == 0)
        let tooSmall = meter(scratchBytes - 1)
        #expect(throws: DatabaseWorkLimitError.self) {
            try binder(workMeter: tooSmall).bind(.literal(.sqlUnknownString(" 7 ")),
                contextualType: .primitive(.int64))
        }
        #expect(tooSmall.peakIntermediateBytes == 0)
        #expect(tooSmall.retainedIntermediateBytes == 0)
        #expect(throws: DatabaseExpressionEvaluationError.invalidCast("integer input")) {
            try binder(workMeter: exact).bind(.literal(.sqlUnknownString("bad")), contextualType: .primitive(.int64))
        }
        #expect(exact.retainedIntermediateBytes == 0)
        let decimalScratch = meter(4096)
        let numeric = try binder(workMeter: decimalScratch).bind(.literal(.sqlUnknownString("1.25")),
            contextualType: .primitive(.decimal))
        #expect(try DatabaseExpressionEvaluator(fields: [:]).evaluate(numeric.materializing(in: []))
            == .decimal(ExactDecimal(coefficient: 125, scale: 2)))
        #expect(decimalScratch.retainedIntermediateBytes == 0)
        #expect(throws: DatabaseExpressionEvaluationError.invalidCast("numeric input")) {
            try binder(workMeter: decimalScratch).bind(.literal(.sqlUnknownString("1e2e3")),
                contextualType: .primitive(.decimal))
        }
        #expect(decimalScratch.retainedIntermediateBytes == 0)
    }

    @Test("Function lookup uses exact names and the admitted catalog schema")
    func exactFunctionSignatures() throws {
        let lower = try binder().bind(.function(FunctionCall(name: "lower", arguments: [
            .literal(.sqlUnknownString("ABC")),
        ], schema: "pg_catalog")))
        #expect(try DatabaseExpressionEvaluator(fields: [:]).evaluate(lower.materializing(in: [])) == .string("abc"))
        for function in [FunctionCall(name: "LOWER", arguments: [.literal(.string("ABC"))]),
                         FunctionCall(name: "pg_catalog.lower", arguments: [.literal(.string("ABC"))]),
                         FunctionCall(name: "lower", arguments: [.literal(.string("ABC"))], schema: "public")] {
            #expect(throws: DatabaseExpressionEvaluationError.unsupportedFunction(function.name)) {
                try binder().bind(.function(function))
            }
        }
        let length = try binder().bind(.function(FunctionCall(name: "length", arguments: [.literal(.string("ABC"))])))
        #expect(length.type == .sql(.integer))
        #expect(try DatabaseExpressionEvaluator(fields: [:]).evaluate(length.materializing(in: [])) == .int32(3))
        let unknown = try binder().bind(.function(FunctionCall(name: "abs", arguments: [.literal(.sqlUnknownString("-7.0"))])))
        #expect(unknown.type == .sql(.doublePrecision))
        #expect(try DatabaseExpressionEvaluator(fields: [:]).evaluate(unknown.materializing(in: [])) == .float64(7))
        let narrow = try binder().bind(.function(FunctionCall(name: "abs", arguments: [
            .literal(.queryValue(.primitive(.int16(-7)), type: .sql(.smallint))),
        ])))
        #expect(narrow.type == .sql(.smallint))
        #expect(try DatabaseExpressionEvaluator(fields: [:]).evaluate(narrow.materializing(in: [])) == .int16(7))
        let overflow = try binder().bind(.function(FunctionCall(name: "abs", arguments: [
            .literal(.queryValue(.primitive(.int16(.min)), type: .sql(.smallint))),
        ])))
        #expect(throws: DatabaseExpressionEvaluationError.numericOverflow) {
            try DatabaseExpressionEvaluator(fields: [:]).evaluate(overflow.materializing(in: []))
        }
    }

    @Test("Slot materialization executes the bound address and preserves lazy branches")
    func positionalMaterialization() throws {
        let column = try QueryColumn(number: 1, name: "value", type: .sql(.integer), isNullable: false)
        let selected = try binder(columns: [column]).bind(.column(ColumnRef("value")))
        #expect(try DatabaseExpressionEvaluator(fields: ["value": .int32(999)])
            .evaluate(selected.materializing(in: [.primitive(.int32(7))])) == .int32(7))
        let lazy = try binder().bind(.caseWhen(cases: [CaseWhenPair(condition: .literal(.bool(true)),
            result: .literal(.int(1)))], elseResult: .divide(.literal(.int(1)), .literal(.int(0)))))
        #expect(try DatabaseExpressionEvaluator(fields: [:]).evaluate(lazy.materializing(in: [])) == .int64(1))
    }

    @Test("Typed NULL cast sources are checked before NULL identity lowering")
    func typedNullCastEdges() throws {
        let invalid = Expression.cast(.cast(.literal(.null), targetType: .uuid), targetType: .integer)
        #expect(throws: DatabaseExpressionEvaluationError.invalidCast("unsupported source-to-target conversion")) {
            try binder().bind(invalid)
        }
        let legal = try binder().bind(.cast(.cast(.literal(.null), targetType:
            .timestamp(withTimeZone: false, precision: 6)), targetType: .timestamp(withTimeZone: false, precision: 3)))
        #expect(legal.type == .sql(.timestamp(withTimeZone: false, precision: 3)))
        #expect(try DatabaseExpressionEvaluator(fields: [:]).evaluate(legal.materializing(in: [])) == .null)
        let legalParameterCasts: [(DataType, DataType)] = [
            (.timestamp(withTimeZone: false, precision: 6), .timestamp(withTimeZone: false, precision: 3)),
            (.time(withTimeZone: false, precision: 6), .time(withTimeZone: false, precision: 3)),
            (.decimal(precision: nil, scale: nil), .decimal(precision: 6, scale: 2)),
        ]
        for (source, target) in legalParameterCasts {
            let parameter = QueryParameter(position: 1, value: .primitive(.null), type: .sql(source))
            let bound = try binder(parameters: [parameter]).bind(.cast(.parameter(.position(1)), targetType: target))
            #expect(bound.type == .sql(target))
            #expect(bound.isNullable)
            #expect(try DatabaseExpressionEvaluator(fields: [:]).evaluate(bound.materializing(in: [])) == .null)
        }
        for (source, target): (DataType, DataType) in [(.uuid, .integer),
            (.integer, .timestamp(withTimeZone: false, precision: 3))] {
            let parameter = QueryParameter(position: 1, value: .primitive(.null), type: .sql(source))
            #expect(throws: DatabaseExpressionEvaluationError.invalidCast("unsupported source-to-target conversion")) {
                try binder(parameters: [parameter]).bind(.cast(.parameter(.position(1)), targetType: target))
            }
        }
    }

    @Test("Ordered parameter inference is finalized before rows")
    func orderedParameterInference() throws {
        let parameter = Expression.parameter(.position(1))
        func query(_ expressions: [Expression], filter: Expression? = nil) -> SelectQuery {
            SelectQuery(projection: .items(expressions.map { ProjectionItem($0) }),
                        source: .values([], columnNames: nil), filter: filter)
        }
        let supplied = [QueryParameter(position: 1, value: .primitive(.null))]
        #expect(try binder(parameters: supplied).bind(query([parameter]))[0].type == .sql(.text))
        for expressions in [[Expression.isNull(parameter)], [parameter, .isNull(parameter)],
                            [.isNull(parameter), .cast(parameter, targetType: .integer)]] {
            #expect(throws: DatabaseExpressionEvaluationError.self) { try binder(parameters: supplied).bind(query(expressions)) }
        }
        let state = CanonicalParameterBindingState(parameters: supplied)
        let bound = try binder(parameterState: state).bindSelect(query([
            .cast(parameter, targetType: .integer), .cast(parameter, targetType: .text),
        ]))
        try state.finalize()
        #expect(state.resolvedParameters[0].type == .sql(.integer))
        #expect(bound.columns.map(\.type) == [.sql(.integer), .sql(.text)])
        let nonnull = CanonicalParameterBindingState(parameters: [QueryParameter(position: 1, value: .primitive(.int32(7)))])
        let cast = try binder(parameterState: nonnull).bind(.cast(parameter, targetType: .text))
        try nonnull.finalize()
        #expect(try DatabaseExpressionEvaluator(fields: [:]).evaluate(cast.materializing(in: [])) == .string("7"))
        let projectedFirst = CanonicalParameterBindingState(parameters: supplied)
        _ = try binder(columns: [try QueryColumn(number: 1, name: "i", type: .sql(.integer), isNullable: false)],
            parameterState: projectedFirst).bindSelect(query([
                .function(FunctionCall(name: "abs", arguments: [parameter])),
            ], filter: .equal(.column(ColumnRef("i")), parameter)))
        try projectedFirst.finalize()
        #expect(projectedFirst.resolvedParameters[0].type == .sql(.doublePrecision))
    }

    @Test("Equal expression values retain distinct semantic failure occurrences")
    func occurrenceAddressedFailure() throws {
        var occurrences = QueryOccurrenceArena()
        let root = try occurrences.append()
        let first = try occurrences.append(parent: root, field: .projection, index: 0)
        let second = try occurrences.append(parent: root, field: .projection, index: 1)
        let repeated = Expression.column(ColumnRef("id"))
        let declaration = try QueryColumn(number: 1, name: "id", type: .sql(.integer), isNullable: false)
        let firstScope = CanonicalQueryTypeBinder(columns: [declaration], occurrences: occurrences, root: root,
            resolveColumn: { _ in 0 }, wildcardSlots: { _ in [0] })
        #expect(try firstScope.bind(repeated, occurrence: first).columnSlot == 0)
        let binder = CanonicalQueryTypeBinder(columns: [], occurrences: occurrences, root: root,
            resolveColumn: { reference in throw DatabaseExpressionEvaluationError.missingColumn(reference.column) },
            wildcardSlots: { _ in [] })
        do {
            _ = try binder.bind(repeated, occurrence: second)
            Issue.record("Missing column must fail before a row is needed")
        } catch let failure as CanonicalSQLBindingFailure {
            #expect(failure.occurrence == second)
            #expect(failure.occurrence != first)
            if case .expression(.missingColumn("id")) = failure.cause {} else {
                Issue.record("Binding lost its original typed cause")
            }
        }
    }

    private var limits: QueryValueLimits {
        QueryValueLimits(maximumRetainedBytes: 4096, maximumTotalNodes: 100,
                         maximumNestingDepth: 10, maximumCollectionElements: 100)
    }

    private func binder(columns: [QueryColumn] = [], parameters: [QueryParameter] = [],
                        parameterState: CanonicalParameterBindingState? = nil,
                        workMeter: DatabaseWorkMeter? = nil) -> CanonicalQueryTypeBinder {
        CanonicalQueryTypeBinder(columns: columns, parameters: parameters, parameterState: parameterState, resolveColumn: { reference in
            let matches = columns.indices.filter { columns[$0].name == reference.column }
            guard !matches.isEmpty else { throw DatabaseExpressionEvaluationError.missingColumn(reference.column) }
            guard matches.count == 1 else { throw DatabaseExpressionEvaluationError.ambiguousColumn(reference.column) }
            return matches[0]
        }, wildcardSlots: { _ in Array(columns.indices) }, workMeter: workMeter)
    }
}
