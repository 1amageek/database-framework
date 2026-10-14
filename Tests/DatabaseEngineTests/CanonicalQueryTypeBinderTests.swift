import DatabaseTypes
import DatabaseKit
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Canonical SQL expression type binding")
struct CanonicalQueryTypeBinderTests {
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
        let expression = try binder.bind(.add(.parameter(.position(1)), .literal(.int(2))))
        #expect(expression.type == .sql(.bigint))
        #expect(expression.children[0].type == .sql(.bigint))
        #expect(throws: DatabaseExpressionEvaluationError.unboundParameter) { try binder.bind(.parameter(.position(2))) }
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
            try binder().bind(.function(FunctionCall(name: "LENGTH", arguments: [.literal(.queryValue(.json(json)))])))
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
        #expect(throws: DatabaseExpressionEvaluationError.unsupportedFunction("FUTURE_FUNCTION")) {
            try binder().bind(.function(FunctionCall(name: "FUTURE_FUNCTION", arguments: [])))
        }
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try binder().bind(.function(FunctionCall(name: "LOWER", arguments: [.literal(.int(1))])))
        }
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try binder().bind(SelectQuery(projection: .all, source: .values([], columnNames: nil), filter: .literal(.int(1))))
        }
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try binder().bind(.coalesce([.literal(.array([.int(1)])), .literal(.int(1))]))
        }
    }

    private var limits: QueryValueLimits {
        QueryValueLimits(maximumRetainedBytes: 4096, maximumTotalNodes: 100,
                         maximumNestingDepth: 10, maximumCollectionElements: 100)
    }

    private func binder(columns: [QueryColumn] = [], parameters: [QueryParameter] = []) -> CanonicalQueryTypeBinder {
        CanonicalQueryTypeBinder(columns: columns, parameters: parameters, resolveColumn: { reference in
            let matches = columns.indices.filter { columns[$0].name == reference.column }
            guard !matches.isEmpty else { throw DatabaseExpressionEvaluationError.missingColumn(reference.column) }
            guard matches.count == 1 else { throw DatabaseExpressionEvaluationError.ambiguousColumn(reference.column) }
            return matches[0]
        }, wildcardSlots: { _ in Array(columns.indices) })
    }
}
