import DatabaseTypes
import DatabaseKit
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Selected SQL signatures before source reads")
struct CanonicalSelectedSignatureTests {
    private let numeric = DataType.decimal(precision: nil, scale: nil)
    private var six: [DataType] { [.smallint, .integer, .bigint, numeric, .real, .doublePrecision] }

    @Test("The six-type arithmetic matrix selects exact outputs on empty and declared NULL inputs")
    func arithmeticMatrix() throws {
        let matrix: [[Int]] = [
            [0, 1, 2, 3, 5, 5], [1, 1, 2, 3, 5, 5], [2, 2, 2, 3, 5, 5],
            [3, 3, 3, 3, 5, 5], [5, 5, 5, 5, 4, 5], [5, 5, 5, 5, 5, 5],
        ]
        let operators: [(Expression, Expression) -> Expression] = [Expression.add, Expression.subtract, Expression.multiply, Expression.divide, Expression.modulo]
        for (operation, make) in operators.enumerated() {
            for left in six.indices {
                for right in six.indices {
                    let expression = make(null(six[left]), null(six[right]))
                    if operation == 4 && (left >= 4 || right >= 4) {
                        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(empty(expression)) }
                    } else {
                        let expected = QueryColumnType.sql(six[matrix[left][right]])
                        let selected = try binder().bind(expression)
                        #expect(selected.type == expected)
                        let inputs: [QueryColumnType] = left < 3 && right < 3
                            ? [.sql(six[left]), .sql(six[right])] : [expected, expected]
                        #expect(selected.children.map(\.type) == inputs)
                        #expect(selected.isNullable)
                        #expect(try binder().bind(empty(expression))[0].type == expected)
                    }
                }
            }
        }
    }

    @Test("Every F02/F03 unary catalog family admits the six declared input types")
    func unaryFunctionMatrix() throws {
        let paired = ["sign", "ceil", "ceiling", "floor", "round", "trunc", "sqrt", "exp", "ln", "log", "log10"]
        let doubleOnly = ["cbrt", "degrees", "radians", "sin", "cos", "tan", "asin", "acos", "atan"]
        for (index, type) in six.enumerated() {
            let absolute = try function("abs", [null(type)])
            #expect(absolute.type == .sql(type))
            #expect(absolute.children[0].type == .sql(type))
            for name in paired + doubleOnly {
                let expected = paired.contains(name) && index == 3 ? numeric : .doublePrecision
                let selected = try function(name, [null(type)])
                #expect(selected.type == .sql(expected))
                #expect(selected.children[0].type == .sql(expected))
            }
            for name in ["scale", "min_scale", "trim_scale"] {
                if index >= 4 {
                    #expect(throws: DatabaseExpressionEvaluationError.self) { try function(name, [null(type)]) }
                } else {
                    let selected = try function(name, [null(type)])
                    #expect(selected.type == .sql(name == "trim_scale" ? numeric : .integer))
                    #expect(selected.children[0].type == .sql(numeric))
                }
            }
        }
        let pi = try function("pi", [])
        #expect(pi.type == .sql(.doublePrecision))
        #expect(!pi.isNullable && pi.children.isEmpty)
    }

    @Test("Binary catalog matrices preserve numeric-only, preferred float and ambiguity decisions")
    func binaryFunctionMatrix() throws {
        let operatorMatrix = [[0, 1, 2, 3], [1, 1, 2, 3], [2, 2, 2, 3], [3, 3, 3, 3]]
        for left in six.indices {
            for right in six.indices {
                let arguments = [null(six[left]), null(six[right])]
                for name in ["mod", "div", "gcd", "lcm", "log"] {
                    if left >= 4 || right >= 4 || (["gcd", "lcm"].contains(name) && left == 0 && right == 0) {
                        #expect(throws: DatabaseExpressionEvaluationError.self) { try function(name, arguments) }
                    } else {
                        let selected = try function(name, arguments)
                        let expected = ["div", "log"].contains(name) ? numeric : six[operatorMatrix[left][right]]
                        #expect(selected.type == .sql(expected))
                        #expect(selected.children.map(\.type) == [.sql(expected), .sql(expected)])
                    }
                }
                let expectedPower = (left == 3 || right == 3) && left < 4 && right < 4 ? numeric : .doublePrecision
                let power = try function("power", arguments)
                #expect(power.type == .sql(expectedPower))
                #expect(power.children.map(\.type) == [.sql(expectedPower), .sql(expectedPower)])
                let atan2 = try function("atan2", arguments)
                #expect(atan2.type == .sql(.doublePrecision))
                #expect(atan2.children.allSatisfy { $0.type == .sql(.doublePrecision) })
            }
        }
    }

    @Test("Scaled and bucket signatures infer unknown integer positions without narrowing declared types")
    func scaledAndBucketSignatures() throws {
        for type in six {
            for name in ["round", "trunc"] {
                if type == .real || type == .doublePrecision {
                    #expect(throws: DatabaseExpressionEvaluationError.self) { try function(name, [null(type), .null]) }
                } else {
                    let selected = try function(name, [null(type), .null])
                    #expect(selected.children.map(\.type) == [.sql(numeric), .sql(.integer)])
                }
            }
        }
        let bucket = try function("width_bucket", [null(numeric), null(.integer), null(.bigint), .null])
        #expect(bucket.type == .sql(.integer))
        #expect(bucket.children.map(\.type) == [.sql(numeric), .sql(numeric), .sql(numeric), .sql(.integer)])
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try function("width_bucket", [null(numeric), null(numeric), null(numeric), null(.bigint)])
        }
        let unknown = try function("abs", [.literal(.sqlUnknownString("-7"))])
        #expect(unknown.children[0].type == .sql(.doublePrecision))
    }

    @Test("Explicit, assignment and implicit casts are admitted separately before NULL propagation")
    func castContextsAndTypedNull() throws {
        for source in six {
            for target in six {
                let selected = try binder().bind(.cast(null(source), targetType: target))
                #expect(selected.type == .sql(target) && selected.isNullable)
                try binder().validateCast(from: .sql(source), to: .sql(target), context: .assignment)
            }
        }
        try binder().validateCast(from: .sql(.integer), to: .sql(numeric), context: .implicit)
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try binder().validateCast(from: .sql(numeric), to: .sql(.integer), context: .implicit)
        }
        for type in six {
            for expression in [Expression.cast(null(type), targetType: .boolean), .cast(null(.boolean), targetType: type)] {
                if type == .integer { _ = try binder().bind(expression) }
                else { #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(expression) } }
            }
        }
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try binder().validateCast(from: .sql(.integer), to: .sql(.boolean), context: .assignment)
        }
        for target in [DataType.decimal(precision: 1, scale: -1000), .decimal(precision: 38, scale: 1000), .decimal(precision: 3, scale: 5)] {
            #expect(try binder().bind(.cast(null(numeric), targetType: target)).type == .sql(target))
        }
        for target in [DataType.decimal(precision: 0, scale: 0), .decimal(precision: 39, scale: 0), .decimal(precision: 38, scale: 1001)] {
            #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(.cast(.null, targetType: target)) }
        }
        let typmod = DataType.decimal(precision: 4, scale: 2)
        for source: QueryColumnType in [.sql(.text), .primitive(.string)] {
            for value: QueryValue in [.primitive(.null), .primitive(.string("1.255"))] {
                let parameter = QueryParameter(position: 1, value: value, type: source)
                #expect(try binder(parameters: [parameter]).bind(.cast(.parameter(.position(1)), targetType: numeric)).type == .sql(numeric))
                let selected = try binder(parameters: [parameter]).bind(.cast(.parameter(.position(1)), targetType: typmod))
                #expect(selected.type == .sql(typmod))
                #expect(selected.castContext == .explicit)
                #expect(selected.isNullable == value.isNull)
            }
        }
        let unknown = try binder().bind(.cast(.literal(.sqlUnknownString("1.255")), targetType: typmod))
        #expect(unknown.type == .sql(typmod))
        #expect(unknown.children[0].type == .sql(numeric))
    }

    @Test("Conditional common types remain separate from operator and source alignment types")
    func conditionalTypes() throws {
        let operation = try binder().bind(.add(null(numeric), null(.real)))
        #expect(operation.type == .sql(.doublePrecision))
        for expression in [Expression.coalesce([null(numeric), null(.real)]), .greatest([null(numeric), null(.real)]), .least([null(numeric), null(.real)])] {
            let selected = try binder().bind(expression)
            #expect(selected.type == .sql(.real))
            #expect(selected.children.map(\.type) == [.sql(.real), .sql(.real)])
        }
        // Set-operation and VALUES sources use PostgreSQL's common type: a floating input wins.
        #expect(try binder().commonType([.sql(numeric), .sql(.real)]) == .sql(.real))
        #expect(try binder().commonType([.sql(numeric), .sql(.doublePrecision)]) == .sql(.doublePrecision))
        let nullIf = try binder().bind(.nullIf(null(.smallint), null(.integer)))
        #expect(nullIf.type == .sql(.smallint))
        #expect(nullIf.children.map(\.type) == [.sql(.smallint), .sql(.integer)])
        for name in ["greatest", "least", "coalesce", "nullif"] {
            #expect(throws: DatabaseExpressionEvaluationError.unsupportedFunction(name)) {
                try function(name, [.null, .null])
            }
        }
    }

    @Test("Simple CASE stores its operand once and binds unreachable branches")
    func simpleCaseLayout() throws {
        let column = try QueryColumn(number: 1, name: "key", type: .sql(.integer), isNullable: false)
        let expression = Expression.caseWhen(operand: .column(ColumnRef("key")), cases: [
            CaseWhenPair(condition: null(.smallint), result: null(numeric)),
            CaseWhenPair(condition: null(.bigint), result: null(.real)),
        ], elseResult: .null)
        let selected = try binder(columns: [column]).bind(expression)
        #expect(selected.type == .sql(.real))
        #expect(selected.children.count == 6)
        #expect(selected.children[0].columnSlot == 0)
        #expect(selected.children.map(\.type) == [.sql(.integer), .sql(.boolean), .sql(.boolean), .sql(.real), .sql(.real), .sql(.real)])
        #expect(selected.children[1].children.map(\.type) == [.sql(.integer), .sql(.smallint)])
        #expect(selected.children[2].children.map(\.type) == [.sql(.integer), .sql(.bigint)])
        let lowered: Expression
        if case .cast(let inner, _) = try selected.loweredExpression { lowered = inner }
        else { lowered = try selected.loweredExpression }
        if case .caseWhen(let operand, let pairs, _) = lowered {
            #expect(operand == .column(ColumnRef("key")))
            #expect(pairs.count == 2)
        } else { Issue.record("Simple CASE lost its semantic form") }
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try binder().bind(.caseWhen(cases: [CaseWhenPair(condition: .bool(true), result: null(.integer))], elseResult: .column(ColumnRef("missing"))))
        }
        let independent = try binder().bind(.caseWhen(operand: null(numeric), cases: [
            CaseWhenPair(condition: null(.doublePrecision), result: .int(1)),
            CaseWhenPair(condition: null(numeric), result: .int(2)),
        ], elseResult: .int(3)))
        #expect(independent.children[0].type == .sql(numeric))
        #expect(independent.children[1].type == .sql(.boolean))
        #expect(independent.children[2].type == .sql(.boolean))
        #expect(independent.children[1].children.map(\.type) == [.sql(.doublePrecision), .sql(.doublePrecision)])
        #expect(independent.children[2].children.map(\.type) == [.sql(numeric), .sql(numeric)])
    }

    @Test("Truth and null-safe predicates are nonnullable while quantified sources retain declared element types")
    func predicatesAndQuantifiedSources() throws {
        let truth = try binder().bind(.truthTest(null(.boolean), value: .unknown, negated: true))
        #expect(truth.type == .sql(.boolean) && !truth.isNullable)
        let distinct = try binder().bind(.isDistinctFrom(null(.integer), null(numeric), negated: false))
        #expect(distinct.type == .sql(.boolean) && !distinct.isNullable)
        for type in [DataType.array(.integer), .array(numeric)] {
            let parameter = QueryParameter(position: 1, value: .primitive(.null), type: .sql(type))
            let selected = try binder(parameters: [parameter]).bind(.quantifiedComparison(null(.real), comparison: .equal,
                quantifier: .any, source: .array(.parameter(.position(1)))))
            #expect(selected.children.map(\.type) == [.sql(.doublePrecision), .sql(type)])
            #expect(selected.type == .sql(.boolean) && selected.isNullable)
        }
        let subquery = SelectQuery(projection: [.wildcard()], source: .values([]))
        let output = try QueryColumn(number: 1, name: "number", type: .sql(.integer), isNullable: true)
        let plan = CanonicalBoundSelect(columns: [output], projection: [], filter: nil, groupBy: [], having: nil, orderBy: [])
        let selected = try binder(subquery: plan).bind(.quantifiedComparison(null(numeric), comparison: .lessThan,
            quantifier: .all, source: .subquery(subquery)))
        #expect(selected.children[0].type == .sql(numeric))
        #expect(selected.subqueryBinding === plan)
        let badPlan = CanonicalBoundSelect(columns: [output, output], projection: [], filter: nil, groupBy: [], having: nil, orderBy: [])
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try binder(subquery: badPlan).bind(.quantifiedComparison(null(.integer), comparison: .equal, quantifier: .any, source: .subquery(subquery)))
        }
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try binder().bind(.quantifiedComparison(null(.integer), comparison: .equal, quantifier: .any, source: .array(null(.integer))))
        }
    }

    @Test("SUM and AVG select the six catalog outputs even with no input rows")
    func aggregateMatrix() throws {
        let sums: [DataType] = [.bigint, .bigint, numeric, numeric, .real, .doublePrecision]
        let averages: [DataType] = [numeric, numeric, numeric, numeric, .doublePrecision, .doublePrecision]
        for index in six.indices {
            for (aggregate, expected) in [(AggregateFunction.sum(null(six[index]), distinct: false), sums[index]), (.avg(null(six[index]), distinct: false), averages[index])] {
                let expression = Expression.aggregate(aggregate)
                let selected = try binder().bind(expression)
                #expect(selected.children[0].type == .sql(six[index]))
                #expect(selected.type == .sql(expected) && selected.isNullable)
                #expect(try binder().bind(empty(expression))[0].type == .sql(expected))
            }
        }
        let source = try QueryColumn(number: 1, name: "value", type: .primitive(.int64), isNullable: true)
        let selected = try binder(columns: [source]).bind(.aggregate(.sum(.column(ColumnRef("value")), distinct: false)))
        #expect(selected.children[0].type == .sql(.bigint))
        #expect(selected.children[0].children[0].columnSlot == 0)
    }

    @Test("Coercion preserves the input's exact semantic occurrence and subquery binding")
    func occurrencePreservation() throws {
        var arena = QueryOccurrenceArena()
        let root = try arena.append()
        let left = try arena.append(parent: root, field: .operand, index: 0)
        let right = try arena.append(parent: root, field: .operand, index: 1)
        let selected = try CanonicalQueryTypeBinder(columns: [], occurrences: arena, root: root,
            resolveColumn: { reference in throw DatabaseExpressionEvaluationError.missingColumn(reference.column) },
            wildcardSlots: { _ in [] }).bind(.add(null(.integer), null(numeric)))
        #expect(selected.occurrence == root)
        #expect(selected.children.map(\.occurrence) == [left, right])
        #expect(selected.children[0].children[0].occurrence == left)
    }

    private func null(_ type: DataType) -> Expression { .literal(.queryValue(.primitive(.null), type: .sql(type))) }
    private func empty(_ expression: Expression) -> SelectQuery {
        SelectQuery(projection: [ProjectionItem(expression)], source: .values([]))
    }
    private func function(_ name: String, _ arguments: [Expression]) throws -> CanonicalBoundExpression {
        try binder().bind(.function(FunctionCall(name: name, arguments: arguments)))
    }
    private func binder(columns: [QueryColumn] = [], parameters: [QueryParameter] = [],
                        subquery: CanonicalBoundSelect? = nil) -> CanonicalQueryTypeBinder {
        CanonicalQueryTypeBinder(columns: columns, parameters: parameters, resolveColumn: { reference in
            guard let index = columns.firstIndex(where: { $0.name == reference.column }) else {
                throw DatabaseExpressionEvaluationError.missingColumn(reference.column)
            }
            return index
        }, wildcardSlots: { _ in Array(columns.indices) }, bindLocatedSubquery: { _, _ in
            guard let subquery else { throw DatabaseExpressionEvaluationError.unsupportedExpression("missing test subquery") }
            return subquery
        })
    }
}
