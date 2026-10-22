import DatabaseKit
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Selected SQL aggregate and window binding")
struct CanonicalSQLAggregateWindowBindingTests {
    @Test("Ordinary aggregate signatures preserve SQL widths and interval types")
    func resultSignatures() throws {
        let numeric = DataType.decimal(precision: nil, scale: nil)
        for (input, sum, average): (DataType, DataType, DataType) in [
            (.smallint, .bigint, numeric), (.integer, .bigint, numeric), (.bigint, numeric, numeric),
            (.real, .real, .doublePrecision), (.doublePrecision, .doublePrecision, .doublePrecision),
            (numeric, numeric, numeric), (.interval(), .interval(), .interval()),
        ] {
            let argument = Expression.cast(.null, targetType: input)
            #expect(try binder().bind(sql(.sum, [argument])).type == .sql(sum))
            #expect(try binder().bind(sql(.avg, [argument])).type == .sql(average))
        }
        let count = try binder().bind(sql(.count, []))
        #expect(count.type == .sql(.bigint))
        #expect(!count.isNullable)
    }

    @Test("SQL normalization has one selected aggregate route")
    func oldAggregateNormalization() throws {
        let selected = try binder().bind(.aggregate(.sum(.int(1), distinct: false)))
        guard case .aggregate(.sql(let call)) = selected.expression else {
            Issue.record("SQL aggregate did not normalize")
            return
        }
        #expect(call.function == .sum)
        #expect(selected.aggregate?.arguments.count == 1)
        #expect(try selected.loweredExpression == sql(.sum, [.int(1)]))
    }

    @Test("ARRAY_AGG selects rich scalar and array declarations")
    func richArrayAggregate() throws {
        let row = Expression.row([.int(1), .string("value")])
        let selected = try binder().bind(sql(.arrayAgg, [row]))
        guard case .sql(.array(.composite(let descriptor))) = selected.type else {
            Issue.record("rich ARRAY_AGG result is not a SQL array")
            return
        }
        #expect(descriptor.columns.count == 2)
        let nested = try binder().bind(sql(.arrayAgg, [.array([.int(1)])]))
        #expect(nested.type == .sql(.array(.bigint)))
        #expect(nested.aggregate?.arguments[0].type == .sql(.array(.bigint)))
        for source: QueryColumnType in [.primitive(.int8), .primitive(.int64, isArray: true)] {
            let column = try QueryColumn(number: 1, name: "value", type: source, isNullable: false)
            let selected = try binder(columns: [column]).bind(sql(.arrayAgg, [.column(ColumnRef("value"))]))
            #expect(selected.type == .sql(.array(.bigint)))
            let argument = try #require(selected.aggregate?.arguments.first)
            #expect(argument.sourceAlignment)
            #expect(argument.type == (source == .primitive(.int8) ? .sql(.bigint) : .sql(.array(.bigint))))
        }
    }

    @Test("Aggregate modifiers bind in a fixed selected order and reuse order arguments")
    func selectedChildren() throws {
        let call = SQLAggregateCall(function: .stringAgg, arguments: [.string("a"), .string(",")],
            distinct: true, orderBy: [SortKey(.string("a"), direction: .descending)], filter: .bool(true))
        let selected = try binder().bind(.aggregate(.sql(call)))
        let aggregate = try #require(selected.aggregate)
        #expect(aggregate.arguments.count == 2)
        #expect(aggregate.orderBy[0].projectionIndex == 0)
        #expect(aggregate.filter?.type == .sql(.boolean))
        #expect(selected.children.count == 4)
        #expect(selected.children[0].type == .sql(.text))
        #expect(selected.children[3].type == .sql(.boolean))
    }

    @Test("DISTINCT order expressions and ordered-set modifiers fail before reads")
    func invalidAggregateModifiers() throws {
        for call in [
            SQLAggregateCall(function: .count, arguments: [], distinct: true),
            SQLAggregateCall(function: .arrayAgg, arguments: [.int(1)], distinct: true, orderBy: [SortKey(.int(2))]),
            SQLAggregateCall(function: .sum, arguments: [.int(1)], withinGroup: [SortKey(.int(1))]),
            SQLAggregateCall(function: .mode, arguments: [], distinct: true, withinGroup: [SortKey(.int(1))]),
            SQLAggregateCall(function: .percentileCont, arguments: [.int(1)]),
            SQLAggregateCall(function: .sum, arguments: [.int(1)], schema: "other"),
        ] {
            #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(.aggregate(.sql(call))) }
        }
    }

    @Test("Statistics and ordered-set fraction arrays retain selected result types")
    func statisticAndOrderedSignatures() throws {
        for function: SQLAggregateFunction in [.varPop, .varSamp, .variance, .stddevPop, .stddevSamp, .stddev] {
            #expect(try binder().bind(sql(function, [.int(1)])).type == .sql(.decimal(precision: nil, scale: nil)))
            #expect(try binder().bind(sql(function, [.cast(.null, targetType: .real)])).type == .sql(.doublePrecision))
        }
        for function: SQLAggregateFunction in [.corr, .covarPop, .covarSamp, .regrAvgx, .regrAvgy,
            .regrSxx, .regrSxy, .regrSyy, .regrSlope, .regrIntercept, .regrR2] {
            #expect(try binder().bind(sql(function, [.int(1), .int(2)])).type == .sql(.doublePrecision))
        }
        let percentile = try binder().bind(.aggregate(.sql(SQLAggregateCall(function: .percentileCont,
            arguments: [.array([.double(0.25), .double(0.75)])],
            withinGroup: [SortKey(.cast(.null, targetType: .interval()))]))))
        #expect(percentile.type == .sql(.array(.interval())))
        #expect(percentile.aggregate?.directArguments.count == 1)
        #expect(percentile.aggregate?.withinGroup.count == 1)
        let disc = try binder().bind(.aggregate(.sql(SQLAggregateCall(function: .percentileDisc,
            arguments: [.double(0.5)], withinGroup: [SortKey(.string("x"))]))))
        #expect(disc.type == .sql(.text))
    }

    @Test("Clause phase rejects forbidden aggregate, GROUPING, and window expressions")
    func clauseLegality() throws {
        let sum = sql(.sum, [.int(1)])
        let rank = analytic(.rank)
        for expression in [sum, rank, .grouping([.int(1)])] {
            #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bindPredicate(expression) }
            #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bindGroupingExpression(expression) }
            #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bindScalarAssignment(expression, to: .sql(.bigint)) }
        }
        try binder().requireScalarPhase(.subquery(SelectQuery(projection: [ProjectionItem(rank)], source: .values([]))))
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bindHaving(rank) }
        #expect(try binder().bindHaving(.equal(sum, .int(1))).type == .sql(.boolean))
    }

    @Test("Aggregate nesting rejects window inputs while windows admit grouped aggregates")
    func expressionNesting() throws {
        let sum = sql(.sum, [.int(1)])
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(sql(.sum, [sum])) }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(sql(.sum, [analytic(.rowNumber)])) }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(analytic(.firstValue, [analytic(.rank)])) }
        let selected = try binder().bind(analytic(.lag, [sum]))
        #expect(selected.window?.arguments[0].aggregate?.call.function == .sum)
        let windowAggregate = Expression.window(WindowCall(function: .aggregate(SQLAggregateCall(function: .sum,
            arguments: [sum])), reference: .specification(WindowSpecification())))
        #expect(try binder().bind(windowAggregate).window?.aggregate?.aggregate?.arguments[0].aggregate != nil)
    }

    @Test("Ordinary aggregate OVER rejects DISTINCT, internal ordering, and ordered sets")
    func aggregateWindowModifiers() throws {
        for call in [
            SQLAggregateCall(function: .count, arguments: [.int(1)], distinct: true),
            SQLAggregateCall(function: .arrayAgg, arguments: [.int(1)], orderBy: [SortKey(.int(1))]),
            SQLAggregateCall(function: .mode, arguments: [], withinGroup: [SortKey(.int(1))]),
        ] {
            let expression = Expression.window(WindowCall(function: .aggregate(call), reference: .specification(WindowSpecification())))
            #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(expression) }
        }
        let filtered = Expression.window(WindowCall(function: .aggregate(SQLAggregateCall(function: .sum,
            arguments: [.int(1)], filter: .bool(true))), reference: .specification(WindowSpecification())))
        #expect(try binder().bind(filtered).window?.aggregate?.aggregate?.filter != nil)
    }

    @Test("Named windows resolve declaration order and distinguish direct use from copying")
    func namedWindowScopes() throws {
        let frame = WindowFrame(mode: .rows, start: .unboundedPreceding, end: .currentRow)
        let definitions = [NamedWindow(name: "base", specification: WindowSpecification(orderBy: [SortKey(.int(1))])),
            NamedWindow(name: "framed", specification: WindowSpecification(inheritedName: "base", frame: frame))]
        let selectedBinder = try binder().preparingWindows(definitions)
        let direct = Expression.window(WindowCall(function: .analytic(.rank, arguments: []), reference: .named("framed")))
        #expect(try selectedBinder.bind(direct).window?.frame?.mode == .rows)
        let copy = Expression.window(WindowCall(function: .analytic(.rank, arguments: []),
            reference: .specification(WindowSpecification(inheritedName: "framed"))))
        #expect(throws: DatabaseExpressionEvaluationError.self) { try selectedBinder.bind(copy) }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(direct) }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().preparingWindows(Array(definitions.reversed())) }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().preparingWindows([definitions[0], definitions[0]]) }
    }

    @Test("Named windows exist before projection binding and survive plan copies")
    func namedWindowProjection() throws {
        let definitions = [NamedWindow(name: "w", specification: WindowSpecification())]
        let expression = Expression.window(WindowCall(function: .analytic(.rowNumber, arguments: []), reference: .named("w")))
        let query = SelectQuery(projection: [ProjectionItem(expression)], source: .values([]), windows: definitions)
        let plan = try binder().bindSelect(query)
        #expect(plan.windows.count == 1)
        #expect(plan.projection[0].window != nil)
    }

    @Test("Frame offsets select exact numeric or temporal signatures and reject row references")
    func frameSignatures() throws {
        let rows = WindowFrame(mode: .rows, start: .preceding(.int(2)), end: .currentRow)
        let selected = try binder().bind(analytic(.rank, specification: WindowSpecification(frame: rows)))
        #expect(selected.window?.frame?.startOffset?.type == .sql(.bigint))
        let range = WindowFrame(mode: .range, start: .preceding(.cast(.null, targetType: .interval())), end: .currentRow)
        let temporal = try binder().bind(analytic(.rank, specification: WindowSpecification(
            orderBy: [SortKey(.cast(.null, targetType: .date))], frame: range)))
        #expect(temporal.window?.frame?.startOffset?.type == .sql(.interval()))
        let column = try QueryColumn(number: 1, name: "x", type: .sql(.bigint), isNullable: false)
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try binder(columns: [column]).bind(analytic(.rank, specification: WindowSpecification(
                frame: WindowFrame(mode: .rows, start: .preceding(.column(ColumnRef("x"))), end: .currentRow))))
        }
    }

    @Test("Frame syntax and RANGE capabilities fail without reading rows")
    func invalidFrames() throws {
        let frames = [WindowFrame(mode: .rows, start: .unboundedFollowing, end: .unboundedFollowing),
            WindowFrame(mode: .rows, start: .following(.int(1)), end: .currentRow),
            WindowFrame(mode: .groups, start: .unboundedPreceding, end: .currentRow),
            WindowFrame(mode: .range, start: .preceding(.int(1)), end: .currentRow)]
        for frame in frames {
            #expect(throws: DatabaseExpressionEvaluationError.self) {
                try binder().bind(analytic(.rank, specification: WindowSpecification(frame: frame)))
            }
        }
        let offset = WindowFrame(mode: .range, start: .preceding(.cast(.null, targetType: .interval())), end: .currentRow)
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try binder().bind(analytic(.rank, specification: WindowSpecification(
                orderBy: [SortKey(.cast(.null, targetType: .time(withTimeZone: true)))], frame: offset)))
        }
    }

    @Test("Navigation offsets select int4 and NULL defaults inherit the value type")
    func navigationSignatures() throws {
        let selected = try binder().bind(analytic(.lag, [.int(42), .int(-1), .null]))
        #expect(selected.type == .sql(.bigint))
        #expect(selected.window?.arguments[1].type == .sql(.integer))
        #expect(selected.window?.arguments[2].type == .sql(.bigint))
        #expect(try binder().bind(analytic(.ntile, [.null])).type == .sql(.integer))
        #expect(try binder().bind(analytic(.nthValue, [.string("x"), .null])).type == .sql(.text))
    }

    @Test("Optional graph producers preserve explicitly selected SQL aggregates and windows")
    func graphProducerCapabilityPreservesSQL() throws {
        let selected = CanonicalQueryTypeBinder(columns: [], allowingSPARQLFunctions: true,
            resolveColumn: { throw DatabaseExpressionEvaluationError.missingColumn($0.column) },
            wildcardSlots: { _ in [] })
        let count = try selected.bind(sql(.count, []))
        #expect(count.type == .sql(.bigint))
        #expect(count.aggregate?.call.function == .count)
        #expect(try selected.bind(analytic(.rowNumber)).window != nil)
        let over = Expression.window(WindowCall(function: .aggregate(SQLAggregateCall(function: .sum,
            arguments: [.int(1)])), reference: .specification(WindowSpecification())))
        #expect(try selected.bind(over).window?.aggregate?.aggregate?.call.function == .sum)
        // Generic aggregates retain the existing SPARQL evaluator selection.
        #expect(try selected.bind(.aggregate(.count(nil, distinct: false))).aggregate == nil)
    }

    @Test("Optional graph producers preserve SQL namespace, clause and nesting rejection")
    func graphProducerCapabilityPreservesRejection() throws {
        let selected = CanonicalQueryTypeBinder(columns: [], allowingSPARQLFunctions: true,
            resolveColumn: { throw DatabaseExpressionEvaluationError.missingColumn($0.column) },
            wildcardSlots: { _ in [] })
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try selected.bind(.aggregate(.sql(SQLAggregateCall(function: .count, arguments: [], schema: "other"))))
        }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try selected.bindPredicate(sql(.count, [])) }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try selected.bind(sql(.sum, [analytic(.rowNumber)])) }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try selected.bind(analytic(.lag, [analytic(.rank)])) }
    }

    private func sql(_ function: SQLAggregateFunction, _ arguments: [Expression]) -> Expression {
        .aggregate(.sql(SQLAggregateCall(function: function, arguments: arguments)))
    }

    private func analytic(_ function: SQLWindowFunction, _ arguments: [Expression] = [],
        specification: WindowSpecification = WindowSpecification()) -> Expression {
        .window(WindowCall(function: .analytic(function, arguments: arguments), reference: .specification(specification)))
    }

    private func binder(columns: [QueryColumn] = []) -> CanonicalQueryTypeBinder {
        CanonicalQueryTypeBinder(columns: columns, resolveColumn: { reference in
            guard let index = columns.firstIndex(where: { $0.name == reference.column }) else {
                throw DatabaseExpressionEvaluationError.missingColumn(reference.column)
            }
            return index
        }, wildcardSlots: { _ in Array(columns.indices) })
    }
}
