import Testing
import DatabaseKit
@testable import QueryAST

@Suite("SQL grouping, aggregate, and window syntax", .timeLimit(.minutes(1)))
struct SQLGroupingWindowSyntaxTests {
    private func roundTrip(_ text: String) throws -> SelectQuery {
        let query = try SQLParser().parseSelect(text)
        let serialized = try SQLSerializer().serialize(query)
        let parsed = try SQLParser().parseSelect(serialized)
        #expect(parsed == query)
        #expect(try SQLSerializer().serialize(parsed) == serialized)
        #expect(try SQLParser().parseSelectWithSource(text).query == query)
        return query
    }

    private func expression(_ query: SelectQuery, _ index: Int = 0) throws -> Expression {
        try projectionExpression(query.projection[index])
    }

    @Test("S09-SYN01 grouping sets preserve compound units, nesting, and duplicates")
    func groupingElements() throws {
        let query = try roundTrip("SELECT a FROM t GROUP BY a, (), ROLLUP((a,b), c), CUBE(a,(b,c)), GROUPING SETS ((a), (a), (), ROLLUP(a,b), GROUPING SETS ((b),()))")
        #expect(query.groupBy?.count == 5)
        #expect(query.groupBy?[1] == .groupingSets([[]]))
        #expect(query.groupBy?[2] == .rollup([[.col("a"), .col("b")], [.col("c")]]))
        guard case .groupingSets(let sets) = try #require(query.groupBy?[4]) else { Issue.record("Expected grouping sets"); return }
        #expect(sets.count == 5)
        #expect(sets[0] == sets[1])
        let compound = try roundTrip("SELECT a,b FROM t GROUP BY ((a,b)), ROW(a,b), (a+b)+c")
        #expect(compound.groupBy?[0] == .groupingSets([[.expression(.col("a")), .expression(.col("b"))]]))
        #expect(compound.groupBy?[1] == .expression(.row([.col("a"), .col("b")])))
        let contextual = try roundTrip("SELECT rollup,cube,grouping FROM t GROUP BY rollup,cube,grouping")
        #expect(contextual.groupBy == ["rollup", "cube", "grouping"].map { .expression(.col($0)) })
        for term in ["ROLLUP((a+1)*2)", "CUBE((a+1)*2)", "GROUPING SETS ((a+1)*2)"] {
            let grouped = try roundTrip("SELECT (a+1)*2,count(*) FROM (VALUES(1))t(a) GROUP BY " + term)
            let scalar = try expression(grouped)
            let expected: GroupingElement
            if term.hasPrefix("ROLLUP") { expected = .rollup([[scalar]]) }
            else if term.hasPrefix("CUBE") { expected = .cube([[scalar]]) }
            else { expected = .groupingSets([[.expression(scalar)]]) }
            #expect(grouped.groupBy == [expected])
        }
        let explicit = try roundTrip("SELECT a FROM t GROUP BY ROLLUP(ROW(a,b)), CUBE(ROW(a,b)), GROUPING SETS (ROW(a,b))")
        #expect(explicit.groupBy == [.rollup([[.row([.col("a"), .col("b")])]]),
            .cube([[.row([.col("a"), .col("b")])]]), .groupingSets([[.expression(.row([.col("a"), .col("b")]))]])])
    }

    @Test("S09-SYN02 every aggregate name uses the canonical SQL call")
    func aggregateInventory() throws {
        for name in SQLAggregateFunction.allCases {
            let arguments: String
            switch name {
            case .count: arguments = "*"
            case .mode: arguments = ""
            case .stringAgg, .jsonObjectAgg, .jsonbObjectAgg, .corr, .covarPop, .covarSamp,
                 .regrCount, .regrAvgx, .regrAvgy, .regrSxx, .regrSxy, .regrSyy, .regrSlope, .regrIntercept, .regrR2:
                arguments = "a,b"
            default: arguments = "a"
            }
            let within = [.percentileCont, .percentileDisc, .mode].contains(name) ? " WITHIN GROUP (ORDER BY b)" : ""
            let query = try roundTrip("SELECT " + name.rawValue + "(" + arguments + ")" + within + " FILTER (WHERE flag) FROM t")
            guard case .aggregate(.sql(let call)) = try expression(query) else { Issue.record("Expected SQL aggregate: \(name.rawValue)"); continue }
            #expect(call.function == name)
            #expect(call.filter == .col("flag"))
        }
    }

    @Test("S09-SYN02 aggregate modifiers and expression delimiter remain distinct children")
    func aggregateChildren() throws {
        let query = try roundTrip("SELECT pg_catalog.string_agg(DISTINCT a, :delimiter ORDER BY b DESC NULLS FIRST) FILTER (WHERE flag), percentile_cont(:fraction) WITHIN GROUP (ORDER BY x) FILTER (WHERE ok) FROM t")
        guard case .aggregate(.sql(let call)) = try expression(query) else { Issue.record("Expected SQL aggregate"); return }
        #expect(call.schema == "pg_catalog")
        #expect(call.arguments == [.col("a"), .parameter(.name("delimiter"))])
        #expect(call.distinct)
        #expect(call.orderBy == [SortKey(.col("b"), direction: .descending, nulls: .first)])
        #expect(call.filter == .col("flag"))
        guard case .aggregate(.sql(let ordered)) = try expression(query, 1) else { Issue.record("Expected ordered-set aggregate"); return }
        #expect(ordered.withinGroup == [SortKey(.col("x"))])
        #expect(ordered.filter == .col("ok"))
        let text = "SELECT percentile_cont(:fraction) WITHIN GROUP (ORDER BY x) FILTER (WHERE ok)"
        let parsed = try SQLParser().parseSelectWithSource(text)
        let projection = try #require(parsed.occurrences.child(of: parsed.root, field: .projection, index: 0))
        let filter = try #require(parsed.occurrences.child(of: projection, field: .aggregateFilter))
        let sort = try #require(parsed.occurrences.child(of: projection, field: .aggregateWithinGroup, index: 0))
        let filterRange = try #require(parsed.sourceMap[filter]).range
        let sortRange = try #require(parsed.sourceMap[sort]).range
        #expect(String(decoding: Array(text.utf8)[filterRange.start..<filterRange.end], as: UTF8.self) == "ok")
        #expect(String(decoding: Array(text.utf8)[sortRange.start..<sortRange.end], as: UTF8.self) == "x")
    }

    @Test("S09-SYN02 exact quoted names and schemas retain identity")
    func qualifiedIdentity() throws {
        let query = try roundTrip("SELECT \"sum\"(a), \"SUM\"(a), other.sum(a), pg_catalog.\"sum\"(a) FROM t")
        guard case .aggregate(.sql) = try expression(query),
              case .function(let quoted) = try expression(query, 1),
              case .aggregate(.sql(let other)) = try expression(query, 2),
              case .aggregate(.sql(let catalog)) = try expression(query, 3) else { Issue.record("Expected exact function identities"); return }
        #expect(quoted.name == "SUM")
        #expect(other.schema == "other")
        #expect(catalog.schema == "pg_catalog")
    }

    @Test("S09-SYN03 every analytic function retains OVER and arguments")
    func analyticInventory() throws {
        for name in SQLWindowFunction.allCases {
            let arguments: String
            switch name {
            case .rowNumber, .rank, .denseRank, .percentRank, .cumeDist: arguments = ""
            case .lag, .lead: arguments = "a, :offset, :fallback"
            case .nthValue: arguments = "a, 2"
            default: arguments = "a"
            }
            let query = try roundTrip("SELECT " + name.rawValue + "(" + arguments + ") OVER (PARTITION BY g ORDER BY k) FROM t")
            guard case .window(let call) = try expression(query), case .analytic(let function, _) = call.function else { Issue.record("Expected analytic window"); continue }
            #expect(function == name)
        }
    }

    @Test("S09-SYN03 direct named references differ from copied specifications")
    func namedWindowReferences() throws {
        let query = try roundTrip("SELECT sum(a) FILTER (WHERE flag) OVER w, sum(a) OVER (w), rank() OVER (ORDER BY k) FROM t WINDOW w AS (PARTITION BY g ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)")
        guard case .window(let direct) = try expression(query), case .window(let copy) = try expression(query, 1) else { Issue.record("Expected window calls"); return }
        #expect(direct.reference == .named("w"))
        #expect(copy.reference == .specification(WindowSpecification(inheritedName: "w")))
        #expect(query.windows.count == 1)
        #expect(query.windows[0].name == "w")
        #expect(query.windows[0].specification.partitionBy == [.col("g")])
    }

    @Test("S09-SYN04 frame modes, bounds, and exclusions stabilize")
    func windowFrames() throws {
        for mode in ["ROWS", "RANGE", "GROUPS"] {
            for exclusion in ["NO OTHERS", "CURRENT ROW", "GROUP", "TIES"] {
                _ = try roundTrip("SELECT first_value(a) OVER (ORDER BY k " + mode + " BETWEEN (:n + 1) PRECEDING AND :following FOLLOWING EXCLUDE " + exclusion + ") FROM t")
            }
        }
        let query = try roundTrip("SELECT sum(a) OVER (ROWS UNBOUNDED PRECEDING), sum(a) OVER () FROM t")
        guard case .window(let call) = try expression(query), case .specification(let specification) = call.reference else { Issue.record("Expected frame"); return }
        #expect(specification.frame?.end == .currentRow)
    }

    @Test("S09-SYN04 malformed grammar is rejected without accepting a prefix")
    func malformedSyntax() {
        for text in [
            "SELECT count()", "SELECT count(DISTINCT *)", "SELECT sum(*)", "SELECT sum(a,) FROM t",
            "SELECT sum(a) FILTER (a)", "SELECT sum(a) WITHIN GROUP ()", "SELECT sum(a) OVER",
            "SELECT rank()", "SELECT sum(a) OVER (ROWS BETWEEN 1 PRECEDING 1 FOLLOWING)",
            "SELECT sum(a) OVER (ROWS CURRENT)", "SELECT sum(a) OVER (ORDER BY a ROWS CURRENT ROW EXCLUDE)",
            "SELECT 1 GROUP BY GROUPING SETS ()", "SELECT 1 WINDOW w AS () ,", "SELECT sum(a) OVER (ROWS 1)"
        ] {
            #expect(throws: SQLParser.ParseError.self) { try SQLParser().parseSelect(text) }
        }
    }

    @Test("S09-SYN05 source capture preserves repeated offset and aggregate occurrences")
    func occurrenceChildren() throws {
        let text = "SELECT sum(a ORDER BY a) FILTER (WHERE a) OVER (PARTITION BY a ORDER BY a ROWS BETWEEN a PRECEDING AND a FOLLOWING) FROM t GROUP BY ROLLUP((a,a)), GROUPING SETS ((a),()) WINDOW w AS (PARTITION BY a)"
        let parsed = try SQLParser().parseSelectWithSource(text)
        let projection = try #require(parsed.occurrences.child(of: parsed.root, field: .projection, index: 0))
        let function = try #require(parsed.occurrences.child(of: projection, field: .windowFunction))
        let argument = try #require(parsed.occurrences.child(of: function, field: .aggregateArgument, index: 0))
        let ordering = try #require(parsed.occurrences.child(of: function, field: .aggregateOrderBy, index: 0))
        let filter = try #require(parsed.occurrences.child(of: function, field: .aggregateFilter))
        #expect(Set([argument, ordering, filter]).count == 3)
        let reference = try #require(parsed.occurrences.child(of: projection, field: .windowReference))
        let lower = try #require(parsed.occurrences.child(of: reference, field: .windowFrameStart))
        let upper = try #require(parsed.occurrences.child(of: reference, field: .windowFrameEnd))
        #expect(lower != upper)
        let named = try #require(parsed.occurrences.child(of: parsed.root, field: .namedWindow, index: 0))
        #expect(parsed.sourceMap[named]?.identifierRange != nil)
        #expect(try SQLParser().parseSelect(SQLSerializer().serialize(parsed.query)) == parsed.query)
    }

    @Test("S09-SYN06 fluent grouping and aggregate calls retain their canonical query metadata")
    func fluentConstruction() throws {
        let query = TableRef("t").selectAll().groupBy(elements: [.rollup([[.col("a")]])])
            .window(NamedWindow(name: "w", specification: WindowSpecification(partitionBy: [.col("a")])))
        #expect(query.groupBy == [.rollup([[.col("a")]])])
        #expect(query.windows.count == 1)
        #expect(try SQLParser().parseSelect(SQLSerializer().serialize(query)) == query)
        let global = TableRef("t").selectAll().groupBy(elements: [])
        #expect(try SQLSerializer().serialize(global).contains("GROUP BY ()"))
        #expect(try SQLParser().parseSelect(SQLSerializer().serialize(global)).groupBy != nil)
    }

    @Test("S09-SYN05 retained output and structural grouping budgets are cumulative")
    func boundedChildren() throws {
        let query = try SQLParser().parseSelect("SELECT sum(a) FILTER (WHERE flag) OVER (PARTITION BY g ORDER BY k ROWS BETWEEN :n PRECEDING AND CURRENT ROW) FROM t GROUP BY CUBE((a,b),c) WINDOW w AS (ORDER BY a)")
        let text = try SQLSerializer().serialize(query)
        let exact = QueryStructuralLimits(maximumRetainedBytes: UInt64(text.utf8.count))
        #expect(try SQLSerializer(limits: exact).serialize(query) == text)
        #expect(throws: SQLSerializationError.self) {
            try SQLSerializer(limits: QueryStructuralLimits(maximumRetainedBytes: UInt64(text.utf8.count - 1))).serialize(query)
        }
        #expect(throws: QueryStructuralValidationError.self) {
            try SQLParser(structuralLimits: QueryStructuralLimits(maximumCollectionElements: 2)).parseSelect("SELECT a GROUP BY CUBE(a,b,c)")
        }
    }

    @Test("S09-SYN06 SQL-only calls fail at the SPARQL language boundary")
    func sparqlRejection() throws {
        let expressions: [Expression] = [
            .grouping([.col("a")]),
            .aggregate(.sql(SQLAggregateCall(function: .sum, arguments: [.col("a")]))),
            .window(WindowCall(function: .analytic(.rank, arguments: []), reference: .specification(WindowSpecification())))
        ]
        for expression in expressions {
            #expect(throws: QueryLiteralConversionError.self) {
                try SPARQLQueryBuilder().select(expression, as: "value").toSPARQL()
            }
        }
    }

}
