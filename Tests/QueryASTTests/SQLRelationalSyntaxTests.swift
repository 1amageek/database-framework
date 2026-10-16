import DatabaseKit
import DatabaseTypes
import Testing
@testable import QueryAST

@Suite("S08 relational grammar and canonical SQL")
struct SQLRelationalSyntaxTests {
    private func roundTrip(_ sql: String) throws -> SelectQuery {
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)
        let emitted = try SQLSerializer().serialize(query)
        let reparsed = try parser.parseSelect(emitted)
        #expect(reparsed == query)
        #expect(try SQLSerializer().serialize(reparsed) == emitted)
        return query
    }

    @Test("S08-SYN01 mixed wildcards and optional AS retain positional identity")
    func mixedProjection() throws {
        let query = try roundTrip("SELECT *, v.*, a answer, b AS answer FROM (VALUES (1, 2)) v(a,b)")
        #expect(query.projection == [.wildcard(), .wildcard(qualifier: "v"),
            ProjectionItem(.col("a"), alias: "answer"), ProjectionItem(.col("b"), alias: "answer")])
        #expect(try SQLParser().parseSelect("SELECT a answer FROM t") == SQLParser().parseSelect("SELECT a AS answer FROM t"))
    }

    @Test("S08-SYN01 source aliases wrap complete groups and derived aliases are optional")
    func sourceAliasScope() throws {
        let query = try roundTrip("SELECT * FROM (a JOIN b USING (id)) AS combined(x), (SELECT 1)")
        guard case .join(let cross) = query.source,
              case .aliased(.join, "combined", let columns) = cross.left,
              case .subquery = cross.right else { Issue.record("Expected grouped alias and unaliased derived query"); return }
        #expect(columns == ["x"])
        #expect(cross.type == .cross)
    }

    @Test("S08-SYN01 explicit JOIN binds more tightly than comma and comma LATERAL is retained")
    func commaJoinPrecedence() throws {
        let query = try roundTrip("SELECT * FROM a, b JOIN c ON b.id = c.id")
        guard case .join(let cross) = query.source, case .join(let joined) = cross.right else {
            Issue.record("Expected explicit join on the right of comma"); return
        }
        #expect(cross.type == .cross)
        #expect(joined.type == .inner)
        let lateral = try roundTrip("SELECT * FROM a, LATERAL (SELECT a.id)")
        guard case .join(let join) = lateral.source else { Issue.record("Expected lateral join"); return }
        #expect(join.type == .lateral)
    }

    @Test("S08-SYN01 expression VALUES preserve parameter and nested query children")
    func expressionValues() throws {
        let query = try roundTrip("VALUES (:x + 1, (SELECT :x)), (:y, NULL)")
        guard case .values(let rows) = query.source else { Issue.record("Expected VALUES"); return }
        #expect(rows.count == 2)
        #expect(rows.allSatisfy { $0.count == 2 })
        #expect(try SQLSerializer().serialize(query).hasPrefix("VALUES "))
        let explicit = SelectQuery(projection: [.wildcard()], source: .subquery(query))
        #expect(try SQLParser().parseSelect(SQLSerializer().serialize(explicit)) == explicit)
        _ = try roundTrip("SELECT * FROM (VALUES (1) ORDER BY 1 LIMIT 1) AS v")
        _ = try roundTrip("SELECT * FROM (VALUES (1) UNION ALL SELECT 2) AS v")
        for sql in ["VALUES ((ROW(1, 2)).*), (3, 4)", "VALUES (3, 4), ((ROW(1, 2)).*)",
                    "VALUES ((ROW(1, 2)).*), (3)", "VALUES (1), (2, 3), ((ROW(4)).*)"] {
            let expanded = try roundTrip(sql)
            let parsed = try SQLParser().parseSelectWithSource(sql)
            #expect(parsed.query == expanded)
            guard case .values(let rawRows) = expanded.source else { Issue.record("Expected raw VALUES cells"); return }
            let rowIndex = try #require(rawRows.firstIndex { row in
                row.contains { if case .fieldExpansion = $0 { return true }; return false }
            })
            #expect(rawRows[rowIndex].count == 1)
            let source = try #require(parsed.occurrences.child(of: parsed.root, field: .source))
            let row = try #require(parsed.occurrences.child(of: source, field: .valuesRow, index: UInt32(rowIndex)))
            let cell = try #require(parsed.occurrences.child(of: row, field: .valuesCell, index: 0))
            let operand = try #require(parsed.occurrences.child(of: cell, field: .operand, index: 0))
            #expect(parsed.sourceMap[cell] != nil)
            #expect(parsed.sourceMap[operand] != nil)
        }
        #expect(throws: SQLParser.ParseError.self) { try SQLParser().parseSelect("VALUES ()") }
        #expect(throws: QueryStructuralValidationError.self) { try SQLParser().parseSelect("VALUES (1), (2, 3)") }
        #expect(throws: QueryStructuralValidationError.self) {
            try SQLParser().parseSelect("VALUES (ROW((ROW(1, 2)).*)), (3, 4)")
        }
        #expect(throws: SQLSerializationError.self) {
            try SQLSerializer().serialize(DataSource.values([[.row([.fieldExpansion(.row([.literal(.int(1)), .literal(.int(2))]))])],
                                                            [.literal(.int(3)), .literal(.int(4))]]))
        }
        #expect(throws: SQLSerializationError.self) {
            try SQLSerializer().serialize(DataSource.values([[.fieldExpansion(.row([.literal(.int(1))]))], []]))
        }
    }

    @Test("S08-SYN02 implicit ROW and composite field/star operands keep their contexts")
    func compositeSyntax() throws {
        let implicit = try SQLParser().parseSelect("SELECT (1, 2)")
        #expect(try implicit == SQLParser().parseSelect("SELECT ROW(1, 2)"))
        let query = try roundTrip("SELECT (ROW(1, ROW(2))).f2, ROW(t.*, 9), row_to_json(t.*), (ROW(3)).* FROM t")
        guard case .row(let fields) = try projectionExpression(query.projection[1]),
              case .fieldExpansion(.wholeRow("t")) = fields[0],
              case .function(let call) = try projectionExpression(query.projection[2]) else {
            Issue.record("Expected context-preserving star operands"); return
        }
        #expect(call.arguments == [.fieldExpansion(.wholeRow("t"))])
    }

    @Test("S08-SYN02 ARRAY consumes a complete single query operand")
    func arrayQuery() throws {
        let query = try roundTrip("SELECT ARRAY(SELECT a FROM t UNION ALL SELECT b FROM u ORDER BY 1 LIMIT 2)")
        guard case .arraySubquery(let nested) = try projectionExpression(query.projection[0]),
              case .setOperation(.union, .all, _, _) = nested.source else {
            Issue.record("Expected complete ARRAY subquery"); return
        }
        #expect(nested.pagination.limit == .count(.fixed(2), withTies: false))
    }

    @Test("S08-SYN03 binary set precedence and multiplicity are independent per node")
    func setPrecedence() throws {
        let query = try roundTrip("SELECT 1 UNION ALL SELECT 2 INTERSECT ALL SELECT 3 EXCEPT DISTINCT SELECT 4")
        guard case .setOperation(.except, .distinct, let left, _) = query.source,
              case .setOperation(.union, .all, _, let right) = left.source,
              case .setOperation(.intersect, .all, _, _) = right.source else {
            Issue.record("Expected INTERSECT precedence and left-associative EXCEPT"); return
        }
    }

    @Test("S08-SYN03 operand modifiers and outer set modifiers retain different scopes")
    func setModifierScopes() throws {
        let query = try roundTrip("(SELECT a FROM t ORDER BY a LIMIT 1) UNION (SELECT b FROM u OFFSET 2) ORDER BY 1 FETCH FIRST 3 ROWS WITH TIES")
        guard case .setOperation(_, _, let left, let right) = query.source else { Issue.record("Expected set query"); return }
        #expect(left.orderBy != nil)
        #expect(left.pagination.limit == .count(.fixed(1), withTies: false))
        #expect(right.pagination.offset == .fixed(2))
        #expect(query.pagination.limit == .count(.fixed(3), withTies: true))
    }

    @Test("S08-SYN03 DISTINCT ON and quoted C/POSIX survive canonical output")
    func distinctAndCollation() throws {
        let query = try roundTrip("SELECT DISTINCT ON (a,b) a, b FROM t ORDER BY b COLLATE \"C\", a COLLATE \"POSIX\"")
        #expect(query.quantifier == .distinctOn([.col("a"), .col("b")]))
        #expect(try SQLParser().parseSelect("SELECT ALL a FROM t").quantifier == .all)
        for sql in ["SELECT DISTINCT ON () a", "SELECT a COLLATE c", "SELECT a COLLATE \"other\""] {
            #expect(throws: SQLParser.ParseError.self) { try SQLParser().parseSelect(sql) }
        }
    }

    @Test("S08-SYN04 pagination preserves count expressions and decisive NULL types")
    func paginationExpressions() throws {
        let query = try roundTrip("SELECT 1 ORDER BY 1 LIMIT (:n + 1) OFFSET :skip")
        #expect(query.pagination.offset == .expression(.parameter(.name("skip"))))
        let omitted = try roundTrip("SELECT 1 FETCH NEXT ROW ONLY")
        #expect(omitted.pagination.limit == .count(.fixed(1), withTies: false))
        let unknown = try roundTrip("SELECT 1 ORDER BY 1 FETCH FIRST NULL ROWS WITH TIES")
        let typed = try roundTrip("SELECT 1 ORDER BY 1 FETCH FIRST (NULL::bigint) ROWS WITH TIES")
        #expect(unknown.pagination != typed.pagination)
        #expect(try roundTrip("SELECT 1 OFFSET 2 LIMIT ALL").pagination.limit == .all)
        for sql in ["SELECT 1 LIMIT 1 LIMIT 2", "SELECT 1 FETCH FIRST 2 ONLY", "SELECT 1 OFFSET", "SELECT 1 LIMIT"] {
            #expect(throws: SQLParser.ParseError.self) { try SQLParser().parseSelect(sql) }
        }
    }

    @Test("S08-SYN04 all public query entries retain CTE columns and hints around sets")
    func commonTableEntries() throws {
        let sql = "WITH q(x) AS NOT MATERIALIZED (VALUES (1)) SELECT x FROM q UNION SELECT 2"
        let query = try roundTrip(sql)
        #expect(query.subqueries?.first?.columns == ["x"])
        #expect(query.subqueries?.first?.materialized == .notMaterialized)
        #expect(try SQLParser().parse(sql) == .select(query))
        #expect(try SQLParser().parseStatements(sql + ";") == [.select(query)])
        #expect(try SQLParser().parseWithSource(sql).statement == .select(query))
    }

    @Test("S08-SYN05 source captures canonical new edges with absolute UTF-8 positions")
    func occurrenceOrderAndRanges() throws {
        let sql = "-- 名前\r\nSELECT DISTINCT ON (missing) t.*, row_to_json(t.*) FROM (VALUES (:p), (:p)) t(x) ORDER BY missing LIMIT :n OFFSET :skip"
        let parser = SQLParser()
        let parsed = try parser.parseSelectWithSource(sql)
        func child(_ parent: QueryOccurrenceID, _ field: QueryOccurrenceField, _ index: UInt32? = nil) throws -> QueryOccurrenceID {
            try #require(parsed.occurrences.child(of: parent, field: field, index: index))
        }
        func text(_ id: QueryOccurrenceID) throws -> String {
            let range = try #require(parsed.sourceMap[id]).range
            return String(decoding: Array(sql.utf8)[range.start..<range.end], as: UTF8.self)
        }
        let key = try child(parsed.root, .distinctOn, 0)
        let order = try child(parsed.root, .orderBy, 0)
        #expect(key != order)
        #expect(try text(key) == "missing")
        #expect(try text(order) == "missing")
        #expect(try text(child(parsed.root, .offset)) == ":skip")
        #expect(try text(child(parsed.root, .limit)) == ":n")
        let source = try child(parsed.root, .source)
        let values = try child(source, .source)
        let first = try child(child(values, .valuesRow, 0), .valuesCell, 0)
        let second = try child(child(values, .valuesRow, 1), .valuesCell, 0)
        #expect(first != second)
        #expect(try text(first) == ":p")
        #expect(try text(second) == ":p")
        let call = try child(parsed.root, .projection, 1)
        let star = try child(call, .argument, 0)
        let row = try child(star, .operand, 0)
        #expect(try text(star) == "t.*")
        #expect(try text(row) == "t")
        _ = try parser.parseSelect("SELECT 9")
        #expect(try text(first) == ":p")
    }

    @Test("S08-SYN05 set source captures both full queries and synthetic wildcard has no fake span")
    func setOccurrences() throws {
        let parsed = try SQLParser().parseSelectWithSource("SELECT missing UNION ALL SELECT missing")
        let source = try #require(parsed.occurrences.child(of: parsed.root, field: .source))
        let left = try #require(parsed.occurrences.child(of: source, field: .setLeft))
        let right = try #require(parsed.occurrences.child(of: source, field: .setRight))
        #expect(left != right)
        let wildcard = try #require(parsed.occurrences.child(of: parsed.root, field: .projection, index: 0))
        #expect(parsed.sourceMap[wildcard] == nil)
    }

    @Test("S08-SYN05 every nested query shares limits and source snapshots survive failed reuse")
    func nestedLimits() throws {
        let sql = "SELECT ARRAY(SELECT ROW(:p, (SELECT :p)))"
        let parser = SQLParser()
        let original = try parser.parseSelectWithSource(sql)
        #expect(throws: SQLParser.ParseError.self) { try parser.parseSelect("VALUES (1), ()") }
        #expect(try original.query == SQLParser().parseSelect(sql))
        #expect(throws: (any Error).self) {
            try SQLParser(structuralLimits: QueryStructuralLimits(maximumNestingDepth: 3)).parseSelectWithSource(sql)
        }
        let deep = "SELECT " + String(repeating: "ROW(", count: 40) + ":p" + String(repeating: ")", count: 40)
        _ = try SQLParser(structuralLimits: QueryStructuralLimits(maximumNestingDepth: 256)).parseSelectWithSource(deep)
        var continuation = "ROW(1)"
        for _ in 0..<40 { continuation = "ROW(" + continuation + ".f1 + 1, ARRAY[2])" }
        _ = try SQLParser(structuralLimits: QueryStructuralLimits(maximumNestingDepth: 256))
            .parseSelectWithSource("SELECT " + continuation)
        #expect(try SQLParser().parseSelect("SELECT ROW(ROW(1).f1 + 2, ARRAY[ROW(3)], ROW(4) IS NULL)")
            == SQLParser().parseSelect("SELECT ROW((ROW(1).f1 + 2), ARRAY[ROW(3)], (ROW(4) IS NULL))"))
        for sql in ["SELECT ROW(ROW(1),)", "SELECT ARRAY[ROW(1),]", "SELECT ROW(ARRAY[[1],])"] {
            #expect(throws: SQLParser.ParseError.self) { try SQLParser().parseSelect(sql) }
        }
    }

    @Test("S08-SYN06 unsupported sources and future capabilities remain explicit failures")
    func explicitUnsupportedBoundaries() throws {
        #expect(throws: SQLSerializationError.unsupportedSource("Empty zero-column VALUES has no exact SQL spelling")) {
            try SQLSerializer().serialize(SelectQuery(projection: [.wildcard()], source: .values([])))
        }
        #expect(throws: SQLSerializationError.self) { try SQLSerializer().serialize(Expression.wholeRow("t")) }
        #expect(throws: SQLParser.ParseError.self) { try SQLParser().parseSelect("SELECT * FROM unnest(ARRAY[1])") }
        #expect(throws: SQLSerializationError.self) {
            try SQLSerializer(limits: QueryStructuralLimits(maximumRetainedBytes: 10)).serialize(SQLParser().parseSelect("SELECT *, t.* FROM t"))
        }
    }

    @Test("S08-SYN06 anonymous composite literals retain representable metadata and reject the remainder")
    func compositeLiteralMetadata() throws {
        let limits = QueryValueLimits(maximumRetainedBytes: 65536, maximumTotalNodes: 2048,
            maximumNestingDepth: 64, maximumCollectionElements: 1024)
        let anonymous = try QueryCompositeType(columns: [
            QueryColumn(number: 1, name: "f1", type: .sql(.integer), isNullable: false)
        ], limits: limits)
        let value = try QueryCompositeValue(type: anonymous, fields: [.primitive(.int32(7))], limits: limits)
        let text = try SQLSerializer().serialize(Literal.queryValue(.composite(value), type: .sql(.composite(anonymous))))
        let parsed = try SQLParser().parseSelect("SELECT " + text)
        guard case .row(let fields) = try projectionExpression(parsed.projection[0]) else {
            Issue.record("Expected a representable ROW literal"); return
        }
        #expect(fields == [.literal(.queryValue(.primitive(.int32(7)), type: .sql(.integer)))])
        let named = try QueryCompositeType(columns: [
            QueryColumn(number: 1, name: "named", type: .sql(.integer), isNullable: false)
        ], limits: limits)
        let namedValue = try QueryCompositeValue(type: named, fields: [.primitive(.int32(7))], limits: limits)
        #expect(throws: SQLSerializationError.self) {
            try SQLSerializer().serialize(Literal.queryValue(.composite(namedValue)))
        }
        let empty = try QueryArrayValue(elementType: .composite(anonymous), dimensions: [], elements: [], limits: limits)
        #expect(throws: SQLSerializationError.self) {
            try SQLSerializer().serialize(Literal.queryValue(.array(empty)))
        }
        #expect(throws: SQLSerializationError.self) { try SQLSerializer().serialize(DataType.composite(named)) }
    }
}
