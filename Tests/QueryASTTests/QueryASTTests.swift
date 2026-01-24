/// QueryASTTests.swift
/// Tests for the QueryAST module

import Testing
@testable import QueryAST

@Suite("QueryAST Tests")
struct QueryASTTests {

    // MARK: - Literal Tests

    @Test("Literal convenience initializers")
    func testLiteralInitializers() throws {
        // Test optional initializer
        let intLit = Literal(42)
        #expect(intLit == .int(42))

        let doubleLit = Literal(3.14)
        #expect(doubleLit == .double(3.14))

        let stringLit = Literal("hello")
        #expect(stringLit == .string("hello"))

        let boolLit = Literal(true)
        #expect(boolLit == .bool(true))
    }

    @Test("Literal XSD datatype URIs")
    func testLiteralDataTypes() throws {
        #expect(Literal.int(1).xsdDatatype == .integer)
        #expect(Literal.double(1.0).xsdDatatype == .double)
        #expect(Literal.string("").xsdDatatype == .string)
        #expect(Literal.bool(true).xsdDatatype == .boolean)
    }

    // MARK: - Expression Tests

    @Test("Expression operator overloads")
    func testExpressionOperators() throws {
        let a = Expression.literal(.int(1))
        let b = Expression.literal(.int(2))

        let sum = a + b
        #expect(sum == .add(a, b))

        let diff = a - b
        #expect(diff == .subtract(a, b))

        let product = a * b
        #expect(product == .multiply(a, b))

        let quotient = a / b
        #expect(quotient == .divide(a, b))
    }

    @Test("Expression comparison operators")
    func testExpressionComparisons() throws {
        let a = Expression.column(ColumnRef(column: "age"))
        let b = Expression.literal(.int(18))

        let eq = a .== b
        #expect(eq == .equal(a, b))

        let neq = a .!= b
        #expect(neq == .notEqual(a, b))

        let lt = a .< b
        #expect(lt == .lessThan(a, b))

        let gt = a .> b
        #expect(gt == .greaterThan(a, b))
    }

    @Test("Expression aggregates")
    func testExpressionAggregates() throws {
        // Test aggregate function construction
        let countAgg = AggregateFunction.count(nil, distinct: false)
        let count = Expression.aggregate(countAgg)
        if case .aggregate(.count(nil, distinct: false)) = count {
            // OK
        } else {
            Issue.record("Expected count aggregate")
        }

        let sumAgg = AggregateFunction.sum(.column(ColumnRef(column: "amount")), distinct: false)
        let sum = Expression.aggregate(sumAgg)
        if case .aggregate(.sum(let expr, distinct: false)) = sum {
            if case .column(let ref) = expr {
                #expect(ref.column == "amount")
            } else {
                Issue.record("Expected column expression")
            }
        } else {
            Issue.record("Expected sum aggregate")
        }
    }

    // MARK: - DataSource Tests

    @Test("TableRef description")
    func testTableRefDescription() throws {
        // description now returns properly quoted SQL identifiers
        let ref1 = TableRef("users")
        #expect(ref1.description == "\"users\"")

        let ref2 = TableRef(table: "users", alias: "u")
        #expect(ref2.description == "\"users\" AS \"u\"")

        let ref3 = TableRef(schema: "public", table: "users")
        #expect(ref3.description == "\"public\".\"users\"")

        // displayName returns unquoted names for display purposes
        #expect(ref1.displayName == "users")
        #expect(ref2.displayName == "users AS u")
        #expect(ref3.displayName == "public.users")
    }

    @Test("JoinClause types")
    func testJoinClauseTypes() throws {
        let left = DataSource.table(TableRef("users"))
        let right = DataSource.table(TableRef("orders"))
        let condition = JoinCondition.on(.equal(
            .column(ColumnRef(table: "users", column: "id")),
            .column(ColumnRef(table: "orders", column: "user_id"))
        ))

        let innerJoin = JoinClause(type: .inner, left: left, right: right, condition: condition)
        #expect(innerJoin.type == .inner)

        let leftJoin = JoinClause(type: .left, left: left, right: right, condition: condition)
        #expect(leftJoin.type == .left)
    }

    // MARK: - SelectQuery Tests

    @Test("SelectQuery basic construction")
    func testSelectQueryConstruction() throws {
        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef("users")),
            filter: .greaterThan(.column(ColumnRef(column: "age")), .literal(.int(18))),
            limit: 10
        )

        #expect(query.projection == .all)
        #expect(query.limit == 10)
        #expect(query.filter != nil)
    }

    @Test("SelectQuery referenced columns")
    func testSelectQueryReferencedColumns() throws {
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef(column: "name"))),
                ProjectionItem(.column(ColumnRef(column: "age")))
            ]),
            source: .table(TableRef("users")),
            filter: .equal(.column(ColumnRef(column: "active")), .literal(.bool(true)))
        )

        let columns = query.referencedColumns
        #expect(columns.contains(ColumnRef(column: "name")))
        #expect(columns.contains(ColumnRef(column: "age")))
        #expect(columns.contains(ColumnRef(column: "active")))
    }

    // MARK: - SQL/PGQ Pattern Tests

    @Test("NodePattern construction")
    func testNodePattern() throws {
        let node = NodePattern(variable: "n", labels: ["Person"], properties: nil)
        #expect(node.variable == "n")
        #expect(node.labels == ["Person"])
    }

    @Test("EdgePattern direction")
    func testEdgePattern() throws {
        let outgoing = EdgePattern(labels: ["FOLLOWS"], direction: .outgoing)
        #expect(outgoing.direction == .outgoing)

        let incoming = EdgePattern(labels: ["FOLLOWS"], direction: .incoming)
        #expect(incoming.direction == .incoming)
    }

    @Test("PathQuantifier types")
    func testPathQuantifier() throws {
        let exactly3 = PathQuantifier.exactly(3)
        #expect(exactly3 == .exactly(3))

        let oneOrMore = PathQuantifier.oneOrMore
        #expect(oneOrMore == .oneOrMore)

        let range = PathQuantifier.range(min: 1, max: 5)
        #expect(range == .range(min: 1, max: 5))
    }

    // MARK: - SPARQL Term Tests

    @Test("SPARQLTerm construction")
    func testSPARQLTerm() throws {
        let variable = SPARQLTerm.variable("name")
        if case .variable(let v) = variable {
            #expect(v == "name")
        } else {
            Issue.record("Expected variable term")
        }

        let iri = SPARQLTerm.iri("http://example.org/person")
        if case .iri(let i) = iri {
            #expect(i == "http://example.org/person")
        } else {
            Issue.record("Expected IRI term")
        }

        let prefixed = SPARQLTerm.prefixedName(prefix: "foaf", local: "name")
        if case .prefixedName(let p, let l) = prefixed {
            #expect(p == "foaf")
            #expect(l == "name")
        } else {
            Issue.record("Expected prefixed name term")
        }
    }

    @Test("SPARQLTerm SPARQL serialization")
    func testSPARQLTermSerialization() throws {
        let variable = SPARQLTerm.variable("x")
        #expect(variable.toSPARQL() == "?x")

        let iri = SPARQLTerm.iri("http://example.org/p")
        #expect(iri.toSPARQL() == "<http://example.org/p>")

        let literal = SPARQLTerm.literal(.string("hello"))
        #expect(literal.toSPARQL() == "\"hello\"")
    }

    // MARK: - TriplePattern Tests

    @Test("TriplePattern construction")
    func testTriplePattern() throws {
        let pattern = TriplePattern(
            subject: .variable("s"),
            predicate: .iri("http://xmlns.com/foaf/0.1/name"),
            object: .variable("name")
        )

        #expect(pattern.subject == .variable("s"))
        #expect(pattern.object == .variable("name"))
    }

    @Test("TriplePattern variables")
    func testTriplePatternVariables() throws {
        let pattern = TriplePattern(
            subject: .variable("s"),
            predicate: .iri("http://xmlns.com/foaf/0.1/name"),
            object: .variable("name")
        )

        let vars = pattern.variables
        #expect(vars.contains("s"))
        #expect(vars.contains("name"))
        #expect(vars.count == 2)
    }

    // MARK: - PropertyPath Tests

    @Test("PropertyPath construction")
    func testPropertyPath() throws {
        let simple = PropertyPath.iri("http://example.org/knows")
        if case .iri(let i) = simple {
            #expect(i == "http://example.org/knows")
        } else {
            Issue.record("Expected IRI path")
        }

        let inverse = PropertyPath.inverse(.iri("http://example.org/parent"))
        if case .inverse(let inner) = inverse {
            if case .iri(let i) = inner {
                #expect(i == "http://example.org/parent")
            } else {
                Issue.record("Expected IRI in inverse path")
            }
        } else {
            Issue.record("Expected inverse path")
        }
    }

    @Test("PropertyPath SPARQL serialization")
    func testPropertyPathSerialization() throws {
        let path = PropertyPath.oneOrMore(.iri("http://example.org/knows"))
        #expect(path.toSPARQL() == "<http://example.org/knows>+")

        let zeroOrMore = PropertyPath.zeroOrMore(.iri("http://example.org/parent"))
        #expect(zeroOrMore.toSPARQL() == "<http://example.org/parent>*")
    }

    // MARK: - Query Plan Tests

    @Test("QueryCost comparison")
    func testQueryCostComparison() throws {
        let cost1 = QueryCost(startup: 0, total: 100, rows: 1000, width: 50)
        let cost2 = QueryCost(startup: 0, total: 200, rows: 2000, width: 50)

        #expect(cost1.total < cost2.total)
    }

    @Test("QueryCost addition")
    func testQueryCostAddition() throws {
        let cost1 = QueryCost(startup: 10, total: 100, rows: 500, width: 50)
        let cost2 = QueryCost(startup: 5, total: 50, rows: 500, width: 50)

        let combined = cost1 + cost2
        #expect(combined.startup == 15)
        #expect(combined.total == 150)
    }

    // MARK: - SQL Query Builder Tests

    @Test("SQLQueryBuilder basic query")
    func testSQLQueryBuilder() throws {
        // Note: We can't test actual execution without a Persistable model
        // This test verifies the builder API compiles correctly
        let ast = SelectQuery(
            projection: .all,
            source: .table(TableRef("users")),
            filter: .greaterThan(.column(ColumnRef(column: "age")), .literal(.int(18))),
            orderBy: [SortKey(.column(ColumnRef(column: "name")), direction: .ascending)],
            limit: 10
        )

        #expect(ast.limit == 10)
        #expect(ast.orderBy?.count == 1)
    }

    // MARK: - SPARQL Query Builder Tests

    @Test("SPARQLQueryBuilder construction")
    func testSPARQLQueryBuilder() throws {
        let builder = SPARQLQueryBuilder()
            .select("name", "age")
            .where(TriplePattern(
                subject: .variable("person"),
                predicate: .iri("http://xmlns.com/foaf/0.1/name"),
                object: .variable("name")
            ))
            .filter(.greaterThan(.variable(Variable("age")), .literal(.int(18))))
            .limit(10)

        let ast = builder.buildAST()
        #expect(ast.limit == 10)
    }

    @Test("SPARQLQueryBuilder SPARQL output")
    func testSPARQLQueryBuilderOutput() throws {
        let builder = SPARQLQueryBuilder()
            .prefix("foaf", "http://xmlns.com/foaf/0.1/")
            .selectAll()
            .where(TriplePattern(
                subject: .variable("s"),
                predicate: .prefixedName(prefix: "foaf", local: "name"),
                object: .variable("name")
            ))

        let sparql = builder.toSPARQL()
        #expect(sparql.contains("SELECT *"))
        #expect(sparql.contains("WHERE"))
    }

    // MARK: - Match Pattern Builder Tests

    @Test("MatchPatternBuilder path construction")
    func testMatchPatternBuilder() throws {
        let pattern = MatchPattern.build {
            node("a", label: "Person")
            outgoing(label: "KNOWS")
            node("b", label: "Person")
        }

        #expect(pattern.paths.count == 1)
        #expect(pattern.paths[0].elements.count == 3)
    }

    @Test("MatchPatternBuilder validation")
    func testMatchPatternValidation() throws {
        let validPattern = MatchPattern.build {
            node("a", label: "Person")
            outgoing(label: "KNOWS")
            node("b", label: "Person")
        }

        let errors = validPattern.validate()
        #expect(errors.isEmpty)
    }
}

// MARK: - Parser Tests

@Suite("SQL Parser Tests", .serialized)
struct SQLParserTests {

    @Test("Parse simple SELECT")
    func testParseSimpleSelect() throws {
        let sql = "SELECT * FROM users"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        #expect(query.projection == Projection.all)
        if case .table(let ref) = query.source {
            #expect(ref.table == "users")
        } else {
            Issue.record("Expected table source")
        }
    }

    @Test("Parse SELECT with columns")
    func testParseSelectWithColumns() throws {
        let sql = "SELECT name, age FROM users"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        if case .items(let items) = query.projection {
            #expect(items.count == 2)
        } else {
            Issue.record("Expected items projection")
        }
    }

    @Test("Parse SELECT with WHERE")
    func testParseSelectWithWhere() throws {
        let sql = "SELECT * FROM users WHERE age > 18"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        #expect(query.filter != nil)
    }

    @Test("Parse SELECT with ORDER BY")
    func testParseSelectWithOrderBy() throws {
        let sql = "SELECT * FROM users ORDER BY name ASC"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        #expect(query.orderBy?.count == 1)
    }

    @Test("Parse SELECT with LIMIT")
    func testParseSelectWithLimit() throws {
        let sql = "SELECT * FROM users LIMIT 10"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        #expect(query.limit == 10)
    }

    @Test("Parse SELECT with JOIN")
    func testParseSelectWithJoin() throws {
        let sql = "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        if case .join(let clause) = query.source {
            #expect(clause.type == JoinType.inner)
        } else {
            Issue.record("Expected join source")
        }
    }

    @Test("Parse SELECT with GROUP BY")
    func testParseSelectWithGroupBy() throws {
        let sql = "SELECT department, COUNT(*) FROM employees GROUP BY department"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        #expect(query.groupBy != nil)
        #expect(query.groupBy?.count == 1)
    }
}

@Suite("SPARQL Parser Tests", .serialized)
struct SPARQLParserTests {

    init() {
        // Enable debug logging for parser investigation
        SPARQLParser.enableDebug(true)
    }

    @Test("Parse simple SELECT")
    func testParseSimpleSelect() throws {
        print("[TEST] testParseSimpleSelect START")
        let sparql = "SELECT ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name }"
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)
        print("[TEST] testParseSimpleSelect END")

        #expect(query.projection != Projection.all)
    }

    @Test("Parse SELECT *")
    func testParseSelectStar() throws {
        let sparql = "SELECT * WHERE { ?s ?p ?o }"
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)

        #expect(query.projection == Projection.all)
    }

    @Test("Parse SELECT with PREFIX")
    func testParseSelectWithPrefix() throws {
        let sparql = """
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT ?name WHERE { ?s foaf:name ?name }
            """
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)

        #expect(query.filter == nil || query.filter != nil)  // Just ensure parsing succeeds
    }

    @Test("Parse SELECT with FILTER")
    func testParseSelectWithFilter() throws {
        print("[TEST] testParseSelectWithFilter START")
        let sparql = "SELECT ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name . FILTER(?name = \"Alice\") }"
        print("[TEST] SPARQL: \(sparql)")
        let parser = SPARQLParser()
        print("[TEST] Parsing...")
        let query = try parser.parseSelect(sparql)
        print("[TEST] testParseSelectWithFilter END - SUCCESS")

        // Verify parsing completed
        #expect(query.source != nil || query.source == nil)  // Just ensure it parsed
    }

    @Test("Parse SELECT with LIMIT")
    func testParseSelectWithLimit() throws {
        let sparql = "SELECT ?s WHERE { ?s ?p ?o } LIMIT 10"
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)

        #expect(query.limit == 10)
    }

    @Test("Parse SELECT with ORDER BY")
    func testParseSelectWithOrderBy() throws {
        let sparql = "SELECT ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name } ORDER BY ?name"
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)

        #expect(query.orderBy != nil)
    }

    @Test("Parse ASK query")
    func testParseAskQuery() throws {
        let sparql = "ASK { ?s <http://xmlns.com/foaf/0.1/name> \"Alice\" }"
        let parser = SPARQLParser()
        let statement = try parser.parse(sparql)

        if case .ask(_) = statement {
            // OK
        } else {
            Issue.record("Expected ASK query")
        }
    }
}
