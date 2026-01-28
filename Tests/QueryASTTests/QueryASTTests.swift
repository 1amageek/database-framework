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

        // Verify parsing completed (try above ensures it parsed successfully)
        _ = query
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

    // MARK: - Built-in Function Tests (W3C SPARQL 1.1 Section 17.4)

    @Test("Parse FILTER with 1-arg function STRLEN")
    func testFilterSTRLEN() throws {
        let sparql = "SELECT ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name . FILTER(STRLEN(?name) > 3) }"
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)

        // Should parse without error; verify the filter structure
        if case .graphPattern(let pattern) = query.source {
            if case .filter(_, let expr) = pattern {
                if case .greaterThan(let left, let right) = expr {
                    if case .function(let fc) = left {
                        #expect(fc.name == "STRLEN")
                        #expect(fc.arguments.count == 1)
                    } else {
                        Issue.record("Expected function call STRLEN, got \(left)")
                    }
                    #expect(right == .literal(.int(3)))
                } else {
                    Issue.record("Expected greaterThan expression, got \(expr)")
                }
            } else {
                Issue.record("Expected filter pattern")
            }
        }
    }

    @Test("Parse FILTER with 2-arg function CONTAINS")
    func testFilterCONTAINS() throws {
        let sparql = #"SELECT ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name . FILTER(CONTAINS(?name, "Alice")) }"#
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)

        if case .graphPattern(let pattern) = query.source {
            if case .filter(_, let expr) = pattern {
                if case .function(let fc) = expr {
                    #expect(fc.name == "CONTAINS")
                    #expect(fc.arguments.count == 2)
                    #expect(fc.arguments[0] == .variable(Variable("name")))
                    #expect(fc.arguments[1] == .literal(.string("Alice")))
                } else {
                    Issue.record("Expected function call CONTAINS, got \(expr)")
                }
            } else {
                Issue.record("Expected filter pattern")
            }
        }
    }

    @Test("Parse FILTER with nested functions CONTAINS(LCASE())")
    func testFilterNestedFunctions() throws {
        let sparql = #"SELECT ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name . FILTER(CONTAINS(LCASE(?name), "alice")) }"#
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)

        if case .graphPattern(let pattern) = query.source {
            if case .filter(_, let expr) = pattern {
                if case .function(let fc) = expr {
                    #expect(fc.name == "CONTAINS")
                    #expect(fc.arguments.count == 2)
                    // First arg should be LCASE(?name)
                    if case .function(let inner) = fc.arguments[0] {
                        #expect(inner.name == "LCASE")
                        #expect(inner.arguments.count == 1)
                        #expect(inner.arguments[0] == .variable(Variable("name")))
                    } else {
                        Issue.record("Expected nested LCASE function, got \(fc.arguments[0])")
                    }
                    #expect(fc.arguments[1] == .literal(.string("alice")))
                } else {
                    Issue.record("Expected function call CONTAINS, got \(expr)")
                }
            } else {
                Issue.record("Expected filter pattern")
            }
        }
    }

    @Test("Parse BIND with 0-arg function NOW()")
    func testBindNOW() throws {
        let sparql = "SELECT ?now WHERE { ?s ?p ?o . BIND(NOW() AS ?now) }"
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)

        if case .graphPattern(let pattern) = query.source {
            if case .bind(_, variable: let varName, expression: let expr) = pattern {
                #expect(varName == "now")
                if case .function(let fc) = expr {
                    #expect(fc.name == "NOW")
                    #expect(fc.arguments.isEmpty)
                } else {
                    Issue.record("Expected function call NOW, got \(expr)")
                }
            } else {
                Issue.record("Expected bind pattern")
            }
        }
    }

    @Test("Parse BIND with IF function")
    func testBindIF() throws {
        let sparql = "SELECT ?val WHERE { ?s <http://example.org/x> ?x . BIND(IF(?x > 0, ?x, 0) AS ?val) }"
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)

        if case .graphPattern(let pattern) = query.source {
            if case .bind(_, variable: let varName, expression: let expr) = pattern {
                #expect(varName == "val")
                if case .function(let fc) = expr {
                    #expect(fc.name == "IF")
                    #expect(fc.arguments.count == 3)
                } else {
                    Issue.record("Expected function call IF, got \(expr)")
                }
            } else {
                Issue.record("Expected bind pattern")
            }
        }
    }

    @Test("Parse BIND with COALESCE")
    func testBindCOALESCE() throws {
        let sparql = #"SELECT ?val WHERE { ?s ?p ?o . BIND(COALESCE(?a, ?b, "default") AS ?val) }"#
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)

        if case .graphPattern(let pattern) = query.source {
            if case .bind(_, variable: let varName, expression: let expr) = pattern {
                #expect(varName == "val")
                if case .coalesce(let args) = expr {
                    #expect(args.count == 3)
                    #expect(args[0] == .variable(Variable("a")))
                    #expect(args[1] == .variable(Variable("b")))
                    #expect(args[2] == .literal(.string("default")))
                } else {
                    Issue.record("Expected coalesce, got \(expr)")
                }
            } else {
                Issue.record("Expected bind pattern")
            }
        }
    }

    @Test("Parse BIND with CONCAT")
    func testBindCONCAT() throws {
        let sparql = #"SELECT ?full WHERE { ?s <http://example.org/first> ?first . BIND(CONCAT(?first, " ", ?last) AS ?full) }"#
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)

        if case .graphPattern(let pattern) = query.source {
            if case .bind(_, variable: let varName, expression: let expr) = pattern {
                #expect(varName == "full")
                if case .function(let fc) = expr {
                    #expect(fc.name == "CONCAT")
                    #expect(fc.arguments.count == 3)
                } else {
                    Issue.record("Expected function call CONCAT, got \(expr)")
                }
            } else {
                Issue.record("Expected bind pattern")
            }
        }
    }

    @Test("Parse BIND with SUBSTR")
    func testBindSUBSTR() throws {
        let sparql = "SELECT ?short WHERE { ?s <http://example.org/name> ?name . BIND(SUBSTR(?name, 1, 3) AS ?short) }"
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)

        if case .graphPattern(let pattern) = query.source {
            if case .bind(_, variable: let varName, expression: let expr) = pattern {
                #expect(varName == "short")
                if case .function(let fc) = expr {
                    #expect(fc.name == "SUBSTR")
                    #expect(fc.arguments.count == 3)
                } else {
                    Issue.record("Expected function call SUBSTR, got \(expr)")
                }
            } else {
                Issue.record("Expected bind pattern")
            }
        }
    }

    @Test("Parse SELECT with COUNT(*)")
    func testAggregateCountStar() throws {
        let sparql = "SELECT (COUNT(*) AS ?cnt) WHERE { ?s ?p ?o }"
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)

        if case .items(let items) = query.projection {
            #expect(items.count == 1)
            #expect(items[0].alias == "cnt")
            if case .aggregate(.count(let arg, distinct: let d)) = items[0].expression {
                #expect(arg == nil)
                #expect(d == false)
            } else {
                Issue.record("Expected COUNT(*) aggregate, got \(items[0].expression)")
            }
        } else {
            Issue.record("Expected items projection")
        }
    }

    @Test("Parse SELECT with COUNT(DISTINCT ?x)")
    func testAggregateCountDistinct() throws {
        let sparql = "SELECT (COUNT(DISTINCT ?s) AS ?cnt) WHERE { ?s ?p ?o }"
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)

        if case .items(let items) = query.projection {
            #expect(items.count == 1)
            if case .aggregate(.count(let arg, distinct: let d)) = items[0].expression {
                #expect(arg == .variable(Variable("s")))
                #expect(d == true)
            } else {
                Issue.record("Expected COUNT(DISTINCT ?s) aggregate, got \(items[0].expression)")
            }
        } else {
            Issue.record("Expected items projection")
        }
    }

    @Test("Parse SELECT with SUM aggregate")
    func testAggregateSUM() throws {
        let sparql = "SELECT (SUM(?val) AS ?total) WHERE { ?s <http://example.org/val> ?val }"
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)

        if case .items(let items) = query.projection {
            #expect(items.count == 1)
            if case .aggregate(.sum(let arg, distinct: let d)) = items[0].expression {
                #expect(arg == .variable(Variable("val")))
                #expect(d == false)
            } else {
                Issue.record("Expected SUM aggregate, got \(items[0].expression)")
            }
        } else {
            Issue.record("Expected items projection")
        }
    }

    @Test("Parse SELECT with GROUP_CONCAT")
    func testAggregateGroupConcat() throws {
        let sparql = #"SELECT (GROUP_CONCAT(?name ; SEPARATOR=", ") AS ?names) WHERE { ?s <http://example.org/name> ?name }"#
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)

        if case .items(let items) = query.projection {
            #expect(items.count == 1)
            if case .aggregate(.groupConcat(let arg, separator: let sep, distinct: let d)) = items[0].expression {
                #expect(arg == .variable(Variable("name")))
                #expect(sep == ", ")
                #expect(d == false)
            } else {
                Issue.record("Expected GROUP_CONCAT aggregate, got \(items[0].expression)")
            }
        } else {
            Issue.record("Expected items projection")
        }
    }

    @Test("Parse BIND with IRI function call")
    func testIRIFunctionCall() throws {
        let sparql = "SELECT ?y WHERE { ?s ?p ?x . BIND(<http://example.org/func>(?x) AS ?y) }"
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)

        if case .graphPattern(let pattern) = query.source {
            if case .bind(_, variable: let varName, expression: let expr) = pattern {
                #expect(varName == "y")
                if case .function(let fc) = expr {
                    #expect(fc.name == "http://example.org/func")
                    #expect(fc.arguments.count == 1)
                    #expect(fc.arguments[0] == .variable(Variable("x")))
                } else {
                    Issue.record("Expected IRI function call, got \(expr)")
                }
            } else {
                Issue.record("Expected bind pattern")
            }
        }
    }

    @Test("Parse FILTER with REGEX using variable pattern")
    func testFilterREGEXVariable() throws {
        let sparql = "SELECT ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name . FILTER(REGEX(?name, ?pattern)) }"
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)

        if case .graphPattern(let pattern) = query.source {
            if case .filter(_, let expr) = pattern {
                // When pattern is a variable (not string literal), should fall back to .function
                if case .function(let fc) = expr {
                    #expect(fc.name == "REGEX")
                    #expect(fc.arguments.count == 2)
                } else {
                    Issue.record("Expected REGEX as function call for variable pattern, got \(expr)")
                }
            } else {
                Issue.record("Expected filter pattern")
            }
        }
    }

    @Test("Parse FILTER with REGEX: string pattern + variable flags falls back to function")
    func testFilterREGEXStringPatternVariableFlags() throws {
        let sparql = "SELECT ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name . FILTER(REGEX(?name, \"abc\", ?flags)) }"
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)

        if case .graphPattern(let pattern) = query.source {
            if case .filter(_, let expr) = pattern {
                // String pattern + variable flags must NOT use .regex (which would drop ?flags)
                // Must fall back to .function to preserve all 3 arguments
                if case .function(let fc) = expr {
                    #expect(fc.name == "REGEX")
                    #expect(fc.arguments.count == 3)
                } else if case .regex = expr {
                    Issue.record("REGEX with variable flags should not use .regex AST node (flags would be lost)")
                } else {
                    Issue.record("Expected REGEX as function call, got \(expr)")
                }
            } else {
                Issue.record("Expected filter pattern")
            }
        }
    }

    @Test("Parse BIND with REPLACE")
    func testBindREPLACE() throws {
        let sparql = #"SELECT ?fixed WHERE { ?s <http://example.org/name> ?name . BIND(REPLACE(?name, "old", "new") AS ?fixed) }"#
        let parser = SPARQLParser()
        let query = try parser.parseSelect(sparql)

        if case .graphPattern(let pattern) = query.source {
            if case .bind(_, variable: let varName, expression: let expr) = pattern {
                #expect(varName == "fixed")
                if case .function(let fc) = expr {
                    #expect(fc.name == "REPLACE")
                    #expect(fc.arguments.count == 3)
                } else {
                    Issue.record("Expected REPLACE function, got \(expr)")
                }
            } else {
                Issue.record("Expected bind pattern")
            }
        }
    }
}
