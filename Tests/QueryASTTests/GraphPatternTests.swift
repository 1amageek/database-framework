/// GraphPatternTests.swift
/// Comprehensive tests for SPARQL GraphPattern types

import Testing
@testable import QueryAST

// MARK: - GraphPattern Builder Tests

@Suite("GraphPattern Builder Tests")
struct GraphPatternBuilderTests {

    @Test("GraphPattern.bgp with varargs")
    func testBgpVarargs() throws {
        let pattern = GraphPattern.bgp(
            TriplePattern(subject: .variable("s"), predicate: .variable("p"), object: .variable("o")),
            TriplePattern(subject: .variable("s"), predicate: .iri("http://example.org/name"), object: .variable("name"))
        )

        if case .basic(let triples) = pattern {
            #expect(triples.count == 2)
        } else {
            Issue.record("Expected basic pattern")
        }
    }

    @Test("GraphPattern.bgp with array")
    func testBgpArray() throws {
        let triples = [
            TriplePattern(subject: .variable("s"), predicate: .variable("p"), object: .variable("o"))
        ]
        let pattern = GraphPattern.bgp(triples)

        if case .basic(let t) = pattern {
            #expect(t.count == 1)
        }
    }

    @Test("GraphPattern.filtered")
    func testFiltered() throws {
        let base = GraphPattern.basic([])
        let filtered = GraphPattern.filtered(base, .equal(.variable(Variable("x")), .literal(.int(1))))

        if case .filter(_, let condition) = filtered {
            if case .equal = condition {
                // OK
            } else {
                Issue.record("Expected equal condition")
            }
        } else {
            Issue.record("Expected filter pattern")
        }
    }

    @Test("GraphPattern.leftJoin (OPTIONAL)")
    func testLeftJoin() throws {
        let left = GraphPattern.basic([])
        let right = GraphPattern.basic([])
        let optional = GraphPattern.leftJoin(left, right)

        if case .optional = optional {
            // OK
        } else {
            Issue.record("Expected optional pattern")
        }
    }

    @Test("GraphPattern.unionAll")
    func testUnionAll() throws {
        let p1 = GraphPattern.basic([])
        let p2 = GraphPattern.basic([])
        let p3 = GraphPattern.basic([])
        let union = GraphPattern.unionAll(p1, p2, p3)

        // unionAll creates nested unions
        if case .union = union {
            // OK
        } else {
            Issue.record("Expected union pattern")
        }
    }

    @Test("GraphPattern.unionAll empty")
    func testUnionAllEmpty() throws {
        let empty = GraphPattern.unionAll()
        if case .basic(let triples) = empty {
            #expect(triples.isEmpty)
        }
    }

    @Test("GraphPattern.unionTwo")
    func testUnionTwo() throws {
        let left = GraphPattern.basic([])
        let right = GraphPattern.basic([])
        let union = GraphPattern.unionTwo(left, right)

        if case .union = union {
            // OK
        } else {
            Issue.record("Expected union pattern")
        }
    }

    @Test("GraphPattern.difference (MINUS)")
    func testDifference() throws {
        let left = GraphPattern.basic([])
        let right = GraphPattern.basic([])
        let minus = GraphPattern.difference(left, right)

        if case .minus = minus {
            // OK
        } else {
            Issue.record("Expected minus pattern")
        }
    }

    @Test("GraphPattern.binding (BIND)")
    func testBinding() throws {
        let base = GraphPattern.basic([])
        let bind = GraphPattern.binding(base, "total", .add(.variable(Variable("a")), .variable(Variable("b"))))

        if case .bind(_, let variable, _) = bind {
            #expect(variable == "total")
        } else {
            Issue.record("Expected bind pattern")
        }
    }

    @Test("GraphPattern.inlineData (VALUES)")
    func testInlineData() throws {
        let values = GraphPattern.inlineData(
            ["x", "y"],
            [
                [.int(1), .int(2)],
                [.int(3), .int(4)]
            ]
        )

        if case .values(let variables, let bindings) = values {
            #expect(variables == ["x", "y"])
            #expect(bindings.count == 2)
        } else {
            Issue.record("Expected values pattern")
        }
    }

    @Test("GraphPattern.federated (SERVICE)")
    func testFederated() throws {
        let pattern = GraphPattern.basic([])
        let service = GraphPattern.federated("http://dbpedia.org/sparql", pattern, silent: true)

        if case .service(let endpoint, _, let silent) = service {
            #expect(endpoint == "http://dbpedia.org/sparql")
            #expect(silent == true)
        } else {
            Issue.record("Expected service pattern")
        }
    }

    @Test("GraphPattern.named (GRAPH)")
    func testNamed() throws {
        let pattern = GraphPattern.basic([])
        let named = GraphPattern.named(.iri("http://example.org/graph1"), pattern)

        if case .graph(let name, _) = named {
            if case .iri(let iri) = name {
                #expect(iri == "http://example.org/graph1")
            }
        } else {
            Issue.record("Expected graph pattern")
        }
    }

    @Test("GraphPattern.path (property path)")
    func testPath() throws {
        let path = GraphPattern.path(
            subject: .variable("s"),
            path: .oneOrMore(.iri("http://example.org/knows")),
            object: .variable("o")
        )

        if case .propertyPath(let subject, let p, let object) = path {
            if case .variable(let s) = subject {
                #expect(s == "s")
            }
            if case .oneOrMore = p {
                // OK
            }
            if case .variable(let o) = object {
                #expect(o == "o")
            }
        } else {
            Issue.record("Expected property path pattern")
        }
    }
}

// MARK: - GraphPattern Analysis Tests

@Suite("GraphPattern Analysis Tests")
struct GraphPatternAnalysisTests {

    @Test("variables - basic pattern")
    func testVariablesBasic() throws {
        let pattern = GraphPattern.basic([
            TriplePattern(subject: .variable("s"), predicate: .iri("http://example.org/p"), object: .variable("o"))
        ])

        let vars = pattern.variables
        #expect(vars.contains("s"))
        #expect(vars.contains("o"))
        #expect(vars.count == 2)
    }

    @Test("variables - join pattern")
    func testVariablesJoin() throws {
        let left = GraphPattern.basic([
            TriplePattern(subject: .variable("a"), predicate: .variable("p"), object: .variable("b"))
        ])
        let right = GraphPattern.basic([
            TriplePattern(subject: .variable("b"), predicate: .variable("q"), object: .variable("c"))
        ])
        let joined = GraphPattern.join(left, right)

        let vars = joined.variables
        #expect(vars.contains("a"))
        #expect(vars.contains("b"))
        #expect(vars.contains("c"))
        #expect(vars.contains("p"))
        #expect(vars.contains("q"))
    }

    @Test("variables - optional pattern")
    func testVariablesOptional() throws {
        let left = GraphPattern.basic([
            TriplePattern(subject: .variable("s"), predicate: .iri("http://example.org/name"), object: .variable("name"))
        ])
        let right = GraphPattern.basic([
            TriplePattern(subject: .variable("s"), predicate: .iri("http://example.org/age"), object: .variable("age"))
        ])
        let optional = GraphPattern.optional(left, right)

        let vars = optional.variables
        #expect(vars.contains("s"))
        #expect(vars.contains("name"))
        #expect(vars.contains("age"))
    }

    @Test("variables - union pattern")
    func testVariablesUnion() throws {
        let left = GraphPattern.basic([
            TriplePattern(subject: .variable("x"), predicate: .iri("http://example.org/a"), object: .variable("a"))
        ])
        let right = GraphPattern.basic([
            TriplePattern(subject: .variable("x"), predicate: .iri("http://example.org/b"), object: .variable("b"))
        ])
        let union = GraphPattern.union(left, right)

        let vars = union.variables
        #expect(vars.contains("x"))
        #expect(vars.contains("a"))
        #expect(vars.contains("b"))
    }

    @Test("variables - minus pattern")
    func testVariablesMinus() throws {
        let left = GraphPattern.basic([
            TriplePattern(subject: .variable("s"), predicate: .variable("p"), object: .variable("o"))
        ])
        let right = GraphPattern.basic([
            TriplePattern(subject: .variable("s"), predicate: .iri("http://example.org/hidden"), object: .variable("h"))
        ])
        let minus = GraphPattern.minus(left, right)

        // MINUS does not project variables from the right
        let vars = minus.variables
        #expect(vars.contains("s"))
        #expect(vars.contains("p"))
        #expect(vars.contains("o"))
        #expect(!vars.contains("h"))
    }

    @Test("variables - bind pattern")
    func testVariablesBind() throws {
        let base = GraphPattern.basic([
            TriplePattern(subject: .variable("s"), predicate: .iri("http://example.org/value"), object: .variable("v"))
        ])
        let bind = GraphPattern.bind(base, variable: "doubled", expression: .multiply(.variable(Variable("v")), .literal(.int(2))))

        let vars = bind.variables
        #expect(vars.contains("s"))
        #expect(vars.contains("v"))
        #expect(vars.contains("doubled"))
    }

    @Test("variables - values pattern")
    func testVariablesValues() throws {
        let values = GraphPattern.values(variables: ["x", "y"], bindings: [[.int(1), .int(2)]])

        let vars = values.variables
        #expect(vars == Set(["x", "y"]))
    }

    @Test("variables - property path")
    func testVariablesPropertyPath() throws {
        let path = GraphPattern.propertyPath(
            subject: .variable("start"),
            path: .zeroOrMore(.iri("http://example.org/link")),
            object: .variable("end")
        )

        let vars = path.variables
        #expect(vars.contains("start"))
        #expect(vars.contains("end"))
    }

    @Test("requiredVariables - basic pattern")
    func testRequiredVariablesBasic() throws {
        let pattern = GraphPattern.basic([
            TriplePattern(subject: .variable("s"), predicate: .iri("http://example.org/p"), object: .variable("o"))
        ])

        let required = pattern.requiredVariables
        #expect(required.contains("s"))
        #expect(required.contains("o"))
    }

    @Test("requiredVariables - optional pattern")
    func testRequiredVariablesOptional() throws {
        let left = GraphPattern.basic([
            TriplePattern(subject: .variable("s"), predicate: .iri("http://example.org/name"), object: .variable("name"))
        ])
        let right = GraphPattern.basic([
            TriplePattern(subject: .variable("s"), predicate: .iri("http://example.org/age"), object: .variable("age"))
        ])
        let optional = GraphPattern.optional(left, right)

        let required = optional.requiredVariables
        #expect(required.contains("s"))
        #expect(required.contains("name"))
        #expect(!required.contains("age"))  // age is optional
    }

    @Test("requiredVariables - union pattern")
    func testRequiredVariablesUnion() throws {
        let left = GraphPattern.basic([
            TriplePattern(subject: .variable("x"), predicate: .iri("http://example.org/a"), object: .variable("a"))
        ])
        let right = GraphPattern.basic([
            TriplePattern(subject: .variable("x"), predicate: .iri("http://example.org/b"), object: .variable("b"))
        ])
        let union = GraphPattern.union(left, right)

        // Only variables required in BOTH branches are required
        let required = union.requiredVariables
        #expect(required.contains("x"))
        #expect(!required.contains("a"))
        #expect(!required.contains("b"))
    }

    @Test("tripleCount")
    func testTripleCount() throws {
        let basic = GraphPattern.basic([
            TriplePattern(subject: .variable("s"), predicate: .variable("p"), object: .variable("o")),
            TriplePattern(subject: .variable("s"), predicate: .iri("http://example.org/name"), object: .variable("n"))
        ])
        #expect(basic.tripleCount == 2)

        let joined = GraphPattern.join(basic, basic)
        #expect(joined.tripleCount == 4)

        let values = GraphPattern.values(variables: ["x"], bindings: [[.int(1)]])
        #expect(values.tripleCount == 0)

        let path = GraphPattern.propertyPath(subject: .variable("s"), path: .iri("http://example.org/p"), object: .variable("o"))
        #expect(path.tripleCount == 1)
    }

    @Test("complexity")
    func testComplexity() throws {
        let basic = GraphPattern.basic([
            TriplePattern(subject: .variable("s"), predicate: .variable("p"), object: .variable("o"))
        ])
        #expect(basic.complexity == 1)

        let joined = GraphPattern.join(basic, basic)
        #expect(joined.complexity == 1)  // 1 * 1

        let union = GraphPattern.union(basic, basic)
        #expect(union.complexity == 2)  // 1 + 1

        let service = GraphPattern.service(endpoint: "http://example.org", pattern: basic, silent: false)
        #expect(service.complexity == 10)  // Network overhead
    }
}

// MARK: - GraphPattern Transformation Tests

@Suite("GraphPattern Transformation Tests")
struct GraphPatternTransformationTests {

    @Test("flattened - nested joins")
    func testFlattenedNestedJoins() throws {
        let p1 = GraphPattern.basic([
            TriplePattern(subject: .variable("a"), predicate: .variable("p1"), object: .variable("b"))
        ])
        let p2 = GraphPattern.basic([
            TriplePattern(subject: .variable("b"), predicate: .variable("p2"), object: .variable("c"))
        ])
        let p3 = GraphPattern.basic([
            TriplePattern(subject: .variable("c"), predicate: .variable("p3"), object: .variable("d"))
        ])

        let nested = GraphPattern.join(GraphPattern.join(p1, p2), p3)
        let flattened = nested.flattened()

        if case .basic(let triples) = flattened {
            #expect(triples.count == 3)
        } else {
            Issue.record("Expected flattened basic pattern")
        }
    }

    @Test("flattened - filter preserved")
    func testFlattenedFilterPreserved() throws {
        let base = GraphPattern.basic([
            TriplePattern(subject: .variable("s"), predicate: .variable("p"), object: .variable("o"))
        ])
        let filtered = GraphPattern.filter(base, .greaterThan(.variable(Variable("o")), .literal(.int(10))))

        let flattened = filtered.flattened()
        if case .filter(_, let condition) = flattened {
            if case .greaterThan = condition {
                // OK
            } else {
                Issue.record("Expected greaterThan condition")
            }
        } else {
            Issue.record("Expected filter pattern")
        }
    }

    @Test("flattened - optional preserved")
    func testFlattenedOptionalPreserved() throws {
        let left = GraphPattern.basic([])
        let right = GraphPattern.basic([])
        let optional = GraphPattern.optional(left, right)

        let flattened = optional.flattened()
        if case .optional = flattened {
            // OK
        } else {
            Issue.record("Expected optional pattern")
        }
    }

    @Test("optimized")
    func testOptimized() throws {
        let base = GraphPattern.basic([
            TriplePattern(subject: .variable("s"), predicate: .variable("p"), object: .variable("o"))
        ])

        let optimized = base.optimized()
        // Currently optimized just calls flattened
        if case .basic = optimized {
            // OK
        }
    }
}

// MARK: - GraphPattern SPARQL Serialization Tests

@Suite("GraphPattern SPARQL Serialization Tests")
struct GraphPatternSPARQLSerializationTests {

    @Test("toSPARQL - basic pattern")
    func testToSPARQLBasic() throws {
        let pattern = GraphPattern.basic([
            TriplePattern(
                subject: .variable("s"),
                predicate: .iri("http://xmlns.com/foaf/0.1/name"),
                object: .variable("name")
            )
        ])

        let sparql = pattern.toSPARQL()
        #expect(sparql.contains("?s"))
        #expect(sparql.contains("<http://xmlns.com/foaf/0.1/name>"))
        #expect(sparql.contains("?name"))
    }

    @Test("toSPARQL - optional pattern")
    func testToSPARQLOptional() throws {
        let left = GraphPattern.basic([
            TriplePattern(subject: .variable("s"), predicate: .iri("http://example.org/a"), object: .variable("a"))
        ])
        let right = GraphPattern.basic([
            TriplePattern(subject: .variable("s"), predicate: .iri("http://example.org/b"), object: .variable("b"))
        ])
        let optional = GraphPattern.optional(left, right)

        let sparql = optional.toSPARQL()
        #expect(sparql.contains("OPTIONAL"))
    }

    @Test("toSPARQL - union pattern")
    func testToSPARQLUnion() throws {
        let left = GraphPattern.basic([])
        let right = GraphPattern.basic([])
        let union = GraphPattern.union(left, right)

        let sparql = union.toSPARQL()
        #expect(sparql.contains("UNION"))
    }

    @Test("toSPARQL - filter pattern")
    func testToSPARQLFilter() throws {
        let base = GraphPattern.basic([])
        let filtered = GraphPattern.filter(base, .equal(.variable(Variable("x")), .literal(.int(1))))

        let sparql = filtered.toSPARQL()
        #expect(sparql.contains("FILTER"))
    }

    @Test("toSPARQL - minus pattern")
    func testToSPARQLMinus() throws {
        let left = GraphPattern.basic([])
        let right = GraphPattern.basic([])
        let minus = GraphPattern.minus(left, right)

        let sparql = minus.toSPARQL()
        #expect(sparql.contains("MINUS"))
    }

    @Test("toSPARQL - graph pattern")
    func testToSPARQLGraph() throws {
        let inner = GraphPattern.basic([])
        let named = GraphPattern.graph(name: .iri("http://example.org/graph1"), pattern: inner)

        let sparql = named.toSPARQL()
        #expect(sparql.contains("GRAPH"))
        #expect(sparql.contains("<http://example.org/graph1>"))
    }

    @Test("toSPARQL - service pattern")
    func testToSPARQLService() throws {
        let inner = GraphPattern.basic([])
        let service = GraphPattern.service(endpoint: "http://dbpedia.org/sparql", pattern: inner, silent: false)

        let sparql = service.toSPARQL()
        #expect(sparql.contains("SERVICE"))
        #expect(sparql.contains("<http://dbpedia.org/sparql>"))
    }

    @Test("toSPARQL - service silent")
    func testToSPARQLServiceSilent() throws {
        let inner = GraphPattern.basic([])
        let service = GraphPattern.service(endpoint: "http://example.org", pattern: inner, silent: true)

        let sparql = service.toSPARQL()
        #expect(sparql.contains("SERVICE SILENT"))
    }

    @Test("toSPARQL - bind pattern")
    func testToSPARQLBind() throws {
        let base = GraphPattern.basic([])
        let bind = GraphPattern.bind(base, variable: "total", expression: .literal(.int(42)))

        let sparql = bind.toSPARQL()
        #expect(sparql.contains("BIND"))
        #expect(sparql.contains("?total"))
    }

    @Test("toSPARQL - values pattern")
    func testToSPARQLValues() throws {
        let values = GraphPattern.values(
            variables: ["x", "y"],
            bindings: [
                [.int(1), .int(2)],
                [.int(3), nil]
            ]
        )

        let sparql = values.toSPARQL()
        #expect(sparql.contains("VALUES"))
        #expect(sparql.contains("?x"))
        #expect(sparql.contains("?y"))
        #expect(sparql.contains("UNDEF"))
    }

    @Test("toSPARQL - property path")
    func testToSPARQLPropertyPath() throws {
        let path = GraphPattern.propertyPath(
            subject: .variable("s"),
            path: .oneOrMore(.iri("http://example.org/knows")),
            object: .variable("o")
        )

        let sparql = path.toSPARQL()
        #expect(sparql.contains("?s"))
        #expect(sparql.contains("?o"))
        #expect(sparql.contains("+"))
    }
}

// MARK: - SelectQuery SPARQL Serialization Tests

@Suite("SelectQuery SPARQL Serialization Tests")
struct SelectQuerySPARQLSerializationTests {

    @Test("toSPARQL - basic SELECT")
    func testToSPARQLBasicSelect() throws {
        let query = SelectQuery(
            projection: .all,
            source: .graphPattern(GraphPattern.basic([
                TriplePattern(subject: .variable("s"), predicate: .variable("p"), object: .variable("o"))
            ]))
        )

        let sparql = query.toSPARQL()
        #expect(sparql.contains("SELECT *"))
        #expect(sparql.contains("WHERE"))
    }

    @Test("toSPARQL - SELECT with prefixes")
    func testToSPARQLWithPrefixes() throws {
        let query = SelectQuery(
            projection: .all,
            source: .graphPattern(GraphPattern.basic([]))
        )

        let sparql = query.toSPARQL(prefixes: ["foaf": "http://xmlns.com/foaf/0.1/"])
        #expect(sparql.contains("PREFIX foaf: <http://xmlns.com/foaf/0.1/>"))
    }

    @Test("toSPARQL - SELECT DISTINCT")
    func testToSPARQLDistinct() throws {
        let query = SelectQuery(
            projection: .all,
            source: .graphPattern(GraphPattern.basic([])),
            distinct: true
        )

        let sparql = query.toSPARQL()
        #expect(sparql.contains("SELECT DISTINCT"))
    }

    @Test("toSPARQL - SELECT with LIMIT")
    func testToSPARQLWithLimit() throws {
        let query = SelectQuery(
            projection: .all,
            source: .graphPattern(GraphPattern.basic([])),
            limit: 10
        )

        let sparql = query.toSPARQL()
        #expect(sparql.contains("LIMIT 10"))
    }

    @Test("toSPARQL - SELECT with OFFSET")
    func testToSPARQLWithOffset() throws {
        let query = SelectQuery(
            projection: .all,
            source: .graphPattern(GraphPattern.basic([])),
            offset: 5
        )

        let sparql = query.toSPARQL()
        #expect(sparql.contains("OFFSET 5"))
    }

    @Test("toSPARQL - SELECT with ORDER BY")
    func testToSPARQLWithOrderBy() throws {
        let query = SelectQuery(
            projection: .all,
            source: .graphPattern(GraphPattern.basic([])),
            orderBy: [SortKey(.variable(Variable("name")), direction: .ascending)]
        )

        let sparql = query.toSPARQL()
        #expect(sparql.contains("ORDER BY"))
        #expect(sparql.contains("ASC"))
    }

    @Test("toSPARQL - SELECT with GROUP BY")
    func testToSPARQLWithGroupBy() throws {
        let query = SelectQuery(
            projection: .items([ProjectionItem(.variable(Variable("x")))]),
            source: .graphPattern(GraphPattern.basic([])),
            groupBy: [.variable(Variable("x"))]
        )

        let sparql = query.toSPARQL()
        #expect(sparql.contains("GROUP BY"))
    }

    @Test("toSPARQL - SELECT with HAVING")
    func testToSPARQLWithHaving() throws {
        let query = SelectQuery(
            projection: .items([ProjectionItem(.variable(Variable("x")))]),
            source: .graphPattern(GraphPattern.basic([])),
            groupBy: [.variable(Variable("x"))],
            having: .greaterThan(
                .aggregate(.count(nil, distinct: false)),
                .literal(.int(5))
            )
        )

        let sparql = query.toSPARQL()
        #expect(sparql.contains("HAVING"))
    }
}
