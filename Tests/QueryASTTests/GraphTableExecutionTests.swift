/// GraphTableExecutionTests.swift
/// Comprehensive tests for SQL/PGQ GRAPH_TABLE execution patterns
///
/// Coverage: Path patterns, variable length, property filters, path modes, label expressions

import Testing
@testable import QueryAST

// MARK: - GRAPH_TABLE Execution Tests

@Suite("GRAPH_TABLE Execution Tests")
struct GraphTableExecutionTests {

    // MARK: - Simple Path Pattern Tests

    @Test("Simple path pattern")
    func testSimplePathPattern() throws {
        // SELECT * FROM GRAPH_TABLE(social_graph,
        //   MATCH (a:Person)-[:KNOWS]->(b:Person)
        //   COLUMNS (a.name AS person1, b.name AS person2)
        // )

        let source = GraphTableSource.match(
            graph: "social_graph",
            from: NodePattern(variable: "a", labels: ["Person"]),
            via: EdgePattern(labels: ["KNOWS"], direction: .outgoing),
            to: NodePattern(variable: "b", labels: ["Person"])
        )
        .returning([
            (.column(ColumnRef(table: "a", column: "name")), "person1"),
            (.column(ColumnRef(table: "b", column: "name")), "person2")
        ])

        #expect(source.graphName == "social_graph")
        #expect(source.matchPattern.paths.count == 1)
        #expect(source.columns?.count == 2)
        #expect(source.columns?[0].alias == "person1")
        #expect(source.columns?[1].alias == "person2")

        // Verify path structure
        let path = source.matchPattern.paths[0]
        #expect(path.elements.count == 3)

        if case .node(let startNode) = path.elements[0] {
            #expect(startNode.variable == "a")
            #expect(startNode.labels == ["Person"])
        } else {
            Issue.record("Expected node element")
        }

        if case .edge(let edge) = path.elements[1] {
            #expect(edge.labels == ["KNOWS"])
            #expect(edge.direction == .outgoing)
        } else {
            Issue.record("Expected edge element")
        }

        if case .node(let endNode) = path.elements[2] {
            #expect(endNode.variable == "b")
            #expect(endNode.labels == ["Person"])
        } else {
            Issue.record("Expected node element")
        }
    }

    @Test("Bidirectional edge pattern")
    func testBidirectionalEdge() throws {
        // MATCH (a:Person)-[:FRIEND]-(b:Person)

        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(NodePattern(variable: "a", labels: ["Person"])),
                .edge(EdgePattern(labels: ["FRIEND"], direction: .undirected)),
                .node(NodePattern(variable: "b", labels: ["Person"]))
            ])
        ])

        let source = GraphTableSource(graphName: "social", matchPattern: pattern)

        if case .edge(let edge) = source.matchPattern.paths[0].elements[1] {
            #expect(edge.direction == .undirected)
        }
    }

    @Test("Incoming edge pattern")
    func testIncomingEdge() throws {
        // MATCH (a:Person)<-[:FOLLOWS]-(b:Person)

        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(NodePattern(variable: "a", labels: ["Person"])),
                .edge(EdgePattern(labels: ["FOLLOWS"], direction: .incoming)),
                .node(NodePattern(variable: "b", labels: ["Person"]))
            ])
        ])

        let source = GraphTableSource(graphName: "social", matchPattern: pattern)

        if case .edge(let edge) = source.matchPattern.paths[0].elements[1] {
            #expect(edge.direction == .incoming)
        }
    }

    // MARK: - Variable Length Path Tests

    @Test("Variable length path: fixed range")
    func testVariableLengthPathFixed() throws {
        // MATCH (a:Person)-[:FOLLOWS*1..3]->(b:Person)

        let innerPath = PathPattern(elements: [
            .edge(EdgePattern(labels: ["FOLLOWS"], direction: .outgoing))
        ])

        let quantifiedElement = PathElement.quantified(
            innerPath,
            quantifier: .range(min: 1, max: 3)
        )

        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(NodePattern(variable: "a", labels: ["Person"])),
                quantifiedElement,
                .node(NodePattern(variable: "b", labels: ["Person"]))
            ])
        ])

        let source = GraphTableSource(graphName: "social", matchPattern: pattern)

        if case .quantified(_, let quantifier) = source.matchPattern.paths[0].elements[1] {
            #expect(quantifier == .range(min: 1, max: 3))
        } else {
            Issue.record("Expected quantified element")
        }
    }

    @Test("Variable length path: one or more")
    func testVariableLengthOneOrMore() throws {
        // MATCH (a)-[:LINK+]->(b)

        let innerPath = PathPattern(elements: [
            .edge(EdgePattern(labels: ["LINK"], direction: .outgoing))
        ])

        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(NodePattern(variable: "a")),
                .quantified(innerPath, quantifier: .oneOrMore),
                .node(NodePattern(variable: "b"))
            ])
        ])

        let source = GraphTableSource(graphName: "graph", matchPattern: pattern)

        if case .quantified(_, let quantifier) = source.matchPattern.paths[0].elements[1] {
            #expect(quantifier == .oneOrMore)
        }
    }

    @Test("Variable length path: zero or more")
    func testVariableLengthZeroOrMore() throws {
        // MATCH (a)-[:LINK*]->(b)

        let innerPath = PathPattern(elements: [
            .edge(EdgePattern(labels: ["LINK"], direction: .outgoing))
        ])

        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(NodePattern(variable: "a")),
                .quantified(innerPath, quantifier: .zeroOrMore),
                .node(NodePattern(variable: "b"))
            ])
        ])

        let source = GraphTableSource(graphName: "graph", matchPattern: pattern)

        if case .quantified(_, let quantifier) = source.matchPattern.paths[0].elements[1] {
            #expect(quantifier == .zeroOrMore)
        }
    }

    @Test("Variable length path: exactly N")
    func testVariableLengthExactly() throws {
        // MATCH (a)-[:STEP{3}]->(b)

        let innerPath = PathPattern(elements: [
            .edge(EdgePattern(labels: ["STEP"], direction: .outgoing))
        ])

        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(NodePattern(variable: "a")),
                .quantified(innerPath, quantifier: .exactly(3)),
                .node(NodePattern(variable: "b"))
            ])
        ])

        let source = GraphTableSource(graphName: "graph", matchPattern: pattern)

        if case .quantified(_, let quantifier) = source.matchPattern.paths[0].elements[1] {
            #expect(quantifier == .exactly(3))
        }
    }

    // MARK: - Multiple Paths in MATCH Tests

    @Test("Multiple paths in MATCH")
    func testMultiplePaths() throws {
        // MATCH (a)-[:KNOWS]->(b), (b)-[:WORKS_AT]->(c:Company)

        let path1 = PathPattern(elements: [
            .node(NodePattern(variable: "a")),
            .edge(EdgePattern(labels: ["KNOWS"], direction: .outgoing)),
            .node(NodePattern(variable: "b"))
        ])

        let path2 = PathPattern(elements: [
            .node(NodePattern(variable: "b")),
            .edge(EdgePattern(labels: ["WORKS_AT"], direction: .outgoing)),
            .node(NodePattern(variable: "c", labels: ["Company"]))
        ])

        let pattern = MatchPattern(paths: [path1, path2])
        let source = GraphTableSource(graphName: "social", matchPattern: pattern)

        #expect(source.matchPattern.paths.count == 2)

        // Verify shared variable 'b'
        let vars = source.definedVariables
        #expect(vars.contains("a"))
        #expect(vars.contains("b"))
        #expect(vars.contains("c"))
    }

    @Test("Three connected paths")
    func testThreeConnectedPaths() throws {
        // MATCH (a)-[:R1]->(b), (b)-[:R2]->(c), (c)-[:R3]->(d)

        let path1 = PathPattern(elements: [
            .node(NodePattern(variable: "a")),
            .edge(EdgePattern(labels: ["R1"], direction: .outgoing)),
            .node(NodePattern(variable: "b"))
        ])

        let path2 = PathPattern(elements: [
            .node(NodePattern(variable: "b")),
            .edge(EdgePattern(labels: ["R2"], direction: .outgoing)),
            .node(NodePattern(variable: "c"))
        ])

        let path3 = PathPattern(elements: [
            .node(NodePattern(variable: "c")),
            .edge(EdgePattern(labels: ["R3"], direction: .outgoing)),
            .node(NodePattern(variable: "d"))
        ])

        let pattern = MatchPattern(paths: [path1, path2, path3])
        let source = GraphTableSource(graphName: "graph", matchPattern: pattern)

        #expect(source.matchPattern.paths.count == 3)

        let vars = source.definedVariables
        #expect(vars.count == 4)  // a, b, c, d
    }

    // MARK: - Property Filter in MATCH Tests

    @Test("Property filter in node pattern")
    func testPropertyFilterInNode() throws {
        // MATCH (p:Person {age: 30})-[:LIVES_IN]->(c:City {country: 'Japan'})

        let personNode = NodePattern(
            variable: "p",
            labels: ["Person"],
            properties: [("age", .literal(.int(30)))]
        )

        let cityNode = NodePattern(
            variable: "c",
            labels: ["City"],
            properties: [("country", .literal(.string("Japan")))]
        )

        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(personNode),
                .edge(EdgePattern(labels: ["LIVES_IN"], direction: .outgoing)),
                .node(cityNode)
            ])
        ])

        let source = GraphTableSource(graphName: "geo", matchPattern: pattern)

        if case .node(let pNode) = source.matchPattern.paths[0].elements[0] {
            #expect(pNode.properties?.count == 1)
            #expect(pNode.properties?[0].0 == "age")
        }

        if case .node(let cNode) = source.matchPattern.paths[0].elements[2] {
            #expect(cNode.properties?.count == 1)
            #expect(cNode.properties?[0].0 == "country")
        }
    }

    @Test("Multiple property filters")
    func testMultiplePropertyFilters() throws {
        // MATCH (e:Employee {department: 'Engineering', active: true})

        let employeeNode = NodePattern(
            variable: "e",
            labels: ["Employee"],
            properties: [
                ("department", .literal(.string("Engineering"))),
                ("active", .literal(.bool(true)))
            ]
        )

        let pattern = MatchPattern(paths: [
            PathPattern(elements: [.node(employeeNode)])
        ])

        let source = GraphTableSource(graphName: "hr", matchPattern: pattern)

        if case .node(let node) = source.matchPattern.paths[0].elements[0] {
            #expect(node.properties?.count == 2)
        }
    }

    @Test("Property filter on edge")
    func testPropertyFilterOnEdge() throws {
        // MATCH (a)-[:TRANSACTION {amount: 1000}]->(b)

        let edgePattern = EdgePattern(
            labels: ["TRANSACTION"],
            properties: [("amount", .literal(.int(1000)))],
            direction: .outgoing
        )

        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(NodePattern(variable: "a")),
                .edge(edgePattern),
                .node(NodePattern(variable: "b"))
            ])
        ])

        let source = GraphTableSource(graphName: "finance", matchPattern: pattern)

        if case .edge(let edge) = source.matchPattern.paths[0].elements[1] {
            #expect(edge.properties?.count == 1)
            #expect(edge.properties?[0].0 == "amount")
        }
    }

    // MARK: - Path Mode Tests

    @Test("Shortest path mode")
    func testShortestPathMode() throws {
        // MATCH ANY SHORTEST (a)-[:LINK*]->(b)

        let source = GraphTableSource.shortestPath(
            graph: "network",
            from: NodePattern(variable: "a"),
            via: "LINK",
            to: NodePattern(variable: "b")
        )

        #expect(source.matchPattern.paths[0].mode == .anyShortest)
    }

    @Test("All shortest paths mode")
    func testAllShortestPathsMode() throws {
        // MATCH ALL SHORTEST (a)-[:LINK*]->(b)

        let source = GraphTableSource.allShortestPaths(
            graph: "network",
            from: NodePattern(variable: "a"),
            via: "LINK",
            to: NodePattern(variable: "b")
        )

        #expect(source.matchPattern.paths[0].mode == .allShortest)
    }

    @Test("Simple path mode (no repeated nodes)")
    func testSimplePathMode() throws {
        // MATCH path = SIMPLE (a)-[:LINK*]->(b)

        let path = PathPattern(
            elements: [
                .node(NodePattern(variable: "a")),
                .quantified(
                    PathPattern(elements: [.edge(EdgePattern(labels: ["LINK"], direction: .outgoing))]),
                    quantifier: .zeroOrMore
                ),
                .node(NodePattern(variable: "b"))
            ],
            mode: .simple
        )

        let pattern = MatchPattern(paths: [path])
        let source = GraphTableSource(graphName: "graph", matchPattern: pattern)

        #expect(source.matchPattern.paths[0].mode == .simple)
    }

    @Test("Trail mode (no repeated edges)")
    func testTrailPathMode() throws {
        let path = PathPattern(
            elements: [
                .node(NodePattern(variable: "a")),
                .quantified(
                    PathPattern(elements: [.edge(EdgePattern(labels: ["STEP"], direction: .outgoing))]),
                    quantifier: .zeroOrMore
                ),
                .node(NodePattern(variable: "b"))
            ],
            mode: .trail
        )

        let pattern = MatchPattern(paths: [path])
        let source = GraphTableSource(graphName: "graph", matchPattern: pattern)

        #expect(source.matchPattern.paths[0].mode == .trail)
    }

    @Test("Acyclic path mode")
    func testAcyclicPathMode() throws {
        let path = PathPattern(
            elements: [
                .node(NodePattern(variable: "start")),
                .quantified(
                    PathPattern(elements: [.edge(EdgePattern(labels: ["NEXT"], direction: .outgoing))]),
                    quantifier: .oneOrMore
                ),
                .node(NodePattern(variable: "end"))
            ],
            mode: .acyclic
        )

        let pattern = MatchPattern(paths: [path])
        let source = GraphTableSource(graphName: "dag", matchPattern: pattern)

        #expect(source.matchPattern.paths[0].mode == .acyclic)
    }

    // MARK: - Label Expression Tests

    @Test("Multiple node labels (OR)")
    func testMultipleNodeLabels() throws {
        // MATCH (n:Person|Employee)-[]->(m)
        // Represented as multiple labels in the pattern

        let multiLabelNode = NodePattern(variable: "n", labels: ["Person", "Employee"])

        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(multiLabelNode),
                .edge(EdgePattern(direction: .outgoing)),
                .node(NodePattern(variable: "m"))
            ])
        ])

        let source = GraphTableSource(graphName: "org", matchPattern: pattern)

        if case .node(let node) = source.matchPattern.paths[0].elements[0] {
            #expect(node.labels?.count == 2)
            #expect(node.labels?.contains("Person") == true)
            #expect(node.labels?.contains("Employee") == true)
        }
    }

    @Test("Multiple edge labels (OR)")
    func testMultipleEdgeLabels() throws {
        // MATCH (a)-[:KNOWS|WORKS_WITH]->(b)

        let multiLabelEdge = EdgePattern(labels: ["KNOWS", "WORKS_WITH"], direction: .outgoing)

        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(NodePattern(variable: "a")),
                .edge(multiLabelEdge),
                .node(NodePattern(variable: "b"))
            ])
        ])

        let source = GraphTableSource(graphName: "social", matchPattern: pattern)

        if case .edge(let edge) = source.matchPattern.paths[0].elements[1] {
            #expect(edge.labels?.count == 2)
            #expect(edge.labels?.contains("KNOWS") == true)
            #expect(edge.labels?.contains("WORKS_WITH") == true)
        }
    }

    @Test("Any label (wildcard)")
    func testAnyLabel() throws {
        // MATCH (a)-[r]->(b) - no label specified means any label

        let anyEdge = EdgePattern(variable: "r", direction: .outgoing)

        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(NodePattern(variable: "a")),
                .edge(anyEdge),
                .node(NodePattern(variable: "b"))
            ])
        ])

        let source = GraphTableSource(graphName: "graph", matchPattern: pattern)

        if case .edge(let edge) = source.matchPattern.paths[0].elements[1] {
            #expect(edge.labels == nil)  // nil means any label
        }
    }

    // MARK: - Path Alternation Tests

    @Test("Path alternation")
    func testPathAlternation() throws {
        // MATCH (a)((-[:A]->)|(-[:B]->))(b)
        // Alternative paths between a and b

        let pathA = PathPattern(elements: [
            .edge(EdgePattern(labels: ["A"], direction: .outgoing))
        ])

        let pathB = PathPattern(elements: [
            .edge(EdgePattern(labels: ["B"], direction: .outgoing))
        ])

        let alternation = PathElement.alternation([pathA, pathB])

        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(NodePattern(variable: "a")),
                alternation,
                .node(NodePattern(variable: "b"))
            ])
        ])

        let source = GraphTableSource(graphName: "graph", matchPattern: pattern)

        if case .alternation(let alternatives) = source.matchPattern.paths[0].elements[1] {
            #expect(alternatives.count == 2)
        } else {
            Issue.record("Expected alternation element")
        }
    }

    // MARK: - SQL Integration Tests

    @Test("GraphTableSource toSQL output")
    func testToSQLOutput() throws {
        let source = GraphTableSource.match(
            graph: "social",
            from: NodePattern(variable: "a", labels: ["Person"]),
            via: EdgePattern(labels: ["KNOWS"], direction: .outgoing),
            to: NodePattern(variable: "b", labels: ["Person"])
        )
        .returning([
            (.column(ColumnRef(table: "a", column: "name")), "source_name"),
            (.column(ColumnRef(table: "b", column: "name")), "target_name")
        ])

        let sql = source.toSQL()

        #expect(sql.contains("GRAPH_TABLE(social"))
        #expect(sql.contains("MATCH"))
        #expect(sql.contains("COLUMNS"))
    }

    @Test("GraphTableSource in SelectQuery")
    func testGraphTableInSelectQuery() throws {
        let source = GraphTableSource.match(
            graph: "social",
            from: NodePattern(variable: "a", labels: ["Person"]),
            via: EdgePattern(labels: ["KNOWS"], direction: .outgoing),
            to: NodePattern(variable: "b", labels: ["Person"])
        )

        let query = SelectQuery(
            projection: .all,
            source: .graphTable(source),
            filter: .greaterThan(.column(ColumnRef(column: "age")), .literal(.int(18)))
        )

        #expect(query.projection == .all)
        #expect(query.filter != nil)

        if case .graphTable(let gt) = query.source {
            #expect(gt.graphName == "social")
        } else {
            Issue.record("Expected graphTable source")
        }
    }

    // MARK: - Validation Tests

    @Test("Validation: valid pattern passes")
    func testValidationValidPattern() throws {
        let source = GraphTableSource.match(
            graph: "social",
            from: NodePattern(variable: "a"),
            via: EdgePattern(direction: .outgoing),
            to: NodePattern(variable: "b")
        )

        let errors = source.validate()
        #expect(errors.isEmpty)
    }

    @Test("Validation: undefined variable in column")
    func testValidationUndefinedVariable() throws {
        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(NodePattern(variable: "a"))
            ])
        ])

        let source = GraphTableSource(
            graphName: "social",
            matchPattern: pattern,
            columns: [
                GraphTableColumn(expression: .column(ColumnRef(column: "undefined")), alias: "col")
            ]
        )

        let errors = source.validate()
        #expect(errors.contains(where: {
            if case .undefinedVariable(_, _) = $0 { return true }
            return false
        }))
    }

    @Test("Validation: empty pattern")
    func testValidationEmptyPattern() throws {
        let pattern = MatchPattern(paths: [])
        let source = GraphTableSource(graphName: "social", matchPattern: pattern)

        let errors = source.validate()
        #expect(errors.contains(.emptyPattern))
    }
}
