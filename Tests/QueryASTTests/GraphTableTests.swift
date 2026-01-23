/// GraphTableTests.swift
/// Comprehensive tests for SQL/PGQ GRAPH_TABLE types

import Testing
@testable import QueryAST

// MARK: - GraphTableSource Tests

@Suite("GraphTableSource Tests")
struct GraphTableSourceTests {

    @Test("GraphTableSource basic construction")
    func testBasicConstruction() throws {
        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(NodePattern(variable: "a", labels: ["Person"])),
                .edge(EdgePattern(labels: ["KNOWS"], direction: .outgoing)),
                .node(NodePattern(variable: "b", labels: ["Person"]))
            ])
        ])

        let source = GraphTableSource(
            graphName: "social",
            matchPattern: pattern
        )

        #expect(source.graphName == "social")
        #expect(source.matchPattern.paths.count == 1)
        #expect(source.columns == nil)
    }

    @Test("GraphTableSource with columns")
    func testWithColumns() throws {
        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(NodePattern(variable: "a")),
                .edge(EdgePattern(direction: .outgoing)),
                .node(NodePattern(variable: "b"))
            ])
        ])

        let source = GraphTableSource(
            graphName: "social",
            matchPattern: pattern,
            columns: [
                GraphTableColumn(expression: .column(ColumnRef(table: "a", column: "name")), alias: "source_name"),
                GraphTableColumn(expression: .column(ColumnRef(table: "b", column: "name")), alias: "target_name")
            ]
        )

        #expect(source.columns?.count == 2)
        #expect(source.columns?[0].alias == "source_name")
        #expect(source.columns?[1].alias == "target_name")
    }

    @Test("GraphTableSource.match builder")
    func testMatchBuilder() throws {
        let source = GraphTableSource.match(
            graph: "social",
            from: NodePattern(variable: "a", labels: ["Person"]),
            via: EdgePattern(labels: ["KNOWS"], direction: .outgoing),
            to: NodePattern(variable: "b", labels: ["Person"])
        )

        #expect(source.graphName == "social")
        #expect(source.matchPattern.paths.count == 1)
        #expect(source.matchPattern.paths[0].elements.count == 3)
    }

    @Test("GraphTableSource.returning builder")
    func testReturningBuilder() throws {
        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(NodePattern(variable: "a")),
                .edge(EdgePattern(direction: .outgoing)),
                .node(NodePattern(variable: "b"))
            ])
        ])

        let source = GraphTableSource(graphName: "social", matchPattern: pattern)
            .returning([
                (.column(ColumnRef(table: "a", column: "id")), "source_id"),
                (.column(ColumnRef(table: "b", column: "id")), "target_id")
            ])

        #expect(source.columns?.count == 2)
    }

    @Test("GraphTableSource definedVariables")
    func testDefinedVariables() throws {
        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(NodePattern(variable: "person")),
                .edge(EdgePattern(variable: "rel", direction: .outgoing)),
                .node(NodePattern(variable: "friend"))
            ])
        ])

        let source = GraphTableSource(graphName: "social", matchPattern: pattern)
        let vars = source.definedVariables

        #expect(vars.contains("person"))
        #expect(vars.contains("rel"))
        #expect(vars.contains("friend"))
        #expect(vars.count == 3)
    }

    @Test("GraphTableSource exposedColumns")
    func testExposedColumns() throws {
        let pattern = MatchPattern(paths: [
            PathPattern(elements: [.node(NodePattern(variable: "a"))])
        ])

        let source = GraphTableSource(
            graphName: "social",
            matchPattern: pattern,
            columns: [
                GraphTableColumn(expression: .literal(.int(1)), alias: "col1"),
                GraphTableColumn(expression: .literal(.int(2)), alias: "col2")
            ]
        )

        let exposed = source.exposedColumns
        #expect(exposed == ["col1", "col2"])
    }

    @Test("GraphTableSource validate - valid pattern")
    func testValidateValidPattern() throws {
        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(NodePattern(variable: "a")),
                .edge(EdgePattern(direction: .outgoing)),
                .node(NodePattern(variable: "b"))
            ])
        ])

        let source = GraphTableSource(graphName: "social", matchPattern: pattern)
        let errors = source.validate()

        #expect(errors.isEmpty)
    }

    @Test("GraphTableSource validate - undefined variable in column")
    func testValidateUndefinedVariable() throws {
        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(NodePattern(variable: "a"))
            ])
        ])

        let source = GraphTableSource(
            graphName: "social",
            matchPattern: pattern,
            columns: [
                GraphTableColumn(expression: .column(ColumnRef(column: "undefined_var")), alias: "col1")
            ]
        )

        let errors = source.validate()
        #expect(errors.contains(where: {
            if case .undefinedVariable(let v, _) = $0 {
                return v == "undefined_var"
            }
            return false
        }))
    }

    @Test("GraphTableSource toSQL")
    func testToSQL() throws {
        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(NodePattern(variable: "a", labels: ["Person"])),
                .edge(EdgePattern(labels: ["KNOWS"], direction: .outgoing)),
                .node(NodePattern(variable: "b", labels: ["Person"]))
            ])
        ])

        let source = GraphTableSource(
            graphName: "social",
            matchPattern: pattern,
            columns: [
                GraphTableColumn(expression: .column(ColumnRef(column: "a")), alias: "source")
            ]
        )

        let sql = source.toSQL()
        #expect(sql.contains("GRAPH_TABLE(social"))
        #expect(sql.contains("MATCH"))
        #expect(sql.contains("COLUMNS"))
    }
}

// MARK: - Shortest Path Tests

@Suite("GraphTableSource Shortest Path Tests")
struct GraphTableSourceShortestPathTests {

    @Test("shortestPath builder")
    func testShortestPathBuilder() throws {
        let source = GraphTableSource.shortestPath(
            graph: "social",
            from: NodePattern(variable: "start", labels: ["Person"]),
            via: "KNOWS",
            to: NodePattern(variable: "end", labels: ["Person"]),
            maxHops: 5
        )

        #expect(source.graphName == "social")
        #expect(source.matchPattern.paths.count == 1)
        #expect(source.matchPattern.paths[0].mode == .anyShortest)
    }

    @Test("shortestPath without maxHops")
    func testShortestPathNoMaxHops() throws {
        let source = GraphTableSource.shortestPath(
            graph: "social",
            from: NodePattern(variable: "start"),
            via: "FOLLOWS",
            to: NodePattern(variable: "end")
        )

        #expect(source.matchPattern.paths[0].mode == .anyShortest)
    }

    @Test("allShortestPaths builder")
    func testAllShortestPathsBuilder() throws {
        let source = GraphTableSource.allShortestPaths(
            graph: "social",
            from: NodePattern(variable: "start"),
            via: "KNOWS",
            to: NodePattern(variable: "end")
        )

        #expect(source.matchPattern.paths[0].mode == .allShortest)
    }

    @Test("reachable builder")
    func testReachableBuilder() throws {
        let source = GraphTableSource.reachable(
            graph: "social",
            from: NodePattern(variable: "start", labels: ["Person"]),
            via: "FOLLOWS",
            maxDepth: 3
        )

        #expect(source.graphName == "social")
        #expect(source.matchPattern.paths[0].mode == .simple)
    }

    @Test("reachable without maxDepth")
    func testReachableNoMaxDepth() throws {
        let source = GraphTableSource.reachable(
            graph: "hierarchy",
            from: NodePattern(variable: "root"),
            via: "HAS_CHILD"
        )

        #expect(source.matchPattern.paths[0].mode == .simple)
    }
}

// MARK: - Common Pattern Templates Tests

@Suite("GraphTableSource Pattern Templates Tests")
struct GraphTableSourcePatternTemplatesTests {

    @Test("friendOfFriend pattern")
    func testFriendOfFriendPattern() throws {
        let source = GraphTableSource.friendOfFriend(
            graph: "social",
            person: "alice123",
            friendEdge: "FRIEND"
        )

        #expect(source.graphName == "social")
        #expect(source.columns?.count == 2)
        #expect(source.columns?[0].alias == "friend_of_friend")
        #expect(source.columns?[1].alias == "via")
    }

    @Test("friendOfFriend with default edge")
    func testFriendOfFriendDefaultEdge() throws {
        let source = GraphTableSource.friendOfFriend(
            graph: "social",
            person: "user1"
        )

        #expect(source.graphName == "social")
    }

    @Test("triangle pattern")
    func testTrianglePattern() throws {
        let source = GraphTableSource.triangle(
            graph: "social",
            edgeLabel: "KNOWS"
        )

        #expect(source.graphName == "social")
        #expect(source.columns?.count == 3)
        #expect(source.columns?[0].alias == "node1")
        #expect(source.columns?[1].alias == "node2")
        #expect(source.columns?[2].alias == "node3")
    }
}

// MARK: - GraphTableValidationError Tests

@Suite("GraphTableValidationError Tests")
struct GraphTableValidationErrorTests {

    @Test("patternError wrapping")
    func testPatternErrorWrapping() throws {
        let patternError = PatternValidationError.pathMustStartWithNode(path: 0)
        let graphError = GraphTableValidationError.patternError(patternError)

        if case .patternError(let inner) = graphError {
            #expect(inner == patternError)
        } else {
            Issue.record("Expected patternError")
        }
    }

    @Test("undefinedVariable error")
    func testUndefinedVariableError() throws {
        let error = GraphTableValidationError.undefinedVariable("x", in: "col1")

        if case .undefinedVariable(let v, let col) = error {
            #expect(v == "x")
            #expect(col == "col1")
        }
    }

    @Test("duplicateColumnAlias error")
    func testDuplicateColumnAliasError() throws {
        let error = GraphTableValidationError.duplicateColumnAlias("name")

        if case .duplicateColumnAlias(let alias) = error {
            #expect(alias == "name")
        }
    }

    @Test("emptyPattern error")
    func testEmptyPatternError() throws {
        let error = GraphTableValidationError.emptyPattern
        #expect(error == .emptyPattern)
    }
}

// MARK: - GraphTableColumn Tests

@Suite("GraphTableColumn Tests")
struct GraphTableColumnTests {

    @Test("GraphTableColumn construction")
    func testConstruction() throws {
        let column = GraphTableColumn(
            expression: .column(ColumnRef(table: "a", column: "name")),
            alias: "person_name"
        )

        #expect(column.alias == "person_name")
        if case .column(let ref) = column.expression {
            #expect(ref.table == "a")
            #expect(ref.column == "name")
        }
    }

    @Test("GraphTableColumn equality")
    func testEquality() throws {
        let col1 = GraphTableColumn(expression: .literal(.int(1)), alias: "x")
        let col2 = GraphTableColumn(expression: .literal(.int(1)), alias: "x")
        let col3 = GraphTableColumn(expression: .literal(.int(2)), alias: "x")
        let col4 = GraphTableColumn(expression: .literal(.int(1)), alias: "y")

        #expect(col1 == col2)
        #expect(col1 != col3)
        #expect(col1 != col4)
    }

    @Test("GraphTableColumn hashable")
    func testHashable() throws {
        var set = Set<GraphTableColumn>()
        set.insert(GraphTableColumn(expression: .literal(.int(1)), alias: "a"))
        set.insert(GraphTableColumn(expression: .literal(.int(2)), alias: "b"))
        set.insert(GraphTableColumn(expression: .literal(.int(1)), alias: "a"))

        #expect(set.count == 2)
    }
}
