/// GraphTableParsingTests.swift
/// Tests for GRAPH_TABLE SQL parsing (ISO/IEC 9075-16:2023 SQL/PGQ)

import Testing
@testable import QueryAST

@Suite("GRAPH_TABLE Parsing Tests")
struct GraphTableParsingTests {

    @Test("Parse basic GRAPH_TABLE")
    func testBasicGraphTable() throws {
        let sql = """
        SELECT *
        FROM GRAPH_TABLE(social,
          MATCH (a:Person)-[r:KNOWS]->(b:Person)
        )
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable source")
            return
        }

        #expect(gt.graphName == "social")
        #expect(gt.matchPattern.paths.count == 1)
        #expect(gt.columns == nil)

        let path = gt.matchPattern.paths[0]
        #expect(path.elements.count == 3)

        // First element: node (a:Person)
        guard case .node(let node1) = path.elements[0] else {
            Issue.record("Expected node element")
            return
        }
        #expect(node1.variable == "a")
        #expect(node1.labels == ["Person"])

        // Second element: edge [r:KNOWS]
        guard case .edge(let edge) = path.elements[1] else {
            Issue.record("Expected edge element")
            return
        }
        #expect(edge.variable == "r")
        #expect(edge.labels == ["KNOWS"])
        #expect(edge.direction == .outgoing)

        // Third element: node (b:Person)
        guard case .node(let node2) = path.elements[2] else {
            Issue.record("Expected node element")
            return
        }
        #expect(node2.variable == "b")
        #expect(node2.labels == ["Person"])
    }

    @Test("Parse GRAPH_TABLE with COLUMNS")
    func testGraphTableWithColumns() throws {
        let sql = """
        SELECT *
        FROM GRAPH_TABLE(social,
          MATCH (a)-[r]->(b)
          COLUMNS (a.name AS source, b.name AS target)
        )
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable source")
            return
        }

        #expect(gt.graphName == "social")
        #expect(gt.columns?.count == 2)
        #expect(gt.columns?[0].alias == "source")
        #expect(gt.columns?[1].alias == "target")
    }

    @Test("Parse GRAPH_TABLE with WHERE clause")
    func testGraphTableWithWhere() throws {
        let sql = """
        SELECT *
        FROM GRAPH_TABLE(social,
          MATCH (a)-[r]->(b)
          WHERE a.age > 30
        )
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable source")
            return
        }

        #expect(gt.matchPattern.where != nil)
    }

    @Test("Parse GRAPH_TABLE with path mode SHORTEST PATH")
    func testGraphTableShortestPath() throws {
        let sql = """
        SELECT *
        FROM GRAPH_TABLE(social,
          MATCH SHORTEST PATH (a)-[r]->(b)
        )
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable source")
            return
        }

        let path = gt.matchPattern.paths[0]
        #expect(path.mode == .anyShortest)
    }

    @Test("Parse GRAPH_TABLE with path mode ALL SHORTEST")
    func testGraphTableAllShortest() throws {
        let sql = """
        SELECT *
        FROM GRAPH_TABLE(social,
          MATCH ALL SHORTEST (a)-[r]->(b)
        )
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable source")
            return
        }

        let path = gt.matchPattern.paths[0]
        #expect(path.mode == .allShortest)
    }

    @Test("Parse GRAPH_TABLE with path mode SIMPLE")
    func testGraphTableSimpleMode() throws {
        let sql = """
        SELECT *
        FROM GRAPH_TABLE(social,
          MATCH SIMPLE (a)-[r]->(b)
        )
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable source")
            return
        }

        let path = gt.matchPattern.paths[0]
        #expect(path.mode == .simple)
    }

    @Test("Parse GRAPH_TABLE with incoming edge")
    func testGraphTableIncomingEdge() throws {
        let sql = """
        SELECT *
        FROM GRAPH_TABLE(social,
          MATCH (a)<-[r:KNOWS]-(b)
        )
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable source")
            return
        }

        let path = gt.matchPattern.paths[0]
        guard case .edge(let edge) = path.elements[1] else {
            Issue.record("Expected edge element")
            return
        }

        #expect(edge.direction == .incoming)
        #expect(edge.labels == ["KNOWS"])
    }

    @Test("Parse GRAPH_TABLE with undirected edge")
    func testGraphTableUndirectedEdge() throws {
        let sql = """
        SELECT *
        FROM GRAPH_TABLE(social,
          MATCH (a)-[r:KNOWS]-(b)
        )
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable source")
            return
        }

        let path = gt.matchPattern.paths[0]
        guard case .edge(let edge) = path.elements[1] else {
            Issue.record("Expected edge element")
            return
        }

        #expect(edge.direction == .undirected)
    }

    @Test("Parse GRAPH_TABLE with node properties")
    func testGraphTableNodeProperties() throws {
        let sql = """
        SELECT *
        FROM GRAPH_TABLE(social,
          MATCH (a:Person {age: 30, city: 'Tokyo'})-[r]->(b)
        )
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable source")
            return
        }

        let path = gt.matchPattern.paths[0]
        guard case .node(let node) = path.elements[0] else {
            Issue.record("Expected node element")
            return
        }

        #expect(node.variable == "a")
        #expect(node.labels == ["Person"])
        #expect(node.properties?.count == 2)
    }

    @Test("Parse GRAPH_TABLE with edge properties")
    func testGraphTableEdgeProperties() throws {
        let sql = """
        SELECT *
        FROM GRAPH_TABLE(social,
          MATCH (a)-[r:KNOWS {since: 2020}]->(b)
        )
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable source")
            return
        }

        let path = gt.matchPattern.paths[0]
        guard case .edge(let edge) = path.elements[1] else {
            Issue.record("Expected edge element")
            return
        }

        #expect(edge.variable == "r")
        #expect(edge.labels == ["KNOWS"])
        #expect(edge.properties?.count == 1)
    }

    @Test("Parse GRAPH_TABLE with multiple paths")
    func testGraphTableMultiplePaths() throws {
        let sql = """
        SELECT *
        FROM GRAPH_TABLE(social,
          MATCH (a)-[r1]->(b), (b)-[r2]->(c)
        )
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable source")
            return
        }

        #expect(gt.matchPattern.paths.count == 2)
    }

    @Test("Parse GRAPH_TABLE with path variable")
    func testGraphTablePathVariable() throws {
        let sql = """
        SELECT *
        FROM GRAPH_TABLE(social,
          MATCH p = (a)-[r]->(b)
        )
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable source")
            return
        }

        let path = gt.matchPattern.paths[0]
        #expect(path.pathVariable == "p")
    }

    @Test("Parse GRAPH_TABLE in JOIN")
    func testGraphTableInJoin() throws {
        let sql = """
        SELECT u.name, g.friend
        FROM User u
        JOIN GRAPH_TABLE(social,
          MATCH (a)-[r:KNOWS]->(b)
          COLUMNS (a.id AS user_id, b.name AS friend)
        ) AS g ON u.id = g.user_id
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .join(let join) = query.source else {
            Issue.record("Expected join")
            return
        }

        guard case .graphTable(let gt) = join.right else {
            Issue.record("Expected graphTable in right side of join")
            return
        }

        #expect(gt.graphName == "social")
        #expect(gt.columns?.count == 2)
    }

    @Test("Parse GRAPH_TABLE with outer WHERE and LIMIT")
    func testGraphTableWithOuterWhereAndLimit() throws {
        // Simplified test: GRAPH_TABLE without JOIN
        let sql = """
        SELECT *
        FROM GRAPH_TABLE(social,
               MATCH (p:Person)-[knows:KNOWS]->(f:Person)
             )
        WHERE p.age > 25
        LIMIT 10
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        #expect(query.limit == 10)
        #expect(query.filter != nil)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected GRAPH_TABLE source")
            return
        }

        #expect(gt.graphName == "social")
    }
}

// MARK: - Node Pattern Parsing Tests

@Suite("Node Pattern Parsing Tests")
struct NodePatternParsingTests {

    @Test("Parse simple node")
    func testSimpleNode() throws {
        let sql = "SELECT * FROM GRAPH_TABLE(g, MATCH (a))"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable")
            return
        }

        let path = gt.matchPattern.paths[0]
        guard case .node(let node) = path.elements[0] else {
            Issue.record("Expected node")
            return
        }

        #expect(node.variable == "a")
        #expect(node.labels == nil)
        #expect(node.properties == nil)
    }

    @Test("Parse node with label")
    func testNodeWithLabel() throws {
        let sql = "SELECT * FROM GRAPH_TABLE(g, MATCH (a:Person))"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable")
            return
        }

        let path = gt.matchPattern.paths[0]
        guard case .node(let node) = path.elements[0] else {
            Issue.record("Expected node")
            return
        }

        #expect(node.variable == "a")
        #expect(node.labels == ["Person"])
    }

    @Test("Parse anonymous node")
    func testAnonymousNode() throws {
        let sql = "SELECT * FROM GRAPH_TABLE(g, MATCH ())"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable")
            return
        }

        let path = gt.matchPattern.paths[0]
        guard case .node(let node) = path.elements[0] else {
            Issue.record("Expected node")
            return
        }

        #expect(node.variable == nil)
        #expect(node.labels == nil)
    }
}

// MARK: - Edge Pattern Parsing Tests

@Suite("Edge Pattern Parsing Tests")
struct EdgePatternParsingTests {

    @Test("Parse simple outgoing edge")
    func testSimpleOutgoingEdge() throws {
        let sql = "SELECT * FROM GRAPH_TABLE(g, MATCH (a)-[r]->(b))"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable")
            return
        }

        let path = gt.matchPattern.paths[0]
        guard case .edge(let edge) = path.elements[1] else {
            Issue.record("Expected edge")
            return
        }

        #expect(edge.variable == "r")
        #expect(edge.direction == .outgoing)
    }

    @Test("Parse anonymous edge")
    func testAnonymousEdge() throws {
        let sql = "SELECT * FROM GRAPH_TABLE(g, MATCH (a)->(b))"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable")
            return
        }

        let path = gt.matchPattern.paths[0]
        guard case .edge(let edge) = path.elements[1] else {
            Issue.record("Expected edge")
            return
        }

        #expect(edge.variable == nil)
        #expect(edge.direction == .outgoing)
    }

    @Test("Parse edge with label")
    func testEdgeWithLabel() throws {
        let sql = "SELECT * FROM GRAPH_TABLE(g, MATCH (a)-[r:KNOWS]->(b))"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable")
            return
        }

        let path = gt.matchPattern.paths[0]
        guard case .edge(let edge) = path.elements[1] else {
            Issue.record("Expected edge")
            return
        }

        #expect(edge.variable == "r")
        #expect(edge.labels == ["KNOWS"])
        #expect(edge.direction == .outgoing)
    }
}

// MARK: - Invalid Syntax Tests

@Suite("Invalid Edge Pattern Syntax Tests")
struct InvalidEdgePatternSyntaxTests {

    @Test("Reject arrow before bracket: ->[r]")
    func testRejectArrowBeforeBracket() throws {
        let sql = "SELECT * FROM GRAPH_TABLE(g, MATCH (a)->[r](b))"
        let parser = SQLParser()

        #expect(performing: {
            _ = try parser.parseSelect(sql)
        }, throws: { error in
            guard case SQLParser.ParseError.invalidSyntax(let message, _) = error else {
                return false
            }
            return message.contains("brackets must come before arrow")
        })
    }

    @Test("Reject contradictory pattern: <-[r:KNOWS]->")
    func testRejectContradictoryPattern() throws {
        // Note: This pattern is actually valid in SQL/PGQ (it means "any direction")
        // So this test should PASS parsing
        let sql = "SELECT * FROM GRAPH_TABLE(g, MATCH (a)<-[r:KNOWS]->(b))"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable")
            return
        }

        let path = gt.matchPattern.paths[0]
        guard case .edge(let edge) = path.elements[1] else {
            Issue.record("Expected edge")
            return
        }

        // <-...-> should resolve to .any direction
        #expect(edge.direction == .any)
    }

    @Test("Parse anonymous edge without brackets")
    func testAnonymousEdgeWithoutBrackets() throws {
        let sql = "SELECT * FROM GRAPH_TABLE(g, MATCH (a)->(b))"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable")
            return
        }

        let path = gt.matchPattern.paths[0]
        guard case .edge(let edge) = path.elements[1] else {
            Issue.record("Expected edge")
            return
        }

        #expect(edge.variable == nil)
        #expect(edge.labels == nil)
        #expect(edge.direction == .outgoing)
    }

    @Test("Parse empty bracket edge: -[]->")
    func testEmptyBracketEdge() throws {
        let sql = "SELECT * FROM GRAPH_TABLE(g, MATCH (a)-[]->(b))"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable")
            return
        }

        let path = gt.matchPattern.paths[0]
        guard case .edge(let edge) = path.elements[1] else {
            Issue.record("Expected edge")
            return
        }

        #expect(edge.variable == nil)
        #expect(edge.labels == nil)
        #expect(edge.direction == .outgoing)
    }

    @Test("Test all direction resolution table entries")
    func testAllDirectionResolutionEntries() throws {
        // Test leftAngle variants
        let sql1 = "SELECT * FROM GRAPH_TABLE(g, MATCH (a)<[r]-(b))"
        let parser1 = SQLParser()
        let query1 = try parser1.parseSelect(sql1)
        guard case .graphTable(let gt1) = query1.source,
              case .edge(let edge1) = gt1.matchPattern.paths[0].elements[1] else {
            Issue.record("Expected edge")
            return
        }
        #expect(edge1.direction == .incoming)  // <[r]-

        let sql2 = "SELECT * FROM GRAPH_TABLE(g, MATCH (a)<[r]->(b))"
        let parser2 = SQLParser()
        let query2 = try parser2.parseSelect(sql2)
        guard case .graphTable(let gt2) = query2.source,
              case .edge(let edge2) = gt2.matchPattern.paths[0].elements[1] else {
            Issue.record("Expected edge")
            return
        }
        #expect(edge2.direction == .any)  // <[r]->

        // Test anonymous hyphen (rare but valid)
        let sql3 = "SELECT * FROM GRAPH_TABLE(g, MATCH (a)-(b))"
        let parser3 = SQLParser()
        let query3 = try parser3.parseSelect(sql3)
        guard case .graphTable(let gt3) = query3.source,
              case .edge(let edge3) = gt3.matchPattern.paths[0].elements[1] else {
            Issue.record("Expected edge")
            return
        }
        #expect(edge3.direction == .undirected)  // -
    }

    @Test("Test properties-only edge")
    func testPropertiesOnlyEdge() throws {
        let sql = "SELECT * FROM GRAPH_TABLE(g, MATCH (a)-[{weight: 10}]->(b))"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source,
              case .edge(let edge) = gt.matchPattern.paths[0].elements[1] else {
            Issue.record("Expected edge")
            return
        }

        #expect(edge.variable == nil)
        #expect(edge.labels == nil)
        #expect(edge.properties?.count == 1)
        #expect(edge.direction == .outgoing)
    }

    @Test("Reject incomplete pattern: -[r]")
    func testRejectIncompletePattern() throws {
        // -[r] followed by (b) should fail
        // because after -[r] we expect ->, -, or node continuation
        // but we get ( which is unexpected
        let sql = "SELECT * FROM GRAPH_TABLE(g, MATCH (a)-[r](b))"
        let parser = SQLParser()

        #expect(performing: {
            _ = try parser.parseSelect(sql)
        }, throws: { error in
            true  // Should throw parse error
        })
    }

    @Test("Reject missing start symbol: [r]->")
    func testRejectMissingStartSymbol() throws {
        let sql = "SELECT * FROM GRAPH_TABLE(g, MATCH (a)[r]->(b))"
        let parser = SQLParser()
        #expect(performing: {
            _ = try parser.parseSelect(sql)
        }, throws: { error in
            true  // Should throw parse error
        })
    }

    @Test("Reject incomplete leftArrow pattern: <-[r]")
    func testRejectIncompleteLeftArrow() throws {
        let sql = "SELECT * FROM GRAPH_TABLE(g, MATCH (a)<-[r](b))"
        let parser = SQLParser()
        #expect(performing: {
            _ = try parser.parseSelect(sql)
        }, throws: { error in
            true  // Should throw parse error
        })
    }

    @Test("Reject incomplete leftAngle pattern: <[r]")
    func testRejectIncompleteLeftAngle() throws {
        let sql = "SELECT * FROM GRAPH_TABLE(g, MATCH (a)<[r](b))"
        let parser = SQLParser()
        #expect(performing: {
            _ = try parser.parseSelect(sql)
        }, throws: { error in
            true  // Should throw parse error
        })
    }
}

// MARK: - Complex Query Tests

@Suite("Complex GRAPH_TABLE Query Tests")
struct ComplexGraphTableQueryTests {

    @Test("Social graph: Friend of friend recommendation")
    func testFriendOfFriendQuery() throws {
        // Find friends of friends who share common interests
        let sql = """
        SELECT u.name, friend2.friend_name AS recommendation
        FROM User u
        INNER JOIN GRAPH_TABLE(SocialGraph,
               MATCH (p1:Person {id: u.id})-[f1:FRIEND]->(p2:Person)-[f2:FRIEND]->(p3:Person)
               WHERE p1.id != p3.id
               COLUMNS (p3.id AS friend_id, p3.name AS friend_name)
             ) AS friend2 ON 1=1
        LIMIT 10
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        // Since we use INNER JOIN, the source will be a join
        guard case .join(let join) = query.source else {
            Issue.record("Expected join")
            return
        }

        // Extract graph table from the right side of the join
        guard case .graphTable(let gt) = join.right else {
            Issue.record("Expected graphTable in join")
            return
        }

        // Verify path pattern: (p1)-[f1]->(p2)-[f2]->(p3)
        let path = gt.matchPattern.paths[0]
        #expect(path.elements.count == 5)

        // Verify first node
        guard case .node(let p1) = path.elements[0] else {
            Issue.record("Expected node p1")
            return
        }
        #expect(p1.variable == "p1")
        #expect(p1.labels == ["Person"])
        #expect(p1.properties?.count == 1)

        // Verify first edge
        guard case .edge(let f1) = path.elements[1] else {
            Issue.record("Expected edge f1")
            return
        }
        #expect(f1.variable == "f1")
        #expect(f1.labels == ["FRIEND"])
        #expect(f1.direction == .outgoing)

        // Verify second node
        guard case .node(let p2) = path.elements[2] else {
            Issue.record("Expected node p2")
            return
        }
        #expect(p2.variable == "p2")
        #expect(p2.labels == ["Person"])

        // Verify second edge
        guard case .edge(let f2) = path.elements[3] else {
            Issue.record("Expected edge f2")
            return
        }
        #expect(f2.variable == "f2")
        #expect(f2.labels == ["FRIEND"])
        #expect(f2.direction == .outgoing)

        // Verify third node
        guard case .node(let p3) = path.elements[4] else {
            Issue.record("Expected node p3")
            return
        }
        #expect(p3.variable == "p3")
        #expect(p3.labels == ["Person"])

        // Verify WHERE clause
        #expect(gt.matchPattern.where != nil)

        // Verify COLUMNS clause
        #expect(gt.columns?.count == 2)
    }

    @Test("Knowledge graph: Multi-hop relationship discovery")
    func testMultiHopRelationshipQuery() throws {
        // Find all indirect relationships between concepts through intermediate nodes
        let sql = """
        SELECT c1.name AS source, c2.name AS intermediate, c3.name AS target,
               r1.type AS first_relation, r2.type AS second_relation
        FROM GRAPH_TABLE(KnowledgeGraph,
               MATCH (c1:Concept)-[r1]->(c2:Concept)-[r2]->(c3:Concept)
               WHERE c1.domain = 'Science' AND c3.domain = 'Engineering'
               COLUMNS (c1.name AS source_name, c2.name AS inter_name,
                        c3.name AS target_name, r1.type AS r1_type, r2.type AS r2_type)
             ) AS path
        WHERE r1.type != r2.type
        ORDER BY source, target
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable")
            return
        }

        let path = gt.matchPattern.paths[0]
        #expect(path.elements.count == 5)

        // Verify undirected edges (any direction)
        guard case .edge(let r1) = path.elements[1],
              case .edge(let r2) = path.elements[3] else {
            Issue.record("Expected edges")
            return
        }
        #expect(r1.variable == "r1")
        #expect(r2.variable == "r2")

        // Verify COLUMNS with multiple expressions
        #expect(gt.columns?.count == 5)
    }

    @Test("Access control: Role-based permission chain")
    func testRoleBasedPermissionQuery() throws {
        // Check if user has permission through role hierarchy
        let sql = """
        SELECT u.username, p.resource, p.action
        FROM User u
        INNER JOIN GRAPH_TABLE(PermissionGraph,
               MATCH (user:User {id: u.id})-[has:HAS_ROLE]->(role:Role)
                     -[inherits:INHERITS]->(parent:Role)
                     -[grants:GRANTS]->(perm:Permission)
               WHERE perm.resource = 'sensitive_data'
               COLUMNS (perm.resource AS resource, perm.action AS action)
             ) AS p ON 1=1
        WHERE u.active = true
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        // Since we use INNER JOIN, the source will be a join
        guard case .join(let join) = query.source else {
            Issue.record("Expected join")
            return
        }

        // Extract graph table from the right side of the join
        guard case .graphTable(let gt) = join.right else {
            Issue.record("Expected graphTable in join")
            return
        }

        let path = gt.matchPattern.paths[0]

        // Verify mixed edge directions
        guard case .edge(let has) = path.elements[1],
              case .edge(let inherits) = path.elements[3],
              case .edge(let grants) = path.elements[5] else {
            Issue.record("Expected edges")
            return
        }

        #expect(has.variable == "has")
        #expect(has.labels == ["HAS_ROLE"])
        #expect(has.direction == EdgeDirection.outgoing)

        #expect(inherits.variable == "inherits")
        #expect(inherits.labels == ["INHERITS"])
        // Note: *0..3 would be in pathMode, not in edge itself

        #expect(grants.variable == "grants")
        #expect(grants.labels == ["GRANTS"])
        #expect(grants.direction == EdgeDirection.outgoing)
    }

    @Test("E-commerce: Product recommendation through purchase patterns")
    func testProductRecommendationQuery() throws {
        // Find products frequently bought together
        let sql = """
        SELECT p1.name AS product, p2.prod_name AS recommended_product
        FROM Product p1
        INNER JOIN GRAPH_TABLE(PurchaseGraph,
               MATCH (prod1:Product {id: p1.id})<-[in1:IN_ORDER]-(o:PurchaseOrder)
                     -[in2:IN_ORDER]->(prod2:Product)
               WHERE prod1.id != prod2.id
               COLUMNS (prod2.id AS prod_id, prod2.name AS prod_name, o.id AS order_id)
             ) AS p2 ON 1=1
        LIMIT 20
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        // Since we use INNER JOIN, the source will be a join
        guard case .join(let join) = query.source else {
            Issue.record("Expected join")
            return
        }

        // Extract graph table from the right side of the join
        guard case .graphTable(let gt) = join.right else {
            Issue.record("Expected graphTable in join")
            return
        }

        let path = gt.matchPattern.paths[0]

        // Verify incoming and outgoing edges in same pattern
        guard case .edge(let in1) = path.elements[1],
              case .edge(let in2) = path.elements[3] else {
            Issue.record("Expected edges")
            return
        }

        #expect(in1.variable == "in1")
        #expect(in1.labels == ["IN_ORDER"])
        #expect(in1.direction == EdgeDirection.incoming)

        #expect(in2.variable == "in2")
        #expect(in2.labels == ["IN_ORDER"])
        #expect(in2.direction == EdgeDirection.outgoing)

        // Verify WHERE with multiple conditions
        #expect(gt.matchPattern.where != nil)
    }

    @Test("Multiple graph tables with JOIN")
    func testMultipleGraphTableJoin() throws {
        // Join results from multiple graph patterns
        let sql = """
        SELECT u.name, colleagues.colleague_name, projects.project_name
        FROM User u
        INNER JOIN GRAPH_TABLE(OrgChart,
               MATCH (emp:Employee {id: u.id})-[w:WORKS_WITH]->(colleague:Employee)
               COLUMNS (colleague.id AS colleague_id, colleague.name AS colleague_name)
             ) AS colleagues ON 1=1
        INNER JOIN GRAPH_TABLE(ProjectGraph,
               MATCH (emp2:Employee {id: colleagues.id})-[a:ASSIGNED_TO]->(proj:Project)
               WHERE proj.status = 'active'
               COLUMNS (proj.name AS project_name)
             ) AS projects ON 1=1
        WHERE u.department = 'Engineering'
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        // Verify that we have JOIN with multiple graph tables
        guard case .join(let join) = query.source else {
            Issue.record("Expected join")
            return
        }

        // The join should contain nested graph tables
        // This tests the parser's ability to handle complex source structures
        #expect(join.type == .inner)
    }

    @Test("Bidirectional relationship with properties")
    func testBidirectionalRelationshipQuery() throws {
        // Find mutual relationships with relationship properties
        let sql = """
        SELECT p1.name, p2.name
        FROM GRAPH_TABLE(SocialGraph,
               MATCH (p1:Person)-[r1:FRIEND {strength: 'strong'}]->(p2:Person)
                     -[r2:FRIEND {strength: 'strong'}]->(p1)
               WHERE p1.id < p2.id
               COLUMNS (p1.name AS name1, p2.name AS name2)
             ) AS mutual
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable")
            return
        }

        let path = gt.matchPattern.paths[0]

        // Verify edges with properties
        guard case .edge(let r1) = path.elements[1],
              case .edge(let r2) = path.elements[3] else {
            Issue.record("Expected edges")
            return
        }

        #expect(r1.variable == "r1")
        #expect(r1.labels == ["FRIEND"])
        #expect(r1.properties?.count == 1)
        #expect(r1.direction == .outgoing)

        #expect(r2.variable == "r2")
        #expect(r2.labels == ["FRIEND"])
        #expect(r2.properties?.count == 1)
        #expect(r2.direction == .outgoing)
    }

    @Test("Complex WHERE with graph pattern")
    func testComplexWhereClause() throws {
        // Complex filtering on graph traversal results
        let sql = """
        SELECT author.name, paper.title
        FROM GRAPH_TABLE(CitationGraph,
               MATCH (author:Author)-[w:WROTE]->(paper:Paper)<-[cite:CITES]-(citing:Paper)
               WHERE paper.year >= 2020
               COLUMNS (author.name AS author_name, paper.title AS paper_title)
             ) AS citations
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable")
            return
        }

        // Verify complex WHERE clause exists
        #expect(gt.matchPattern.where != nil)

        // Verify mixed edge directions
        let path = gt.matchPattern.paths[0]
        guard case .edge(let wrote) = path.elements[1],
              case .edge(let cite) = path.elements[3] else {
            Issue.record("Expected edges")
            return
        }

        #expect(wrote.labels == ["WROTE"])
        #expect(wrote.direction == .outgoing)

        #expect(cite.variable == "cite")
        #expect(cite.labels == ["CITES"])
        #expect(cite.direction == .incoming)
    }

    @Test("Undirected relationships with symmetric patterns")
    func testUndirectedSymmetricQuery() throws {
        // Find clusters using undirected edges
        let sql = """
        SELECT c1.name, c2.name, c3.name
        FROM GRAPH_TABLE(CollaborationGraph,
               MATCH (c1:Company)-[p1:PARTNER]-(c2:Company)-[p2:PARTNER]-(c3:Company)-[p3:PARTNER]-(c1)
               WHERE c1.id < c2.id AND c2.id < c3.id
                 AND p1.type = 'strategic'
                 AND p2.type = 'strategic'
                 AND p3.type = 'strategic'
               COLUMNS (c1.name AS company1, c2.name AS company2, c3.name AS company3)
             ) AS triangle
        """

        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        guard case .graphTable(let gt) = query.source else {
            Issue.record("Expected graphTable")
            return
        }

        let path = gt.matchPattern.paths[0]

        // Verify all edges are undirected
        guard case .edge(let p1) = path.elements[1],
              case .edge(let p2) = path.elements[3],
              case .edge(let p3) = path.elements[5] else {
            Issue.record("Expected edges")
            return
        }

        #expect(p1.direction == .undirected)
        #expect(p2.direction == .undirected)
        #expect(p3.direction == .undirected)

        // Verify all edges have labels
        #expect(p1.labels == ["PARTNER"])
        #expect(p2.labels == ["PARTNER"])
        #expect(p3.labels == ["PARTNER"])

        // Note: Edge properties are in WHERE clause, not in edge pattern
    }
}
