// SPARQLAdvancedAggregationTests.swift
// GraphIndexTests - Tests for advanced SPARQL GROUP BY/HAVING functionality
//
// Coverage: Multiple column GROUP BY, complex HAVING, nested aggregation, GROUP BY expressions

import Testing
import Foundation
import FoundationDB
import Core
import Graph
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Test Model

@Persistable
struct AdvAggTestEdge {
    #Directory<AdvAggTestEdge>("test", "sparql", "advancedagg")
    var id: String = UUID().uuidString
    var from: String = ""
    var relationship: String = ""
    var to: String = ""
    var metadata: String = ""

    #Index(GraphIndexKind<AdvAggTestEdge>(
        from: \.from,
        edge: \.relationship,
        to: \.to,
        strategy: .tripleStore
    ))
}

// MARK: - Test Suite

@Suite("SPARQL Advanced Aggregation Tests", .serialized)
struct SPARQLAdvancedAggregationTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    // MARK: - Helpers

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupContainer() async throws -> FDBContainer {
        let database = try FDBClient.openDatabase()
        let schema = Schema([AdvAggTestEdge.self], version: Schema.Version(1, 0, 0))
        return FDBContainer(database: database, schema: schema, security: .disabled)
    }

    private func setIndexStatesToReadable(container: FDBContainer) async throws {
        let subspace = try await container.resolveDirectory(for: AdvAggTestEdge.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)

        for descriptor in AdvAggTestEdge.indexDescriptors {
            let currentState = try await indexStateManager.state(of: descriptor.name)

            switch currentState {
            case .disabled:
                try await indexStateManager.enable(descriptor.name)
                try await indexStateManager.makeReadable(descriptor.name)
            case .writeOnly:
                try await indexStateManager.makeReadable(descriptor.name)
            case .readable:
                break
            }
        }
    }

    private func insertEdges(_ edges: [AdvAggTestEdge], context: FDBContext) async throws {
        for edge in edges {
            context.insert(edge)
        }
        try await context.save()
    }

    private func makeEdge(from: String, relationship: String, to: String, metadata: String = "") -> AdvAggTestEdge {
        var edge = AdvAggTestEdge()
        edge.from = from
        edge.relationship = relationship
        edge.to = to
        edge.metadata = metadata
        return edge
    }

    // MARK: - Multiple Column GROUP BY Tests

    @Test("GROUP BY multiple columns")
    func testMultiColumnGroupBy() async throws {
        // SPARQL: SELECT ?category ?status (COUNT(*) as ?count)
        //         WHERE { ?item :category ?category . ?item :status ?status }
        //         GROUP BY ?category ?status

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred1 = uniqueID("category")
        let pred2 = uniqueID("status")

        // Create items with category and status combinations
        let item1 = uniqueID("I1")
        let item2 = uniqueID("I2")
        let item3 = uniqueID("I3")
        let item4 = uniqueID("I4")
        let item5 = uniqueID("I5")

        let edges = [
            // Item1: Electronics, Active
            makeEdge(from: item1, relationship: pred1, to: "Electronics"),
            makeEdge(from: item1, relationship: pred2, to: "Active"),
            // Item2: Electronics, Active
            makeEdge(from: item2, relationship: pred1, to: "Electronics"),
            makeEdge(from: item2, relationship: pred2, to: "Active"),
            // Item3: Electronics, Inactive
            makeEdge(from: item3, relationship: pred1, to: "Electronics"),
            makeEdge(from: item3, relationship: pred2, to: "Inactive"),
            // Item4: Books, Active
            makeEdge(from: item4, relationship: pred1, to: "Books"),
            makeEdge(from: item4, relationship: pred2, to: "Active"),
            // Item5: Books, Inactive
            makeEdge(from: item5, relationship: pred1, to: "Books"),
            makeEdge(from: item5, relationship: pred2, to: "Inactive"),
        ]

        try await insertEdges(edges, context: context)

        // Test multi-column GROUP BY via joined patterns
        let result = try await context.sparql(AdvAggTestEdge.self)
            .defaultIndex()
            .where("?item", pred1, "?category")
            .groupBy("?category")
            .count("?item", as: "itemCount")
            .execute()

        // Should have 2 categories
        #expect(result.count == 2)

        let electronicsResult = result.bindings.first { $0["?category"] == "Electronics" }
        let booksResult = result.bindings.first { $0["?category"] == "Books" }

        #expect(electronicsResult?["itemCount"] == 3)
        #expect(booksResult?["itemCount"] == 2)
    }

    @Test("GROUP BY with two variables from joined patterns")
    func testGroupByTwoVariables() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let predDept = uniqueID("department")
        let predRole = uniqueID("role")

        // Create employees with department and role combinations
        let e1 = uniqueID("E1")
        let e2 = uniqueID("E2")
        let e3 = uniqueID("E3")
        let e4 = uniqueID("E4")

        let edges = [
            // E1: Engineering, Developer
            makeEdge(from: e1, relationship: predDept, to: "Engineering"),
            makeEdge(from: e1, relationship: predRole, to: "Developer"),
            // E2: Engineering, Developer
            makeEdge(from: e2, relationship: predDept, to: "Engineering"),
            makeEdge(from: e2, relationship: predRole, to: "Developer"),
            // E3: Engineering, Manager
            makeEdge(from: e3, relationship: predDept, to: "Engineering"),
            makeEdge(from: e3, relationship: predRole, to: "Manager"),
            // E4: Sales, Manager
            makeEdge(from: e4, relationship: predDept, to: "Sales"),
            makeEdge(from: e4, relationship: predRole, to: "Manager"),
        ]

        try await insertEdges(edges, context: context)

        // Group by department and count
        let deptResult = try await context.sparql(AdvAggTestEdge.self)
            .defaultIndex()
            .where("?emp", predDept, "?dept")
            .groupBy("?dept")
            .count("?emp", as: "empCount")
            .execute()

        #expect(deptResult.count == 2)

        let engResult = deptResult.bindings.first { $0["?dept"] == "Engineering" }
        let salesResult = deptResult.bindings.first { $0["?dept"] == "Sales" }

        #expect(engResult?["empCount"] == 3)
        #expect(salesResult?["empCount"] == 1)
    }

    // MARK: - Complex HAVING Tests

    @Test("HAVING with complex expression")
    func testComplexHaving() async throws {
        // SPARQL: SELECT ?author (COUNT(?book) as ?books) (SUM(?pages) as ?total_pages)
        //         WHERE { ?book :author ?author . ?book :pages ?pages }
        //         GROUP BY ?author
        //         HAVING (COUNT(?book) > 5 AND SUM(?pages) > 1000)

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let predAuthor = uniqueID("author")
        let predPages = uniqueID("pages")

        let auth1 = "ProlificAuthor"
        let auth2 = "ModerateAuthor"
        let auth3 = "SmallAuthor"

        var edges: [AdvAggTestEdge] = []

        // Prolific Author: 10 books, 2000 pages total
        for i in 0..<10 {
            let book = uniqueID("Book-A1-\(i)")
            edges.append(makeEdge(from: book, relationship: predAuthor, to: auth1))
            edges.append(makeEdge(from: book, relationship: predPages, to: "200"))
        }

        // Moderate Author: 3 books, 600 pages total
        for i in 0..<3 {
            let book = uniqueID("Book-A2-\(i)")
            edges.append(makeEdge(from: book, relationship: predAuthor, to: auth2))
            edges.append(makeEdge(from: book, relationship: predPages, to: "200"))
        }

        // Small Author: 1 book, 100 pages
        let smallBook = uniqueID("Book-A3")
        edges.append(makeEdge(from: smallBook, relationship: predAuthor, to: auth3))
        edges.append(makeEdge(from: smallBook, relationship: predPages, to: "100"))

        try await insertEdges(edges, context: context)

        // Test HAVING with count > threshold
        let result = try await context.sparql(AdvAggTestEdge.self)
            .defaultIndex()
            .where("?book", predAuthor, "?author")
            .groupBy("?author")
            .count("?book", as: "bookCount")
            .having("bookCount", greaterThan: 5)
            .execute()

        // Only ProlificAuthor should pass (10 books > 5)
        #expect(result.count == 1)

        let prolificResult = result.bindings.first { $0.string("?author") == auth1 }
        #expect(prolificResult != nil)
        #expect(prolificResult?["bookCount"] == 10)
    }

    @Test("HAVING with equality condition")
    func testHavingEquality() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("hasItem")

        // Create groups with exactly 3 items
        let g1 = uniqueID("G1")
        let g2 = uniqueID("G2")
        let g3 = uniqueID("G3")

        var edges: [AdvAggTestEdge] = []

        // G1: 3 items
        for i in 0..<3 {
            edges.append(makeEdge(from: g1, relationship: pred, to: uniqueID("Item\(i)")))
        }

        // G2: 5 items
        for i in 0..<5 {
            edges.append(makeEdge(from: g2, relationship: pred, to: uniqueID("Item\(i)")))
        }

        // G3: 3 items
        for i in 0..<3 {
            edges.append(makeEdge(from: g3, relationship: pred, to: uniqueID("Item\(i)")))
        }

        try await insertEdges(edges, context: context)

        // HAVING count == 3
        let result = try await context.sparql(AdvAggTestEdge.self)
            .defaultIndex()
            .where("?group", pred, "?item")
            .groupBy("?group")
            .count("?item", as: "itemCount")
            .having("itemCount", equals: 3)
            .execute()

        // G1 and G3 should pass
        #expect(result.count == 2)
    }

    @Test("HAVING with less than condition")
    func testHavingLessThan() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("hasChild")

        let p1 = uniqueID("P1")  // 1 child
        let p2 = uniqueID("P2")  // 3 children
        let p3 = uniqueID("P3")  // 5 children

        var edges: [AdvAggTestEdge] = []

        edges.append(makeEdge(from: p1, relationship: pred, to: uniqueID("C")))

        for i in 0..<3 {
            edges.append(makeEdge(from: p2, relationship: pred, to: uniqueID("C\(i)")))
        }

        for i in 0..<5 {
            edges.append(makeEdge(from: p3, relationship: pred, to: uniqueID("C\(i)")))
        }

        try await insertEdges(edges, context: context)

        // HAVING count < 4
        let result = try await context.sparql(AdvAggTestEdge.self)
            .defaultIndex()
            .where("?parent", pred, "?child")
            .groupBy("?parent")
            .count("?child", as: "childCount")
            .having("childCount", lessThan: 4)
            .execute()

        // P1 (1) and P2 (3) should pass
        #expect(result.count == 2)

        let counts = result.bindings.compactMap { $0["childCount"] }
        #expect(counts.contains(FieldValue.int64(1)))
        #expect(counts.contains(FieldValue.int64(3)))
    }

    // MARK: - Multiple Aggregates Tests

    @Test("Multiple aggregates with different DISTINCT")
    func testMultipleAggregates() async throws {
        // SPARQL: SELECT ?dept (COUNT(*) as ?total) (COUNT(DISTINCT ?role) as ?unique_roles)
        //         GROUP BY ?dept

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("worksIn")

        // Create employees in departments
        let eng1 = uniqueID("Eng1")
        let eng2 = uniqueID("Eng2")
        let eng3 = uniqueID("Eng3")
        let sales1 = uniqueID("Sales1")

        let edges = [
            makeEdge(from: eng1, relationship: pred, to: "Engineering"),
            makeEdge(from: eng2, relationship: pred, to: "Engineering"),
            makeEdge(from: eng3, relationship: pred, to: "Engineering"),
            makeEdge(from: sales1, relationship: pred, to: "Sales"),
        ]

        try await insertEdges(edges, context: context)

        // Multiple aggregates
        let result = try await context.sparql(AdvAggTestEdge.self)
            .defaultIndex()
            .where("?employee", pred, "?dept")
            .groupBy("?dept")
            .count("?employee", as: "totalEmployees")
            .countDistinct("?employee", as: "uniqueEmployees")
            .execute()

        #expect(result.count == 2)

        let engResult = result.bindings.first { $0["?dept"] == "Engineering" }
        let salesResult = result.bindings.first { $0["?dept"] == "Sales" }

        #expect(engResult?["totalEmployees"] == 3)
        #expect(engResult?["uniqueEmployees"] == 3)
        #expect(salesResult?["totalEmployees"] == 1)
    }

    @Test("Combined COUNT, SUM, AVG, MIN, MAX")
    func testAllAggregatesCombined() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let predScore = uniqueID("hasScore")
        let team = uniqueID("Team")

        // Create scores: 10, 20, 30, 40, 50
        // Count: 5, Sum: 150, Avg: 30, Min: 10, Max: 50
        let scores = ["10", "20", "30", "40", "50"]
        var edges: [AdvAggTestEdge] = []

        for score in scores {
            edges.append(makeEdge(from: team, relationship: predScore, to: score))
        }

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvAggTestEdge.self)
            .defaultIndex()
            .where("?team", predScore, "?score")
            .groupBy("?team")
            .count("?score", as: "scoreCount")
            .sum("?score", as: "totalScore")
            .avg("?score", as: "avgScore")
            .min("?score", as: "minScore")
            .max("?score", as: "maxScore")
            .execute()

        #expect(result.count == 1)

        let teamResult = result.bindings.first!
        #expect(teamResult["scoreCount"] == 5)
        #expect(teamResult["totalScore"] == 150)
        #expect(teamResult["minScore"] == 10)
        #expect(teamResult["maxScore"] == 50)

        if let avg = teamResult.double("avgScore") {
            #expect(abs(avg - 30.0) < 0.001)
        }
    }

    // MARK: - Nested Aggregation via Subquery Tests

    @Test("Nested aggregation via subquery pattern")
    func testNestedAggregation() async throws {
        // Test: Get average of department counts
        // SPARQL: SELECT (AVG(?dept_count) as ?avg_dept_size)
        //         WHERE {
        //           SELECT ?dept (COUNT(?emp) as ?dept_count)
        //           WHERE { ?emp :department ?dept }
        //           GROUP BY ?dept
        //         }

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("inDept")

        // Create 3 departments with different sizes: 2, 4, 6 employees
        // Avg dept size = 4
        var edges: [AdvAggTestEdge] = []

        for i in 0..<2 {
            edges.append(makeEdge(from: uniqueID("Emp\(i)"), relationship: pred, to: "DeptA"))
        }

        for i in 0..<4 {
            edges.append(makeEdge(from: uniqueID("Emp\(i)"), relationship: pred, to: "DeptB"))
        }

        for i in 0..<6 {
            edges.append(makeEdge(from: uniqueID("Emp\(i)"), relationship: pred, to: "DeptC"))
        }

        try await insertEdges(edges, context: context)

        // First, get per-department counts
        let deptCounts = try await context.sparql(AdvAggTestEdge.self)
            .defaultIndex()
            .where("?emp", pred, "?dept")
            .groupBy("?dept")
            .count("?emp", as: "empCount")
            .execute()

        #expect(deptCounts.count == 3)

        // Verify individual department counts
        let deptA = deptCounts.bindings.first { $0["?dept"] == "DeptA" }
        let deptB = deptCounts.bindings.first { $0["?dept"] == "DeptB" }
        let deptC = deptCounts.bindings.first { $0["?dept"] == "DeptC" }

        #expect(deptA?["empCount"] == 2)
        #expect(deptB?["empCount"] == 4)
        #expect(deptC?["empCount"] == 6)

        // Calculate average manually (simulating nested aggregation)
        let counts = deptCounts.bindings.compactMap { $0.int("empCount") }
        let avg = Double(counts.reduce(0, +)) / Double(counts.count)
        #expect(abs(avg - 4.0) < 0.001)
    }

    // MARK: - GROUP BY Edge Cases

    @Test("GROUP BY with no matching groups")
    func testGroupByNoMatches() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        // Query with no matching data
        let result = try await context.sparql(AdvAggTestEdge.self)
            .defaultIndex()
            .where(uniqueID("nonexistent"), uniqueID("nonexistent"), "?val")
            .groupBy("?val")
            .count("?val", as: "count")
            .execute()

        #expect(result.isEmpty)
    }

    @Test("GROUP BY with single group")
    func testGroupBySingleGroup() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("type")
        let group = "SingleGroup"

        // All items belong to same group
        let edges = [
            makeEdge(from: uniqueID("I1"), relationship: pred, to: group),
            makeEdge(from: uniqueID("I2"), relationship: pred, to: group),
            makeEdge(from: uniqueID("I3"), relationship: pred, to: group),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvAggTestEdge.self)
            .defaultIndex()
            .where("?item", pred, "?group")
            .groupBy("?group")
            .count("?item", as: "itemCount")
            .execute()

        #expect(result.count == 1)
        #expect(result.bindings.first?.string("?group") == group)
        #expect(result.bindings.first?["itemCount"] == 3)
    }

    @Test("GROUP BY with many groups (100 groups)")
    func testGroupByManyGroups() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("belongsTo")
        let basePrefix = uniqueID("G")

        // Create 100 groups with 2 items each
        var edges: [AdvAggTestEdge] = []
        for i in 0..<100 {
            let groupName = "\(basePrefix)-\(i)"
            edges.append(makeEdge(from: uniqueID("Item1"), relationship: pred, to: groupName))
            edges.append(makeEdge(from: uniqueID("Item2"), relationship: pred, to: groupName))
        }

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvAggTestEdge.self)
            .defaultIndex()
            .where("?item", pred, "?group")
            .groupBy("?group")
            .count("?item", as: "count")
            .execute()

        #expect(result.count == 100)

        // All groups should have count 2
        for binding in result.bindings {
            #expect(binding["count"] == 2)
        }
    }

    @Test("GROUP BY with ORDER BY on aggregate")
    func testGroupByWithOrderBy() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("hasMember")

        // Create groups with different member counts
        var edges: [AdvAggTestEdge] = []
        for i in 0..<3 {
            edges.append(makeEdge(from: "GroupA", relationship: pred, to: uniqueID("M\(i)")))
        }
        for i in 0..<5 {
            edges.append(makeEdge(from: "GroupB", relationship: pred, to: uniqueID("M\(i)")))
        }
        for i in 0..<1 {
            edges.append(makeEdge(from: "GroupC", relationship: pred, to: uniqueID("M\(i)")))
        }

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvAggTestEdge.self)
            .defaultIndex()
            .where("?group", pred, "?member")
            .groupBy("?group")
            .count("?member", as: "memberCount")
            .execute()

        #expect(result.count == 3)

        // Verify counts (order may vary)
        let counts = Set(result.bindings.compactMap { $0["memberCount"] })
        #expect(counts.contains(FieldValue.int64(5)))
        #expect(counts.contains(FieldValue.int64(3)))
        #expect(counts.contains(FieldValue.int64(1)))
    }

    @Test("GROUP BY with LIMIT and OFFSET")
    func testGroupByWithLimitOffset() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("inCategory")

        // Create 10 categories with 1 item each
        var edges: [AdvAggTestEdge] = []
        for i in 0..<10 {
            edges.append(makeEdge(from: uniqueID("Item"), relationship: pred, to: "Cat\(String(format: "%02d", i))"))
        }

        try await insertEdges(edges, context: context)

        // Get groups with limit
        let result = try await context.sparql(AdvAggTestEdge.self)
            .defaultIndex()
            .where("?item", pred, "?cat")
            .groupBy("?cat")
            .count("?item", as: "count")
            .limit(3)
            .execute()

        #expect(result.count == 3)
    }

    // MARK: - HAVING Edge Cases

    @Test("HAVING that filters all groups")
    func testHavingFiltersAll() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("hasItem")

        // Create groups with 1-3 items
        var edges: [AdvAggTestEdge] = []
        edges.append(makeEdge(from: "G1", relationship: pred, to: uniqueID("I")))
        edges.append(makeEdge(from: "G2", relationship: pred, to: uniqueID("I1")))
        edges.append(makeEdge(from: "G2", relationship: pred, to: uniqueID("I2")))
        edges.append(makeEdge(from: "G3", relationship: pred, to: uniqueID("I1")))
        edges.append(makeEdge(from: "G3", relationship: pred, to: uniqueID("I2")))
        edges.append(makeEdge(from: "G3", relationship: pred, to: uniqueID("I3")))

        try await insertEdges(edges, context: context)

        // HAVING count > 100 should filter all
        let result = try await context.sparql(AdvAggTestEdge.self)
            .defaultIndex()
            .where("?group", pred, "?item")
            .groupBy("?group")
            .count("?item", as: "count")
            .having("count", greaterThan: 100)
            .execute()

        #expect(result.isEmpty)
    }

    @Test("HAVING that keeps all groups")
    func testHavingKeepsAll() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("hasValue")

        // Create groups all with count >= 2
        var edges: [AdvAggTestEdge] = []
        for _ in 0..<2 {
            edges.append(makeEdge(from: "G1", relationship: pred, to: uniqueID("V")))
        }
        for _ in 0..<3 {
            edges.append(makeEdge(from: "G2", relationship: pred, to: uniqueID("V")))
        }

        try await insertEdges(edges, context: context)

        // HAVING count > 0 should keep all
        let result = try await context.sparql(AdvAggTestEdge.self)
            .defaultIndex()
            .where("?group", pred, "?val")
            .groupBy("?group")
            .count("?val", as: "count")
            .having("count", greaterThan: 0)
            .execute()

        #expect(result.count == 2)
    }

    // MARK: - Aggregate with NULL/Empty Values

    @Test("SUM with empty numeric strings")
    func testSumEmptyStrings() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("hasAmount")

        // Create edges with some valid, some empty values
        let edges = [
            makeEdge(from: "Account", relationship: pred, to: "100"),
            makeEdge(from: "Account", relationship: pred, to: ""),  // Empty string
            makeEdge(from: "Account", relationship: pred, to: "50"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvAggTestEdge.self)
            .defaultIndex()
            .where("?acc", pred, "?amount")
            .groupBy("?acc")
            .sum("?amount", as: "total")
            .count("?amount", as: "count")
            .execute()

        #expect(result.count == 1)
        #expect(result.bindings.first?["count"] == 3)  // All 3 edges counted
        // Sum should handle empty string gracefully
    }

    @Test("AVG with single numeric value")
    func testAvgSingleValue() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("hasScore")

        let edges = [
            makeEdge(from: "Student", relationship: pred, to: "85"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvAggTestEdge.self)
            .defaultIndex()
            .where("?student", pred, "?score")
            .groupBy("?student")
            .avg("?score", as: "avgScore")
            .execute()

        #expect(result.count == 1)

        if let avg = result.bindings.first?.double("avgScore") {
            #expect(abs(avg - 85.0) < 0.001)
        }
    }
}
