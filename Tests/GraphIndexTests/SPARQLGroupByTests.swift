// SPARQLGroupByTests.swift
// GraphIndexTests - Tests for SPARQL GROUP BY functionality

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
struct SocialEdgeForGroupBy {
    #Directory<SocialEdgeForGroupBy>("test", "sparql", "groupby")
    var id: String = UUID().uuidString
    var from: String = ""
    var relationship: String = ""
    var to: String = ""
    var weight: String = ""

    #Index(GraphIndexKind<SocialEdgeForGroupBy>(
        from: \.from,
        edge: \.relationship,
        to: \.to,
        strategy: .tripleStore
    ))
}

// MARK: - Test Suite

@Suite("SPARQL GROUP BY Tests", .serialized)
struct SPARQLGroupByTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    // MARK: - Helpers

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupContainer() async throws -> FDBContainer {
        let database = try FDBClient.openDatabase()
        let schema = Schema([SocialEdgeForGroupBy.self], version: Schema.Version(1, 0, 0))
        return FDBContainer(database: database, schema: schema, security: .disabled)
    }

    private func setIndexStatesToReadable(container: FDBContainer) async throws {
        let subspace = try await container.resolveDirectory(for: SocialEdgeForGroupBy.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)

        for descriptor in SocialEdgeForGroupBy.indexDescriptors {
            let currentState = try await indexStateManager.state(of: descriptor.name)

            switch currentState {
            case .disabled:
                try await indexStateManager.enable(descriptor.name)
                try await indexStateManager.makeReadable(descriptor.name)
            case .writeOnly:
                try await indexStateManager.makeReadable(descriptor.name)
            case .readable:
                // Already readable, no action needed
                break
            }
        }
    }

    private func insertEdges(_ edges: [SocialEdgeForGroupBy], context: FDBContext) async throws {
        for edge in edges {
            context.insert(edge)
        }
        try await context.save()
    }

    private func makeEdge(from: String, relationship: String, to: String, weight: String = "1") -> SocialEdgeForGroupBy {
        var edge = SocialEdgeForGroupBy()
        edge.from = from
        edge.relationship = relationship
        edge.to = to
        edge.weight = weight
        return edge
    }

    // MARK: - Basic GROUP BY Tests

    @Test("GROUP BY with COUNT")
    func testGroupByCount() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let p1 = uniqueID("P1")
        let p2 = uniqueID("P2")
        let f1 = uniqueID("F1")
        let f2 = uniqueID("F2")
        let f3 = uniqueID("F3")

        // Create edges: P1 knows F1, F2; P2 knows F1, F2, F3
        let edges = [
            makeEdge(from: p1, relationship: "knows", to: f1),
            makeEdge(from: p1, relationship: "knows", to: f2),
            makeEdge(from: p2, relationship: "knows", to: f1),
            makeEdge(from: p2, relationship: "knows", to: f2),
            makeEdge(from: p2, relationship: "knows", to: f3),
        ]

        try await insertEdges(edges, context: context)

        // GROUP BY ?from and COUNT friends
        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where("?person", "knows", "?friend")
            .groupBy("?person")
            .count("?friend", as: "friendCount")
            .execute()

        #expect(result.count == 2)

        // Find the counts for each person
        let p1Result = result.bindings.first { $0["?person"] == p1 }
        let p2Result = result.bindings.first { $0["?person"] == p2 }

        #expect(p1Result?["friendCount"] == "2")
        #expect(p2Result?["friendCount"] == "3")
    }

    @Test("GROUP BY with multiple aggregates")
    func testGroupByMultipleAggregates() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let team1 = uniqueID("T1")
        let team2 = uniqueID("T2")

        // Create edges with different "to" values for min/max testing
        let edges = [
            makeEdge(from: team1, relationship: "hasScore", to: "A"),
            makeEdge(from: team1, relationship: "hasScore", to: "B"),
            makeEdge(from: team1, relationship: "hasScore", to: "C"),
            makeEdge(from: team2, relationship: "hasScore", to: "D"),
            makeEdge(from: team2, relationship: "hasScore", to: "E"),
        ]

        try await insertEdges(edges, context: context)

        // GROUP BY team with COUNT, MIN, MAX
        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where("?team", "hasScore", "?score")
            .groupBy("?team")
            .count("?score", as: "scoreCount")
            .min("?score", as: "minScore")
            .max("?score", as: "maxScore")
            .execute()

        #expect(result.count == 2)

        let t1Result = result.bindings.first { $0["?team"] == team1 }
        let t2Result = result.bindings.first { $0["?team"] == team2 }

        #expect(t1Result?["scoreCount"] == "3")
        #expect(t1Result?["minScore"] == "A")
        #expect(t1Result?["maxScore"] == "C")

        #expect(t2Result?["scoreCount"] == "2")
        #expect(t2Result?["minScore"] == "D")
        #expect(t2Result?["maxScore"] == "E")
    }

    @Test("GROUP BY with HAVING filter")
    func testGroupByWithHaving() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let p1 = uniqueID("P1")
        let p2 = uniqueID("P2")
        let p3 = uniqueID("P3")
        let predicate = uniqueID("knows") // Use unique predicate to avoid interference

        // Create edges: P1 -> 1 friend, P2 -> 3 friends, P3 -> 5 friends
        var edges: [SocialEdgeForGroupBy] = []
        edges.append(makeEdge(from: p1, relationship: predicate, to: uniqueID("F")))
        for i in 0..<3 {
            edges.append(makeEdge(from: p2, relationship: predicate, to: uniqueID("F\(i)")))
        }
        for i in 0..<5 {
            edges.append(makeEdge(from: p3, relationship: predicate, to: uniqueID("F\(i)")))
        }

        try await insertEdges(edges, context: context)

        // GROUP BY person HAVING count > 2
        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where("?person", predicate, "?friend")
            .groupBy("?person")
            .count("?friend", as: "friendCount")
            .having("friendCount", greaterThan: 2)
            .execute()

        // Only P2 and P3 should pass the filter
        #expect(result.count == 2)

        let persons = result.bindings.compactMap { $0["?person"] }
        #expect(persons.contains(p2))
        #expect(persons.contains(p3))
        #expect(!persons.contains(p1))
    }

    @Test("COUNT(*) aggregate")
    func testCountAll() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let person = uniqueID("P")
        let edges = [
            makeEdge(from: person, relationship: "likes", to: "A"),
            makeEdge(from: person, relationship: "likes", to: "B"),
            makeEdge(from: person, relationship: "likes", to: "C"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where("?person", "likes", "?item")
            .groupBy("?person")
            .countAll(as: "totalCount")
            .execute()

        #expect(result.count == 1)
        #expect(result.firstNumericAggregate("totalCount") == 3)
    }

    @Test("GROUP_CONCAT aggregate")
    func testGroupConcat() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let person = uniqueID("P")
        let predicate = uniqueID("likes")  // Use unique predicate to avoid interference
        let edges = [
            makeEdge(from: person, relationship: predicate, to: "Apple"),
            makeEdge(from: person, relationship: predicate, to: "Banana"),
            makeEdge(from: person, relationship: predicate, to: "Cherry"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where("?person", predicate, "?fruit")
            .groupBy("?person")
            .groupConcat("?fruit", separator: ", ", as: "allFruits")
            .execute()

        #expect(result.count == 1)

        let allFruits = result.firstAggregate("allFruits")
        #expect(allFruits != nil)

        // Check that all fruits are in the concatenated string
        #expect(allFruits!.contains("Apple"))
        #expect(allFruits!.contains("Banana"))
        #expect(allFruits!.contains("Cherry"))
    }

    @Test("SAMPLE aggregate")
    func testSampleAggregate() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let person = uniqueID("P")
        let edges = [
            makeEdge(from: person, relationship: "visited", to: "Paris"),
            makeEdge(from: person, relationship: "visited", to: "London"),
            makeEdge(from: person, relationship: "visited", to: "Tokyo"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where("?person", "visited", "?city")
            .groupBy("?person")
            .sample("?city", as: "sampleCity")
            .execute()

        #expect(result.count == 1)

        let sampleCity = result.firstAggregate("sampleCity")
        #expect(sampleCity != nil)
        #expect(["Paris", "London", "Tokyo"].contains(sampleCity!))
    }

    @Test("COUNT DISTINCT aggregate")
    func testCountDistinct() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        // Test COUNT DISTINCT by grouping by tag and counting photos
        // Multiple photos use the same tags
        let photo1 = uniqueID("Photo1")
        let photo2 = uniqueID("Photo2")
        let photo3 = uniqueID("Photo3")
        let predicate = uniqueID("tagged")

        // Create edges:
        // photo1 -> tagged -> nature
        // photo1 -> tagged -> travel
        // photo2 -> tagged -> nature
        // photo3 -> tagged -> nature
        let edges = [
            makeEdge(from: photo1, relationship: predicate, to: "nature"),
            makeEdge(from: photo1, relationship: predicate, to: "travel"),
            makeEdge(from: photo2, relationship: predicate, to: "nature"),
            makeEdge(from: photo3, relationship: predicate, to: "nature"),
        ]

        try await insertEdges(edges, context: context)

        // GROUP BY tag, count photos
        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where("?photo", predicate, "?tag")
            .groupBy("?tag")
            .count("?photo", as: "totalPhotos")
            .countDistinct("?photo", as: "uniquePhotos")
            .execute()

        #expect(result.count == 2)  // nature, travel

        // Find results for each tag
        let natureResult = result.bindings.first { $0["?tag"] == "nature" }
        let travelResult = result.bindings.first { $0["?tag"] == "travel" }

        // nature: 3 photos (photo1, photo2, photo3) - all unique
        #expect(natureResult?["totalPhotos"] == "3")
        #expect(natureResult?["uniquePhotos"] == "3")

        // travel: 1 photo (photo1)
        #expect(travelResult?["totalPhotos"] == "1")
        #expect(travelResult?["uniquePhotos"] == "1")
    }

    @Test("Empty group results")
    func testEmptyGroupResults() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        // Query with no matching data
        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where(uniqueID("nonexistent"), "knows", "?friend")
            .groupBy("?friend")
            .count("?friend", as: "count")
            .execute()

        #expect(result.isEmpty)
    }

    @Test("GROUP BY with LIMIT")
    func testGroupByWithLimit() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        // Create 5 different persons with varying friend counts
        var edges: [SocialEdgeForGroupBy] = []
        for i in 0..<5 {
            let person = "P\(i)-\(uniqueID(""))"
            for j in 0..<(i + 1) {
                edges.append(makeEdge(from: person, relationship: "knows", to: "F\(j)"))
            }
        }

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where("?person", "knows", "?friend")
            .groupBy("?person")
            .count("?friend", as: "friendCount")
            .limit(3)
            .execute()

        #expect(result.count == 3)
    }

    // MARK: - SUM Aggregate Tests

    @Test("SUM aggregate with integer values")
    func testSumAggregateInteger() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let team1 = uniqueID("T1")
        let team2 = uniqueID("T2")
        let predicate = uniqueID("hasScore")

        // Create edges with numeric values in the "to" field
        // Team1: scores 10, 20, 30 = sum 60
        // Team2: scores 5, 15 = sum 20
        let edges = [
            makeEdge(from: team1, relationship: predicate, to: "10"),
            makeEdge(from: team1, relationship: predicate, to: "20"),
            makeEdge(from: team1, relationship: predicate, to: "30"),
            makeEdge(from: team2, relationship: predicate, to: "5"),
            makeEdge(from: team2, relationship: predicate, to: "15"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where("?team", predicate, "?score")
            .groupBy("?team")
            .sum("?score", as: "totalScore")
            .execute()

        #expect(result.count == 2)

        let t1Result = result.bindings.first { $0["?team"] == team1 }
        let t2Result = result.bindings.first { $0["?team"] == team2 }

        #expect(t1Result?["totalScore"] == "60")
        #expect(t2Result?["totalScore"] == "20")
    }

    @Test("SUM aggregate with decimal values")
    func testSumAggregateDecimal() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let account = uniqueID("A")
        let predicate = uniqueID("hasAmount")

        // Create edges with decimal values: 10.5, 20.25, 30.25 = 61.0
        let edges = [
            makeEdge(from: account, relationship: predicate, to: "10.5"),
            makeEdge(from: account, relationship: predicate, to: "20.25"),
            makeEdge(from: account, relationship: predicate, to: "30.25"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where("?account", predicate, "?amount")
            .groupBy("?account")
            .sum("?amount", as: "totalAmount")
            .execute()

        #expect(result.count == 1)

        let totalAmount = result.firstAggregate("totalAmount")
        #expect(totalAmount != nil)

        // Check that sum is 61.0
        if let total = totalAmount, let value = Double(total) {
            #expect(abs(value - 61.0) < 0.001)
        } else {
            Issue.record("Expected numeric total amount")
        }
    }

    @Test("SUM aggregate with mixed numeric and non-numeric values")
    func testSumAggregateMixedValues() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let group = uniqueID("G")
        let predicate = uniqueID("hasValue")

        // Create edges with mixed values: 10, "abc", 20
        // Non-numeric values should be ignored, sum = 30
        let edges = [
            makeEdge(from: group, relationship: predicate, to: "10"),
            makeEdge(from: group, relationship: predicate, to: "abc"),
            makeEdge(from: group, relationship: predicate, to: "20"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where("?group", predicate, "?value")
            .groupBy("?group")
            .sum("?value", as: "totalValue")
            .execute()

        #expect(result.count == 1)

        let totalValue = result.firstAggregate("totalValue")
        #expect(totalValue == "30")
    }

    // MARK: - AVG Aggregate Tests

    @Test("AVG aggregate with integer values")
    func testAvgAggregateInteger() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let class1 = uniqueID("C1")
        let class2 = uniqueID("C2")
        let predicate = uniqueID("hasGrade")

        // Create edges with grades as "to" field values
        // Class1: 80, 90, 100 = avg 90
        // Class2: 70, 80 = avg 75
        let edges = [
            makeEdge(from: class1, relationship: predicate, to: "80"),
            makeEdge(from: class1, relationship: predicate, to: "90"),
            makeEdge(from: class1, relationship: predicate, to: "100"),
            makeEdge(from: class2, relationship: predicate, to: "70"),
            makeEdge(from: class2, relationship: predicate, to: "80"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where("?class", predicate, "?grade")
            .groupBy("?class")
            .avg("?grade", as: "avgGrade")
            .execute()

        #expect(result.count == 2)

        let c1Result = result.bindings.first { $0["?class"] == class1 }
        let c2Result = result.bindings.first { $0["?class"] == class2 }

        // Check class1 average = 90
        if let avgStr = c1Result?["avgGrade"], let avg = Double(avgStr) {
            #expect(abs(avg - 90.0) < 0.001)
        } else {
            Issue.record("Expected numeric average for class1")
        }

        // Check class2 average = 75
        if let avgStr = c2Result?["avgGrade"], let avg = Double(avgStr) {
            #expect(abs(avg - 75.0) < 0.001)
        } else {
            Issue.record("Expected numeric average for class2")
        }
    }

    @Test("AVG aggregate with decimal values")
    func testAvgAggregateDecimal() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let sensor = uniqueID("S")
        let predicate = uniqueID("hasReading")

        // Create edges with decimal readings: 23.5, 24.5, 25.0 = avg 24.333...
        let edges = [
            makeEdge(from: sensor, relationship: predicate, to: "23.5"),
            makeEdge(from: sensor, relationship: predicate, to: "24.5"),
            makeEdge(from: sensor, relationship: predicate, to: "25.0"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where("?sensor", predicate, "?reading")
            .groupBy("?sensor")
            .avg("?reading", as: "avgReading")
            .execute()

        #expect(result.count == 1)

        let avgReading = result.firstAggregate("avgReading")
        #expect(avgReading != nil)

        // Check that average is approximately 24.333
        if let avgStr = avgReading, let avg = Double(avgStr) {
            #expect(abs(avg - 24.333333) < 0.001)
        } else {
            Issue.record("Expected numeric average reading")
        }
    }

    @Test("AVG aggregate with single value")
    func testAvgAggregateSingleValue() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let item = uniqueID("I")
        let predicate = uniqueID("hasValue")

        // Single value: 42
        let edges = [
            makeEdge(from: item, relationship: predicate, to: "42"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where("?item", predicate, "?value")
            .groupBy("?item")
            .avg("?value", as: "avgValue")
            .execute()

        #expect(result.count == 1)

        let avgValue = result.firstAggregate("avgValue")
        #expect(avgValue != nil)

        // Average of single value should be that value
        if let avgStr = avgValue, let avg = Double(avgStr) {
            #expect(abs(avg - 42.0) < 0.001)
        } else {
            Issue.record("Expected numeric average value")
        }
    }

    @Test("AVG aggregate returns nil for non-numeric values")
    func testAvgAggregateNonNumeric() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let group = uniqueID("G")
        let predicate = uniqueID("hasLabel")

        // Create edges with non-numeric values
        let edges = [
            makeEdge(from: group, relationship: predicate, to: "alpha"),
            makeEdge(from: group, relationship: predicate, to: "beta"),
            makeEdge(from: group, relationship: predicate, to: "gamma"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where("?group", predicate, "?label")
            .groupBy("?group")
            .avg("?label", as: "avgLabel")
            .execute()

        #expect(result.count == 1)

        // AVG of non-numeric values should return nil
        let avgLabel = result.firstAggregate("avgLabel")
        // Non-numeric values cannot be averaged, so result should be nil
        #expect(avgLabel == nil)
    }

    // MARK: - Combined SUM and AVG Tests

    @Test("SUM and AVG combined in single query")
    func testSumAndAvgCombined() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let dept = uniqueID("D")
        let predicate = uniqueID("hasSalary")

        // Create employees with salaries: 50000, 60000, 70000
        // Sum = 180000, Avg = 60000
        let edges = [
            makeEdge(from: dept, relationship: predicate, to: "50000"),
            makeEdge(from: dept, relationship: predicate, to: "60000"),
            makeEdge(from: dept, relationship: predicate, to: "70000"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where("?dept", predicate, "?salary")
            .groupBy("?dept")
            .sum("?salary", as: "totalSalary")
            .avg("?salary", as: "avgSalary")
            .count("?salary", as: "employeeCount")
            .execute()

        #expect(result.count == 1)

        let deptResult = result.bindings.first { $0["?dept"] == dept }
        #expect(deptResult != nil)

        #expect(deptResult?["totalSalary"] == "180000")
        #expect(deptResult?["employeeCount"] == "3")

        if let avgStr = deptResult?["avgSalary"], let avg = Double(avgStr) {
            #expect(abs(avg - 60000.0) < 0.001)
        } else {
            Issue.record("Expected numeric average salary")
        }
    }

    @Test("SUM aggregate with negative values")
    func testSumAggregateNegative() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let account = uniqueID("A")
        let predicate = uniqueID("hasBalance")

        // Create edges with positive and negative values: 100, -30, -20 = 50
        let edges = [
            makeEdge(from: account, relationship: predicate, to: "100"),
            makeEdge(from: account, relationship: predicate, to: "-30"),
            makeEdge(from: account, relationship: predicate, to: "-20"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where("?account", predicate, "?balance")
            .groupBy("?account")
            .sum("?balance", as: "netBalance")
            .execute()

        #expect(result.count == 1)

        let netBalance = result.firstAggregate("netBalance")
        #expect(netBalance == "50")
    }

    @Test("AVG aggregate with zero values")
    func testAvgAggregateWithZeros() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let group = uniqueID("G")
        let predicate = uniqueID("hasValue")

        // Create edges with zeros and positive values: 0, 5, 10 = avg 5.0
        // Note: Graph index stores unique (from, edge, to) triples, so we need unique "to" values
        let edges = [
            makeEdge(from: group, relationship: predicate, to: "0"),
            makeEdge(from: group, relationship: predicate, to: "5"),
            makeEdge(from: group, relationship: predicate, to: "10"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where("?group", predicate, "?value")
            .groupBy("?group")
            .avg("?value", as: "avgValue")
            .execute()

        #expect(result.count == 1)

        let avgValue = result.firstAggregate("avgValue")
        #expect(avgValue != nil)

        if let avgStr = avgValue, let avg = Double(avgStr) {
            #expect(abs(avg - 5.0) < 0.001)
        } else {
            Issue.record("Expected numeric average value")
        }
    }
}
