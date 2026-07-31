#if FOUNDATION_DB
// SPARQLGroupByTests.swift
// GraphIndexTests - Tests for SPARQL GROUP BY functionality

import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Test Model

@Persistable
struct SocialEdgeForGroupBy {
    #Directory<SocialEdgeForGroupBy>("sparql_group_by_tests")
    var id: String = UUID().uuidString
    var subject: RDFTerm = .iri(.xsdString)
    var predicate: RDFTerm = .iri(.xsdString)
    var object: RDFTerm = .string("")

    #Index(
        .rdfDataset,
        from: \SocialEdgeForGroupBy.subject,
        edge: \SocialEdgeForGroupBy.predicate,
        to: \SocialEdgeForGroupBy.object
    )
}

// MARK: - Test Suite

@Suite("SPARQL GROUP BY Tests", .serialized, .foundationDBScenario, .heartbeat)
struct SPARQLGroupByTests {

    init() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
    }

    // MARK: - Helpers

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func resource(_ identifier: String) throws -> RDFTerm {
        try .iri(
            validating: "https://example.invalid/resource/\(identifier)"
        )
    }

    private func predicate(_ identifier: String) throws -> RDFTerm {
        try .iri(
            validating: "https://example.invalid/predicate/\(identifier)"
        )
    }

    private func value(_ term: RDFTerm) -> ExecutionTerm {
        .value(.rdfTerm(term))
    }

    private func subjectTerm(_ identifier: String) throws -> ExecutionTerm {
        if identifier.hasPrefix("?") {
            return .variable(identifier)
        }
        return value(try resource(identifier))
    }

    private func predicateTerm(_ identifier: String) throws -> ExecutionTerm {
        value(try predicate(identifier))
    }

    private func objectTerm(_ value: String) -> ExecutionTerm {
        value.hasPrefix("?") ? .variable(value) : self.value(.string(value))
    }

    private func setupContainer() async throws -> DBContainer {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [try SocialEdgeForGroupBy.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SocialEdgeForGroupBy.self)]),
            security: .disabled,
        )
        if try await database.namespaceExists(path: ["sparql_group_by_tests"]) {
            try await database.removeNamespace(path: ["sparql_group_by_tests"])
        }
        try await container.ensureIndexesReady()
        return container
    }

    private func insertEdges(_ edges: [SocialEdgeForGroupBy], context: DatabaseContext) async throws {
        for edge in edges {
            try context.insert(edge)
        }
        try await context.save()
    }

    private func makeEdge(
        from: String,
        relationship: String,
        to: RDFTerm
    ) throws -> SocialEdgeForGroupBy {
        var statement = SocialEdgeForGroupBy()
        statement.subject = try resource(from)
        statement.predicate = try predicate(relationship)
        statement.object = to
        return statement
    }

    private func makeEdge(
        from: String,
        relationship: String,
        to: String
    ) throws -> SocialEdgeForGroupBy {
        try makeEdge(
            from: from,
            relationship: relationship,
            to: .string(to)
        )
    }

    // MARK: - Basic GROUP BY Tests

    @Test("GROUP BY with COUNT")
    func testGroupByCount() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let p1 = uniqueID("P1")
        let p2 = uniqueID("P2")
        let f1 = uniqueID("F1")
        let f2 = uniqueID("F2")
        let f3 = uniqueID("F3")

        // Create edges: P1 knows F1, F2; P2 knows F1, F2, F3
        let edges = [
            try makeEdge(from: p1, relationship: "knows", to: f1),
            try makeEdge(from: p1, relationship: "knows", to: f2),
            try makeEdge(from: p2, relationship: "knows", to: f1),
            try makeEdge(from: p2, relationship: "knows", to: f2),
            try makeEdge(from: p2, relationship: "knows", to: f3),
        ]

        try await insertEdges(edges, context: context)

        // GROUP BY ?from and COUNT friends
        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where(try subjectTerm("?person"), try predicateTerm("knows"), objectTerm("?friend"))
            .groupBy("?person")
            .count("?friend", as: "friendCount")
            .execute()

        #expect(result.count == 2)

        // Find the counts for each person
        let p1Value = FieldValue.rdfTerm(try resource(p1))
        let p2Value = FieldValue.rdfTerm(try resource(p2))
        let p1Result = result.bindings.first {
            $0["?person"] == p1Value
        }
        let p2Result = result.bindings.first {
            $0["?person"] == p2Value
        }

        #expect(p1Result?.string("friendCount") == "2")
        #expect(p2Result?.string("friendCount") == "3")
    }

    @Test("GROUP BY with multiple aggregates")
    func testGroupByMultipleAggregates() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let team1 = uniqueID("T1")
        let team2 = uniqueID("T2")

        // Create edges with different "to" values for min/max testing
        let edges = [
            try makeEdge(from: team1, relationship: "hasScore", to: "A"),
            try makeEdge(from: team1, relationship: "hasScore", to: "B"),
            try makeEdge(from: team1, relationship: "hasScore", to: "C"),
            try makeEdge(from: team2, relationship: "hasScore", to: "D"),
            try makeEdge(from: team2, relationship: "hasScore", to: "E"),
        ]

        try await insertEdges(edges, context: context)

        // GROUP BY team with COUNT, MIN, MAX
        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where(try subjectTerm("?team"), try predicateTerm("hasScore"), objectTerm("?score"))
            .groupBy("?team")
            .count("?score", as: "scoreCount")
            .min("?score", as: "minScore")
            .max("?score", as: "maxScore")
            .execute()

        #expect(result.count == 2)

        let team1Value = FieldValue.rdfTerm(try resource(team1))
        let team2Value = FieldValue.rdfTerm(try resource(team2))
        let t1Result = result.bindings.first {
            $0["?team"] == team1Value
        }
        let t2Result = result.bindings.first {
            $0["?team"] == team2Value
        }

        #expect(t1Result?.string("scoreCount") == "3")
        #expect(t1Result?.string("minScore") == "A")
        #expect(t1Result?.string("maxScore") == "C")

        #expect(t2Result?.string("scoreCount") == "2")
        #expect(t2Result?.string("minScore") == "D")
        #expect(t2Result?.string("maxScore") == "E")
    }

    @Test("GROUP BY with HAVING filter")
    func testGroupByWithHaving() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let p1 = uniqueID("P1")
        let p2 = uniqueID("P2")
        let p3 = uniqueID("P3")
        let predicate = uniqueID("knows") // Use unique predicate to avoid interference

        // Create edges: P1 -> 1 friend, P2 -> 3 friends, P3 -> 5 friends
        var edges: [SocialEdgeForGroupBy] = []
        edges.append(try makeEdge(from: p1, relationship: predicate, to: uniqueID("F")))
        for i in 0..<3 {
            edges.append(try makeEdge(from: p2, relationship: predicate, to: uniqueID("F\(i)")))
        }
        for i in 0..<5 {
            edges.append(try makeEdge(from: p3, relationship: predicate, to: uniqueID("F\(i)")))
        }

        try await insertEdges(edges, context: context)

        // GROUP BY person HAVING count > 2
        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where(try subjectTerm("?person"), try predicateTerm(predicate), objectTerm("?friend"))
            .groupBy("?person")
            .count("?friend", as: "friendCount")
            .having("friendCount", greaterThan: 2)
            .execute()

        // Only P2 and P3 should pass the filter
        #expect(result.count == 2)

        let persons = Set(result.bindings.compactMap { $0["?person"] })
        let p1Value = FieldValue.rdfTerm(try resource(p1))
        let p2Value = FieldValue.rdfTerm(try resource(p2))
        let p3Value = FieldValue.rdfTerm(try resource(p3))
        #expect(persons.contains(p2Value))
        #expect(persons.contains(p3Value))
        #expect(!persons.contains(p1Value))
    }

    @Test("COUNT(*) aggregate")
    func testCountAll() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let person = uniqueID("P")
        let edges = [
            try makeEdge(from: person, relationship: "likes", to: "A"),
            try makeEdge(from: person, relationship: "likes", to: "B"),
            try makeEdge(from: person, relationship: "likes", to: "C"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where(try subjectTerm("?person"), try predicateTerm("likes"), objectTerm("?item"))
            .groupBy("?person")
            .countAll(as: "totalCount")
            .execute()

        #expect(result.count == 1)
        #expect(result.firstNumericAggregate("totalCount") == 3)
    }

    @Test("GROUP_CONCAT aggregate")
    func testGroupConcat() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let person = uniqueID("P")
        let predicate = uniqueID("likes")  // Use unique predicate to avoid interference
        let edges = [
            try makeEdge(from: person, relationship: predicate, to: "Apple"),
            try makeEdge(from: person, relationship: predicate, to: "Banana"),
            try makeEdge(from: person, relationship: predicate, to: "Cherry"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where(try subjectTerm("?person"), try predicateTerm(predicate), objectTerm("?fruit"))
            .groupBy("?person")
            .groupConcat("?fruit", separator: ", ", as: "allFruits")
            .execute()

        #expect(result.count == 1)

        let allFruits = result.firstAggregateString("allFruits")
        #expect(allFruits != nil)

        // Check that all fruits are in the concatenated string
        #expect(allFruits!.contains("Apple"))
        #expect(allFruits!.contains("Banana"))
        #expect(allFruits!.contains("Cherry"))
    }

    @Test("SAMPLE aggregate")
    func testSampleAggregate() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let person = uniqueID("P")
        let edges = [
            try makeEdge(from: person, relationship: "visited", to: "Paris"),
            try makeEdge(from: person, relationship: "visited", to: "London"),
            try makeEdge(from: person, relationship: "visited", to: "Tokyo"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where(try subjectTerm("?person"), try predicateTerm("visited"), objectTerm("?city"))
            .groupBy("?person")
            .sample("?city", as: "sampleCity")
            .execute()

        #expect(result.count == 1)

        let sampleCity = result.firstAggregateString("sampleCity")
        #expect(sampleCity != nil)
        #expect(["Paris", "London", "Tokyo"].contains(sampleCity!))
    }

    @Test("COUNT DISTINCT aggregate")
    func testCountDistinct() async throws {
        let container = try await setupContainer()

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
            try makeEdge(from: photo1, relationship: predicate, to: "nature"),
            try makeEdge(from: photo1, relationship: predicate, to: "travel"),
            try makeEdge(from: photo2, relationship: predicate, to: "nature"),
            try makeEdge(from: photo3, relationship: predicate, to: "nature"),
        ]

        try await insertEdges(edges, context: context)

        // GROUP BY tag, count photos
        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where(try subjectTerm("?photo"), try predicateTerm(predicate), objectTerm("?tag"))
            .groupBy("?tag")
            .count("?photo", as: "totalPhotos")
            .countDistinct("?photo", as: "uniquePhotos")
            .execute()

        #expect(result.count == 2)  // nature, travel

        // Find results for each tag
        let natureResult = result.bindings.first {
            $0["?tag"] == .rdfTerm(.string("nature"))
        }
        let travelResult = result.bindings.first {
            $0["?tag"] == .rdfTerm(.string("travel"))
        }

        // nature: 3 photos (photo1, photo2, photo3) - all unique
        #expect(natureResult?.string("totalPhotos") == "3")
        #expect(natureResult?.string("uniquePhotos") == "3")

        // travel: 1 photo (photo1)
        #expect(travelResult?.string("totalPhotos") == "1")
        #expect(travelResult?.string("uniquePhotos") == "1")
    }

    @Test("Empty group results")
    func testEmptyGroupResults() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        // Query with no matching data
        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where(
                try subjectTerm(uniqueID("nonexistent")),
                try predicateTerm("knows"),
                objectTerm("?friend")
            )
            .groupBy("?friend")
            .count("?friend", as: "count")
            .execute()

        #expect(result.isEmpty)
    }

    @Test("GROUP BY with LIMIT")
    func testGroupByWithLimit() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        // Create 5 different persons with varying friend counts
        var edges: [SocialEdgeForGroupBy] = []
        for i in 0..<5 {
            let person = "P\(i)-\(uniqueID(""))"
            for j in 0..<(i + 1) {
                edges.append(try makeEdge(from: person, relationship: "knows", to: "F\(j)"))
            }
        }

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where(try subjectTerm("?person"), try predicateTerm("knows"), objectTerm("?friend"))
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

        let context = container.newContext()

        let team1 = uniqueID("T1")
        let team2 = uniqueID("T2")
        let predicate = uniqueID("hasScore")

        // Create edges with numeric values in the "to" field
        // Team1: scores 10, 20, 30 = sum 60
        // Team2: scores 5, 15 = sum 20
        let edges = [
            try makeEdge(from: team1, relationship: predicate, to: .integer(10)),
            try makeEdge(from: team1, relationship: predicate, to: .integer(20)),
            try makeEdge(from: team1, relationship: predicate, to: .integer(30)),
            try makeEdge(from: team2, relationship: predicate, to: .integer(5)),
            try makeEdge(from: team2, relationship: predicate, to: .integer(15)),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where(try subjectTerm("?team"), try predicateTerm(predicate), objectTerm("?score"))
            .groupBy("?team")
            .sum("?score", as: "totalScore")
            .execute()

        #expect(result.count == 2)

        let team1Value = FieldValue.rdfTerm(try resource(team1))
        let team2Value = FieldValue.rdfTerm(try resource(team2))
        let t1Result = result.bindings.first {
            $0["?team"] == team1Value
        }
        let t2Result = result.bindings.first {
            $0["?team"] == team2Value
        }

        #expect(t1Result?.string("totalScore") == "60")
        #expect(t2Result?.string("totalScore") == "20")
    }

    @Test("SUM aggregate with decimal values")
    func testSumAggregateDecimal() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let account = uniqueID("A")
        let predicate = uniqueID("hasAmount")

        // Create edges with decimal values: 10.5, 20.25, 30.25 = 61.0
        let edges = [
            try makeEdge(from: account, relationship: predicate, to: .decimal(10.5)),
            try makeEdge(from: account, relationship: predicate, to: .decimal(20.25)),
            try makeEdge(from: account, relationship: predicate, to: .decimal(30.25)),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where(try subjectTerm("?account"), try predicateTerm(predicate), objectTerm("?amount"))
            .groupBy("?account")
            .sum("?amount", as: "totalAmount")
            .execute()

        #expect(result.count == 1)

        let totalAmount = result.firstAggregateString("totalAmount")
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

        let context = container.newContext()

        let group = uniqueID("G")
        let predicate = uniqueID("hasValue")

        // Create edges with mixed values: 10, "abc", 20.
        // A type error in the aggregate input leaves the projected aggregate
        // unbound according to SPARQL expression-error semantics.
        let edges = [
            try makeEdge(from: group, relationship: predicate, to: .integer(10)),
            try makeEdge(from: group, relationship: predicate, to: "abc"),
            try makeEdge(from: group, relationship: predicate, to: .integer(20)),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where(try subjectTerm("?group"), try predicateTerm(predicate), objectTerm("?value"))
            .groupBy("?group")
            .sum("?value", as: "totalValue")
            .execute()

        #expect(result.count == 1)
        #expect(result.firstAggregate("totalValue") == nil)
    }

    // MARK: - AVG Aggregate Tests

    @Test("AVG aggregate with integer values")
    func testAvgAggregateInteger() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let class1 = uniqueID("C1")
        let class2 = uniqueID("C2")
        let predicate = uniqueID("hasGrade")

        // Create edges with grades as "to" field values
        // Class1: 80, 90, 100 = avg 90
        // Class2: 70, 80 = avg 75
        let edges = [
            try makeEdge(from: class1, relationship: predicate, to: .integer(80)),
            try makeEdge(from: class1, relationship: predicate, to: .integer(90)),
            try makeEdge(from: class1, relationship: predicate, to: .integer(100)),
            try makeEdge(from: class2, relationship: predicate, to: .integer(70)),
            try makeEdge(from: class2, relationship: predicate, to: .integer(80)),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where(try subjectTerm("?class"), try predicateTerm(predicate), objectTerm("?grade"))
            .groupBy("?class")
            .avg("?grade", as: "avgGrade")
            .execute()

        #expect(result.count == 2)

        let class1Value = FieldValue.rdfTerm(try resource(class1))
        let class2Value = FieldValue.rdfTerm(try resource(class2))
        let c1Result = result.bindings.first {
            $0["?class"] == class1Value
        }
        let c2Result = result.bindings.first {
            $0["?class"] == class2Value
        }

        // Check class1 average = 90
        if let avgStr = c1Result?.string("avgGrade"), let avg = Double(avgStr) {
            #expect(abs(avg - 90.0) < 0.001)
        } else {
            Issue.record("Expected numeric average for class1")
        }

        // Check class2 average = 75
        if let avgStr = c2Result?.string("avgGrade"), let avg = Double(avgStr) {
            #expect(abs(avg - 75.0) < 0.001)
        } else {
            Issue.record("Expected numeric average for class2")
        }
    }

    @Test("AVG aggregate with decimal values")
    func testAvgAggregateDecimal() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let sensor = uniqueID("S")
        let predicate = uniqueID("hasReading")

        // Create edges with decimal readings: 23.5, 24.5, 25.0 = avg 24.333...
        let edges = [
            try makeEdge(from: sensor, relationship: predicate, to: .decimal(23.5)),
            try makeEdge(from: sensor, relationship: predicate, to: .decimal(24.5)),
            try makeEdge(from: sensor, relationship: predicate, to: .decimal(25.0)),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where(try subjectTerm("?sensor"), try predicateTerm(predicate), objectTerm("?reading"))
            .groupBy("?sensor")
            .avg("?reading", as: "avgReading")
            .execute()

        #expect(result.count == 1)

        let avgReading = result.firstAggregateString("avgReading")
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

        let context = container.newContext()

        let item = uniqueID("I")
        let predicate = uniqueID("hasValue")

        // Single value: 42
        let edges = [
            try makeEdge(from: item, relationship: predicate, to: .integer(42)),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where(try subjectTerm("?item"), try predicateTerm(predicate), objectTerm("?value"))
            .groupBy("?item")
            .avg("?value", as: "avgValue")
            .execute()

        #expect(result.count == 1)

        let avgValue = result.firstAggregateString("avgValue")
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

        let context = container.newContext()

        let group = uniqueID("G")
        let predicate = uniqueID("hasLabel")

        // Create edges with non-numeric values
        let edges = [
            try makeEdge(from: group, relationship: predicate, to: "alpha"),
            try makeEdge(from: group, relationship: predicate, to: "beta"),
            try makeEdge(from: group, relationship: predicate, to: "gamma"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where(try subjectTerm("?group"), try predicateTerm(predicate), objectTerm("?label"))
            .groupBy("?group")
            .avg("?label", as: "avgLabel")
            .execute()

        #expect(result.count == 1)

        // AVG of non-numeric values should return nil
        let avgLabel = result.firstAggregateString("avgLabel")
        // Non-numeric values cannot be averaged, so result should be nil
        #expect(avgLabel == nil)
    }

    // MARK: - Combined SUM and AVG Tests

    @Test("SUM and AVG combined in single query")
    func testSumAndAvgCombined() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let dept = uniqueID("D")
        let predicate = uniqueID("hasSalary")

        // Create employees with salaries: 50000, 60000, 70000
        // Sum = 180000, Avg = 60000
        let edges = [
            try makeEdge(from: dept, relationship: predicate, to: .integer(50_000)),
            try makeEdge(from: dept, relationship: predicate, to: .integer(60_000)),
            try makeEdge(from: dept, relationship: predicate, to: .integer(70_000)),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where(try subjectTerm("?dept"), try predicateTerm(predicate), objectTerm("?salary"))
            .groupBy("?dept")
            .sum("?salary", as: "totalSalary")
            .avg("?salary", as: "avgSalary")
            .count("?salary", as: "employeeCount")
            .execute()

        #expect(result.count == 1)

        let departmentValue = FieldValue.rdfTerm(try resource(dept))
        let deptResult = result.bindings.first {
            $0["?dept"] == departmentValue
        }
        #expect(deptResult != nil)

        #expect(deptResult?.string("totalSalary") == "180000")
        #expect(deptResult?.string("employeeCount") == "3")

        if let avgStr = deptResult?.string("avgSalary"), let avg = Double(avgStr) {
            #expect(abs(avg - 60000.0) < 0.001)
        } else {
            Issue.record("Expected numeric average salary")
        }
    }

    @Test("SUM aggregate with negative values")
    func testSumAggregateNegative() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let account = uniqueID("A")
        let predicate = uniqueID("hasBalance")

        // Create edges with positive and negative values: 100, -30, -20 = 50
        let edges = [
            try makeEdge(from: account, relationship: predicate, to: .integer(100)),
            try makeEdge(from: account, relationship: predicate, to: .integer(-30)),
            try makeEdge(from: account, relationship: predicate, to: .integer(-20)),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where(try subjectTerm("?account"), try predicateTerm(predicate), objectTerm("?balance"))
            .groupBy("?account")
            .sum("?balance", as: "netBalance")
            .execute()

        #expect(result.count == 1)

        let netBalance = result.firstAggregateString("netBalance")
        #expect(netBalance == "50")
    }

    @Test("AVG aggregate with zero values")
    func testAvgAggregateWithZeros() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let group = uniqueID("G")
        let predicate = uniqueID("hasValue")

        // Create edges with zeros and positive values: 0, 5, 10 = avg 5.0
        // Note: Graph index stores unique (from, edge, to) triples, so we need unique "to" values
        let edges = [
            try makeEdge(from: group, relationship: predicate, to: .integer(0)),
            try makeEdge(from: group, relationship: predicate, to: .integer(5)),
            try makeEdge(from: group, relationship: predicate, to: .integer(10)),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(SocialEdgeForGroupBy.self)
            .defaultIndex()
            .where(try subjectTerm("?group"), try predicateTerm(predicate), objectTerm("?value"))
            .groupBy("?group")
            .avg("?value", as: "avgValue")
            .execute()

        #expect(result.count == 1)

        let avgValue = result.firstAggregateString("avgValue")
        #expect(avgValue != nil)

        if let avgStr = avgValue, let avg = Double(avgStr) {
            #expect(abs(avg - 5.0) < 0.001)
        } else {
            Issue.record("Expected numeric average value")
        }
    }
}
#endif
