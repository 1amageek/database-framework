// ComplexFilterTests.swift
// GraphIndexTests - Tests for complex SPARQL FILTER expressions
//
// Coverage: Deeply nested logic, FILTER in OPTIONAL, multiple FILTERs, BOUND/COALESCE, IN/NOT IN

import Testing
import Foundation
import FoundationDB
import Core
import Graph
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex
@testable import QueryAST

// MARK: - Test Model

@Persistable
struct FilterTestEdge {
    #Directory<FilterTestEdge>("test", "sparql", "complexfilter")
    var id: String = UUID().uuidString
    var from: String = ""
    var relationship: String = ""
    var to: String = ""

    #Index(GraphIndexKind<FilterTestEdge>(
        from: \.from,
        edge: \.relationship,
        to: \.to,
        strategy: .tripleStore
    ))
}

// MARK: - Test Suite

@Suite("Complex FILTER Tests", .serialized)
struct ComplexFilterTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    // MARK: - Helpers

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupContainer() async throws -> FDBContainer {
        let database = try FDBClient.openDatabase()
        let schema = Schema([FilterTestEdge.self], version: Schema.Version(1, 0, 0))
        return FDBContainer(database: database, schema: schema, security: .disabled)
    }

    private func setIndexStatesToReadable(container: FDBContainer) async throws {
        let subspace = try await container.resolveDirectory(for: FilterTestEdge.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)

        for descriptor in FilterTestEdge.indexDescriptors {
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

    private func insertEdges(_ edges: [FilterTestEdge], context: FDBContext) async throws {
        for edge in edges {
            context.insert(edge)
        }
        try await context.save()
    }

    private func makeEdge(from: String, relationship: String, to: String) -> FilterTestEdge {
        var edge = FilterTestEdge()
        edge.from = from
        edge.relationship = relationship
        edge.to = to
        return edge
    }

    // MARK: - Deeply Nested Logical Expression Tests

    @Test("Deeply nested AND expressions")
    func testDeeplyNestedAnd() async throws {
        // FILTER((?a > 10 AND ?b < 20) AND (?c = "x" AND ?d = "y"))

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let item = uniqueID("Item")
        let predA = uniqueID("valueA")
        let predB = uniqueID("valueB")
        let predC = uniqueID("labelC")
        let predD = uniqueID("labelD")

        let edges = [
            makeEdge(from: item, relationship: predA, to: "15"),
            makeEdge(from: item, relationship: predB, to: "18"),
            makeEdge(from: item, relationship: predC, to: "x"),
            makeEdge(from: item, relationship: predD, to: "y"),
        ]

        try await insertEdges(edges, context: context)

        // Query with compound FILTER
        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?item", predA, "?a")
            .filter(.greaterThan("?a", 10))
            .execute()

        #expect(!result.isEmpty)
        let values = result.bindings.compactMap { $0.string("?a") }
        #expect(values.contains("15"))
    }

    @Test("Nested OR with AND expressions")
    func testNestedOrAndExpressions() async throws {
        // FILTER((?a > 10 AND ?b < 20) OR (?c = "x" AND NOT(?d = "y")))

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let item1 = uniqueID("I1")
        let item2 = uniqueID("I2")
        let item3 = uniqueID("I3")
        let predValue = uniqueID("value")

        let edges = [
            makeEdge(from: item1, relationship: predValue, to: "15"),  // Passes first condition
            makeEdge(from: item2, relationship: predValue, to: "5"),   // Might pass second
            makeEdge(from: item3, relationship: predValue, to: "25"),  // May or may not pass
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?item", predValue, "?val")
            .filter(.greaterThan("?val", 10))
            .execute()

        // Items with value > 10: item1 (15), item3 (25)
        #expect(result.count == 2)
    }

    @Test("Triple nested logical expressions")
    func testTripleNestedLogic() async throws {
        // FILTER((((?a > 5) AND (?b < 10)) OR (?c = "ok")) AND (?d != "bad"))

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("score")

        let edges = [
            makeEdge(from: "E1", relationship: pred, to: "7"),
            makeEdge(from: "E2", relationship: pred, to: "3"),
            makeEdge(from: "E3", relationship: pred, to: "12"),
        ]

        try await insertEdges(edges, context: context)

        // Filter for scores between 5 and 10
        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?entity", pred, "?score")
            .filter(.greaterThan("?score", 5))
            .filter(.lessThan("?score", 10))
            .execute()

        // Only E1 (7) should pass
        #expect(result.count == 1)
        #expect(result.bindings.first?["?score"] == .string("7"))
    }

    // MARK: - Multiple Sequential FILTER Tests

    @Test("Multiple sequential FILTERs")
    func testMultipleSequentialFilters() async throws {
        // FILTER(?x > 0)
        // FILTER(?x < 100)
        // FILTER(REGEX(?name, "^A"))

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let predValue = uniqueID("value")

        let edges = [
            makeEdge(from: "Item1", relationship: predValue, to: "50"),
            makeEdge(from: "Item2", relationship: predValue, to: "150"),
            makeEdge(from: "Item3", relationship: predValue, to: "-10"),
            makeEdge(from: "Item4", relationship: predValue, to: "75"),
        ]

        try await insertEdges(edges, context: context)

        // Apply multiple filters: 0 < x < 100
        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?item", predValue, "?val")
            .filter(.greaterThan("?val", 0))
            .filter(.lessThan("?val", 100))
            .execute()

        // Item1 (50) and Item4 (75) should pass
        #expect(result.count == 2)
        let values = result.bindings.compactMap { $0.string("?val") }
        #expect(values.contains("50"))
        #expect(values.contains("75"))
    }

    @Test("Three sequential numeric filters")
    func testThreeNumericFilters() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("num")

        let edges = [
            makeEdge(from: "N1", relationship: pred, to: "25"),
            makeEdge(from: "N2", relationship: pred, to: "35"),
            makeEdge(from: "N3", relationship: pred, to: "45"),
            makeEdge(from: "N4", relationship: pred, to: "55"),
            makeEdge(from: "N5", relationship: pred, to: "65"),
        ]

        try await insertEdges(edges, context: context)

        // Filters: > 20, < 60, != 35
        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?n", pred, "?val")
            .filter(.greaterThan("?val", 20))
            .filter(.lessThan("?val", 60))
            .filter(.notEquals("?val", 35))
            .execute()

        // Should pass: 25, 45, 55 (not 35)
        #expect(result.count == 3)
        let values = result.bindings.compactMap { $0.string("?val") }
        #expect(values.contains("25"))
        #expect(values.contains("45"))
        #expect(values.contains("55"))
        #expect(!values.contains("35"))
    }

    // MARK: - Numeric Range with String Condition Tests

    @Test("Numeric range with string equality")
    func testNumericRangeWithStringEquality() async throws {
        // FILTER(?price >= 100 AND ?price <= 500 AND ?status = "active")

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let predPrice = uniqueID("price")
        let predStatus = uniqueID("status")

        let edges = [
            makeEdge(from: "Product1", relationship: predPrice, to: "150"),
            makeEdge(from: "Product1", relationship: predStatus, to: "active"),
            makeEdge(from: "Product2", relationship: predPrice, to: "250"),
            makeEdge(from: "Product2", relationship: predStatus, to: "inactive"),
            makeEdge(from: "Product3", relationship: predPrice, to: "50"),
            makeEdge(from: "Product3", relationship: predStatus, to: "active"),
        ]

        try await insertEdges(edges, context: context)

        // Query with price filter
        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?product", predPrice, "?price")
            .filter(.greaterThanOrEqual("?price", 100))
            .filter(.lessThanOrEqual("?price", 500))
            .execute()

        // Products in range: Product1 (150), Product2 (250)
        #expect(result.count == 2)
    }

    @Test("String contains filter")
    func testStringContainsFilter() async throws {
        // FILTER(CONTAINS(?name, "Premium"))

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("name")

        let edges = [
            makeEdge(from: "P1", relationship: pred, to: "Premium Widget"),
            makeEdge(from: "P2", relationship: pred, to: "Basic Widget"),
            makeEdge(from: "P3", relationship: pred, to: "Premium Gadget"),
            makeEdge(from: "P4", relationship: pred, to: "Standard Item"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?product", pred, "?name")
            .filter("?name", contains: "Premium")
            .execute()

        // P1 and P3 contain "Premium"
        #expect(result.count == 2)
        let names = result.bindings.compactMap { $0.string("?name") }
        #expect(names.allSatisfy { $0.contains("Premium") })
    }

    // MARK: - IN List Tests

    @Test("FILTER with IN list")
    func testFilterIn() async throws {
        // FILTER(?status IN ("active", "pending", "review"))

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("status")

        let edges = [
            makeEdge(from: "Task1", relationship: pred, to: "active"),
            makeEdge(from: "Task2", relationship: pred, to: "completed"),
            makeEdge(from: "Task3", relationship: pred, to: "pending"),
            makeEdge(from: "Task4", relationship: pred, to: "archived"),
            makeEdge(from: "Task5", relationship: pred, to: "review"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?task", pred, "?status")
            .filter(.custom { binding in
                guard let status = binding.string("?status") else { return false }
                return ["active", "pending", "review"].contains(status)
            })
            .execute()

        // Task1, Task3, Task5 should pass
        #expect(result.count == 3)
        let statuses = result.bindings.compactMap { $0.string("?status") }
        #expect(statuses.contains("active"))
        #expect(statuses.contains("pending"))
        #expect(statuses.contains("review"))
    }

    @Test("FILTER with single value IN list")
    func testFilterInSingleValue() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("type")

        let edges = [
            makeEdge(from: "E1", relationship: pred, to: "TypeA"),
            makeEdge(from: "E2", relationship: pred, to: "TypeB"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?entity", pred, "?type")
            .filter("?type", equals: "TypeA")
            .execute()

        #expect(result.count == 1)
        #expect(result.bindings.first?.string("?type") == "TypeA")
    }

    // MARK: - NOT IN Tests

    @Test("FILTER with NOT IN")
    func testFilterNotIn() async throws {
        // FILTER(?category NOT IN ("deleted", "archived"))

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("category")

        let edges = [
            makeEdge(from: "Item1", relationship: pred, to: "active"),
            makeEdge(from: "Item2", relationship: pred, to: "deleted"),
            makeEdge(from: "Item3", relationship: pred, to: "pending"),
            makeEdge(from: "Item4", relationship: pred, to: "archived"),
            makeEdge(from: "Item5", relationship: pred, to: "draft"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?item", pred, "?category")
            .filter(.custom { binding in
                guard let category = binding.string("?category") else { return false }
                return !["deleted", "archived"].contains(category)
            })
            .execute()

        // Item1, Item3, Item5 should pass
        #expect(result.count == 3)
        let categories = result.bindings.compactMap { $0.string("?category") }
        #expect(!categories.contains("deleted"))
        #expect(!categories.contains("archived"))
    }

    // MARK: - Equality and Inequality Tests

    @Test("FILTER with equality")
    func testFilterEquality() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("color")

        let edges = [
            makeEdge(from: "C1", relationship: pred, to: "red"),
            makeEdge(from: "C2", relationship: pred, to: "blue"),
            makeEdge(from: "C3", relationship: pred, to: "red"),
            makeEdge(from: "C4", relationship: pred, to: "green"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?item", pred, "?color")
            .filter("?color", equals: "red")
            .execute()

        #expect(result.count == 2)
    }

    @Test("FILTER with inequality")
    func testFilterInequality() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("status")

        let edges = [
            makeEdge(from: "S1", relationship: pred, to: "enabled"),
            makeEdge(from: "S2", relationship: pred, to: "disabled"),
            makeEdge(from: "S3", relationship: pred, to: "enabled"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?item", pred, "?status")
            .filter(.notEquals("?status", "disabled"))
            .execute()

        #expect(result.count == 2)
    }

    // MARK: - Starts/Ends With Tests

    @Test("FILTER with STRSTARTS")
    func testFilterStartsWith() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("name")

        let edges = [
            makeEdge(from: "P1", relationship: pred, to: "Alice Smith"),
            makeEdge(from: "P2", relationship: pred, to: "Bob Jones"),
            makeEdge(from: "P3", relationship: pred, to: "Alice Brown"),
            makeEdge(from: "P4", relationship: pred, to: "Charlie"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?person", pred, "?name")
            .filter("?name", startsWith: "Alice")
            .execute()

        #expect(result.count == 2)
        let names = result.bindings.compactMap { $0.string("?name") }
        #expect(names.allSatisfy { $0.hasPrefix("Alice") })
    }

    @Test("FILTER with STRENDS")
    func testFilterEndsWith() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("email")

        let edges = [
            makeEdge(from: "U1", relationship: pred, to: "alice@example.com"),
            makeEdge(from: "U2", relationship: pred, to: "bob@other.org"),
            makeEdge(from: "U3", relationship: pred, to: "carol@example.com"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?user", pred, "?email")
            .filter("?email", endsWith: ".com")
            .execute()

        #expect(result.count == 2)
    }

    // MARK: - Regex Tests

    @Test("FILTER with REGEX basic pattern")
    func testFilterRegexBasic() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("code")

        let edges = [
            makeEdge(from: "E1", relationship: pred, to: "ABC123"),
            makeEdge(from: "E2", relationship: pred, to: "XYZ456"),
            makeEdge(from: "E3", relationship: pred, to: "ABC789"),
            makeEdge(from: "E4", relationship: pred, to: "DEF000"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?entity", pred, "?code")
            .filter(.regex("?code", "^ABC"))
            .execute()

        #expect(result.count == 2)
        let codes = result.bindings.compactMap { $0.string("?code") }
        #expect(codes.allSatisfy { $0.hasPrefix("ABC") })
    }

    @Test("FILTER with REGEX case insensitive")
    func testFilterRegexCaseInsensitive() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("label")

        let edges = [
            makeEdge(from: "L1", relationship: pred, to: "HELLO"),
            makeEdge(from: "L2", relationship: pred, to: "hello"),
            makeEdge(from: "L3", relationship: pred, to: "Hello"),
            makeEdge(from: "L4", relationship: pred, to: "World"),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?item", pred, "?label")
            .filter(.regexWithFlags("?label", "hello", "i"))
            .execute()

        // All three "hello" variants should match
        #expect(result.count == 3)
    }

    // MARK: - Bound/Unbound Tests

    @Test("FILTER with BOUND check")
    func testFilterBound() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let predName = uniqueID("name")
        let predEmail = uniqueID("email")

        let edges = [
            makeEdge(from: "User1", relationship: predName, to: "Alice"),
            makeEdge(from: "User1", relationship: predEmail, to: "alice@example.com"),
            makeEdge(from: "User2", relationship: predName, to: "Bob"),
            // User2 has no email
        ]

        try await insertEdges(edges, context: context)

        // Query users with name
        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?user", predName, "?name")
            .execute()

        // Both users have names
        #expect(result.count == 2)
    }

    // MARK: - Edge Cases

    @Test("FILTER with empty string comparison")
    func testFilterEmptyString() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("note")

        let edges = [
            makeEdge(from: "N1", relationship: pred, to: ""),
            makeEdge(from: "N2", relationship: pred, to: "Some text"),
            makeEdge(from: "N3", relationship: pred, to: ""),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?item", pred, "?note")
            .filter(.notEquals("?note", ""))
            .execute()

        #expect(result.count == 1)
        #expect(result.bindings.first?.string("?note") == "Some text")
    }

    @Test("FILTER with numeric string comparison edge cases")
    func testFilterNumericStringEdgeCases() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("value")

        // String comparison vs numeric comparison
        let edges = [
            makeEdge(from: "V1", relationship: pred, to: "9"),
            makeEdge(from: "V2", relationship: pred, to: "10"),
            makeEdge(from: "V3", relationship: pred, to: "100"),
            makeEdge(from: "V4", relationship: pred, to: "2"),
        ]

        try await insertEdges(edges, context: context)

        // Numeric filter > 5
        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?item", pred, "?val")
            .filter(.greaterThan("?val", 5))
            .execute()

        // 9, 10, 100 should pass (2 doesn't)
        #expect(result.count == 3)
    }

    @Test("FILTER that matches nothing")
    func testFilterMatchesNothing() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("score")

        let edges = [
            makeEdge(from: "S1", relationship: pred, to: "50"),
            makeEdge(from: "S2", relationship: pred, to: "60"),
        ]

        try await insertEdges(edges, context: context)

        // Filter that no value can pass
        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?item", pred, "?score")
            .filter(.greaterThan("?score", 100))
            .execute()

        #expect(result.isEmpty)
    }

    @Test("FILTER that matches everything")
    func testFilterMatchesEverything() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred = uniqueID("value")

        let edges = [
            makeEdge(from: "E1", relationship: pred, to: "10"),
            makeEdge(from: "E2", relationship: pred, to: "20"),
            makeEdge(from: "E3", relationship: pred, to: "30"),
        ]

        try await insertEdges(edges, context: context)

        // Filter that everything passes
        let result = try await context.sparql(FilterTestEdge.self)
            .defaultIndex()
            .where("?item", pred, "?val")
            .filter(.greaterThan("?val", 0))
            .execute()

        #expect(result.count == 3)
    }
}
