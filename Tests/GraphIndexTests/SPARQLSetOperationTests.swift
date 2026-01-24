// SPARQLSetOperationTests.swift
// GraphIndexTests - Tests for SPARQL set operations (UNION, MINUS)
//
// Coverage: Multi-way UNION, UNION with different projections, MINUS, nested UNION, UNION + FILTER

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
struct SetOpTestEdge {
    #Directory<SetOpTestEdge>("test", "sparql", "setops")
    var id: String = UUID().uuidString
    var from: String = ""
    var relationship: String = ""
    var to: String = ""

    #Index(GraphIndexKind<SetOpTestEdge>(
        from: \.from,
        edge: \.relationship,
        to: \.to,
        strategy: .tripleStore
    ))
}

// MARK: - Test Suite

@Suite("SPARQL Set Operation Tests", .serialized)
struct SPARQLSetOperationTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    // MARK: - Helpers

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupContainer() async throws -> FDBContainer {
        let database = try FDBClient.openDatabase()
        let schema = Schema([SetOpTestEdge.self], version: Schema.Version(1, 0, 0))
        return FDBContainer(database: database, schema: schema, security: .disabled)
    }

    private func setIndexStatesToReadable(container: FDBContainer) async throws {
        let subspace = try await container.resolveDirectory(for: SetOpTestEdge.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)

        for descriptor in SetOpTestEdge.indexDescriptors {
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

    private func insertEdges(_ edges: [SetOpTestEdge], context: FDBContext) async throws {
        for edge in edges {
            context.insert(edge)
        }
        try await context.save()
    }

    private func makeEdge(from: String, relationship: String, to: String) -> SetOpTestEdge {
        var edge = SetOpTestEdge()
        edge.from = from
        edge.relationship = relationship
        edge.to = to
        return edge
    }

    // MARK: - Basic UNION Tests

    @Test("Simple 2-way UNION")
    func testSimpleTwoWayUnion() async throws {
        // { ?s :knows ?o } UNION { ?s :follows ?o }

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let dave = uniqueID("Dave")
        let knowsPred = uniqueID("knows")
        let followsPred = uniqueID("follows")

        let edges = [
            makeEdge(from: alice, relationship: knowsPred, to: bob),
            makeEdge(from: alice, relationship: followsPred, to: carol),
            makeEdge(from: alice, relationship: followsPred, to: dave),
        ]

        try await insertEdges(edges, context: context)

        // Query via knows
        let knowsResult = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where(alice, knowsPred, "?target")
            .execute()

        // Query via follows
        let followsResult = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where(alice, followsPred, "?target")
            .execute()

        // Simulate UNION: combine results
        var allTargets = Set<String>()
        for binding in knowsResult.bindings {
            if let target = binding["?target"] {
                allTargets.insert(target)
            }
        }
        for binding in followsResult.bindings {
            if let target = binding["?target"] {
                allTargets.insert(target)
            }
        }

        #expect(allTargets.count == 3)
        #expect(allTargets.contains(bob))
        #expect(allTargets.contains(carol))
        #expect(allTargets.contains(dave))
    }

    @Test("3-way UNION")
    func testThreeWayUnion() async throws {
        // { ?s :knows ?o } UNION { ?s :follows ?o } UNION { ?s :likes ?o }

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let dave = uniqueID("Dave")
        let eve = uniqueID("Eve")
        let knowsPred = uniqueID("knows")
        let followsPred = uniqueID("follows")
        let likesPred = uniqueID("likes")

        let edges = [
            makeEdge(from: alice, relationship: knowsPred, to: bob),
            makeEdge(from: alice, relationship: followsPred, to: carol),
            makeEdge(from: alice, relationship: likesPred, to: dave),
            makeEdge(from: alice, relationship: likesPred, to: eve),
        ]

        try await insertEdges(edges, context: context)

        // Query each separately and combine
        let knowsResult = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where(alice, knowsPred, "?target")
            .execute()

        let followsResult = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where(alice, followsPred, "?target")
            .execute()

        let likesResult = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where(alice, likesPred, "?target")
            .execute()

        // Combine all results
        var allTargets = Set<String>()
        for result in [knowsResult, followsResult, likesResult] {
            for binding in result.bindings {
                if let target = binding["?target"] {
                    allTargets.insert(target)
                }
            }
        }

        #expect(allTargets.count == 4)
        #expect(allTargets.contains(bob))
        #expect(allTargets.contains(carol))
        #expect(allTargets.contains(dave))
        #expect(allTargets.contains(eve))
    }

    // MARK: - UNION with Different Projections Tests

    @Test("UNION with aliased variables")
    func testUnionWithAliasedVariables() async throws {
        // { ?person :name ?name } UNION { ?person :nickname ?name }
        // Both patterns project to ?name

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let person = uniqueID("Person")
        let namePred = uniqueID("name")
        let nicknamePred = uniqueID("nickname")

        let edges = [
            makeEdge(from: person, relationship: namePred, to: "Robert"),
            makeEdge(from: person, relationship: nicknamePred, to: "Bob"),
            makeEdge(from: person, relationship: nicknamePred, to: "Bobby"),
        ]

        try await insertEdges(edges, context: context)

        // Get formal name
        let nameResult = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where(person, namePred, "?displayName")
            .execute()

        // Get nicknames
        let nicknameResult = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where(person, nicknamePred, "?displayName")
            .execute()

        // Combined
        var displayNames = Set<String>()
        for binding in nameResult.bindings {
            if let name = binding["?displayName"] {
                displayNames.insert(name)
            }
        }
        for binding in nicknameResult.bindings {
            if let name = binding["?displayName"] {
                displayNames.insert(name)
            }
        }

        #expect(displayNames.count == 3)
        #expect(displayNames.contains("Robert"))
        #expect(displayNames.contains("Bob"))
        #expect(displayNames.contains("Bobby"))
    }

    // MARK: - MINUS (Set Difference) Tests

    @Test("MINUS set difference")
    func testMinusSetDifference() async throws {
        // SELECT ?person WHERE {
        //   ?person a :User
        // } MINUS {
        //   ?person :status "banned"
        // }

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let typePred = uniqueID("type")
        let statusPred = uniqueID("status")

        let edges = [
            makeEdge(from: "User1", relationship: typePred, to: "User"),
            makeEdge(from: "User2", relationship: typePred, to: "User"),
            makeEdge(from: "User3", relationship: typePred, to: "User"),
            makeEdge(from: "User2", relationship: statusPred, to: "banned"),
        ]

        try await insertEdges(edges, context: context)

        // Get all users
        let allUsers = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("?person", typePred, "User")
            .execute()

        // Get banned users
        let bannedUsers = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("?person", statusPred, "banned")
            .execute()

        // Compute difference
        let allUserSet = Set(allUsers.bindings.compactMap { $0["?person"] })
        let bannedSet = Set(bannedUsers.bindings.compactMap { $0["?person"] })
        let activeUsers = allUserSet.subtracting(bannedSet)

        #expect(activeUsers.count == 2)
        #expect(activeUsers.contains("User1"))
        #expect(activeUsers.contains("User3"))
        #expect(!activeUsers.contains("User2"))
    }

    @Test("MINUS with no overlap")
    func testMinusNoOverlap() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let predA = uniqueID("hasA")
        let predB = uniqueID("hasB")

        let edges = [
            makeEdge(from: "E1", relationship: predA, to: "V1"),
            makeEdge(from: "E2", relationship: predA, to: "V2"),
            makeEdge(from: "E3", relationship: predB, to: "V3"),
        ]

        try await insertEdges(edges, context: context)

        // Get entities with A
        let withA = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("?entity", predA, "?val")
            .execute()

        // Get entities with B
        let withB = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("?entity", predB, "?val")
            .execute()

        let setA = Set(withA.bindings.compactMap { $0["?entity"] })
        let setB = Set(withB.bindings.compactMap { $0["?entity"] })
        let diff = setA.subtracting(setB)

        // E1, E2 have A but not B
        #expect(diff.count == 2)
    }

    @Test("MINUS removes all matches")
    func testMinusRemovesAll() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let predType = uniqueID("type")
        let predFlag = uniqueID("flagged")

        let edges = [
            makeEdge(from: "Item1", relationship: predType, to: "Widget"),
            makeEdge(from: "Item2", relationship: predType, to: "Widget"),
            makeEdge(from: "Item1", relationship: predFlag, to: "true"),
            makeEdge(from: "Item2", relationship: predFlag, to: "true"),
        ]

        try await insertEdges(edges, context: context)

        let widgets = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("?item", predType, "Widget")
            .execute()

        let flagged = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("?item", predFlag, "true")
            .execute()

        let widgetSet = Set(widgets.bindings.compactMap { $0["?item"] })
        let flaggedSet = Set(flagged.bindings.compactMap { $0["?item"] })
        let unflagged = widgetSet.subtracting(flaggedSet)

        // All widgets are flagged, so difference is empty
        #expect(unflagged.isEmpty)
    }

    // MARK: - UNION with FILTER Tests

    @Test("UNION with FILTER on each branch")
    func testUnionWithFilterOnBranches() async throws {
        // { ?s :price ?p . FILTER(?p < 100) }
        // UNION
        // { ?s :discount ?d . FILTER(?d > 0.5) }

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pricePred = uniqueID("price")
        let discountPred = uniqueID("discount")

        let edges = [
            makeEdge(from: "Product1", relationship: pricePred, to: "50"),
            makeEdge(from: "Product2", relationship: pricePred, to: "150"),
            makeEdge(from: "Product3", relationship: discountPred, to: "0.7"),
            makeEdge(from: "Product4", relationship: discountPred, to: "0.3"),
        ]

        try await insertEdges(edges, context: context)

        // Branch 1: price < 100
        let cheapProducts = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("?product", pricePred, "?price")
            .filter(.lessThan("?price", "100"))
            .execute()

        // Branch 2: discount > 0.5
        let highDiscountProducts = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("?product", discountPred, "?discount")
            .filter(.greaterThan("?discount", "0.5"))
            .execute()

        // Combine
        var qualifyingProducts = Set<String>()
        for binding in cheapProducts.bindings {
            if let product = binding["?product"] {
                qualifyingProducts.insert(product)
            }
        }
        for binding in highDiscountProducts.bindings {
            if let product = binding["?product"] {
                qualifyingProducts.insert(product)
            }
        }

        #expect(qualifyingProducts.count == 2)
        #expect(qualifyingProducts.contains("Product1"))  // cheap
        #expect(qualifyingProducts.contains("Product3"))  // high discount
    }

    @Test("UNION with common FILTER applied after")
    func testUnionWithCommonFilter() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let predA = uniqueID("valueA")
        let predB = uniqueID("valueB")

        let edges = [
            makeEdge(from: "E1", relationship: predA, to: "10"),
            makeEdge(from: "E2", relationship: predA, to: "30"),
            makeEdge(from: "E3", relationship: predB, to: "20"),
            makeEdge(from: "E4", relationship: predB, to: "40"),
        ]

        try await insertEdges(edges, context: context)

        // Get values from A
        let fromA = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("?entity", predA, "?val")
            .execute()

        // Get values from B
        let fromB = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("?entity", predB, "?val")
            .execute()

        // Combine and filter for val > 15
        var results = [(String, String)]()
        for binding in fromA.bindings {
            if let entity = binding["?entity"], let val = binding["?val"],
               let numVal = Int(val), numVal > 15 {
                results.append((entity, val))
            }
        }
        for binding in fromB.bindings {
            if let entity = binding["?entity"], let val = binding["?val"],
               let numVal = Int(val), numVal > 15 {
                results.append((entity, val))
            }
        }

        // E2 (30 from A), E3 (20 from B), E4 (40 from B) pass
        #expect(results.count == 3)
    }

    // MARK: - UNION with Duplicates Tests

    @Test("UNION removes duplicates")
    func testUnionRemovesDuplicates() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred1 = uniqueID("rel1")
        let pred2 = uniqueID("rel2")

        // Same entity appears via both predicates
        let edges = [
            makeEdge(from: "Source", relationship: pred1, to: "Target"),
            makeEdge(from: "Source", relationship: pred2, to: "Target"),
        ]

        try await insertEdges(edges, context: context)

        let result1 = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("Source", pred1, "?target")
            .execute()

        let result2 = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("Source", pred2, "?target")
            .execute()

        // Using Set automatically deduplicates
        var targets = Set<String>()
        for binding in result1.bindings {
            if let t = binding["?target"] { targets.insert(t) }
        }
        for binding in result2.bindings {
            if let t = binding["?target"] { targets.insert(t) }
        }

        #expect(targets.count == 1)
        #expect(targets.contains("Target"))
    }

    // MARK: - Empty Set Operations Tests

    @Test("UNION with empty left branch")
    func testUnionEmptyLeft() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred1 = uniqueID("emptyPred")
        let pred2 = uniqueID("hasSomething")

        let edges = [
            makeEdge(from: "Entity", relationship: pred2, to: "Value"),
        ]

        try await insertEdges(edges, context: context)

        let emptyResult = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("?e", pred1, "?v")
            .execute()

        let nonEmptyResult = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("?e", pred2, "?v")
            .execute()

        var combined = Set<String>()
        for binding in emptyResult.bindings {
            if let v = binding["?v"] { combined.insert(v) }
        }
        for binding in nonEmptyResult.bindings {
            if let v = binding["?v"] { combined.insert(v) }
        }

        #expect(combined.count == 1)
        #expect(combined.contains("Value"))
    }

    @Test("UNION with empty right branch")
    func testUnionEmptyRight() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred1 = uniqueID("hasSomething")
        let pred2 = uniqueID("emptyPred")

        let edges = [
            makeEdge(from: "Entity", relationship: pred1, to: "Value"),
        ]

        try await insertEdges(edges, context: context)

        let nonEmptyResult = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("?e", pred1, "?v")
            .execute()

        let emptyResult = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("?e", pred2, "?v")
            .execute()

        var combined = Set<String>()
        for binding in nonEmptyResult.bindings {
            if let v = binding["?v"] { combined.insert(v) }
        }
        for binding in emptyResult.bindings {
            if let v = binding["?v"] { combined.insert(v) }
        }

        #expect(combined.count == 1)
    }

    @Test("UNION both branches empty")
    func testUnionBothEmpty() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred1 = uniqueID("empty1")
        let pred2 = uniqueID("empty2")

        // No edges inserted

        let result1 = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("?e", pred1, "?v")
            .execute()

        let result2 = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("?e", pred2, "?v")
            .execute()

        #expect(result1.isEmpty)
        #expect(result2.isEmpty)
    }

    @Test("MINUS from empty set")
    func testMinusFromEmpty() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred1 = uniqueID("empty")
        let pred2 = uniqueID("something")

        let edges = [
            makeEdge(from: "E1", relationship: pred2, to: "V1"),
        ]

        try await insertEdges(edges, context: context)

        let emptyResult = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("?e", pred1, "?v")
            .execute()

        let nonEmptyResult = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("?e", pred2, "?v")
            .execute()

        let emptySet = Set(emptyResult.bindings.compactMap { $0["?e"] })
        let nonEmptySet = Set(nonEmptyResult.bindings.compactMap { $0["?e"] })

        // Empty MINUS nonEmpty = Empty
        let diff = emptySet.subtracting(nonEmptySet)
        #expect(diff.isEmpty)
    }

    @Test("MINUS empty set")
    func testMinusEmptySet() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let pred1 = uniqueID("something")
        let pred2 = uniqueID("empty")

        let edges = [
            makeEdge(from: "E1", relationship: pred1, to: "V1"),
            makeEdge(from: "E2", relationship: pred1, to: "V2"),
        ]

        try await insertEdges(edges, context: context)

        let nonEmptyResult = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("?e", pred1, "?v")
            .execute()

        let emptyResult = try await context.sparql(SetOpTestEdge.self)
            .defaultIndex()
            .where("?e", pred2, "?v")
            .execute()

        let nonEmptySet = Set(nonEmptyResult.bindings.compactMap { $0["?e"] })
        let emptySet = Set(emptyResult.bindings.compactMap { $0["?e"] })

        // NonEmpty MINUS Empty = NonEmpty
        let diff = nonEmptySet.subtracting(emptySet)
        #expect(diff.count == 2)
    }

    // MARK: - GraphPattern AST Tests

    @Test("GraphPattern UNION construction")
    func testGraphPatternUnionConstruction() throws {
        let pattern1 = GraphPattern.basic([
            TriplePattern(
                subject: .variable("s"),
                predicate: .iri("http://example.org/knows"),
                object: .variable("o")
            )
        ])

        let pattern2 = GraphPattern.basic([
            TriplePattern(
                subject: .variable("s"),
                predicate: .iri("http://example.org/follows"),
                object: .variable("o")
            )
        ])

        let union = GraphPattern.union(pattern1, pattern2)

        if case .union(let left, let right) = union {
            if case .basic(let leftPatterns) = left {
                #expect(leftPatterns.count == 1)
            }
            if case .basic(let rightPatterns) = right {
                #expect(rightPatterns.count == 1)
            }
        } else {
            Issue.record("Expected union pattern")
        }
    }

    @Test("GraphPattern MINUS construction")
    func testGraphPatternMinusConstruction() throws {
        let mainPattern = GraphPattern.basic([
            TriplePattern(
                subject: .variable("person"),
                predicate: .iri("http://example.org/type"),
                object: .iri("http://example.org/User")
            )
        ])

        let excludePattern = GraphPattern.basic([
            TriplePattern(
                subject: .variable("person"),
                predicate: .iri("http://example.org/status"),
                object: .literal(.string("banned"))
            )
        ])

        let minus = GraphPattern.minus(mainPattern, excludePattern)

        if case .minus(let main, let exclude) = minus {
            if case .basic(let mainPatterns) = main {
                #expect(mainPatterns.count == 1)
            }
            if case .basic(let excludePatterns) = exclude {
                #expect(excludePatterns.count == 1)
            }
        } else {
            Issue.record("Expected minus pattern")
        }
    }

    @Test("Nested UNION patterns")
    func testNestedUnionPatterns() throws {
        // { { ?a :p1 ?b } UNION { ?a :p2 ?b } } UNION { ?a :p3 ?b }

        let p1 = GraphPattern.basic([
            TriplePattern(subject: .variable("a"), predicate: .iri("p1"), object: .variable("b"))
        ])
        let p2 = GraphPattern.basic([
            TriplePattern(subject: .variable("a"), predicate: .iri("p2"), object: .variable("b"))
        ])
        let p3 = GraphPattern.basic([
            TriplePattern(subject: .variable("a"), predicate: .iri("p3"), object: .variable("b"))
        ])

        let innerUnion = GraphPattern.union(p1, p2)
        let outerUnion = GraphPattern.union(innerUnion, p3)

        if case .union(let left, let right) = outerUnion {
            if case .union(_, _) = left {
                // Nested union on left
            } else {
                Issue.record("Expected nested union on left")
            }
            if case .basic(_) = right {
                // Simple pattern on right
            } else {
                Issue.record("Expected basic pattern on right")
            }
        } else {
            Issue.record("Expected outer union")
        }
    }
}
