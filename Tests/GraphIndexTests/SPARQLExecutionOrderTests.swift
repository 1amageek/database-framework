// SPARQLExecutionOrderTests.swift
// GraphIndexTests - Tests for W3C SPARQL 1.1 Section 15 execution order
//
// Tests ORDER BY, MINUS execution, HAVING through executeSPARQLPattern,
// and filter pushdown correctness.

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
struct ExecOrderEdge {
    #Directory<ExecOrderEdge>("test", "sparql", "execorder")
    var id: String = UUID().uuidString
    var from: String = ""
    var edge: String = ""
    var to: String = ""

    #Index(GraphIndexKind<ExecOrderEdge>(
        from: \.from,
        edge: \.edge,
        to: \.to,
        strategy: .tripleStore
    ))
}

// MARK: - Test Suite

@Suite("SPARQL Execution Order Tests", .serialized)
struct SPARQLExecutionOrderTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    // MARK: - Helpers

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupContainer() async throws -> FDBContainer {
        let database = try FDBClient.openDatabase()
        let schema = Schema([ExecOrderEdge.self], version: Schema.Version(1, 0, 0))
        return FDBContainer(database: database, schema: schema, security: .disabled)
    }

    private func setIndexStatesToReadable(container: FDBContainer) async throws {
        let subspace = try await container.resolveDirectory(for: ExecOrderEdge.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)

        for descriptor in ExecOrderEdge.indexDescriptors {
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

    private func insertEdges(_ edges: [ExecOrderEdge], context: FDBContext) async throws {
        for edge in edges {
            context.insert(edge)
        }
        try await context.save()
    }

    private func makeEdge(from: String, edge: String, to: String) -> ExecOrderEdge {
        var e = ExecOrderEdge()
        e.from = from
        e.edge = edge
        e.to = to
        return e
    }

    // MARK: - ORDER BY Tests

    @Test("ORDER BY ascending sorts results correctly")
    func testOrderByAscending() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let agePred = uniqueID("age")
        let edges = [
            makeEdge(from: "Alice", edge: agePred, to: "30"),
            makeEdge(from: "Bob", edge: agePred, to: "25"),
            makeEdge(from: "Charlie", edge: agePred, to: "35"),
            makeEdge(from: "Diana", edge: agePred, to: "20"),
        ]
        try await insertEdges(edges, context: context)

        let result = try await context.sparql(ExecOrderEdge.self)
            .defaultIndex()
            .where("?person", agePred, "?age")
            .orderBy("?age")
            .execute()

        #expect(result.count == 4)

        let ages = result.bindings.compactMap { $0.string("?age") }
        #expect(ages == ["20", "25", "30", "35"])
    }

    @Test("ORDER BY descending sorts results correctly")
    func testOrderByDescending() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let scorePred = uniqueID("score")
        let edges = [
            makeEdge(from: "P1", edge: scorePred, to: "100"),
            makeEdge(from: "P2", edge: scorePred, to: "300"),
            makeEdge(from: "P3", edge: scorePred, to: "200"),
        ]
        try await insertEdges(edges, context: context)

        let result = try await context.sparql(ExecOrderEdge.self)
            .defaultIndex()
            .where("?player", scorePred, "?score")
            .orderByDesc("?score")
            .execute()

        #expect(result.count == 3)

        let scores = result.bindings.compactMap { $0.string("?score") }
        #expect(scores == ["300", "200", "100"])
    }

    @Test("ORDER BY with LIMIT respects order before limiting")
    func testOrderByWithLimit() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let rankPred = uniqueID("rank")
        // Create numeric ranks for consistent ordering
        let edges = [
            makeEdge(from: "ItemA", edge: rankPred, to: "3"),
            makeEdge(from: "ItemB", edge: rankPred, to: "1"),
            makeEdge(from: "ItemC", edge: rankPred, to: "5"),
            makeEdge(from: "ItemD", edge: rankPred, to: "2"),
            makeEdge(from: "ItemE", edge: rankPred, to: "4"),
        ]
        try await insertEdges(edges, context: context)

        // Get top 3 by rank (ascending)
        let result = try await context.sparql(ExecOrderEdge.self)
            .defaultIndex()
            .where("?item", rankPred, "?rank")
            .orderBy("?rank")
            .limit(3)
            .execute()

        #expect(result.count == 3)

        let ranks = result.bindings.compactMap { $0.string("?rank") }
        #expect(ranks == ["1", "2", "3"])
    }

    @Test("ORDER BY multiple keys")
    func testOrderByMultipleKeys() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let deptPred = uniqueID("department")
        let namePred = uniqueID("name")

        let edges = [
            makeEdge(from: "E1", edge: deptPred, to: "Sales"),
            makeEdge(from: "E1", edge: namePred, to: "Zach"),
            makeEdge(from: "E2", edge: deptPred, to: "Sales"),
            makeEdge(from: "E2", edge: namePred, to: "Alice"),
            makeEdge(from: "E3", edge: deptPred, to: "Engineering"),
            makeEdge(from: "E3", edge: namePred, to: "Bob"),
        ]
        try await insertEdges(edges, context: context)

        let result = try await context.sparql(ExecOrderEdge.self)
            .defaultIndex()
            .where("?emp", deptPred, "?dept")
            .where("?emp", namePred, "?name")
            .orderBy("?dept")
            .orderBy("?name")
            .execute()

        #expect(result.count == 3)

        let names = result.bindings.compactMap { $0.string("?name") }
        // Engineering first, then Sales; within Sales: Alice before Zach
        #expect(names == ["Bob", "Alice", "Zach"])
    }

    // MARK: - MINUS Execution Tests

    @Test("MINUS execution removes matching bindings")
    func testMinusExecution() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let typePred = uniqueID("type")
        let bannedPred = uniqueID("banned")

        let edges = [
            makeEdge(from: "User1", edge: typePred, to: "User"),
            makeEdge(from: "User2", edge: typePred, to: "User"),
            makeEdge(from: "User3", edge: typePred, to: "User"),
            makeEdge(from: "User2", edge: bannedPred, to: "true"),
        ]
        try await insertEdges(edges, context: context)

        // Build MINUS pattern: all users MINUS banned users
        let leftPattern = ExecutionPattern.basic([
            ExecutionTriple("?person", typePred, "User")
        ])
        let rightPattern = ExecutionPattern.basic([
            ExecutionTriple("?person", bannedPred, "true")
        ])
        let minusPattern = ExecutionPattern.minus(leftPattern, rightPattern)

        let result = try await context.executeSPARQLPattern(
            minusPattern,
            on: ExecOrderEdge.self
        )

        let users = Set(result.bindings.compactMap { $0.string("?person") })
        #expect(users.count == 2)
        #expect(users.contains("User1"))
        #expect(users.contains("User3"))
        #expect(!users.contains("User2"))
    }

    @Test("MINUS with no shared variables keeps all left bindings")
    func testMinusNoSharedVariables() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let predA = uniqueID("hasA")
        let predB = uniqueID("hasB")

        let edges = [
            makeEdge(from: "X1", edge: predA, to: "V1"),
            makeEdge(from: "X2", edge: predA, to: "V2"),
            makeEdge(from: "Y1", edge: predB, to: "V3"),
        ]
        try await insertEdges(edges, context: context)

        // ?x hasA ?a MINUS ?y hasB ?b (no shared variables)
        let leftPattern = ExecutionPattern.basic([
            ExecutionTriple("?x", predA, "?a")
        ])
        let rightPattern = ExecutionPattern.basic([
            ExecutionTriple("?y", predB, "?b")
        ])
        let minusPattern = ExecutionPattern.minus(leftPattern, rightPattern)

        let result = try await context.executeSPARQLPattern(
            minusPattern,
            on: ExecOrderEdge.self
        )

        // All left bindings should be kept (no shared variables = no exclusion)
        #expect(result.bindings.count == 2)
    }

    @Test("MINUS removes all when fully compatible")
    func testMinusRemovesAllCompatible() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let typePred = uniqueID("type")
        let flagPred = uniqueID("flag")

        let edges = [
            makeEdge(from: "Item1", edge: typePred, to: "Widget"),
            makeEdge(from: "Item2", edge: typePred, to: "Widget"),
            makeEdge(from: "Item1", edge: flagPred, to: "true"),
            makeEdge(from: "Item2", edge: flagPred, to: "true"),
        ]
        try await insertEdges(edges, context: context)

        // All widgets MINUS flagged items (all are flagged)
        let leftPattern = ExecutionPattern.basic([
            ExecutionTriple("?item", typePred, "Widget")
        ])
        let rightPattern = ExecutionPattern.basic([
            ExecutionTriple("?item", flagPred, "true")
        ])
        let minusPattern = ExecutionPattern.minus(leftPattern, rightPattern)

        let result = try await context.executeSPARQLPattern(
            minusPattern,
            on: ExecOrderEdge.self
        )

        #expect(result.bindings.isEmpty)
    }

    // MARK: - BindingSorter Unit Tests

    @Test("BindingSorter sorts by single key ascending")
    func testBindingSorterSingleKeyAsc() {
        let b1 = VariableBinding().binding("?x", to: .string("C"))
        let b2 = VariableBinding().binding("?x", to: .string("A"))
        let b3 = VariableBinding().binding("?x", to: .string("B"))

        let sorted = BindingSorter.sort([b1, b2, b3], by: [.variable("?x")])

        let values = sorted.compactMap { $0.string("?x") }
        #expect(values == ["A", "B", "C"])
    }

    @Test("BindingSorter sorts by single key descending")
    func testBindingSorterSingleKeyDesc() {
        let b1 = VariableBinding().binding("?x", to: .int64(1))
        let b2 = VariableBinding().binding("?x", to: .int64(3))
        let b3 = VariableBinding().binding("?x", to: .int64(2))

        let sorted = BindingSorter.sort([b1, b2, b3], by: [.variable("?x", ascending: false)])

        let values = sorted.compactMap { $0.int("?x") }
        #expect(values == [3, 2, 1])
    }

    @Test("BindingSorter handles nulls correctly - nulls first by default")
    func testBindingSorterNullsFirst() {
        let b1 = VariableBinding().binding("?x", to: .string("B"))
        let b2 = VariableBinding().binding("?x", to: .null)
        let b3 = VariableBinding().binding("?x", to: .string("A"))
        let b4 = VariableBinding()  // unbound

        let sorted = BindingSorter.sort([b1, b2, b3, b4], by: [.variable("?x")])

        // nil and .null should come first
        let first = sorted[0]["?x"]
        let second = sorted[1]["?x"]
        #expect(first == nil || first == .null)
        #expect(second == nil || second == .null)
        #expect(sorted[2]["?x"] == .string("A"))
        #expect(sorted[3]["?x"] == .string("B"))
    }

    @Test("BindingSorter nullsLast option")
    func testBindingSorterNullsLast() {
        let b1 = VariableBinding().binding("?x", to: .string("B"))
        let b2 = VariableBinding()  // unbound
        let b3 = VariableBinding().binding("?x", to: .string("A"))

        let sorted = BindingSorter.sort([b1, b2, b3], by: [
            .variable("?x", ascending: true, nullsLast: true)
        ])

        #expect(sorted[0]["?x"] == .string("A"))
        #expect(sorted[1]["?x"] == .string("B"))
        #expect(sorted[2]["?x"] == nil)
    }

    @Test("BindingSorter multiple keys")
    func testBindingSorterMultipleKeys() {
        let b1 = VariableBinding()
            .binding("?dept", to: .string("B"))
            .binding("?name", to: .string("Zach"))
        let b2 = VariableBinding()
            .binding("?dept", to: .string("A"))
            .binding("?name", to: .string("Bob"))
        let b3 = VariableBinding()
            .binding("?dept", to: .string("B"))
            .binding("?name", to: .string("Alice"))
        let b4 = VariableBinding()
            .binding("?dept", to: .string("A"))
            .binding("?name", to: .string("Charlie"))

        let sorted = BindingSorter.sort([b1, b2, b3, b4], by: [
            .variable("?dept"),
            .variable("?name")
        ])

        let names = sorted.compactMap { $0.string("?name") }
        #expect(names == ["Bob", "Charlie", "Alice", "Zach"])
    }

    // MARK: - GROUP BY with ORDER BY Tests

    @Test("GROUP BY with ORDER BY on aggregate")
    func testGroupByOrderByAggregate() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let memberPred = uniqueID("hasMember")
        var edges: [ExecOrderEdge] = []

        // GroupA: 3 members
        for i in 0..<3 {
            edges.append(makeEdge(from: "GroupA", edge: memberPred, to: uniqueID("M\(i)")))
        }
        // GroupB: 5 members
        for i in 0..<5 {
            edges.append(makeEdge(from: "GroupB", edge: memberPred, to: uniqueID("M\(i)")))
        }
        // GroupC: 1 member
        edges.append(makeEdge(from: "GroupC", edge: memberPred, to: uniqueID("M0")))

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(ExecOrderEdge.self)
            .defaultIndex()
            .where("?group", memberPred, "?member")
            .groupBy("?group")
            .count("?member", as: "cnt")
            .orderByDesc("cnt")
            .execute()

        #expect(result.count == 3)

        let counts = result.bindings.compactMap { $0.int("cnt") }
        #expect(counts == [5, 3, 1])
    }

    // MARK: - Filter Variable Tests

    @Test("FilterExpression.customWithVariables reports variables correctly")
    func testCustomWithVariablesReportsVariables() {
        let filter = FilterExpression.customWithVariables(
            { _ in true },
            variables: ["?x", "?y"]
        )

        #expect(filter.variables == ["?x", "?y"])
    }

    @Test("Filter on joined variable works correctly")
    func testFilterOnJoinedVariable() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let knowsPred = uniqueID("knows")
        let namePred = uniqueID("name")

        let edges = [
            makeEdge(from: "Alice", edge: knowsPred, to: "Bob"),
            makeEdge(from: "Alice", edge: knowsPred, to: "Charlie"),
            makeEdge(from: "Bob", edge: namePred, to: "Robert"),
            makeEdge(from: "Charlie", edge: namePred, to: "Charles"),
        ]
        try await insertEdges(edges, context: context)

        // Query: Alice knows ?friend, ?friend has name ?name
        // Filter on ?name
        let result = try await context.sparql(ExecOrderEdge.self)
            .defaultIndex()
            .where("Alice", knowsPred, "?friend")
            .where("?friend", namePred, "?name")
            .filter(.equals("?name", .string("Robert")))
            .execute()

        #expect(result.count == 1)
        #expect(result.bindings.first?.string("?friend") == "Bob")
    }
}
