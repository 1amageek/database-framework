#if FOUNDATION_DB
// SPARQLExecutionOrderTests.swift
// GraphIndexTests - Tests for W3C SPARQL 1.1 Section 15 execution order
//
// Tests ORDER BY, MINUS execution, HAVING through executeSPARQLPattern,
// and filter pushdown correctness.

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
@testable import QueryAST

// MARK: - Test Model

@Persistable
struct ExecOrderEdge {
    #Directory<ExecOrderEdge>("sparql_execution_order_tests")
    var id: String = UUID().uuidString
    var subject: RDFTerm = .iri(.xsdString)
    var predicate: RDFTerm = .iri(.xsdString)
    var object: RDFTerm = .string("")

    #Index(
        .rdfDataset,
        from: \ExecOrderEdge.subject,
        edge: \ExecOrderEdge.predicate,
        to: \ExecOrderEdge.object
    )
}

// MARK: - Test Suite

@Suite("SPARQL Execution Order Tests", .serialized, .foundationDBScenario, .heartbeat)
struct SPARQLExecutionOrderTests {

    init() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
    }

    // MARK: - Helpers

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func resource(
        _ identifier: String
    ) throws -> RDFTerm {
        try .iri(
            validating:
                "https://example.invalid/resource/\(identifier)"
        )
    }

    private func predicate(_ identifier: String) throws -> RDFPredicateIRI {
        try RDFPredicateIRI(
            "https://example.invalid/predicate/\(identifier)"
        )
    }

    private func value(_ term: RDFTerm) -> ExecutionTerm {
        .value(.rdfTerm(term))
    }

    private func literalValue(
        _ binding: VariableBinding,
        for variable: String
    ) -> String? {
        guard case .rdfTerm(.literal(let literal)) = binding[variable] else {
            return nil
        }
        return literal.lexicalForm
    }

    private func setupContainer() async throws -> DBContainer {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [try ExecOrderEdge.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        return try await DBContainer.open(
            testing: schema,
            configuration: .testing(backend: .custom(database)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(ExecOrderEdge.self)]),
            security: .disabled,
        )
    }

    private func insertEdges(_ edges: [ExecOrderEdge], context: DatabaseContext) async throws {
        for edge in edges {
            try context.insert(edge)
        }
        try await context.save()
    }

    private func makeEdge(
        from: String,
        edge: RDFPredicateIRI,
        to: RDFTerm
    ) throws -> ExecOrderEdge {
        var statement = ExecOrderEdge()
        statement.subject = try resource(from)
        statement.predicate = edge.term
        statement.object = to
        return statement
    }

    // MARK: - ORDER BY Tests

    @Test("ORDER BY ascending sorts results correctly")
    func testOrderByAscending() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let agePred = try predicate(uniqueID("age"))
        let edges = [
            try makeEdge(from: "Alice", edge: agePred, to: .string("30")),
            try makeEdge(from: "Bob", edge: agePred, to: .string("25")),
            try makeEdge(from: "Charlie", edge: agePred, to: .string("35")),
            try makeEdge(from: "Diana", edge: agePred, to: .string("20")),
        ]
        try await insertEdges(edges, context: context)

        let result = try await context.sparql(ExecOrderEdge.self)
            .defaultIndex()
            .where(.variable("?person"), value(agePred.term), .variable("?age"))
            .orderBy("?age")
            .execute()

        #expect(result.count == 4)

        let ages = result.bindings.compactMap {
            literalValue($0, for: "?age")
        }
        #expect(ages == ["20", "25", "30", "35"])
    }

    @Test("ORDER BY descending sorts results correctly")
    func testOrderByDescending() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let scorePred = try predicate(uniqueID("score"))
        let edges = [
            try makeEdge(from: "P1", edge: scorePred, to: .string("100")),
            try makeEdge(from: "P2", edge: scorePred, to: .string("300")),
            try makeEdge(from: "P3", edge: scorePred, to: .string("200")),
        ]
        try await insertEdges(edges, context: context)

        let result = try await context.sparql(ExecOrderEdge.self)
            .defaultIndex()
            .where(.variable("?player"), value(scorePred.term), .variable("?score"))
            .orderByDesc("?score")
            .execute()

        #expect(result.count == 3)

        let scores = result.bindings.compactMap {
            literalValue($0, for: "?score")
        }
        #expect(scores == ["300", "200", "100"])
    }

    @Test("ORDER BY with LIMIT respects order before limiting")
    func testOrderByWithLimit() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let rankPred = try predicate(uniqueID("rank"))
        // Create numeric ranks for consistent ordering
        let edges = [
            try makeEdge(from: "ItemA", edge: rankPred, to: .string("3")),
            try makeEdge(from: "ItemB", edge: rankPred, to: .string("1")),
            try makeEdge(from: "ItemC", edge: rankPred, to: .string("5")),
            try makeEdge(from: "ItemD", edge: rankPred, to: .string("2")),
            try makeEdge(from: "ItemE", edge: rankPred, to: .string("4")),
        ]
        try await insertEdges(edges, context: context)

        // Get top 3 by rank (ascending)
        let result = try await context.sparql(ExecOrderEdge.self)
            .defaultIndex()
            .where(.variable("?item"), value(rankPred.term), .variable("?rank"))
            .orderBy("?rank")
            .limit(3)
            .execute()

        #expect(result.count == 3)

        let ranks = result.bindings.compactMap {
            literalValue($0, for: "?rank")
        }
        #expect(ranks == ["1", "2", "3"])
    }

    @Test("ORDER BY multiple keys")
    func testOrderByMultipleKeys() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let deptPred = try predicate(uniqueID("department"))
        let namePred = try predicate(uniqueID("name"))

        let edges = [
            try makeEdge(from: "E1", edge: deptPred, to: .string("Sales")),
            try makeEdge(from: "E1", edge: namePred, to: .string("Zach")),
            try makeEdge(from: "E2", edge: deptPred, to: .string("Sales")),
            try makeEdge(from: "E2", edge: namePred, to: .string("Alice")),
            try makeEdge(from: "E3", edge: deptPred, to: .string("Engineering")),
            try makeEdge(from: "E3", edge: namePred, to: .string("Bob")),
        ]
        try await insertEdges(edges, context: context)

        let result = try await context.sparql(ExecOrderEdge.self)
            .defaultIndex()
            .where(.variable("?emp"), value(deptPred.term), .variable("?dept"))
            .where(.variable("?emp"), value(namePred.term), .variable("?name"))
            .orderBy("?dept")
            .orderBy("?name")
            .execute()

        #expect(result.count == 3)

        let names = result.bindings.compactMap {
            literalValue($0, for: "?name")
        }
        // Engineering first, then Sales; within Sales: Alice before Zach
        #expect(names == ["Bob", "Alice", "Zach"])
    }

    // MARK: - MINUS Execution Tests

    @Test("MINUS execution removes matching bindings")
    func testMinusExecution() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let typePred = try predicate(uniqueID("type"))
        let bannedPred = try predicate(uniqueID("banned"))

        let edges = [
            try makeEdge(
                from: "User1",
                edge: typePred,
                to: resource("User")
            ),
            try makeEdge(
                from: "User2",
                edge: typePred,
                to: resource("User")
            ),
            try makeEdge(
                from: "User3",
                edge: typePred,
                to: resource("User")
            ),
            try makeEdge(
                from: "User2",
                edge: bannedPred,
                to: .boolean(true)
            ),
        ]
        try await insertEdges(edges, context: context)

        // Build MINUS pattern: all users MINUS banned users
        let leftPattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?person"),
                predicate: value(typePred.term),
                object: value(try resource("User"))
            )
        ])
        let rightPattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?person"),
                predicate: value(bannedPred.term),
                object: value(.boolean(true))
            )
        ])
        let minusPattern = ExecutionPattern.minus(leftPattern, rightPattern)

        let result = try await context.executeSPARQLPattern(
            minusPattern,
            on: ExecOrderEdge.self
        )

        let users = Set(result.bindings.compactMap { $0["?person"] })
        #expect(users.count == 2)
        #expect(users.contains(.rdfTerm(try resource("User1"))))
        #expect(users.contains(.rdfTerm(try resource("User3"))))
        #expect(!users.contains(.rdfTerm(try resource("User2"))))
    }

    @Test("MINUS with no shared variables keeps all left bindings")
    func testMinusNoSharedVariables() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let predA = try predicate(uniqueID("hasA"))
        let predB = try predicate(uniqueID("hasB"))

        let edges = [
            try makeEdge(from: "X1", edge: predA, to: .string("V1")),
            try makeEdge(from: "X2", edge: predA, to: .string("V2")),
            try makeEdge(from: "Y1", edge: predB, to: .string("V3")),
        ]
        try await insertEdges(edges, context: context)

        // ?x hasA ?a MINUS ?y hasB ?b (no shared variables)
        let leftPattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?x"),
                predicate: value(predA.term),
                object: .variable("?a")
            )
        ])
        let rightPattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?y"),
                predicate: value(predB.term),
                object: .variable("?b")
            )
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

        let context = container.newContext()

        let typePred = try predicate(uniqueID("type"))
        let flagPred = try predicate(uniqueID("flag"))

        let edges = [
            try makeEdge(
                from: "Item1",
                edge: typePred,
                to: resource("Widget")
            ),
            try makeEdge(
                from: "Item2",
                edge: typePred,
                to: resource("Widget")
            ),
            try makeEdge(
                from: "Item1",
                edge: flagPred,
                to: .boolean(true)
            ),
            try makeEdge(
                from: "Item2",
                edge: flagPred,
                to: .boolean(true)
            ),
        ]
        try await insertEdges(edges, context: context)

        // All widgets MINUS flagged items (all are flagged)
        let leftPattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?item"),
                predicate: value(typePred.term),
                object: value(try resource("Widget"))
            )
        ])
        let rightPattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?item"),
                predicate: value(flagPred.term),
                object: value(.boolean(true))
            )
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
    func testBindingSorterSingleKeyAsc() throws {
        let b1 = VariableBinding().binding("?x", to: .string("C"))
        let b2 = VariableBinding().binding("?x", to: .string("A"))
        let b3 = VariableBinding().binding("?x", to: .string("B"))
        let workMeter = DatabaseWorkMeter(
            budget: .init(),
            monotonicClock: TestProcessMonotonicClock()
        )

        let sorted = try BindingSorter.sort(
            [b1, b2, b3],
            by: [.variable("?x")],
            workMeter: workMeter
        )

        let values = sorted.compactMap { $0.string("?x") }
        #expect(values == ["A", "B", "C"])
    }

    @Test("BindingSorter sorts by single key descending")
    func testBindingSorterSingleKeyDesc() throws {
        let b1 = VariableBinding().binding("?x", to: .int64(1))
        let b2 = VariableBinding().binding("?x", to: .int64(3))
        let b3 = VariableBinding().binding("?x", to: .int64(2))
        let workMeter = DatabaseWorkMeter(
            budget: .init(),
            monotonicClock: TestProcessMonotonicClock()
        )

        let sorted = try BindingSorter.sort(
            [b1, b2, b3],
            by: [.variable("?x", ascending: false)],
            workMeter: workMeter
        )

        let values = sorted.compactMap { $0.int("?x") }
        #expect(values == [3, 2, 1])
    }

    @Test("BindingSorter handles nulls correctly - nulls first by default")
    func testBindingSorterNullsFirst() throws {
        let b1 = VariableBinding().binding("?x", to: .string("B"))
        let b2 = VariableBinding().binding("?x", to: .null)
        let b3 = VariableBinding().binding("?x", to: .string("A"))
        let b4 = VariableBinding()  // unbound
        let workMeter = DatabaseWorkMeter(
            budget: .init(),
            monotonicClock: TestProcessMonotonicClock()
        )

        let sorted = try BindingSorter.sort(
            [b1, b2, b3, b4],
            by: [.variable("?x")],
            workMeter: workMeter
        )

        // nil and .null should come first
        let first = sorted[0]["?x"]
        let second = sorted[1]["?x"]
        #expect(first == nil || first == .null)
        #expect(second == nil || second == .null)
        #expect(sorted[2]["?x"] == .string("A"))
        #expect(sorted[3]["?x"] == .string("B"))
    }

    @Test("BindingSorter nullsLast option")
    func testBindingSorterNullsLast() throws {
        let b1 = VariableBinding().binding("?x", to: .string("B"))
        let b2 = VariableBinding()  // unbound
        let b3 = VariableBinding().binding("?x", to: .string("A"))
        let workMeter = DatabaseWorkMeter(
            budget: .init(),
            monotonicClock: TestProcessMonotonicClock()
        )

        let sorted = try BindingSorter.sort(
            [b1, b2, b3],
            by: [.variable("?x", ascending: true, nullsLast: true)],
            workMeter: workMeter
        )

        #expect(sorted[0]["?x"] == .string("A"))
        #expect(sorted[1]["?x"] == .string("B"))
        #expect(sorted[2]["?x"] == nil)
    }

    @Test("BindingSorter multiple keys")
    func testBindingSorterMultipleKeys() throws {
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
        let workMeter = DatabaseWorkMeter(
            budget: .init(),
            monotonicClock: TestProcessMonotonicClock()
        )

        let sorted = try BindingSorter.sort(
            [b1, b2, b3, b4],
            by: [.variable("?dept"), .variable("?name")],
            workMeter: workMeter
        )

        let names = sorted.compactMap { $0.string("?name") }
        #expect(names == ["Bob", "Charlie", "Alice", "Zach"])
    }

    // MARK: - GROUP BY with ORDER BY Tests

    @Test("GROUP BY with ORDER BY on aggregate")
    func testGroupByOrderByAggregate() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let memberPred = try predicate(uniqueID("hasMember"))
        var edges: [ExecOrderEdge] = []

        // GroupA: 3 members
        for i in 0..<3 {
            edges.append(
                try makeEdge(
                    from: "GroupA",
                    edge: memberPred,
                    to: try resource(uniqueID("M\(i)"))
                )
            )
        }
        // GroupB: 5 members
        for i in 0..<5 {
            edges.append(
                try makeEdge(
                    from: "GroupB",
                    edge: memberPred,
                    to: try resource(uniqueID("M\(i)"))
                )
            )
        }
        // GroupC: 1 member
        edges.append(
            try makeEdge(
                from: "GroupC",
                edge: memberPred,
                to: resource(uniqueID("M0"))
            )
        )

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(ExecOrderEdge.self)
            .defaultIndex()
            .where(.variable("?group"), value(memberPred.term), .variable("?member"))
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

        let context = container.newContext()

        let knowsPred = try predicate(uniqueID("knows"))
        let namePred = try predicate(uniqueID("name"))

        let edges = [
            try makeEdge(
                from: "Alice",
                edge: knowsPred,
                to: resource("Bob")
            ),
            try makeEdge(
                from: "Alice",
                edge: knowsPred,
                to: resource("Charlie")
            ),
            try makeEdge(
                from: "Bob",
                edge: namePred,
                to: .string("Robert")
            ),
            try makeEdge(
                from: "Charlie",
                edge: namePred,
                to: .string("Charles")
            ),
        ]
        try await insertEdges(edges, context: context)

        // Query: Alice knows ?friend, ?friend has name ?name
        // Filter on ?name
        let result = try await context.sparql(ExecOrderEdge.self)
            .defaultIndex()
            .where(
                value(try resource("Alice")),
                value(knowsPred.term),
                .variable("?friend")
            )
            .where(.variable("?friend"), value(namePred.term), .variable("?name"))
            .filter(.equals("?name", .rdfTerm(.string("Robert"))))
            .execute()

        #expect(result.count == 1)
        #expect(
            result.bindings.first?["?friend"]
                == .rdfTerm(try resource("Bob"))
        )
    }
}
#endif
