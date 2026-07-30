import DatabaseKit
import DatabaseEngine
import DatabaseWire
@testable import GraphIndex
import StorageKit
import TestSupport
import Testing

@Suite("SPARQL algebra filter semantics")
struct SPARQLAlgebraFilterTests {
    @Test("Empty BGP is the join identity")
    func emptyBasicPatternProducesOneBinding() async throws {
        let result = try await makeExecutor().execute(
            pattern: .basic([]),
            limit: nil,
            offset: 0,
            workMeter: makeWorkMeter()
        )

        #expect(result.0 == [VariableBinding()])
    }

    @Test("FILTER is applied after every composed algebra node")
    func filterAppliesToComposedPatterns() async throws {
        let empty = ExecutionPattern.basic([])
        let patterns: [(String, ExecutionPattern)] = [
            ("join", .join(empty, empty)),
            ("optional", .optional(empty, empty)),
            ("union", .union(empty, empty)),
            ("minus", .minus(empty, empty)),
            (
                "group",
                .groupBy(
                    empty,
                    grouping: .implicitSingleGroup,
                    aggregates: [],
                    having: nil
                )
            ),
            ("lateral", .lateral(empty, empty)),
        ]

        let executor = try makeExecutor()
        for (name, pattern) in patterns {
            let result = try await executor.execute(
                pattern: .filter(pattern, .alwaysFalse),
                limit: nil,
                offset: 0,
                workMeter: makeWorkMeter()
            )
            #expect(result.0.isEmpty, Comment(rawValue: name))
        }
    }

    @Test("Implicit aggregate grouping creates one group for empty input")
    func implicitGroupingCreatesEmptyGroup() async throws {
        let pattern = ExecutionPattern.groupBy(
            try emptyValuesPattern(),
            grouping: .implicitSingleGroup,
            aggregates: [.countAll(as: "?count")],
            having: nil
        )
        let result = try await makeExecutor().execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: makeWorkMeter()
        )

        #expect(result.0.count == 1)
        guard case .rdfTerm(.literal(let count)) = result.0[0]["?count"] else {
            Issue.record("Expected a canonical COUNT literal")
            return
        }
        #expect(count.lexicalForm == "0")
    }

    @Test("Explicit grouping creates no group for empty input")
    func explicitGroupingDoesNotCreateEmptyGroup() async throws {
        let pattern = ExecutionPattern.groupBy(
            try emptyValuesPattern(),
            grouping: .explicit([]),
            aggregates: [.countAll(as: "?count")],
            having: nil
        )
        let result = try await makeExecutor().execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: makeWorkMeter()
        )

        #expect(result.0.isEmpty)
    }

    private func makeExecutor() throws -> SPARQLQueryExecutor {
        SPARQLQueryExecutor(
            database: InMemoryEngine(),
            wallClock: FixedTestWallClock(
                now: Timestamp(secondsSinceUnixEpoch: 0)
            ),
            sources: []
        )
    }

    private func makeWorkMeter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1_000,
                maximumWorkUnits: 10_000,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }

    private func emptyValuesPattern() throws -> ExecutionPattern {
        try GraphPatternConverter.convert(
            .values(variables: ["value"], bindings: [])
        )
    }
}
