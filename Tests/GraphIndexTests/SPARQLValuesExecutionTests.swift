import DatabaseEngine
import DatabaseTypes
import DatabaseWire
import DatabaseKit
import StorageKit
import Synchronization
import Testing
import TestSupport
@_spi(DatabaseExecution) @testable import GraphIndex

@Suite("SPARQL VALUES execution")
struct SPARQLValuesExecutionTests {
    private final class FilterInvocationCounter: Sendable {
        private let count = Mutex(0)

        func record() {
            count.withLock { $0 += 1 }
        }

        var value: Int {
            count.withLock { $0 }
        }
    }

    @Test("Public modifiers retain intermediates in SPARQL evaluation order")
    func retainedPublicModifierPipeline() async throws {
        let pattern = try GraphPatternConverter.convert(
            .values(
                variables: ["sort", "projected", "discarded"],
                bindings: [
                    [.int(2), .string("same"), .string("a")],
                    [.int(1), .string("same"), .string("b")],
                    [.int(3), .string("other"), .string("c")],
                    [.int(0), .string("same"), .string("d")],
                ]
            )
        )
        let engine = InMemoryEngine()
        let transaction = try engine.createTransaction()
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: TestProcessMonotonicClock()
        )

        let retained = try await SPARQLQueryExecutor(
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            datasetScanner: IndexedRDFDatasetScanner(sources: [])
        ).executeRetainedProjectedInTransaction(
            pattern: pattern,
            transaction: transaction,
            orderBy: [.variable("?sort")],
            projectionVariables: ["?projected"],
            projectionIsIdentity: false,
            duplicatePolicy: .distinct,
            offset: 1,
            limit: 1,
            workMeter: meter
        )

        #expect(retained.count == 1)
        #expect(meter.retainedIntermediateRows > 0)
        let result = retained.promoteToResult()
        let bindings = result.bindings
        #expect(bindings.count == 1)
        #expect(bindings[0].count == 1)
        guard case .rdfTerm(.literal(let literal)) = bindings[0]["?projected"] else {
            Issue.record("Expected the projected RDF literal")
            return
        }
        #expect(literal.lexicalForm == "other")
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("An incompatible VALUES row is rejected before filter evaluation")
    func incompatibleRowSkipsFilterAndCandidateConstruction() async throws {
        let counter = FilterInvocationCounter()
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: TestProcessMonotonicClock()
        )
        let engine = InMemoryEngine()
        let transaction = try engine.createTransaction()
        let executor = try SPARQLQueryExecutor(
            database: engine,
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            sources: []
        ).requestScoped(by: meter)
        let table = SPARQLValuesTable(
            variables: ["?value"],
            rowCount: 1,
            cells: [.int64(2)]
        )
        let filter = FilterExpression.customWithVariables(
            { _ in
                counter.record()
                throw SPARQLQueryError.executionFailed(
                    "incompatible VALUES row reached its filter"
                )
            },
            variables: ["?value"]
        )

        do {
            let result = try await executor.evaluateValuesPattern(
                table,
                transaction: transaction,
                activeGraph: .defaultGraph,
                filter: filter,
                seed: VariableBinding(["?value": .int64(1)]),
                resultLimit: nil,
                statistics: ExecutionStatistics()
            )
            let isEmpty = result.bindings.isEmpty
            #expect(isEmpty)
        }
        #expect(counter.value == 0)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("VALUES preserves row order, duplicates, and UNDEF bindings")
    func valuesMultisetSemantics() async throws {
        let pattern = try GraphPatternConverter.convert(
            .values(
                variables: ["value", "optional"],
                bindings: [
                    [.int(1), nil],
                    [.int(1), .string("bound")],
                    [.int(1), nil],
                ]
            )
        )
        let (bindings, _) = try await SPARQLQueryExecutor(
            database: InMemoryEngine(),
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            sources: []
        ).execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: DatabaseWorkMeter(
                budget: ExecutionBudget(),
                monotonicClock: TestProcessMonotonicClock()
            )
        )

        #expect(bindings.count == 3)
        #expect(bindings[0]["?optional"] == nil)
        #expect(bindings[1]["?optional"] != nil)
        #expect(bindings[2]["?optional"] == nil)
        for binding in bindings {
            guard case .rdfTerm(.literal(let literal)) = binding["?value"] else {
                Issue.record("Expected a canonical RDF literal")
                return
            }
            #expect(literal.lexicalForm == "1")
        }
    }

    @Test("An empty VALUES table has no solutions")
    func emptyValuesTable() async throws {
        let pattern = try GraphPatternConverter.convert(
            .values(variables: ["value"], bindings: [])
        )
        let (bindings, _) = try await SPARQLQueryExecutor(
            database: InMemoryEngine(),
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            sources: []
        ).execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: DatabaseWorkMeter(
                budget: ExecutionBudget(),
                monotonicClock: TestProcessMonotonicClock()
            )
        )

        #expect(bindings.isEmpty)
    }

    @Test("VALUES participates in EXISTS without a fallback result")
    func valuesInsideExists() async throws {
        let nonEmpty = SelectQuery(
            projection: .all,
            source: .graphPattern(
                .values(variables: ["value"], bindings: [[.int(1)]])
            )
        )
        let empty = SelectQuery(
            projection: .all,
            source: .graphPattern(
                .values(variables: ["value"], bindings: [])
            )
        )
        let executor = SPARQLQueryExecutor(
            database: InMemoryEngine(),
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            sources: []
        )
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: TestProcessMonotonicClock())

        let (present, _) = try await executor.execute(
            pattern: .filter(
                .basic([]),
                .query(try SPARQLExpressionPlan(.exists(nonEmpty)))
            ),
            limit: nil,
            offset: 0,
            workMeter: meter
        )
        let (absent, _) = try await executor.execute(
            pattern: .filter(
                .basic([]),
                .query(try SPARQLExpressionPlan(.exists(empty)))
            ),
            limit: nil,
            offset: 0,
            workMeter: DatabaseWorkMeter(
                budget: ExecutionBudget(),
                monotonicClock: TestProcessMonotonicClock()
            )
        )

        #expect(present.count == 1)
        #expect(absent.isEmpty)
    }

    @Test("Compatible shared VALUES variables merge without rebinding")
    func compatibleSharedVariableJoin() async throws {
        let pattern = try GraphPatternConverter.convert(
            .join(
                .values(variables: ["value"], bindings: [[.int(1)]]),
                .values(variables: ["value"], bindings: [[.int(1)]])
            )
        )

        let (bindings, _) = try await SPARQLQueryExecutor(
            database: InMemoryEngine(),
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            sources: []
        ).execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: DatabaseWorkMeter(
                budget: ExecutionBudget(),
                monotonicClock: TestProcessMonotonicClock()
            )
        )

        #expect(bindings.count == 1)
        #expect(bindings[0].isBound("?value"))
    }

    @Test("Incompatible shared VALUES variables produce no solution")
    func incompatibleSharedVariableJoin() async throws {
        let pattern = try GraphPatternConverter.convert(
            .join(
                .values(variables: ["value"], bindings: [[.int(1)]]),
                .values(variables: ["value"], bindings: [[.int(2)]])
            )
        )

        let (bindings, _) = try await SPARQLQueryExecutor(
            database: InMemoryEngine(),
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            sources: []
        ).execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: DatabaseWorkMeter(
                budget: ExecutionBudget(),
                monotonicClock: TestProcessMonotonicClock()
            )
        )

        #expect(bindings.isEmpty)
    }

    @Test("LATERAL VALUES respects compatible outer bindings")
    func lateralSharedVariableCompatibility() async throws {
        let pattern = try GraphPatternConverter.convert(
            .lateral(
                .values(variables: ["value"], bindings: [[.int(1)]]),
                .values(
                    variables: ["value", "visible"],
                    bindings: [[.int(1), .string("yes")]]
                )
            )
        )

        let (bindings, _) = try await SPARQLQueryExecutor(
            database: InMemoryEngine(),
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            sources: []
        ).execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: DatabaseWorkMeter(
                budget: ExecutionBudget(),
                monotonicClock: TestProcessMonotonicClock()
            )
        )

        #expect(bindings.count == 1)
        #expect(bindings[0].isBound("?visible"))
    }

    @Test("A VALUES table compiles and executes as a first-class node")
    func valuesTableExecutesAsFirstClassNode() async throws {
        let rowCount = 32
        var rows: [[Literal?]] = []
        rows.reserveCapacity(rowCount)
        for value in 0..<rowCount {
            rows.append([.int(Int64(value))])
        }

        let pattern = try GraphPatternConverter.convert(
            .values(variables: ["value"], bindings: rows)
        )
        guard case .values(let table) = pattern else {
            Issue.record("Expected a first-class VALUES execution node")
            return
        }
        #expect(table.rowCount == rowCount)

        let (bindings, _) = try await SPARQLQueryExecutor(
            database: InMemoryEngine(),
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            sources: []
        ).execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: DatabaseWorkMeter(
                budget: ExecutionBudget(
                    maximumRows: UInt32(rowCount),
                    maximumWorkUnits: 1_000,
                    timeoutMilliseconds: 30_000
                ),
                monotonicClock: TestProcessMonotonicClock()
            )
        )

        #expect(bindings.count == rowCount)
    }
}
