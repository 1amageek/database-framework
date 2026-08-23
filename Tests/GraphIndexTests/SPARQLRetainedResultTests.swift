import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit
import Testing
import TestSupport
@_spi(DatabaseExecution) @testable import GraphIndex

@Suite("SPARQL retained result hand-off")
struct SPARQLRetainedResultTests {
    @Test("Intermediate result remains accounted across suspension and scoped reads")
    func retainedResultLifetime() async throws {
        let clock = TestProcessMonotonicClock()
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 10,
                maximumWorkUnits: 1_000,
                maximumIntermediateRows: 10,
                maximumIntermediateBytes: 1_000_000,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: clock
        )

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
        do {
            let engine = InMemoryEngine()
            let transaction = try engine.createTransaction()
            let query = SelectQuery(
                projection: .items([
                    ProjectionItem(.variable(Variable("value"))),
                ]),
                source: .graphPattern(
                    .values(
                        variables: ["value"],
                        bindings: [[.string("retained")]]
                    )
                )
            )
            let plan = try SPARQLSelectPlanCompiler.compile(query)
            let result = try await SPARQLQueryExecutor(
                database: engine,
                monotonicClock: clock,
                wallClock: FixedTestWallClock(),
                sources: []
            ).executeRetainedInTransaction(
                selectPlan: plan,
                transaction: transaction,
                workMeter: meter
            )

            #expect(result.count == 1)
            #expect(result.projectedVariables == ["?value"])
            #expect(meter.retainedIntermediateRows == 1)
            #expect(meter.retainedIntermediateBytes > 0)
            let foreignMeter = DatabaseWorkMeter(
                budget: meter.budget,
                monotonicClock: clock
            )
            #expect(throws: SPARQLRetainedResultError.workMeterMismatch) {
                _ = try result.retainedValues(
                    for: "?value",
                    workMeter: foreignMeter
                )
            }
            let values = try result.retainedValues(
                for: "?value",
                workMeter: meter
            )
            values.withSpan { values in
                #expect(values.count == 1)
                #expect(
                    values[0] == .rdfTerm(
                        .literal(
                            RDFLiteral(
                                lexicalForm: "retained",
                                datatype: .xsdString
                            )
                        )
                    )
                )
            }

            await Task.yield()

            #expect(result.count == 1)
            #expect(meter.retainedIntermediateRows == 2)
            #expect(meter.retainedIntermediateBytes > 0)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }
}
