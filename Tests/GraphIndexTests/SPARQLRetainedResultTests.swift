import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import Testing
import TestSupport
@_spi(DatabaseExecution) @testable import GraphIndex

@Suite("SPARQL retained result hand-off")
struct SPARQLRetainedResultTests {
    @Persistable
    struct Anchor {
        #Directory<Anchor>("sparql_retained_result_anchor")

        var id: String = ""
    }

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
                    ProjectionItem(.variable(Variable("missing"))),
                ]),
                source: .graphPattern(
                    .values(
                        variables: ["value"],
                        bindings: [
                            [.string("discarded-before")],
                            [.string("retained")],
                            [.string("discarded-after")],
                        ]
                    )
                ),
                limit: 1,
                offset: 1
            )
            let plan = try SPARQLSelectPlanCompiler
                .compileForCanonicalPagination(query)
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
            #expect(result.projectedVariables == ["?value", "?missing"])
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
            #expect(throws: SPARQLRetainedResultError.workMeterMismatch) {
                _ = try result.retainedQueryRows(workMeter: foreignMeter)
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
            let rows = try result.retainedQueryRows(workMeter: meter)
            rows.withElement(at: 0) { row in
                #expect(row.fields.count == 1)
                #expect(row.fields["value"] == values.withSpan { $0[0] })
                #expect(row.fields["?value"] == nil)
            }

            await Task.yield()

            #expect(result.count == 1)
            #expect(values.withSpan { $0.count } == 1)
            rows.withElement(at: 0) { row in
                #expect(row.fields["value"] != nil)
            }
            withExtendedLifetime(values) {
                withExtendedLifetime(rows) {
                    #expect(meter.retainedIntermediateRows == 2)
                    #expect(meter.retainedIntermediateBytes > 0)
                }
            }
            #expect(result.count == 1)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)

        let schema = try Schema(
            entities: [try Anchor.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "sparql-retained-result-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(Anchor.self)]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let pagedQuery = SelectQuery(
            projection: .items([
                ProjectionItem(.variable(Variable("value"))),
                ProjectionItem(.variable(Variable("missing"))),
            ]),
            source: .graphPattern(
                .values(
                    variables: ["value"],
                    bindings: [
                        [.string("discarded-before")],
                        [.string("first-page")],
                        [.string("second-page")],
                        [.string("discarded-after")],
                    ]
                )
            ),
            limit: 2,
            offset: 1
        )
        let firstPage = try await context.query(
            pagedQuery,
            options: ReadExecutionOptions(
                pageSize: 1,
                continuationScope: ByteString([0x44, 0x46, 0x30, 0x36, 0x45])
            )
        )
        #expect(firstPage.rows.count == 1)
        #expect(
            firstPage.rows[0].fields["value"]
                == .rdfTerm(
                    .literal(
                        RDFLiteral(
                            lexicalForm: "first-page",
                            datatype: .xsdString
                        )
                    )
                )
        )
        #expect(firstPage.rows[0].fields["missing"] == nil)
        let continuation = try #require(firstPage.continuation)
        let secondPage = try await context.query(
            pagedQuery,
            options: ReadExecutionOptions(
                pageSize: 1,
                continuation: continuation,
                continuationScope: ByteString([0x44, 0x46, 0x30, 0x36, 0x45])
            )
        )
        #expect(secondPage.rows.count == 1)
        #expect(
            secondPage.rows[0].fields["value"]
                == .rdfTerm(
                    .literal(
                        RDFLiteral(
                            lexicalForm: "second-page",
                            datatype: .xsdString
                        )
                    )
                )
        )
        #expect(secondPage.rows[0].fields["missing"] == nil)
        #expect(secondPage.continuation == nil)

        let capacityQuery = SelectQuery(
            projection: .items([
                ProjectionItem(.variable(Variable("value"))),
                ProjectionItem(.variable(Variable("missing"))),
            ]),
            source: .graphPattern(
                .values(
                    variables: ["value"],
                    bindings: [[.string(String(repeating: "x", count: 256))]]
                )
            )
        )
        let capacityPlan = try SPARQLSelectPlanCompiler
            .compileForCanonicalPagination(capacityQuery)
        let executor = SPARQLQueryExecutor(
            database: InMemoryEngine(),
            monotonicClock: clock,
            wallClock: FixedTestWallClock(),
            sources: []
        )
        let boundOnlyPlan = try SPARQLSelectPlanCompiler
            .compileForCanonicalPagination(
                SelectQuery(
                    projection: .items([
                        ProjectionItem(.variable(Variable("value"))),
                    ]),
                    source: .graphPattern(
                        .values(
                            variables: ["value"],
                            bindings: [[
                                .string(String(repeating: "x", count: 256)),
                            ]]
                        )
                    )
                )
            )
        let boundOnlyMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 10,
                maximumWorkUnits: 1_000,
                maximumIntermediateRows: 10,
                maximumIntermediateBytes: 1_000_000,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: clock
        )
        var boundOnlyPeak: UInt64 = 0
        do {
            let transaction = try InMemoryEngine().createTransaction()
            let result = try await executor.executeRetainedInTransaction(
                selectPlan: boundOnlyPlan,
                transaction: transaction,
                workMeter: boundOnlyMeter
            )
            let rows = try result.retainedQueryRows(workMeter: boundOnlyMeter)
            boundOnlyPeak = boundOnlyMeter.peakIntermediateBytes
            withExtendedLifetime(rows) {}
            #expect(result.count == 1)
        }
        #expect(boundOnlyMeter.retainedIntermediateRows == 0)
        #expect(boundOnlyMeter.retainedIntermediateBytes == 0)
        #expect(boundOnlyPeak > 0)

        let exactMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 10,
                maximumWorkUnits: 1_000,
                maximumIntermediateRows: 10,
                maximumIntermediateBytes: 1_000_000,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: clock
        )
        var exactRowPeak: UInt64 = 0
        do {
            let transaction = try InMemoryEngine().createTransaction()
            let result = try await executor.executeRetainedInTransaction(
                selectPlan: capacityPlan,
                transaction: transaction,
                workMeter: exactMeter
            )
            let sourcePeak = exactMeter.peakIntermediateBytes
            let rows = try result.retainedQueryRows(workMeter: exactMeter)
            exactRowPeak = exactMeter.peakIntermediateBytes
            #expect(exactRowPeak > sourcePeak)
            rows.withElement(at: 0) { row in
                #expect(row.fields.count == 1)
                #expect(row.fields["value"] != nil)
                #expect(row.fields["missing"] == nil)
            }
            withExtendedLifetime(rows) {
                #expect(exactMeter.retainedIntermediateRows == 2)
                #expect(exactMeter.retainedIntermediateBytes > 0)
            }
            #expect(result.count == 1)
        }
        #expect(exactMeter.retainedIntermediateRows == 0)
        #expect(exactMeter.retainedIntermediateBytes == 0)
        #expect(exactRowPeak == boundOnlyPeak)
        guard exactRowPeak > 0 else {
            Issue.record("Retained query-row peak must be nonzero")
            return
        }

        let oneByteShortMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 10,
                maximumWorkUnits: 1_000,
                maximumIntermediateRows: 10,
                maximumIntermediateBytes: exactRowPeak - 1,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: clock
        )
        do {
            let transaction = try InMemoryEngine().createTransaction()
            let result = try await executor.executeRetainedInTransaction(
                selectPlan: capacityPlan,
                transaction: transaction,
                workMeter: oneByteShortMeter
            )
            let retainedSourceRows = oneByteShortMeter.retainedIntermediateRows
            let retainedSourceBytes = oneByteShortMeter.retainedIntermediateBytes
            var oneByteShortFailure: Error?
            do {
                _ = try result.retainedQueryRows(workMeter: oneByteShortMeter)
                Issue.record("Expected retained query-row admission to fail")
            } catch {
                oneByteShortFailure = error
            }
            guard let workLimit = oneByteShortFailure
                    as? DatabaseWorkLimitError,
                  case .maximumIntermediateBytes(
                    let stage,
                    _,
                    _,
                    let maximum
                  ) = workLimit else {
                Issue.record(
                    "Expected retained query-row byte-limit failure, got \(String(describing: oneByteShortFailure))"
                )
                return
            }
            #expect(stage == .resultMaterialization)
            #expect(maximum == exactRowPeak - 1)
            #expect(
                oneByteShortMeter.retainedIntermediateRows
                    == retainedSourceRows
            )
            #expect(
                oneByteShortMeter.retainedIntermediateBytes
                    == retainedSourceBytes
            )
            #expect(result.count == 1)
        }
        #expect(oneByteShortMeter.retainedIntermediateRows == 0)
        #expect(oneByteShortMeter.retainedIntermediateBytes == 0)
    }
}
