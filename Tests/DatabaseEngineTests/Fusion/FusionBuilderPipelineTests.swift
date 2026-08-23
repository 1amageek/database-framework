import DatabaseKit
import StorageKitSystemClock
import Testing
@testable import DatabaseEngine

@Persistable
private struct FusionPipelineItem {
    var id: String
    var value: Int64
}

private struct StaticFusionPipelineQuery: FusionQuery {
    let results: [ScoredResult<FusionPipelineItem>]

    var fusionQueryPlan: FusionQueryPlan<FusionPipelineItem> {
        FusionQueryPlan { candidates, execution in
            let selected: [ScoredResult<FusionPipelineItem>]
            if let candidates {
                selected = results.filter { candidates.contains($0.item.id) }
            } else {
                selected = results
            }
            var output = try FusionQueryResultBuilder<FusionPipelineItem>(
                execution: execution,
                expectedCount: selected.count
            )
            for result in selected {
                try output.append(result)
            }
            return try output.finish()
        }
    }
}

private actor FusionPipelineMeterCapture {
    struct Snapshot: Sendable {
        let workMeter: DatabaseWorkMeter
        let retainedRows: UInt64
        let retainedBytes: UInt64
    }

    private var capturedSnapshot: Snapshot?

    func capture(_ workMeter: DatabaseWorkMeter) {
        capturedSnapshot = Snapshot(
            workMeter: workMeter,
            retainedRows: workMeter.retainedIntermediateRows,
            retainedBytes: workMeter.retainedIntermediateBytes
        )
    }

    func snapshot() -> Snapshot? {
        capturedSnapshot
    }
}

private struct FailingFusionPipelineQuery: FusionQuery {
    let meterCapture: FusionPipelineMeterCapture

    var fusionQueryPlan: FusionQueryPlan<FusionPipelineItem> {
        FusionQueryPlan { _, execution in
            await meterCapture.capture(execution.workMeter)
            throw FusionQueryError.invalidConfiguration(
                "Expected downstream fusion failure"
            )
        }
    }
}

@Suite("Fusion builder pipeline semantics")
struct FusionBuilderPipelineTests {
    @Test("Source result owns its reservation until the consumer releases it")
    func sourceResultRetainsReservationForItsLifetime() async throws {
        let item = FusionPipelineItem(id: "retained", value: 1)
        let query = StaticFusionPipelineQuery(
            results: [ScoredResult(item: item, score: 1)]
        )
        let execution = ReadExecutionContext(
            monotonicClock: SystemStorageClock()
        )

        var result: FusionQueryResult<FusionPipelineItem>? = try await query
            .execute(candidates: nil, execution: execution)

        #expect(result?.count == 1)
        #expect(execution.workMeter.retainedIntermediateRows > 0)
        #expect(execution.workMeter.retainedIntermediateBytes > 0)

        result = nil
        #expect(execution.workMeter.retainedIntermediateRows == 0)
        #expect(execution.workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Rejected source append rolls back every reservation")
    func rejectedSourceAppendReleasesReservations() async throws {
        let item = FusionPipelineItem(id: "rejected", value: 1)
        let query = StaticFusionPipelineQuery(
            results: [ScoredResult(item: item, score: 1)]
        )
        let execution = ReadExecutionContext(
            options: ReadExecutionOptions(
                budget: ExecutionBudget(maximumIntermediateBytes: 1)
            ),
            monotonicClock: SystemStorageClock()
        )

        await #expect(throws: DatabaseWorkLimitError.self) {
            try await query.execute(candidates: nil, execution: execution)
        }
        #expect(execution.workMeter.retainedIntermediateRows == 0)
        #expect(execution.workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Pipeline failure releases retained sources and candidate identities")
    func pipelineFailureReleasesRetainedIntermediates() async throws {
        let item = FusionPipelineItem(id: "retained-before-failure", value: 1)
        let meterCapture = FusionPipelineMeterCapture()
        let builder = FusionBuilder<FusionPipelineItem>(
            stages: [
                SingleStage(
                    query: StaticFusionPipelineQuery(
                        results: [ScoredResult(item: item, score: 1)]
                    )
                ),
                SingleStage(
                    query: FailingFusionPipelineQuery(
                        meterCapture: meterCapture
                    )
                ),
            ],
            strategy: .sum
        )

        do {
            _ = try await builder.execute()
            Issue.record("Expected downstream fusion failure")
        } catch FusionQueryError.invalidConfiguration(let reason) {
            #expect(reason == "Expected downstream fusion failure")
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        guard let snapshot = await meterCapture.snapshot() else {
            Issue.record("The failing stage did not capture its work meter")
            return
        }
        #expect(snapshot.retainedRows > 0)
        #expect(snapshot.retainedBytes > 0)
        #expect(snapshot.workMeter.retainedIntermediateRows == 0)
        #expect(snapshot.workMeter.retainedIntermediateBytes == 0)
    }

    @Test("An empty later stage makes the pipeline result empty")
    func emptyLaterStageDoesNotRestoreEarlierResults() async throws {
        let item = FusionPipelineItem(id: "only", value: 1)
        let first = StaticFusionPipelineQuery(
            results: [ScoredResult(item: item, score: 1)]
        )
        let second = StaticFusionPipelineQuery(results: [])
        let builder = FusionBuilder<FusionPipelineItem>(
            stages: [
                SingleStage(query: first),
                SingleStage(query: second),
            ],
            strategy: .sum
        )

        let results = try await builder.execute()

        #expect(results.isEmpty)
    }

    @Test("Typed builder uses canonical per-source normalization")
    func typedBuilderUsesCanonicalNormalization() async throws {
        let firstItem = FusionPipelineItem(id: "first", value: 1)
        let secondItem = FusionPipelineItem(id: "second", value: 2)
        let first = StaticFusionPipelineQuery(
            results: [
                ScoredResult(item: firstItem, score: 10),
                ScoredResult(item: secondItem, score: 0),
            ]
        )
        let second = StaticFusionPipelineQuery(
            results: [
                ScoredResult(item: secondItem, score: 5),
                ScoredResult(item: firstItem, score: 1),
            ]
        )
        let builder = FusionBuilder<FusionPipelineItem>(
            stages: [Parallel(queries: [first, second])],
            strategy: .weighted([0.25, 0.75])
        )

        let results = try await builder.execute()

        #expect(results.map(\.item.id) == ["second", "first"])
        #expect(results.map(\.score) == [0.75, 0.25])
    }

    @Test("Source results establish canonical rank ordering")
    func sourceResultsEstablishCanonicalRankOrdering() async throws {
        let unordered = StaticFusionPipelineQuery(
            results: [
                ScoredResult(
                    item: FusionPipelineItem(id: "z-low", value: 1),
                    score: 0.1
                ),
                ScoredResult(
                    item: FusionPipelineItem(id: "b-high", value: 2),
                    score: 0.9
                ),
                ScoredResult(
                    item: FusionPipelineItem(id: "a-high", value: 3),
                    score: 0.9
                ),
            ]
        )
        let builder = FusionBuilder<FusionPipelineItem>(
            stages: [SingleStage(query: unordered)],
            strategy: .reciprocalRank(rankConstant: 0)
        )

        let results = try await builder.execute()

        #expect(results.map(\.item.id) == ["a-high", "b-high", "z-low"])
        #expect(results.map(\.score) == [1, 0.5, 1.0 / 3.0])
    }

    @Test("Invalid fusion numeric configuration is a typed failure")
    func invalidNumericConfigurationIsTypedFailure() async throws {
        let item = FusionPipelineItem(id: "only", value: 1)
        let query = StaticFusionPipelineQuery(
            results: [ScoredResult(item: item, score: 1)]
        )

        for builder in [
            FusionBuilder<FusionPipelineItem>(
                stages: [SingleStage(query: query)],
                strategy: .sum
            ).limit(-1),
            FusionBuilder<FusionPipelineItem>(
                stages: [SingleStage(query: query)],
                strategy: .weighted([.nan])
            ),
            FusionBuilder<FusionPipelineItem>(
                stages: [SingleStage(query: query)],
                strategy: .weighted([-1])
            ),
        ] {
            do {
                _ = try await builder.execute()
                Issue.record("Expected invalid fusion configuration")
            } catch FusionQueryError.invalidConfiguration {
                // Expected typed failure.
            } catch {
                Issue.record("Unexpected error: \(error)")
            }
        }

        await #expect(throws: FusionQueryError.self) {
            try await FusionBuilder<FusionPipelineItem>(
                stages: []
            ).execute()
        }
        let duplicateQuery = StaticFusionPipelineQuery(
            results: [
                ScoredResult(item: item, score: 1),
                ScoredResult(item: item, score: 0.5),
            ]
        )
        await #expect(throws: FusionQueryError.self) {
            try await FusionBuilder<FusionPipelineItem>(
                stages: [SingleStage(query: duplicateQuery)]
            ).execute()
        }
    }

    @Test("Maximum RRF rank constant cannot overflow")
    func maximumRRFRankConstantDoesNotOverflow() async throws {
        let item = FusionPipelineItem(id: "only", value: 1)
        let query = StaticFusionPipelineQuery(
            results: [ScoredResult(item: item, score: 1)]
        )
        let builder = FusionBuilder<FusionPipelineItem>(
            stages: [SingleStage(query: query)],
            strategy: .reciprocalRank(rankConstant: .max)
        )

        let results = try await builder.execute()

        #expect(results.count == 1)
        #expect(results[0].score.isFinite)
        #expect(results[0].score > 0)
    }
}
