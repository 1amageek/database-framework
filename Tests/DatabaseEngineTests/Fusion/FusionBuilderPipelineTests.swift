import DatabaseKit
import Testing
@testable import DatabaseEngine

@Persistable
private struct FusionPipelineItem {
    var id: String
    var value: Int64
}

private struct StaticFusionPipelineQuery: FusionQuery {
    let results: [ScoredResult<FusionPipelineItem>]

    func execute(
        candidates: Set<FusionPipelineItem.ID>?
    ) async throws -> [ScoredResult<FusionPipelineItem>] {
        guard let candidates else {
            return results
        }
        return results.filter { candidates.contains($0.item.id) }
    }
}

@Suite("Fusion builder pipeline semantics")
struct FusionBuilderPipelineTests {
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
            algorithm: .sum
        )

        let results = try await builder.execute()

        #expect(results.isEmpty)
    }
}
