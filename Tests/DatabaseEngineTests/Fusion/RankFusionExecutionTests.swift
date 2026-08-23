import DatabaseKit
import DatabaseRuntime
import RankIndex
import StorageKit
import StorageKitSystemClock
import TestSupport
import Testing

@testable import DatabaseEngine

@Persistable
private struct RankFusionExecutionItem {
    var id: String
    var rankValue: Int64
}

@Suite("Rank fusion execution")
struct RankFusionExecutionTests {
    @Test("Rank rejects execution without an upstream candidate set")
    func missingCandidatesFailExplicitly() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let query = Rank(
            RankFusionExecutionItem.fields.rankValue,
            context: container.testBaseContext().indexQueryContext
        )

        do {
            _ = try await query.execute(
                candidates: nil,
                execution: ReadExecutionContext(
                    monotonicClock: SystemStorageClock()
                )
            )
            Issue.record("Expected Rank to reject a missing candidate set")
        } catch let error as FusionQueryError {
            guard case .missingCandidates(stage: "Rank") = error else {
                Issue.record("Unexpected Fusion error: \(error)")
                return
            }
        }
    }

    @Test("Rank accepts an explicitly empty upstream candidate set")
    func emptyCandidatesReturnEmptySuccess() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let query = Rank(
            RankFusionExecutionItem.fields.rankValue,
            context: container.testBaseContext().indexQueryContext
        )

        let result = try await query.execute(
            candidates: [],
            execution: ReadExecutionContext(
                monotonicClock: SystemStorageClock()
            )
        )

        #expect(result.isEmpty)
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(
            testing: try Schema(
                entities: [try RankFusionExecutionItem.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "rank-fusion-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        RankFusionExecutionItem.self
                    )
                ]
            ),
            security: .testingDisabled
        )
    }
}
