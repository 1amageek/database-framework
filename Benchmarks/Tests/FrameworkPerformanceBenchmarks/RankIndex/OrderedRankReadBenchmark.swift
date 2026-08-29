#if FOUNDATION_DB
// Benchmark-only target; excluded from the database-framework test graph.
import Testing
import Foundation
import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import DatabaseRuntime
import RankIndex
import BenchmarkFramework
import StorageKit

@Persistable
private struct RankBenchmarkPlayer {
    #Directory<RankBenchmarkPlayer>("benchmarks", "rank_players")

    var id: String = UUID().uuidString
    var name: String = ""
    var score: Int64 = 0

    #Index(
        .rank(
            name: "score_rank",
            score: \RankBenchmarkPlayer.score
        )
    )
}

@Suite(
    "RankIndex ordered range benchmark",
    .foundationDBBenchmark,
    .serialized,
    .heartbeat
)
struct OrderedRankReadBenchmark {
    private let database: any StorageEngine

    init() async throws {
        self.database = try await FoundationDBBenchmarkEnvironment.shared.makeEngine()
    }

    private func makeContext(playerCount: Int) async throws -> DatabaseContext {
        let schema = try Schema(
            entities: [try RankBenchmarkPlayer.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .benchmarking(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(RankBenchmarkPlayer.self)]
            ),
            security: .benchmarkingDisabled
        )
        let context = container.benchmarkContext()
        for index in 0..<playerCount {
            try context.insert(
                RankBenchmarkPlayer(
                    name: "Player \(index)",
                    score: Int64((index * 7_919) % 100_001)
                )
            )
        }
        try await context.save()
        return context
    }

    @Test("Top reads scale with the requested result count")
    func topReadScaling() async throws {
        let context = try await makeContext(playerCount: 1_000)
        let runner = BenchmarkRunner(
            config: .init(
                warmupIterations: 2,
                measurementIterations: 20,
                throughputDuration: 2.0,
                measureMemory: false
            )
        )

        let result = try await runner.scale(
            name: "RankIndex bounded reverse range read",
            dataSizes: [10, 50, 100, 200]
        ) { @Sendable count in
            context.clearReadVersionCache()
            let players = try await context.rank(RankBenchmarkPlayer.self)
                .by(RankBenchmarkPlayer.fields.score)
                .top(count)
                .execute()
            guard players.count == count else {
                throw RankQueryError.invalidResponse(
                    "Expected \(count) ranked rows, received \(players.count)"
                )
            }
            return players.count
        }

        ConsoleReporter.print(result)
    }
}
#endif
