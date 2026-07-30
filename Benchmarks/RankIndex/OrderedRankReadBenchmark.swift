#if FOUNDATION_DB
import Testing
import TestSupport
import Foundation
import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import DatabaseRuntime
import RankIndex
import BenchmarkFramework
import StorageKit
@testable import TestSupport

@Persistable
private struct RankBenchmarkPlayer {
    #Directory<RankBenchmarkPlayer>("benchmarks", "rank_players")

    var id: String = UUID().uuidString
    var name: String = ""
    var score: Int64 = 0

    #Index(
        .rank,
        field: \RankBenchmarkPlayer.score,
        name: "score_rank"
    )
}

@Suite("RankIndex ordered range benchmark", .serialized, .heartbeat)
struct OrderedRankReadBenchmark {
    private let database: any StorageEngine

    init() async throws {
        self.database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
    }

    private func makeContext(playerCount: Int) async throws -> DatabaseContext {
        if try await database.namespaceExists(path: ["benchmarks", "rank_players"]) {
            try await database.removeNamespace(path: ["benchmarks", "rank_players"])
        }

        let schema = try Schema(
            entities: [try RankBenchmarkPlayer.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(backend: .custom(database)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(RankBenchmarkPlayer.self)]
            ),
            security: .disabled
        )
        try await container.ensureIndexesReady()

        let context = DatabaseContext(container: container)
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
