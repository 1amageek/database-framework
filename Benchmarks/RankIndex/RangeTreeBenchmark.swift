import Testing
import Foundation
import Core
import Rank
import DatabaseEngine
import RankIndex
import BenchmarkFramework
import FoundationDB
@testable import TestSupport

@Persistable
struct Player {
    #Directory<Player>("benchmarks", "players")

    var id: String = UUID().uuidString
    var name: String = ""
    var score: Int64 = 0

    // Rank index on score
    #Index(RankIndexKind<Player, Int64>(field: \.score), name: "score_rank")
}

@Suite("RankIndex: Range Tree Benchmark", .serialized)
struct RangeTreeBenchmark {
    nonisolated(unsafe) private let database: any DatabaseProtocol
    nonisolated(unsafe) private let container: FDBContainer
    nonisolated(unsafe) private let context: FDBContext

    init() async throws {
        try await FDBTestSetup.shared.initialize()
        let db = try FDBClient.openDatabase()
        let schema = Schema([Player.self], version: Schema.Version(1, 0, 0))
        let cont = FDBContainer(database: db, schema: schema, security: .disabled)

        self.database = db
        self.container = cont
        self.context = FDBContext(container: cont)
    }

    @Test("TopKHeap Current Implementation")
    func topKHeapBaseline() async throws {
        // Setup: Create test players with scores
        let playerCount = 1000  // Reduced for faster benchmarks
        var players: [Player] = []

        for i in 0..<playerCount {
            players.append(Player(
                name: "Player \(i)",
                score: Int64.random(in: 0...100000)
            ))
        }

        // Insert all players
        for player in players {
            context.insert(player)
        }
        try await context.save()

        nonisolated(unsafe) let ctx = context

        let runner = BenchmarkRunner(config: .init(
            warmupIterations: 3,
            measurementIterations: 30,
            throughputDuration: 3.0,
            measureMemory: false
        ))

        // Benchmark current TopKHeap implementation
        let result = try await runner.compare(
            name: "RankIndex: TopKHeap Performance (Current)",
            baseline: { @Sendable () async throws -> Int in
                // Current TopKHeap implementation
                let results = try await ctx.rank(Player.self)
                    .by(\.score)
                    .top(100)
                    .execute()
                return results.count
            },
            optimized: { @Sendable () async throws -> Int in
                // Same implementation (Range Tree not yet implemented)
                let results = try await ctx.rank(Player.self)
                    .by(\.score)
                    .top(100)
                    .execute()
                return results.count
            },
            verify: { baseline, optimized in
                #expect(baseline == optimized)
                #expect(baseline <= 100)  // May be less if dataset is smaller
            }
        )


        // Print console report
        ConsoleReporter.print(result)

        Swift.print("\n📝 Note: Range Tree optimization not yet implemented.")
        Swift.print("Current implementation: TopKHeap O(n log k)")
        Swift.print("Expected with Range Tree: O(log n + k)")
        Swift.print("Expected improvement: 100x for large datasets (100k+ items)\n")
    }

    @Test("Rank Query Scalability")
    func rankScalability() async throws {
        let runner = BenchmarkRunner(config: .init(
            warmupIterations: 2,
            measurementIterations: 20,
            throughputDuration: 2.0,
            measureMemory: false
        ))

        nonisolated(unsafe) let ctx = context

        // Test different K values
        let result = try await runner.scale(
            name: "Rank Query Scalability",
            dataSizes: [10, 50, 100]  // K values to test
        ) { @Sendable (k: Int) async throws -> Int in
            // Query top K items
            let topPlayers = try await ctx.rank(Player.self)
                .by(\.score)
                .top(k)
                .execute()

            return topPlayers.count
        }


        // Print console report
        ConsoleReporter.print(result)

        Swift.print("\n📊 Scalability Analysis:")
        for point in result.dataPoints {
            Swift.print("  Top \(point.dataSize): \(String(format: "%.2f", point.metrics.latency.p95))ms (p95)")
        }
        Swift.print("")
    }

    @Test("Different K Values Performance")
    func differentKValues() async throws {
        // Setup: Create fixed dataset
        let playerCount = 1000
        var players: [Player] = []

        for i in 0..<playerCount {
            players.append(Player(
                name: "Player \(i)",
                score: Int64.random(in: 0...100000)
            ))
        }

        for player in players {
            context.insert(player)
        }
        try await context.save()

        nonisolated(unsafe) let ctx = context

        let runner = BenchmarkRunner(config: .init(
            warmupIterations: 2,
            measurementIterations: 20,
            throughputDuration: 2.0,
            measureMemory: false
        ))

        // Test different K values
        let result = try await runner.scale(
            name: "TopK Performance for Different K",
            dataSizes: [10, 50, 100, 200]  // Reduced sizes
        ) { @Sendable (k: Int) async throws -> Int in
            let topPlayers = try await ctx.rank(Player.self)
                .by(\.score)
                .top(k)
                .execute()
            return topPlayers.count
        }


        // Print console report
        ConsoleReporter.print(result)

        Swift.print("\n📊 K Value Analysis:")
        for point in result.dataPoints {
            Swift.print("  Top \(point.dataSize): \(String(format: "%.2f", point.metrics.latency.p95))ms (p95)")
        }
        Swift.print("")
    }
}
