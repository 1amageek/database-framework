#if FOUNDATION_DB
// Benchmark-only target; excluded from the database-framework test graph.
import Testing
import Foundation
import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import DatabaseRuntime
import ScalarIndex
import BenchmarkFramework
import StorageKit

@Persistable
struct User {
    #Directory<User>("benchmarks", "users")

    var id: String = UUID().uuidString
    var email: String = ""
    var name: String = ""
    var age: Int64 = 0

    #Index(
        .ordered(
            name: "email_standard",
            keys: [.ascending(\User.email)]
        )
    )
    #Index(
        .ordered(
            name: "email_covering",
            keys: [.ascending(\User.email)],
            includedFields: [\User.name, \User.age]
        )
    )
}

@Suite("ScalarIndex: Covering Index Benchmark", .serialized, .heartbeat)
struct CoveringIndexBenchmark {
    private let database: any StorageEngine

    init() async throws {
        self.database = try await FoundationDBBenchmarkEnvironment.shared.makeEngine()
    }

    private func makeContext() async throws -> DatabaseContext {
        do {
            try await database.removeNamespace(path: ["benchmarks", "users"])
        } catch {
            // Ignore missing benchmark directories so each benchmark starts clean.
        }

        let schema = try Schema(
            entities: [try User.schemaEntity],
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
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(User.self)]
            ),
            security: .benchmarkingDisabled
        )
        try await container.ensureIndexesReady()
        return container.benchmarkContext()
    }

    @Test("Covering Index Baseline")
    func coveringIndexBaseline() async throws {
        // Setup: Create test users
        let userCount = 300
        var users: [User] = []

        for i in 0..<userCount {
            users.append(User(
                email: "user\(i)@example.com",
                name: "User \(i)",
                age: Int64(20 + (i % 50))
            ))
        }

        // Re-create context after directory cleanup
        let ctx = try await makeContext()

        // Insert all users
        for user in users {
            try ctx.insert(user)
        }
        try await ctx.save()

        let runner = BenchmarkRunner(config: .init(
            warmupIterations: 3,
            measurementIterations: 30,
            throughputDuration: 3.0,
            measureMemory: false
        ))

        // Benchmark: Fetch all users
        let result = try await runner.compare(
            name: "ScalarIndex: Fetch All Users",
            baseline: { @Sendable () async throws -> [User] in
                // Fetch all users
                try await ctx.fetch(User.self).execute()
            },
            optimized: { @Sendable () async throws -> [User] in
                // Same implementation (covering index benefit not yet implemented)
                try await ctx.fetch(User.self).execute()
            },
            verify: { baseline, optimized in
                #expect(baseline.count == optimized.count)
                #expect(baseline.count == userCount)
            }
        )


        // Print console report
        ConsoleReporter.print(result)

        Swift.print("\n📝 Note: Covering Index optimization not yet implemented.")
        Swift.print("This benchmark establishes baseline performance.")
        Swift.print("Expected improvement with covering index:")
        Swift.print("  - 50-80% latency reduction (eliminates primary key lookup)")
        Swift.print("  - Single index scan vs index scan + data fetch\n")
    }

    @Test("Index Scan Scalability")
    func indexScanScalability() async throws {
        let ctx = try await makeContext()

        // Setup: Create test users
        let userCount = 500

        for i in 0..<userCount {
            let user = User(
                email: "scan_user\(i)@example.com",
                name: "Scan User \(i)",
                age: Int64(20 + (i % 50))
            )
            try ctx.insert(user)
        }
        try await ctx.save()

        let runner = BenchmarkRunner(config: .init(
            warmupIterations: 2,
            measurementIterations: 20,
            throughputDuration: 2.0,
            measureMemory: false
        ))

        // Test different scan sizes
        let result = try await runner.scale(
            name: "Index Scan Scalability",
            dataSizes: [10, 50, 100, 200]
        ) { @Sendable (scanSize: Int) async throws -> Int in
            let users = try await ctx.fetch(User.self).limit(scanSize).execute()
            return users.count
        }


        // Print console report
        ConsoleReporter.print(result)

        Swift.print("\n📊 Scan Scalability Analysis:")
        for point in result.dataPoints {
            Swift.print("  \(point.dataSize) entities: \(String(format: "%.2f", point.metrics.latency.p95))ms (p95)")
        }
        Swift.print("")
    }

    @Test("Batch Fetch Performance")
    func batchFetchPerformance() async throws {
        let ctx = try await makeContext()

        // Setup: Create test dataset
        let userCount = 300
        for i in 0..<userCount {
            let user = User(
                email: "batch_user\(i)@example.com",
                name: "Batch User \(i)",
                age: Int64(25 + (i % 40))
            )
            try ctx.insert(user)
        }
        try await ctx.save()

        let runner = BenchmarkRunner(config: .init(
            warmupIterations: 2,
            measurementIterations: 20,
            throughputDuration: 2.0,
            measureMemory: false
        ))

        // Benchmark batch fetches
        let result = try await runner.compare(
            name: "Batch Fetch Performance",
            baseline: { @Sendable () async throws -> Int in
                // Fetch in batches of 50
                var totalCount = 0
                for _ in 0..<6 {
                    let users = try await ctx.fetch(User.self).limit(50).execute()
                    totalCount += users.count
                }
                return totalCount
            },
            optimized: { @Sendable () async throws -> Int in
                // Same implementation (future: optimize batch fetching)
                var totalCount = 0
                for _ in 0..<6 {
                    let users = try await ctx.fetch(User.self).limit(50).execute()
                    totalCount += users.count
                }
                return totalCount
            },
            verify: { baseline, optimized in
                #expect(baseline == optimized)
            }
        )


        // Print console report
        ConsoleReporter.print(result)

        Swift.print("\n📝 Future optimization: Batch point queries to reduce transaction overhead\n")
    }
}
#endif
