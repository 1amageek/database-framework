import Testing
import Foundation
import Core
import DatabaseEngine
import ScalarIndex
import BenchmarkFramework
import FoundationDB
@testable import TestSupport

@Persistable
struct User {
    #Directory<User>("benchmarks", "users")

    var id: String = UUID().uuidString
    var email: String = ""
    var name: String = ""
    var age: Int = 0

    // Standard index on email
    #Index(ScalarIndexKind<User>(fields: [\.email]), name: "email_standard")

    // Covering index on email with stored fields (future optimization)
    #Index(ScalarIndexKind<User>(fields: [\.email]), storedFields: [\User.name, \User.age], name: "email_covering")
}

@Suite("ScalarIndex: Covering Index Benchmark", .serialized)
struct CoveringIndexBenchmark {
    nonisolated(unsafe) private let database: any DatabaseProtocol
    nonisolated(unsafe) private let container: FDBContainer
    nonisolated(unsafe) private let context: FDBContext

    init() async throws {
        try await FDBTestSetup.shared.initialize()
        let db = try FDBClient.openDatabase()
        let schema = Schema([User.self], version: Schema.Version(1, 0, 0))
        let cont = FDBContainer(database: db, schema: schema, security: .disabled)

        self.database = db
        self.container = cont
        self.context = FDBContext(container: cont)
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
                age: 20 + (i % 50)
            ))
        }

        // Insert all users
        for user in users {
            context.insert(user)
        }
        try await context.save()

        nonisolated(unsafe) let ctx = context

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
        // Setup: Create test users
        let userCount = 500

        for i in 0..<userCount {
            let user = User(
                email: "scan_user\(i)@example.com",
                name: "Scan User \(i)",
                age: 20 + (i % 50)
            )
            context.insert(user)
        }
        try await context.save()

        nonisolated(unsafe) let ctx = context

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
            Swift.print("  \(point.dataSize) records: \(String(format: "%.2f", point.metrics.latency.p95))ms (p95)")
        }
        Swift.print("")
    }

    @Test("Batch Fetch Performance")
    func batchFetchPerformance() async throws {
        // Setup: Create test dataset
        let userCount = 300
        for i in 0..<userCount {
            let user = User(
                email: "batch_user\(i)@example.com",
                name: "Batch User \(i)",
                age: 25 + (i % 40)
            )
            context.insert(user)
        }
        try await context.save()

        nonisolated(unsafe) let ctx = context

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
