import Foundation
import Testing
@_spi(Testing) import Database

@Persistable
private struct FullTextBenchmarkDocument {
    #Directory<FullTextBenchmarkDocument>("fulltext-query-benchmark")
    #Index(
        .text(
            name: "fulltext_benchmark_body",
            fields: [\FullTextBenchmarkDocument.body],
            mode: .fullText(tokenizer: .simple)
        )
    )

    var id: String
    var body: String
}

private struct BenchmarkMonotonicClock: StorageMonotonicClock {
    private static let clock = ContinuousClock()
    private static let origin = clock.now

    var now: StorageInstant {
        StorageInstant(
            durationSinceReference: Self.origin.duration(to: Self.clock.now)
        )
    }

    func sleep(
        until deadline: StorageInstant
    ) async throws(StorageClockError) {
        let remaining = now.duration(to: deadline)
        guard remaining > .zero else { return }
        do {
            try await Self.clock.sleep(for: remaining)
        } catch {
            throw .cancelled
        }
    }
}

private struct BenchmarkWallClock: WallClock {
    let now = Timestamp(secondsSinceUnixEpoch: 1_787_312_000)
}

private struct AsyncBenchmarkDistribution {
    let samples: [Double]

    var minimum: Double {
        samples.min() ?? 0
    }

    var median: Double {
        percentile(0.5)
    }

    var p95: Double {
        percentile(0.95)
    }

    private func percentile(_ quantile: Double) -> Double {
        guard !samples.isEmpty else { return 0 }
        let ordered = samples.sorted()
        let position = Int(
            (Double(ordered.count - 1) * quantile).rounded(.up)
        )
        return ordered[position]
    }
}

private func measureAsync(
    name: String,
    warmupCount: Int = 3,
    sampleCount: Int = 15,
    operation: () async throws -> Int
) async throws -> AsyncBenchmarkDistribution {
    var checksum = 0
    for _ in 0..<warmupCount {
        checksum &+= try await operation()
    }

    var samples: [Double] = []
    samples.reserveCapacity(sampleCount)
    for _ in 0..<sampleCount {
        let start = ContinuousClock.now
        checksum &+= try await operation()
        let duration = ContinuousClock.now - start
        samples.append(
            Double(duration.components.seconds) * 1_000_000
                + Double(duration.components.attoseconds) / 1_000_000_000_000
        )
    }

    let distribution = AsyncBenchmarkDistribution(samples: samples)
    print(
        "BENCHMARK \(name) unit=us samples=\(sampleCount) "
            + "min=\(distribution.minimum) median=\(distribution.median) "
            + "p95=\(distribution.p95) checksum=\(checksum)"
    )
    return distribution
}

private func makeBody(for index: Int) -> String {
    var terms = ["common"]
    if index.isMultiple(of: 2) {
        terms.append("even")
    }
    if index.isMultiple(of: 3) {
        terms.append("triad")
    }
    if index.isMultiple(of: 5) {
        terms.append("fifth")
    }
    return terms.joined(separator: " ")
}

@Suite("Canonical full-text query benchmarks", .serialized)
struct FullTextQueryBenchmarks {
    @Test("Ordered posting intersection and union")
    func orderedPostingIntersectionAndUnion() async throws {
        let documentCount = 2_000
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent(
                "database-framework-fulltext-benchmark-\(UUID().uuidString)",
                isDirectory: true
            )
        try FileManager.default.createDirectory(
            at: directory,
            withIntermediateDirectories: true
        )
        let schema = try Schema(
            entities: [try FullTextBenchmarkDocument.schemaEntity],
            version: .init(1, 0, 0)
        )
        let container = try await DBContainer.sqlite(
            for: schema,
            path: directory.appendingPathComponent("database.sqlite").path,
            monotonicClock: BenchmarkMonotonicClock(),
            wallClock: BenchmarkWallClock(),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "fulltext-query-benchmark",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        FullTextBenchmarkDocument.self
                    )
                ]
            ),
            security: .disabledForTesting
        )
        defer {
            await container.shutdown()
            do {
                try FileManager.default.removeItem(at: directory)
            } catch {
                Issue.record("Failed to remove benchmark directory: \(error)")
            }
        }

        let context = container.newContext(
            authorization: .authenticated(
                Principal(identifier: "benchmark", roles: ["benchmark"])
            )
        )
        for start in stride(from: 0, to: documentCount, by: 250) {
            for index in start..<Swift.min(start + 250, documentCount) {
                try context.insert(
                    FullTextBenchmarkDocument(
                        id: String(format: "document-%05d", index),
                        body: makeBody(for: index)
                    )
                )
            }
            try await context.save()
        }

        let intersection = try await measureAsync(
            name: "fulltext.canonical.intersection"
        ) {
            try await context.search(FullTextBenchmarkDocument.self)
                .fullText(FullTextBenchmarkDocument.fields.body)
                .terms(["common", "even", "triad", "fifth"], mode: .all)
                .limit(1)
                .execute()
                .count
        }
        let union = try await measureAsync(
            name: "fulltext.canonical.union"
        ) {
            try await context.search(FullTextBenchmarkDocument.self)
                .fullText(FullTextBenchmarkDocument.fields.body)
                .terms(["even", "triad", "fifth"], mode: .any)
                .limit(1)
                .execute()
                .count
        }

        #expect(intersection.samples.count == 15)
        #expect(union.samples.count == 15)

        let expandedDocumentCount = 4_000
        for start in stride(
            from: documentCount,
            to: expandedDocumentCount,
            by: 250
        ) {
            for index in start..<Swift.min(
                start + 250,
                expandedDocumentCount
            ) {
                try context.insert(
                    FullTextBenchmarkDocument(
                        id: String(format: "document-%05d", index),
                        body: makeBody(for: index)
                    )
                )
            }
            try await context.save()
        }

        let expandedIntersectionCount = try await context.search(
            FullTextBenchmarkDocument.self
        )
        .fullText(FullTextBenchmarkDocument.fields.body)
        .terms(["common", "even", "triad", "fifth"], mode: .all)
        .limit(1)
        .execute()
        .count
        let expandedUnionCount = try await context.search(
            FullTextBenchmarkDocument.self
        )
        .fullText(FullTextBenchmarkDocument.fields.body)
        .terms(["even", "triad", "fifth"], mode: .any)
        .limit(1)
        .execute()
        .count

        #expect(expandedIntersectionCount == 1)
        #expect(expandedUnionCount == 1)
    }
}
