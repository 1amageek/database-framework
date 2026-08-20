import Foundation

struct BenchmarkMeasurement: Sendable {
    let name: String
    let samplesMs: [Double]

    var averageMs: Double {
        guard !samplesMs.isEmpty else { return 0 }
        return samplesMs.reduce(0, +) / Double(samplesMs.count)
    }

    var p50Ms: Double {
        percentile(0.50)
    }

    var p95Ms: Double {
        percentile(0.95)
    }

    var operationsPerSecond: Double {
        let totalSeconds = samplesMs.reduce(0, +) / 1_000
        guard totalSeconds > 0 else { return 0 }
        return Double(samplesMs.count) / totalSeconds
    }

    private func percentile(_ value: Double) -> Double {
        guard !samplesMs.isEmpty else { return 0 }
        let sorted = samplesMs.sorted()
        let position = value * Double(sorted.count - 1)
        let lower = Int(position)
        let upper = min(lower + 1, sorted.count - 1)
        let weight = position - Double(lower)
        return sorted[lower] * (1.0 - weight) + sorted[upper] * weight
    }
}

func measureBenchmark(
    name: String,
    warmupIterations: Int,
    measurementIterations: Int,
    operation: () async throws -> Void
) async throws -> BenchmarkMeasurement {
    for _ in 0..<warmupIterations {
        try await operation()
    }

    var samples: [Double] = []
    samples.reserveCapacity(measurementIterations)
    for _ in 0..<measurementIterations {
        let start = DispatchTime.now().uptimeNanoseconds
        try await operation()
        let end = DispatchTime.now().uptimeNanoseconds
        samples.append(Double(end - start) / 1_000_000)
    }
    return BenchmarkMeasurement(name: name, samplesMs: samples)
}

func printBenchmarkReport(
    title: String,
    measurement: BenchmarkMeasurement
) {
    print(
        "BENCHMARK | \(title) | \(measurement.name)"
            + " | avg_ms=\(measurement.averageMs)"
            + " | p50_ms=\(measurement.p50Ms)"
            + " | p95_ms=\(measurement.p95Ms)"
            + " | ops_s=\(measurement.operationsPerSecond)"
    )
}
