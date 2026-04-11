#if FOUNDATION_DB
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

    var opsPerSecond: Double {
        let totalSeconds = samplesMs.reduce(0, +) / 1_000
        guard totalSeconds > 0 else { return 0 }
        return Double(samplesMs.count) / totalSeconds
    }

    private func percentile(_ value: Double) -> Double {
        guard !samplesMs.isEmpty else { return 0 }
        let sorted = samplesMs.sorted()
        let index = value * Double(sorted.count - 1)
        let lower = Int(index)
        let upper = min(lower + 1, sorted.count - 1)
        let weight = index - Double(lower)
        return sorted[lower] * (1.0 - weight) + sorted[upper] * weight
    }
}

func measureBenchmark(
    name: String,
    warmupIterations: Int = 3,
    measurementIterations: Int = 12,
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

func printBenchmarkReport(title: String, measurements: [BenchmarkMeasurement]) {
    print("")
    print(String(repeating: "=", count: 72))
    print(title)
    print(String(repeating: "=", count: 72))
    print("  Strategy".padding(toLength: 34, withPad: " ", startingAt: 0) + "avg(ms)   p50(ms)   p95(ms)   ops/s")
    print("  " + String(repeating: "-", count: 64))
    for measurement in measurements {
        let name = measurement.name.padding(toLength: 32, withPad: " ", startingAt: 0)
        let avg = String(format: "%7.2f", measurement.averageMs)
        let p50 = String(format: "%8.2f", measurement.p50Ms)
        let p95 = String(format: "%8.2f", measurement.p95Ms)
        let ops = String(format: "%8.1f", measurement.opsPerSecond)
        print("  \(name)  \(avg)  \(p50)  \(p95)  \(ops)")
    }
    print("")
}
#endif
