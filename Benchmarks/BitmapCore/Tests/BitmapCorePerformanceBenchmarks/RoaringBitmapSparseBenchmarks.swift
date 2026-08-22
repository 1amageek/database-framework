import Testing
import BitmapIndex

private struct BenchmarkDistribution {
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

private func measure(
    name: String,
    warmupCount: Int = 3,
    sampleCount: Int = 15,
    operation: () -> Int
) -> BenchmarkDistribution {
    var checksum = 0
    for _ in 0..<warmupCount {
        checksum &+= operation()
    }

    var samples: [Double] = []
    samples.reserveCapacity(sampleCount)
    for _ in 0..<sampleCount {
        let start = ContinuousClock.now
        checksum &+= operation()
        let duration = ContinuousClock.now - start
        samples.append(
            Double(duration.components.seconds) * 1_000_000
                + Double(duration.components.attoseconds) / 1_000_000_000_000
        )
    }

    let distribution = BenchmarkDistribution(samples: samples)
    print(
        "BENCHMARK \(name) unit=us samples=\(sampleCount) "
            + "min=\(distribution.minimum) median=\(distribution.median) "
            + "p95=\(distribution.p95) checksum=\(checksum)"
    )
    return distribution
}

private func sparseBitmap(
    count: Int,
    multiplier: UInt32,
    offset: UInt32 = 0
) -> RoaringBitmap {
    var bitmap = RoaringBitmap()
    for index in 0..<count {
        bitmap.add(UInt32(index) &* multiplier &+ offset)
    }
    return bitmap
}

@Suite("RoaringBitmap sparse-container benchmarks", .serialized)
struct RoaringBitmapSparseBenchmarks {
    @Test("Ascending sparse construction")
    func ascendingSparseConstruction() {
        let distribution = measure(name: "roaring.sparse.construct-ascending") {
            sparseBitmap(count: 4_096, multiplier: 1).cardinality
        }
        #expect(distribution.samples.count == 15)
    }

    @Test("Sparse missing-value membership")
    func sparseMissingValueMembership() {
        let bitmap = sparseBitmap(count: 2_048, multiplier: 2)
        let distribution = measure(name: "roaring.sparse.contains-missing") {
            var misses = 0
            for value in stride(from: UInt32(1), to: 4_096, by: 2) {
                if !bitmap.contains(value) {
                    misses &+= 1
                }
            }
            return misses
        }
        #expect(distribution.samples.count == 15)
    }

    @Test("Sparse intersection")
    func sparseIntersection() {
        let lhs = sparseBitmap(count: 2_048, multiplier: 2)
        let rhs = sparseBitmap(count: 1_366, multiplier: 3)
        let distribution = measure(name: "roaring.sparse.intersection") {
            var checksum = 0
            for _ in 0..<250 {
                checksum &+= (lhs && rhs).cardinality
            }
            return checksum
        }
        #expect(distribution.samples.count == 15)
    }

    @Test("Sparse union")
    func sparseUnion() {
        let lhs = sparseBitmap(count: 1_500, multiplier: 2)
        let rhs = sparseBitmap(count: 1_500, multiplier: 3)
        let distribution = measure(name: "roaring.sparse.union") {
            var checksum = 0
            for _ in 0..<250 {
                checksum &+= (lhs || rhs).cardinality
            }
            return checksum
        }
        #expect(distribution.samples.count == 15)
    }

    @Test("Sparse difference")
    func sparseDifference() {
        let lhs = sparseBitmap(count: 2_048, multiplier: 2)
        let rhs = sparseBitmap(count: 1_366, multiplier: 3)
        let distribution = measure(name: "roaring.sparse.difference") {
            var checksum = 0
            for _ in 0..<250 {
                checksum &+= (lhs - rhs).cardinality
            }
            return checksum
        }
        #expect(distribution.samples.count == 15)
    }
}
