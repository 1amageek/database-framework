import Testing
import Foundation
import Core
import DatabaseEngine
import BitmapIndex
import BenchmarkFramework
@testable import TestSupport

@Suite("BitmapIndex: Serialization Benchmark", .serialized)
struct SerializationBenchmark {
    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    @Test("JSON Serialization Performance")
    func jsonSerializationBaseline() async throws {
        // Create test bitmaps of varying sizes
        var testBitmaps: [RoaringBitmap] = []

        // Sparse bitmap (100 values in 10k range)
        var sparse = RoaringBitmap()
        for i in stride(from: 0, to: 10000, by: 100) {
            sparse.add(UInt32(i))
        }
        testBitmaps.append(sparse)

        // Dense bitmap (8000 values in 10k range)
        var dense = RoaringBitmap()
        for i in 0..<8000 {
            dense.add(UInt32(i))
        }
        testBitmaps.append(dense)

        // Mixed bitmap
        var mixed = RoaringBitmap()
        for i in 0..<1000 {
            mixed.add(UInt32(i))
        }
        for i in 5000..<5500 {
            mixed.add(UInt32(i))
        }
        testBitmaps.append(mixed)

        nonisolated(unsafe) let unsafeBitmaps = testBitmaps

        let runner = BenchmarkRunner(config: .init(
            warmupIterations: 5,
            measurementIterations: 100,
            throughputDuration: 5.0,
            measureMemory: false
        ))

        // Benchmark JSON serialization
        let result = try await runner.compare(
            name: "BitmapIndex: JSON Serialization (Current)",
            baseline: { @Sendable () async throws -> [Data] in
                // Current JSON serialization
                var serialized: [Data] = []
                let encoder = JSONEncoder()
                for bitmap in unsafeBitmaps {
                    let data = try encoder.encode(bitmap)
                    serialized.append(data)
                }
                return serialized
            },
            optimized: { @Sendable () async throws -> [Data] in
                // Same implementation (Binary format not yet implemented)
                var serialized: [Data] = []
                let encoder = JSONEncoder()
                for bitmap in unsafeBitmaps {
                    let data = try encoder.encode(bitmap)
                    serialized.append(data)
                }
                return serialized
            },
            verify: { baseline, optimized in
                #expect(baseline.count == optimized.count)
                #expect(baseline.count == 3)
            }
        )

        // Print console report
        ConsoleReporter.print(result)

        // Calculate sizes
        let encoder = JSONEncoder()
        Swift.print("\n📊 Serialization Size Analysis:")
        Swift.print("  Sparse bitmap (100 values):")
        let sparseData = try encoder.encode(testBitmaps[0])
        Swift.print("    JSON: \(sparseData.count) bytes")

        Swift.print("  Dense bitmap (8000 values):")
        let denseData = try encoder.encode(testBitmaps[1])
        Swift.print("    JSON: \(denseData.count) bytes")

        Swift.print("  Mixed bitmap (~1500 values):")
        let mixedData = try encoder.encode(testBitmaps[2])
        Swift.print("    JSON: \(mixedData.count) bytes")

        Swift.print("\n📝 Note: Binary serialization not yet implemented.")
        Swift.print("Expected with Roaring binary format:")
        Swift.print("  - 50-70% faster serialization")
        Swift.print("  - 30-50% smaller size")
        Swift.print("  - See: https://github.com/RoaringBitmap/RoaringFormatSpec\n")
    }

    @Test("Deserialization Performance")
    func deserializationPerformance() async throws {
        // Create and serialize test bitmaps
        var testData: [Data] = []
        let encoder = JSONEncoder()

        for size in [100, 1000, 5000] {
            var bitmap = RoaringBitmap()
            for i in 0..<size {
                bitmap.add(UInt32(i))
            }
            let data = try encoder.encode(bitmap)
            testData.append(data)
        }

        nonisolated(unsafe) let unsafeData = testData

        let runner = BenchmarkRunner(config: .init(
            warmupIterations: 5,
            measurementIterations: 100,
            throughputDuration: 5.0,
            measureMemory: false
        ))

        // Benchmark deserialization
        let result = try await runner.compare(
            name: "BitmapIndex: JSON Deserialization",
            baseline: { @Sendable () async throws -> [RoaringBitmap] in
                // Current JSON deserialization
                let decoder = JSONDecoder()
                var bitmaps: [RoaringBitmap] = []
                for data in unsafeData {
                    let bitmap = try decoder.decode(RoaringBitmap.self, from: data)
                    bitmaps.append(bitmap)
                }
                return bitmaps
            },
            optimized: { @Sendable () async throws -> [RoaringBitmap] in
                // Same implementation
                let decoder = JSONDecoder()
                var bitmaps: [RoaringBitmap] = []
                for data in unsafeData {
                    let bitmap = try decoder.decode(RoaringBitmap.self, from: data)
                    bitmaps.append(bitmap)
                }
                return bitmaps
            },
            verify: { baseline, optimized in
                #expect(baseline.count == optimized.count)
                #expect(baseline.count == 3)
            }
        )


        // Print console report
        ConsoleReporter.print(result)
    }

    @Test("Round-trip Performance")
    func roundTripPerformance() async throws {
        let runner = BenchmarkRunner(config: .init(
            warmupIterations: 3,
            measurementIterations: 50,
            throughputDuration: 3.0,
            measureMemory: false
        ))

        // Test different bitmap sizes
        let result = try await runner.scale(
            name: "Bitmap Serialization Round-trip",
            dataSizes: [100, 500, 1000, 5000, 10000]
        ) { @Sendable (size: Int) async throws -> Int in
            // Create bitmap
            var bitmap = RoaringBitmap()
            for i in 0..<size {
                bitmap.add(UInt32(i))
            }

            // Serialize
            let encoder = JSONEncoder()
            let data = try encoder.encode(bitmap)

            // Deserialize
            let decoder = JSONDecoder()
            let restored = try decoder.decode(RoaringBitmap.self, from: data)

            return restored.cardinality
        }


        // Print console report
        ConsoleReporter.print(result)

        Swift.print("\n📊 Round-trip Scalability:")
        for point in result.dataPoints {
            Swift.print("  \(point.dataSize) values: \(String(format: "%.2f", point.metrics.latency.p95))ms (p95)")
        }
        Swift.print("")
    }

    @Test("Bitmap Operations Performance")
    func bitmapOperationsPerformance() async throws {
        // Create test bitmaps
        var bitmap1 = RoaringBitmap()
        var bitmap2 = RoaringBitmap()

        for i in 0..<10000 {
            if i % 2 == 0 {
                bitmap1.add(UInt32(i))
            }
            if i % 3 == 0 {
                bitmap2.add(UInt32(i))
            }
        }

        let b1 = bitmap1
        let b2 = bitmap2

        let runner = BenchmarkRunner(config: .init(
            warmupIterations: 5,
            measurementIterations: 100,
            throughputDuration: 5.0,
            measureMemory: false
        ))

        // Benchmark AND operation
        let andResult = try await runner.compare(
            name: "Bitmap AND Operation",
            baseline: { @Sendable () async throws -> Int in
                let result = b1 && b2
                return result.cardinality
            },
            optimized: { @Sendable () async throws -> Int in
                let result = b1 && b2
                return result.cardinality
            }
        )

        // Print results
        ConsoleReporter.print(andResult)

        // Benchmark OR operation
        let orResult = try await runner.compare(
            name: "Bitmap OR Operation",
            baseline: { @Sendable () async throws -> Int in
                let result = b1 || b2
                return result.cardinality
            },
            optimized: { @Sendable () async throws -> Int in
                let result = b1 || b2
                return result.cardinality
            }
        )

        ConsoleReporter.print(orResult)

        Swift.print("\n📊 Operation Performance:")
        Swift.print("  AND (5000 ∩ 3333 values): \(String(format: "%.2f", andResult.baseline.metrics.latency.p95))ms")
        Swift.print("  OR (5000 ∪ 3333 values): \(String(format: "%.2f", orResult.baseline.metrics.latency.p95))ms")
        Swift.print("")
    }
}
