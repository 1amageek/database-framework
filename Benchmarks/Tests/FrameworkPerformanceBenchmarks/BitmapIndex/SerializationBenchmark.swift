#if FOUNDATION_DB
// Benchmark-only target; excluded from the database-framework test graph.
import BenchmarkFramework
import BitmapIndex
import DatabaseTypes
import Foundation
import StorageKit
import Testing

@Suite("BitmapIndex: Persisted Representation Benchmark", .serialized, .heartbeat)
struct PersistedRepresentationBenchmark {
    private let configuration = BenchmarkRunner.Config(
        warmupIterations: 5,
        measurementIterations: 100,
        throughputDuration: 5.0,
        measureMemory: false
    )

    @Test("Binary serialization performance")
    func serializationPerformance() async throws {
        let sizes = [100, 500, 1_000, 5_000, 10_000]
        let bitmaps = makeBitmaps(sizes: sizes)
        let runner = BenchmarkRunner(config: configuration)

        let result = try await runner.scale(
            name: "Bitmap persisted representation encoding",
            dataSizes: sizes
        ) { @Sendable size in
            guard let bitmap = bitmaps[size] else {
                throw BenchmarkInputError.missingBitmap(size)
            }
            return try bitmap.serializedBytes().count
        }

        ConsoleReporter.print(result)
    }

    @Test("Binary deserialization performance")
    func deserializationPerformance() async throws {
        let sizes = [100, 500, 1_000, 5_000, 10_000]
        let bitmaps = makeBitmaps(sizes: sizes)
        var representations: [Int: ByteString] = [:]
        for (size, bitmap) in bitmaps {
            representations[size] = try bitmap.serializedBytes()
        }
        let encodedRepresentations = representations
        let runner = BenchmarkRunner(config: configuration)

        let result = try await runner.scale(
            name: "Bitmap persisted representation decoding",
            dataSizes: sizes
        ) { @Sendable size in
            guard let bytes = encodedRepresentations[size] else {
                throw BenchmarkInputError.missingRepresentation(size)
            }
            return try RoaringBitmap(serializedBytes: bytes).cardinality
        }

        ConsoleReporter.print(result)
    }

    @Test("Binary round-trip performance")
    func roundTripPerformance() async throws {
        let sizes = [100, 500, 1_000, 5_000, 10_000]
        let bitmaps = makeBitmaps(sizes: sizes)
        let runner = BenchmarkRunner(config: configuration)

        let result = try await runner.scale(
            name: "Bitmap persisted representation round-trip",
            dataSizes: sizes
        ) { @Sendable size in
            guard let bitmap = bitmaps[size] else {
                throw BenchmarkInputError.missingBitmap(size)
            }
            let bytes = try bitmap.serializedBytes()
            return try RoaringBitmap(serializedBytes: bytes).cardinality
        }

        ConsoleReporter.print(result)
    }

    @Test("Bitmap set operation performance")
    func bitmapOperationsPerformance() async throws {
        var evenValues = RoaringBitmap()
        var divisibleByThreeValues = RoaringBitmap()
        for value in 0..<10_000 {
            if value.isMultiple(of: 2) {
                evenValues.add(UInt32(value))
            }
            if value.isMultiple(of: 3) {
                divisibleByThreeValues.add(UInt32(value))
            }
        }

        let left = evenValues
        let right = divisibleByThreeValues
        let runner = BenchmarkRunner(config: configuration)
        let result = try await runner.scale(
            name: "Bitmap set operations",
            dataSizes: [0, 1]
        ) { @Sendable operation in
            switch operation {
            case 0:
                return (left && right).cardinality
            case 1:
                return (left || right).cardinality
            default:
                throw BenchmarkInputError.unknownOperation(operation)
            }
        }

        ConsoleReporter.print(result)
    }

    private func makeBitmaps(sizes: [Int]) -> [Int: RoaringBitmap] {
        var bitmaps: [Int: RoaringBitmap] = [:]
        for size in sizes {
            var bitmap = RoaringBitmap()
            for value in 0..<size {
                bitmap.add(UInt32(value))
            }
            bitmaps[size] = bitmap
        }
        return bitmaps
    }
}

private enum BenchmarkInputError: Error, Sendable, Equatable {
    case missingBitmap(Int)
    case missingRepresentation(Int)
    case unknownOperation(Int)
}
#endif
