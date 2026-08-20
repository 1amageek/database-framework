#if FOUNDATION_DB
import DatabaseEngine
import Foundation
import Testing

@Suite("TDigest benchmarks", .serialized, .heartbeat)
struct TDigestBenchmarks {
    @Test("Ingestion scalability and retained state")
    func ingestionScalabilityAndRetainedState() throws {
        let inputSizes = [100_000, 1_000_000]

        for inputSize in inputSizes {
            var digest = try TDigest(compression: 100)
            let start = DispatchTime.now().uptimeNanoseconds

            for value in 0..<inputSize {
                try digest.add(Double(value % 10_000))
            }

            _ = try digest.quantile(0.5)
            let elapsedNanoseconds = DispatchTime.now().uptimeNanoseconds - start
            let elapsedSeconds = max(
                Double(elapsedNanoseconds) / 1_000_000_000,
                .leastNonzeroMagnitude
            )
            let valuesPerSecond = Double(inputSize) / elapsedSeconds

            print(
                "TDigest size=\(inputSize) values/s=\(valuesPerSecond) "
                    + "centroids=\(digest.centroidCount) "
                    + "retainedBytes=\(digest.estimatedMemoryBytes)"
            )
            #expect(digest.count == inputSize)
            #expect(digest.centroidCount < 500)
            #expect(digest.estimatedMemoryBytes < 20_000)
        }
    }
}
#endif
