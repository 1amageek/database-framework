@testable import DatabaseEngine
import Foundation
import Testing

@Suite("Reservoir sampling benchmarks", .serialized, .heartbeat)
struct ReservoirSamplingBenchmarks {
    @Test("Algorithm L processes a large stream")
    func algorithmLProcessesLargeStream() {
        var sampler = ReservoirSampling<Int>(reservoirSize: 100)
        let start = ContinuousClock.now

        for value in 0..<1_000_000 {
            sampler.add(value)
        }

        let elapsed = start.duration(to: ContinuousClock.now)
        #expect(sampler.sample.count == 100)
        #expect(sampler.elementsSeen == 1_000_000)
        print("Reservoir sampling: 1,000,000 values in \(elapsed)")
    }
}
