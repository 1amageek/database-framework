import Foundation
import Synchronization

public struct MemoryMetrics: Codable, Sendable, Hashable {
    public let peakMB: Double
    public let avgMB: Double
    public let samples: Int

    public init(peakMB: Double, avgMB: Double, samples: Int) {
        self.peakMB = peakMB
        self.avgMB = avgMB
        self.samples = samples
    }

    /// Measure memory usage during operation execution
    /// - Parameters:
    ///   - sampleInterval: Interval between samples in seconds (default: 0.1)
    ///   - operation: The operation to measure
    /// - Returns: Memory metrics in megabytes
    public static func measure(
        sampleInterval: TimeInterval = 0.1,
        operation: @Sendable () async throws -> Void
    ) async throws -> MemoryMetrics {
        let samples = Mutex<[Double]>([])
        let samplingTask = Task {
            while !Task.isCancelled {
                let bytes = getCurrentMemoryUsage()
                let mb = Double(bytes) / (1024.0 * 1024.0)
                samples.withLock { $0.append(mb) }
                try? await Task.sleep(nanoseconds: UInt64(sampleInterval * 1_000_000_000))
            }
        }

        try await operation()
        samplingTask.cancel()

        let samplesArray = samples.withLock { Array($0) }
        guard !samplesArray.isEmpty else {
            return MemoryMetrics(peakMB: 0, avgMB: 0, samples: 0)
        }

        let peak = samplesArray.max() ?? 0
        let avg = samplesArray.reduce(0.0, +) / Double(samplesArray.count)

        return MemoryMetrics(peakMB: peak, avgMB: avg, samples: samplesArray.count)
    }

    /// Get current memory usage in bytes
    private static func getCurrentMemoryUsage() -> UInt64 {
        var info = mach_task_basic_info()
        var count = mach_msg_type_number_t(MemoryLayout<mach_task_basic_info>.size) / 4

        let result = withUnsafeMutablePointer(to: &info) {
            $0.withMemoryRebound(to: integer_t.self, capacity: 1) {
                task_info(
                    mach_task_self_,
                    task_flavor_t(MACH_TASK_BASIC_INFO),
                    $0,
                    &count
                )
            }
        }

        guard result == KERN_SUCCESS else { return 0 }
        return info.resident_size
    }

    /// Calculate improvement percentage (positive = less memory)
    /// - Parameters:
    ///   - baseline: Baseline peak memory
    ///   - optimized: Optimized peak memory
    /// - Returns: Improvement percentage (positive = improvement)
    public static func improvement(baseline: Double, optimized: Double) -> Double {
        guard baseline > 0 else { return 0 }
        return ((baseline - optimized) / baseline) * 100.0
    }
}
