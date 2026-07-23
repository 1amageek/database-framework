import Synchronization

/// Accumulates dataset work performed by nested runtime expressions.
///
/// A new accumulator is created for every transaction attempt so statistics
/// from a failed retry are never reported by the successful attempt.
final class SPARQLNestedExpressionStatistics: Sendable {
    private let statistics = Mutex(ExecutionStatistics())

    func record(_ additionalStatistics: ExecutionStatistics) {
        statistics.withLock { statistics in
            statistics = statistics.merged(with: additionalStatistics)
        }
    }

    func snapshot() -> ExecutionStatistics {
        statistics.withLock { $0 }
    }
}
