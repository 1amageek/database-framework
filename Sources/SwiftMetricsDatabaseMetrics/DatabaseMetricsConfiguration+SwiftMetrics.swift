import DatabaseEngine
import Metrics

public extension DatabaseMetricsConfiguration {
    /// Routes database metrics through the application's Swift Metrics backend.
    static let swiftMetrics = DatabaseMetricsConfiguration(
        createCounter: { label, dimensions in
            let counter = Counter(
                label: label,
                dimensions: dimensions.map { ($0.name, $0.value) }
            )
            return DatabaseMetricCounter { value in
                counter.increment(by: value)
            }
        },
        createTimer: { label, dimensions in
            let timer = Metrics.Timer(
                label: label,
                dimensions: dimensions.map { ($0.name, $0.value) }
            )
            return DatabaseMetricTimer(
                recordNanoseconds: { value in
                    timer.recordNanoseconds(Int64(clamping: value))
                },
                recordSeconds: { value in
                    timer.recordSeconds(value)
                }
            )
        }
    )
}
