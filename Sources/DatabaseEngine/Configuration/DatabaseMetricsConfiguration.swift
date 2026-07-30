/// One stable label dimension attached to a database metric.
public struct DatabaseMetricDimension: Sendable, Hashable {
    public let name: String
    public let value: String

    public init(name: String, value: String) {
        self.name = name
        self.value = value
    }
}

/// A counter whose destination is selected by container configuration.
public struct DatabaseMetricCounter: Sendable {
    private let recordIncrement: @Sendable (Int64) -> Void

    public init(recordIncrement: @escaping @Sendable (Int64) -> Void) {
        self.recordIncrement = recordIncrement
    }

    public func increment(by value: Int = 1) {
        recordIncrement(Int64(value))
    }

    public static let disabled = DatabaseMetricCounter { _ in }
}

/// A duration recorder whose destination is selected by container configuration.
public struct DatabaseMetricTimer: Sendable {
    private let recordNanosecondsValue: @Sendable (UInt64) -> Void
    private let recordSecondsValue: @Sendable (Double) -> Void

    public init(
        recordNanoseconds: @escaping @Sendable (UInt64) -> Void,
        recordSeconds: @escaping @Sendable (Double) -> Void
    ) {
        self.recordNanosecondsValue = recordNanoseconds
        self.recordSecondsValue = recordSeconds
    }

    public func recordNanoseconds(_ value: UInt64) {
        recordNanosecondsValue(value)
    }

    public func recordSeconds(_ value: Double) {
        recordSecondsValue(value)
    }

    public static let disabled = DatabaseMetricTimer(
        recordNanoseconds: { _ in },
        recordSeconds: { _ in }
    )
}

/// Container-scoped construction policy for operational database metrics.
public struct DatabaseMetricsConfiguration: Sendable {
    public typealias CounterFactory = @Sendable (
        _ label: String,
        _ dimensions: [DatabaseMetricDimension]
    ) -> DatabaseMetricCounter
    public typealias TimerFactory = @Sendable (
        _ label: String,
        _ dimensions: [DatabaseMetricDimension]
    ) -> DatabaseMetricTimer

    private let createCounter: CounterFactory
    private let createTimer: TimerFactory

    public init(
        createCounter: @escaping CounterFactory,
        createTimer: @escaping TimerFactory
    ) {
        self.createCounter = createCounter
        self.createTimer = createTimer
    }

    public func counter(
        label: String,
        dimensions: [DatabaseMetricDimension] = []
    ) -> DatabaseMetricCounter {
        createCounter(label, dimensions)
    }

    func counter(
        label: String,
        dimensions: [(String, String)]
    ) -> DatabaseMetricCounter {
        createCounter(
            label,
            dimensions.map {
                DatabaseMetricDimension(name: $0.0, value: $0.1)
            }
        )
    }

    public func timer(
        label: String,
        dimensions: [DatabaseMetricDimension] = []
    ) -> DatabaseMetricTimer {
        createTimer(label, dimensions)
    }

    func timer(
        label: String,
        dimensions: [(String, String)]
    ) -> DatabaseMetricTimer {
        createTimer(
            label,
            dimensions.map {
                DatabaseMetricDimension(name: $0.0, value: $0.1)
            }
        )
    }

    public static let disabled = DatabaseMetricsConfiguration(
        createCounter: { _, _ in .disabled },
        createTimer: { _, _ in .disabled }
    )
}
