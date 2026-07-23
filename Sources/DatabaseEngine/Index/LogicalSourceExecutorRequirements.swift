/// Allocation-free flags describing logical query sources required by an index.
public struct LogicalSourceExecutorRequirements: OptionSet, Sendable, Hashable {
    public let rawValue: UInt8

    public init(rawValue: UInt8) {
        self.rawValue = rawValue
    }

    public static var graphTable: Self { Self(rawValue: 1 << 0) }
    public static var sparql: Self { Self(rawValue: 1 << 1) }
}
