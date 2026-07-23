/// Runtime services required to expose an index kind's complete behavior.
public struct IndexRuntimeRequirements: Sendable, Equatable {
    public let requiresEntityReadExecutor: Bool
    public let requiresPolymorphicReadExecutor: Bool
    public let logicalSourceExecutors: LogicalSourceExecutorRequirements

    public init(
        requiresEntityReadExecutor: Bool = false,
        requiresPolymorphicReadExecutor: Bool = false,
        logicalSourceExecutors: LogicalSourceExecutorRequirements = []
    ) {
        self.requiresEntityReadExecutor = requiresEntityReadExecutor
        self.requiresPolymorphicReadExecutor = requiresPolymorphicReadExecutor
        self.logicalSourceExecutors = logicalSourceExecutors
    }

    public static var none: Self { Self() }

    public static var entityAndPolymorphicReads: Self {
        Self(
            requiresEntityReadExecutor: true,
            requiresPolymorphicReadExecutor: true
        )
    }

    public static var graphQueries: Self {
        Self(logicalSourceExecutors: [.graphTable, .sparql])
    }
}
