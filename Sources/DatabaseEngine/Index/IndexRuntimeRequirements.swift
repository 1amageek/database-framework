/// Runtime services required to expose an index type's complete behavior.
public struct IndexRuntimeRequirements: Sendable, Equatable {
    public let requiresEntityReadExecutor: Bool
    public let requiresPolymorphicReadExecutor: Bool
    public let logicalSourceExecutors: LogicalSourceExecutorRequirements
    public let requiresVersionstampedMutations: Bool

    public init(
        requiresEntityReadExecutor: Bool = false,
        requiresPolymorphicReadExecutor: Bool = false,
        logicalSourceExecutors: LogicalSourceExecutorRequirements = [],
        requiresVersionstampedMutations: Bool = false
    ) {
        self.requiresEntityReadExecutor = requiresEntityReadExecutor
        self.requiresPolymorphicReadExecutor = requiresPolymorphicReadExecutor
        self.logicalSourceExecutors = logicalSourceExecutors
        self.requiresVersionstampedMutations = requiresVersionstampedMutations
    }

    public static var none: Self { Self() }

    public static var entityAndPolymorphicReads: Self {
        Self(
            requiresEntityReadExecutor: true,
            requiresPolymorphicReadExecutor: true
        )
    }

    public static var versionHistory: Self {
        Self(
            requiresEntityReadExecutor: true,
            requiresPolymorphicReadExecutor: true,
            requiresVersionstampedMutations: true
        )
    }

    public static var graphQueries: Self {
        Self(logicalSourceExecutors: [.graphTable, .sparql])
    }
}
