import DatabaseKit

/// Immutable feature-reader composition for one runtime generation.
struct FusionReadExecutorRegistry: Sendable {
    private let indexExecutors: [IndexType: any FusionIndexReadExecutor]

    init(
        indexExecutors: [any FusionIndexReadExecutor]
    ) throws(DatabaseRuntimeConfigurationError) {
        var byType: [IndexType: any FusionIndexReadExecutor] = [:]
        byType.reserveCapacity(indexExecutors.count)
        for executor in indexExecutors {
            guard byType.updateValue(
                executor,
                forKey: executor.indexType
            ) == nil else {
                throw .duplicateFusionReadExecutor(executor.indexType)
            }
        }
        self.indexExecutors = byType
    }

    func indexExecutor(
        for type: IndexType
    ) -> (any FusionIndexReadExecutor)? {
        indexExecutors[type]
    }
}
