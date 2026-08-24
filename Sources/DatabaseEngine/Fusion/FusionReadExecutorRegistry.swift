import DatabaseKit

/// Immutable feature-reader composition for one runtime generation.
struct FusionReadExecutorRegistry: Sendable {
    private let indexExecutors: [IndexType: any FusionIndexReadExecutor]
    private let connectedExecutors: [
        IndexType: any FusionConnectedReadExecutor
    ]

    init(
        indexExecutors: [any FusionIndexReadExecutor],
        connectedExecutors: [any FusionConnectedReadExecutor]
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

        var connectedByType: [
            IndexType: any FusionConnectedReadExecutor
        ] = [:]
        connectedByType.reserveCapacity(connectedExecutors.count)
        for executor in connectedExecutors {
            guard connectedByType.updateValue(
                executor,
                forKey: executor.indexType
            ) == nil else {
                throw .duplicateFusionConnectedReadExecutor(
                    executor.indexType
                )
            }
        }
        self.connectedExecutors = connectedByType
    }

    func indexExecutor(
        for type: IndexType
    ) -> (any FusionIndexReadExecutor)? {
        indexExecutors[type]
    }

    func connectedExecutor(
        for type: IndexType
    ) -> (any FusionConnectedReadExecutor)? {
        connectedExecutors[type]
    }
}
