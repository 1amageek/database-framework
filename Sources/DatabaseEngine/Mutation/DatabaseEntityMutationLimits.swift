@_spi(DatabaseExecution)
public struct DatabaseEntityMutationLimits: Sendable, Hashable {
    public let maximumChanges: Int
    public let maximumPreconditions: Int

    public init(
        maximumChanges: Int,
        maximumPreconditions: Int
    ) throws(DatabaseEntityMutationLimitsError) {
        guard maximumChanges > 0 else {
            throw .nonPositiveMaximumChanges(maximumChanges)
        }
        guard maximumPreconditions > 0 else {
            throw .nonPositiveMaximumPreconditions(maximumPreconditions)
        }
        self.maximumChanges = maximumChanges
        self.maximumPreconditions = maximumPreconditions
    }
}
