@_spi(DatabaseExecution)
public struct SPARQLUpdateLimits: Sendable, Hashable {
    public let maximumMutations: Int

    public init(maximumMutations: Int) throws(SPARQLUpdateError) {
        guard maximumMutations > 0 else {
            throw .invalidMaximumMutations(maximumMutations)
        }
        self.maximumMutations = maximumMutations
    }
}
