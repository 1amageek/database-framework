@_spi(DatabaseExecution)
public struct PreparedSPARQLUpdateRequest: Sendable {
    public let firstOperation: PreparedSPARQLUpdateOperation
    public let additionalOperations: [PreparedSPARQLUpdateOperation]

    public init(
        firstOperation: PreparedSPARQLUpdateOperation,
        additionalOperations: consuming [PreparedSPARQLUpdateOperation] = []
    ) {
        self.firstOperation = firstOperation
        self.additionalOperations = consume additionalOperations
    }

    public var count: Int {
        additionalOperations.count + 1
    }

    public func operation(at index: Int) -> PreparedSPARQLUpdateOperation {
        precondition(index >= 0 && index < count)
        return index == 0
            ? firstOperation
            : additionalOperations[index - 1]
    }
}
