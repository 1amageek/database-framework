#if DATABASE_SERVER_GRAPH_INDEXES
struct PreparedSPARQLUpdateRequest: Sendable {
    let firstOperation: PreparedSPARQLUpdateOperation
    let additionalOperations: [PreparedSPARQLUpdateOperation]

    init(
        firstOperation: PreparedSPARQLUpdateOperation,
        additionalOperations: consuming [PreparedSPARQLUpdateOperation] = []
    ) {
        self.firstOperation = firstOperation
        self.additionalOperations = consume additionalOperations
    }

    var count: Int {
        additionalOperations.count + 1
    }

    func operation(at index: Int) -> PreparedSPARQLUpdateOperation {
        precondition(index >= 0 && index < count)
        return index == 0
            ? firstOperation
            : additionalOperations[index - 1]
    }
}

#endif
