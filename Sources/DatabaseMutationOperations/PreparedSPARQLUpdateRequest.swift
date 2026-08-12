import DatabaseQueryOperations
import DatabaseOperationCore
#if DATABASE_MUTATION_OPERATIONS_GRAPH_INDEXES
package struct PreparedSPARQLUpdateRequest: Sendable {
    package let firstOperation: PreparedSPARQLUpdateOperation
    package let additionalOperations: [PreparedSPARQLUpdateOperation]

    package init(
        firstOperation: PreparedSPARQLUpdateOperation,
        additionalOperations: consuming [PreparedSPARQLUpdateOperation] = []
    ) {
        self.firstOperation = firstOperation
        self.additionalOperations = consume additionalOperations
    }

    package var count: Int {
        additionalOperations.count + 1
    }

    package func operation(at index: Int) -> PreparedSPARQLUpdateOperation {
        precondition(index >= 0 && index < count)
        return index == 0
            ? firstOperation
            : additionalOperations[index - 1]
    }
}

#endif
