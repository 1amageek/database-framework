import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseOperationCore
#if DATABASE_GRAPH_OPERATIONS_ENABLED
public struct GraphOperationLimits: Sendable, Hashable {
    public let maximumLoadDocumentBytes: Int

    public init(
        maximumLoadDocumentBytes: Int = 8 * 1_024 * 1_024
    ) throws(GraphOperationLimitsError) {
        guard maximumLoadDocumentBytes > 0 else {
            throw .nonPositiveMaximumLoadDocumentBytes
        }
        self.maximumLoadDocumentBytes = maximumLoadDocumentBytes
    }

    public static let `default` = GraphOperationLimits(
        validatedMaximumLoadDocumentBytes: 8 * 1_024 * 1_024
    )

    private init(validatedMaximumLoadDocumentBytes: Int) {
        self.maximumLoadDocumentBytes = validatedMaximumLoadDocumentBytes
    }
}
#endif
