import DatabaseQueryOperations
import DatabaseOperationCore
#if DATABASE_MUTATION_OPERATIONS_GRAPH_INDEXES
import DatabaseEngine

public struct SPARQLLoadRequest: Sendable {
    public let sourceIRI: String
    public let maximumDocumentBytes: Int
    public let maximumTriples: Int
    public let workMeter: DatabaseWorkMeter

    public init(
        sourceIRI: String,
        maximumDocumentBytes: Int,
        maximumTriples: Int,
        workMeter: DatabaseWorkMeter
    ) {
        self.sourceIRI = sourceIRI
        self.maximumDocumentBytes = maximumDocumentBytes
        self.maximumTriples = maximumTriples
        self.workMeter = workMeter
    }
}

#endif
