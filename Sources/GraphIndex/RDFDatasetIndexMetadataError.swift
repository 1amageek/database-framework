import DatabaseKit

public enum RDFDatasetIndexMetadataError: Error, Sendable, Equatable {
    case invalidFixedGraph(
        indexName: String,
        reason: RDFTermValidationError
    )
}
