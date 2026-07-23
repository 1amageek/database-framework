import DatabaseValue

public enum RDFDatasetIndexMetadataError: Error, Sendable, Equatable {
    case invalidFixedGraph(
        indexName: String,
        reason: DatabaseRDFTermCodecError
    )
}
