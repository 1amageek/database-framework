import DatabaseValue
import Graph

public enum RDFGraphStoreError: Error, Sendable, Equatable {
    case graphAlreadyExists(RDFGraphName)
    case graphNotFound(RDFGraphName)
    case invalidQuad(RDFDatasetValidationError)
    case invalidTermEncoding(DatabaseRDFTermCodecError)
    case invalidPhysicalIndex(RDFQuadIndexPhysicalCodecError)
    case keyTooLarge(actual: Int, maximum: Int)
    case catalogPrefixMismatch
    case catalogTruncatedKey
    case catalogUnexpectedTupleType(UInt8)
    case catalogTrailingTupleData(offset: Int)
    case invalidCatalogGraph(DatabaseRDFTermCodecError)
    case invalidCatalogGraphName(RDFDatasetValidationError)
    case invalidCatalogMarker
    case missingCatalogForStoredQuad(RDFGraphName)
    case quadCountOverflow
}
