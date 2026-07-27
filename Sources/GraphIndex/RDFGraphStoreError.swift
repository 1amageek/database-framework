import DatabaseEngine
import DatabaseTypes
import DatabaseKit

public enum RDFGraphStoreError: Error, Sendable, Equatable {
    case graphAlreadyExists(RDFGraphName)
    case graphNotFound(RDFGraphName)
    case invalidQuad(RDFTermValidationError)
    case invalidTermEncoding(RDFTermStorageError)
    case invalidPhysicalIndex(RDFQuadIndexPhysicalCodecError)
    case keyTooLarge(actual: Int, maximum: Int)
    case catalogPrefixMismatch
    case catalogTruncatedKey
    case catalogUnexpectedTupleType(UInt8)
    case catalogTrailingTupleData(offset: Int)
    case invalidCatalogGraph(RDFTermStorageError)
    case invalidCatalogGraphName(RDFDatasetValidationError)
    case invalidCatalogMarker
    case missingCatalogForStoredQuad(RDFGraphName)
    case quadCountOverflow
}
