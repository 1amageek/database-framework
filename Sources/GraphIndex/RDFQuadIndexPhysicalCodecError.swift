import DatabaseEngine
import DatabaseTypes
import DatabaseKit

public enum RDFQuadIndexPhysicalCodecError: Error, Sendable, Equatable {
    case invalidComponentCount(expected: ClosedRange<Int>, actual: Int)
    case byteCountOverflow
    case unsupportedOrdering(GraphIndexOrdering)
    case prefixMismatch(GraphIndexOrdering)
    case truncatedComponent(position: Int)
    case unexpectedTupleType(position: Int, actualTypeCode: UInt8)
    case invalidTupleEncoding(position: Int)
    case unexpectedTrailingTupleData(offset: Int)
    case invalidEncoding(
        RDFTermRole,
        RDFTermStorageError
    )
    case invalidComponent(
        RDFDatasetIndexComponent,
        RDFTermStorageError
    )
    case invalidQuad(RDFDatasetValidationError)
}
