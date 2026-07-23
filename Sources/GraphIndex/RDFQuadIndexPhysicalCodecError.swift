import DatabaseValue
import Graph

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
        DatabaseRDFTermRole,
        DatabaseRDFTermCodecError
    )
    case invalidComponent(
        RDFDatasetIndexComponent,
        DatabaseRDFTermCodecError
    )
}
