import DatabaseEngine
import DatabaseTypes

/// Failures while scanning canonical RDF dataset indexes.
public enum RDFDatasetScannerError: Error, Sendable, Equatable {
    public enum RetainedByteCountOperation: Sendable, Equatable {
        case addition
        case multiplication
    }

    case physicalIndexFailure(
        source: String,
        reason: RDFQuadIndexPhysicalCodecError
    )
    case invalidRDFComponent(
        source: String,
        component: RDFDatasetIndexComponent,
        reason: RDFTermStorageError
    )
    case namedGraphMergeRequiresBoundGraph
    case retainedByteCountOverflow(
        operation: RetainedByteCountOperation,
        left: UInt64,
        right: UInt64
    )
    case retainedWorklistCapacityExceeded(
        required: UInt64,
        maximum: UInt64
    )
}
