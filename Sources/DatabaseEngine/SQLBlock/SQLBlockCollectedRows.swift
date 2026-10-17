import DatabaseKit
import DatabaseTypes

/// Immutable collecting rows with no public-output or Ready transition.
struct SQLBlockCollectedRows: Sendable {
    let rows: DatabaseSharedRetainedQueryRows
    let visibleRange: Range<Int>
    let metadata: [String: FieldValue]
    let metadataReservation: DatabaseIntermediateReservation?

    init(_ response: consuming CanonicalRetainedQueryResponse) {
        self.rows = response.rows
        self.visibleRange = response.visibleRange
        self.metadata = response.metadata
        self.metadataReservation = response.metadataReservation
    }
}
