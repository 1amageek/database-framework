import DatabaseKit
import DatabaseTypes

/// Immutable collecting rows with no public-output or Ready transition.
struct SQLBlockCollectedRows: Sendable {
    let rows: DatabaseSharedRetainedQueryRows
    let visibleRange: Range<Int>
    let metadata: [String: FieldValue]
    let affectedRows: Int?
    let metadataReservation: DatabaseIntermediateReservation?

    init(_ response: consuming CanonicalRetainedQueryResponse) {
        self.rows = response.rows
        self.visibleRange = response.visibleRange
        self.metadata = response.metadata
        self.affectedRows = response.affectedRows
        self.metadataReservation = response.metadataReservation
    }

    init(rows: DatabaseSharedRetainedQueryRows, visibleRange: Range<Int>? = nil,
         metadata: [String: FieldValue] = [:], affectedRows: Int?,
         metadataReservation: DatabaseIntermediateReservation? = nil) {
        self.rows = rows; self.visibleRange = visibleRange ?? 0..<rows.count
        self.metadata = metadata; self.affectedRows = affectedRows; self.metadataReservation = metadataReservation
    }
}
