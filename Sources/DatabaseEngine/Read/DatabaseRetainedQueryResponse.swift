import DatabaseKit
import DatabaseTypes

/// Copyable ownership for response metadata retained between query stages.
///
/// The reservation is shared with the producer result so the metadata's COW
/// storage never outlives its request charge during asynchronous post-processing.
package struct DatabaseRetainedQueryMetadata: Sendable {
    package static let empty = DatabaseRetainedQueryMetadata(
        values: [:],
        reservation: nil,
        workMeter: nil
    )

    package let values: [String: FieldValue]
    private let reservation: DatabaseIntermediateReservation?
    private let workMeter: DatabaseWorkMeter?

    package init(
        values: [String: FieldValue],
        reservation: DatabaseIntermediateReservation?,
        workMeter: DatabaseWorkMeter?
    ) {
        precondition(values.isEmpty || reservation != nil)
        self.values = values
        self.reservation = reservation
        self.workMeter = workMeter
    }

    package func validateWorkMeter(
        _ expectedWorkMeter: DatabaseWorkMeter,
        sourceName: String
    ) throws {
        guard let workMeter else {
            guard values.isEmpty else {
                throw CanonicalReadError.executorWorkMeterMismatch(
                    sourceName: sourceName
                )
            }
            return
        }
        guard workMeter === expectedWorkMeter else {
            throw CanonicalReadError.executorWorkMeterMismatch(
                sourceName: sourceName
            )
        }
    }
}

/// Request-accounted canonical response retained between internal operators.
///
/// The backing row owner and its reservation remain alive until the last copy
/// of this value is released. Public output promotion is package-scoped so an
/// intermediate executor cannot accidentally create an unaccounted Array.
@_spi(DatabaseExecution)
public struct DatabaseRetainedQueryResponse: Sendable {
    package let rows: DatabaseSharedRetainedArray<QueryRow>
    package let visibleRange: Range<Int>
    private let retainedMetadata: DatabaseRetainedQueryMetadata

    public let continuation: QueryContinuation?
    public let affectedRows: Int?

    package init(
        rows: DatabaseSharedRetainedArray<QueryRow>,
        visibleRange: Range<Int>,
        continuation: QueryContinuation?,
        retainedMetadata: DatabaseRetainedQueryMetadata = .empty,
        affectedRows: Int? = nil
    ) {
        precondition(
            visibleRange.lowerBound >= rows.startIndex
                && visibleRange.upperBound <= rows.endIndex
        )
        self.rows = rows
        self.visibleRange = visibleRange
        self.continuation = continuation
        self.retainedMetadata = retainedMetadata
        self.affectedRows = affectedRows
    }

    public var count: Int { visibleRange.count }
    public var isEmpty: Bool { visibleRange.isEmpty }

    package func validateWorkMeter(
        _ expectedWorkMeter: DatabaseWorkMeter,
        sourceName: String
    ) throws {
        guard rows.workMeter === expectedWorkMeter else {
            throw CanonicalReadError.executorWorkMeterMismatch(
                sourceName: sourceName
            )
        }
        try retainedMetadata.validateWorkMeter(
            expectedWorkMeter,
            sourceName: sourceName
        )
    }

    /// Returns one visible row together with the shared reservation owner.
    public func row(at index: Int) -> DatabaseRetainedQueryRow {
        precondition(index >= 0 && index < count)
        return DatabaseRetainedQueryRow(
            owner: rows,
            index: visibleRange.lowerBound + index
        )
    }

    package var visibleRows: DatabaseSharedRetainedArrayView<QueryRow> {
        rows.boundedView(visibleRange)
    }

    /// Promotes retained storage only at a top-level public result boundary.
    @_spi(DatabaseExecution)
    public consuming func promoteToPublicResponse() -> QueryResponse {
        let visibleRange = visibleRange
        let continuation = continuation
        let metadata = retainedMetadata.values
        let affectedRows = affectedRows
        guard !visibleRange.isEmpty else {
            return QueryResponse(
                rows: [],
                continuation: continuation,
                metadata: metadata,
                affectedRows: affectedRows
            )
        }

        var outputRows = rows.promoteToOutput()
        if visibleRange.upperBound < outputRows.count {
            outputRows.removeLast(outputRows.count - visibleRange.upperBound)
        }
        if visibleRange.lowerBound > 0 {
            outputRows.removeFirst(visibleRange.lowerBound)
        }
        return QueryResponse(
            rows: outputRows,
            continuation: continuation,
            metadata: metadata,
            affectedRows: affectedRows
        )
    }
}
