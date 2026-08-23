import DatabaseKit

/// Request-accounted canonical rows retained for an internal execution stage.
///
/// This SPI keeps the canonical row owner and its intermediate-memory
/// reservation alive while another package target decodes the rows. It must
/// not be promoted to an ordinary Array except at a top-level public result
/// boundary.
@_spi(DatabaseExecution)
public struct DatabaseRetainedQueryRows: Sendable {
    package let owner: DatabaseSharedRetainedArray<QueryRow>
    package let visibleRange: Range<Int>

    package init(
        owner: DatabaseSharedRetainedArray<QueryRow>,
        visibleRange: Range<Int>
    ) {
        precondition(
            visibleRange.lowerBound >= owner.startIndex
                && visibleRange.upperBound <= owner.endIndex
        )
        self.owner = owner
        self.visibleRange = visibleRange
    }

    public var count: Int { visibleRange.count }
    public var isEmpty: Bool { visibleRange.isEmpty }

    public func validateWorkMeter(
        _ expectedWorkMeter: DatabaseWorkMeter,
        sourceName: String
    ) throws {
        guard owner.workMeter === expectedWorkMeter else {
            throw CanonicalReadError.executorWorkMeterMismatch(
                sourceName: sourceName
            )
        }
    }

    /// Returns one row together with the shared reservation owner.
    public func row(at index: Int) -> DatabaseRetainedQueryRow {
        precondition(index >= 0 && index < count)
        return DatabaseRetainedQueryRow(
            owner: owner,
            index: visibleRange.lowerBound + index
        )
    }
}
