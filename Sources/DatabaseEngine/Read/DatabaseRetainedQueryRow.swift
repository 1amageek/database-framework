import DatabaseKit

/// One canonical query row whose request-accounted storage remains retained.
///
/// Keeping this value alive keeps the producer's reservation alive. Cross-target
/// consumers receive semantic accessors instead of the backing dictionaries, so
/// a COW container cannot accidentally escape without its owner.
@_spi(DatabaseExecution)
public struct DatabaseRetainedQueryRow: Sendable {
    private let owner: DatabaseSharedRetainedArray<QueryRow>
    private let index: Int

    package init(
        owner: DatabaseSharedRetainedArray<QueryRow>,
        index: Int
    ) {
        precondition(index >= owner.startIndex && index < owner.endIndex)
        self.owner = owner
        self.index = index
    }

    /// Reads a scalar annotation without exposing its backing value container.
    public func float64Annotation(named name: String) -> Double? {
        owner.withElement(at: index) { row in
            row.annotations[name]?.float64Value
        }
    }

    /// Materializes a row only for DatabaseEngine's own reservation-transfer
    /// boundaries. The retained wrapper must remain alive until the destination
    /// reservation has been admitted.
    package func materializeForRetainedTransfer() -> QueryRow {
        owner.withElement(at: index) { row in row }
    }
}
