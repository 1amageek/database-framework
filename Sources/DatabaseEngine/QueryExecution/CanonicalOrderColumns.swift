import DatabaseKit

/// One decoration pass retains one descriptor for its current immutable layout.
struct CanonicalOrderColumns {
    private var current: (layout: CanonicalSourceLayout, columns: [QueryColumn],
                          claim: DatabaseIntermediateReservation)?

    mutating func columns(for layout: CanonicalSourceLayout, meter: DatabaseWorkMeter) throws -> [QueryColumn] {
        if let current, current.layout === layout { return current.columns }
        current = nil
        let footprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
            count: layout.slots.count, element: QueryColumn.self)
        let claim = try meter.reserveIntermediate(bytes: footprint.bytes, at: .sortInput)
        let columns = try layout.typedColumns()
        current = (layout, columns, claim)
        return columns
    }
}
