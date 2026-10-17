import DatabaseKit

/// An immutable generation with independent cursors and no operational parent.
struct SQLRecursiveFrontier: Sendable {
    private let schema: CanonicalRelationSchema
    private let rows: DatabaseSharedRetainedArray<CanonicalRelation>
    private let meter: DatabaseWorkMeter

    init(schema: CanonicalRelationSchema, rows: DatabaseSharedRetainedArray<CanonicalRelation>,
         meter: DatabaseWorkMeter) {
        self.schema = schema; self.rows = rows; self.meter = meter
    }

    func consumer() -> any SQLRelationProducer {
        let rows = rows
        return SQLRelationPullProducer(schema: schema, meter: meter) { index in
            index < rows.count ? rows[index] : nil
        }
    }
}
