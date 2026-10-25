import DatabaseKit

struct CanonicalRelation: Sendable {
    let schema: CanonicalRelationSchema
    let rows: CanonicalRetainedRows

    init(schema: CanonicalRelationSchema, rows: CanonicalRetainedRows, schemaLayout: CanonicalSourceLayout? = nil) {
        self.schema = schemaLayout.map { schema.retaining(layout: $0) } ?? schema
        self.rows = rows
    }
}
