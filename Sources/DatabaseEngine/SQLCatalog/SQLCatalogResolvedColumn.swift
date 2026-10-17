import DatabaseKit

struct SQLCatalogResolvedColumn: Sendable {
    let declaration: SQLColumn
    let field: FieldSchema?
    let type: QueryColumnType
    let isNullable: Bool
}
