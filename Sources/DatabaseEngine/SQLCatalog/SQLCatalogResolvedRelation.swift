import DatabaseKit

struct SQLCatalogResolvedRelation: Sendable {
    let declaration: SQLRelation
    let entity: Schema.Entity
    let columns: [SQLCatalogResolvedColumn]
}
