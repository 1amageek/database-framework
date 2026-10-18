import DatabaseKit

extension SQLDDLExecutor {
    /// Admits the temporary metadata copies before candidate arrays are constructed.
    func reserveMetadata(for command: SQLDDLStatement) async throws -> DatabaseIntermediateReservation {
        let lease = await transaction.currentSQLSchemaLease()
        let catalog = lease.sqlCatalog.catalog
        let canonical = try SQLCatalogCodec.admit(catalog, fingerprint: lease.fingerprint,
            protectedContracts: lease.sqlCatalog.protectedContracts, limits: .default)
        var footprint = try DatabaseIntermediateFootprint(bytes: UInt64(canonical)).multiplied(by: 4)
        var elements = UInt64(catalog.namespaces.count + catalog.relations.count + catalog.sequences.count
            + catalog.views.count + catalog.materializedViews.count + catalog.indexes.count)
        for relation in catalog.relations {
            try workMeter.consume(at: .bindingCandidate)
            elements = try DatabaseIntermediateFootprint(bytes: elements)
                .adding(DatabaseIntermediateFootprint(bytes: UInt64(relation.columns.count + relation.constraints.count))).bytes
        }
        switch command {
        case .createTable(let table):
            if case .columns(let columns, let constraints) = table.source {
                elements = try DatabaseIntermediateFootprint(bytes: elements)
                    .adding(DatabaseIntermediateFootprint(bytes: UInt64(columns.count + constraints.count))).bytes
                for column in columns {
                    elements = try DatabaseIntermediateFootprint(bytes: elements)
                        .adding(DatabaseIntermediateFootprint(bytes: UInt64(column.constraints.count))).bytes
                }
            }
        case .alterTable(let table):
            elements = try DatabaseIntermediateFootprint(bytes: elements)
                .adding(DatabaseIntermediateFootprint(bytes: UInt64(table.actions.count))).bytes
        default: break
        }
        footprint = try footprint.adding(DatabaseIntermediateFootprint(bytes: elements).multiplied(by: 1024))
            .adding(DatabaseIntermediateFootprint(bytes: 1024))
        return try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .bindingCandidate)
    }
}
