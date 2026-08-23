import DatabaseKit

extension DatabaseContext {
    package func authorizeFieldReads(
        _ plan: DatabaseFieldReadAuthorizationPlan
    ) throws {
        if try ActiveDatabaseReadAuthorizationAdmission.admission?.coversFields(
            plan.fieldsByEntity,
            context: self
        ) == true {
            return
        }
        for entity in plan.fieldsByEntity.keys.sorted() {
            guard let fields = plan.fieldsByEntity[entity] else { continue }
            try container.securityDelegate?.evaluateFieldRead(
                entity: entity,
                fields: fields
            )
        }
    }

    package func authorizeRDFDatasetFieldRead() throws {
        try authorizeFieldReads(
            .rdfDataset(schema: container.schema)
        )
    }

    @_spi(DatabaseExecution)
    public func authorizeIndexFieldRead(
        entity: Schema.Entity,
        descriptor: IndexDescriptor
    ) throws {
        try authorizeFieldReads(
            .index(entity: entity, descriptor: descriptor)
        )
    }
}
