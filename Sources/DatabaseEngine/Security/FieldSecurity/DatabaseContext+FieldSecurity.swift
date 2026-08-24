import DatabaseKit

extension DatabaseContext {
    package func authorizeFieldReads(
        _ plan: DatabaseFieldReadAuthorizationPlan
    ) throws {
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

    package func authorizeIndexFieldRead(
        entity: Schema.Entity,
        descriptor: IndexDescriptor
    ) throws {
        let plan = DatabaseFieldReadAuthorizationPlan.index(
            entity: entity,
            descriptor: descriptor
        )
        guard !plan.isCovered(
            by: RequestFieldAuthorization.fieldsByEntity
        ) else {
            return
        }
        try authorizeFieldReads(plan)
    }
}
