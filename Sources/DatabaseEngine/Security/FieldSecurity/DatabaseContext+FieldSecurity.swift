import DatabaseKit

extension DatabaseContext {
    package func authorizeFieldReads(
        _ plan: DatabaseFieldReadAuthorizationPlan
    ) throws {
        try readPolicy().authorizeFields(plan)
    }

    package func authorizeIndexFieldRead(
        entity: Schema.Entity,
        descriptor: IndexDescriptor
    ) throws {
        let plan = DatabaseFieldReadAuthorizationPlan.index(
            entity: entity,
            descriptor: descriptor
        )
        try authorizeFieldReads(plan)
    }
}
