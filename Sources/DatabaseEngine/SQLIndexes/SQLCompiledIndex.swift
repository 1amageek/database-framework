import DatabaseKit
import DatabaseTypes

struct SQLCompiledIndex: Sendable {
    let declaration: SQLIndex
    let relation: SQLRelation
    let keys: [CanonicalBoundExpression]
    let predicate: CanonicalBoundExpression?

    func key(row: CanonicalSourceRow, environment: SQLTemporalEnvironment,
        workMeter: DatabaseWorkMeter) async throws -> SQLIndexKey? {
        if let predicate {
            let result = try await DatabaseContext.evaluateCanonicalScalar(predicate, on: row,
                temporalEnvironment: environment, workMeter: workMeter)
            guard case .primitive(.bool(true)) = result.value else { return nil }
        }
        var values = try DatabaseRetainedArrayBuilder<CanonicalSQLValueIdentity>(workMeter: workMeter,
            stage: .indexScan, layout: DatabaseRetainedArrayLayout.forElement(CanonicalSQLValueIdentity.self), expectedCount: keys.count)
        var payloads = try DatabaseRetainedArrayBuilder<DatabaseQueryPayloadLease>(workMeter: workMeter,
            stage: .indexScan, layout: DatabaseRetainedArrayLayout.forElement(DatabaseQueryPayloadLease.self), expectedCount: keys.count)
        for lease in row.payloadLeases { try payloads.append(footprint: DatabaseIntermediateFootprint(), make: { lease }) }
        var hasNull = false
        for expression in keys {
            let evaluated = try await DatabaseContext.evaluateCanonicalScalar(expression, on: row,
                temporalEnvironment: environment, workMeter: workMeter)
            hasNull = hasNull || evaluated.value.isNull
            let identity = try CanonicalSQLValueIdentity.prepare(evaluated.value, type: expression.type,
                environment: environment, workMeter: workMeter, stage: .indexScan)
            try values.append(footprint: identity.payloadFootprint(workMeter: workMeter, stage: .indexScan), make: { identity })
            for lease in evaluated.leases { try payloads.append(footprint: DatabaseIntermediateFootprint(), make: { lease }) }
        }
        return try SQLIndexKey(values: values.finish().moveToSharedOwnership(at: .indexScan),
            hasNull: hasNull, leases: payloads.finish().moveToSharedOwnership(at: .indexScan))
    }
}
