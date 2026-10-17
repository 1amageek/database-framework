import DatabaseKit
import DatabaseTypes

/// Canonical current-row projection shared by mutation and constraint execution.
enum SQLConstraintRows {
    static func limits(_ meter: DatabaseWorkMeter) -> QueryValueLimits {
        QueryValueLimits(maximumRetainedBytes: meter.budget.maximumIntermediateBytes,
            maximumTotalNodes: meter.budget.maximumWorkUnits, maximumNestingDepth: meter.budget.maximumWorkUnits,
            maximumCollectionElements: meter.budget.maximumWorkUnits)
    }

    static func row(fields: FieldObject, relation: SQLRelation, physicalSchema: Schema,
        environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter,
        recomputingStored: Bool = false) async throws -> CanonicalSourceRow {
        guard let entity = physicalSchema.entities.first(where: { SQLConstraintCompiler.same($0.name, relation.entityName) }) else {
            throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "physical entity is missing")
        }
        let metadataBytes = try DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<QueryColumnType?>.stride
            + MemoryLayout<DatabaseQueryPayloadLease>.stride * 3 + MemoryLayout<Bool>.stride + MemoryLayout<Int>.stride))
            .multiplied(by: UInt64(relation.columns.count)).adding(DatabaseIntermediateFootprint(bytes: 256))
        let metadataClaim = try workMeter.reserveIntermediate(bytes: metadataBytes.bytes, at: .mutationPlanning)
        // Construct optional elements directly; array covariance requires runtime casting in Embedded.
        let types: [QueryColumnType?] = try relation.columns.map {
            Optional(try SQLConstraintCompiler.columnType($0, relation: relation, physicalSchema: physicalSchema))
        }
        let layoutBytes = try CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
            fieldNames: relation.columns.lazy.map { $0.name }, sourceName: relation.name,
            workMeter: workMeter, stage: .mutationPlanning)
        let layoutLease = try workMeter.reserveIntermediate(bytes: layoutBytes.bytes, at: .mutationPlanning)
        let layout = CanonicalSourceLayout(slots: relation.columns.map { .init(name: $0.name, scope: relation.name) },
            scopes: [relation.name], reservation: layoutLease, types: types, nullability: Array(repeating: true, count: types.count))
        let valueLimits = limits(workMeter)
        func value(_ slot: Int) throws -> QueryValue {
            guard case .field(let number) = relation.columns[slot].backing,
                  let field = entity.fields.first(where: { $0.fieldNumber == number }) else { return .primitive(.null) }
            let payload = fields[field.name] ?? .null
            if let type = field.queryType { return try QueryFieldStorageAdapter.decode(payload, type: type, limits: valueLimits) }
            return .primitive(payload)
        }
        let footprint = try CanonicalRelationalFootprintMeter.positionalValueArrayFootprint(count: types.count,
            valueAt: value, annotations: [:], version: nil, workMeter: workMeter, stage: .mutationPlanning)
        let claim = try workMeter.reserveIntermediate(rows: footprint.rows, bytes: footprint.bytes, at: .mutationPlanning)
        var values = try relation.columns.indices.map(value)
        var leases = [DatabaseQueryPayloadLease(retaining: claim), DatabaseQueryPayloadLease(retaining: metadataClaim)]
        for (slot, column) in relation.columns.enumerated() {
            guard case .generated(let expression) = column.policy else { continue }
            if case .field = column.backing, !recomputingStored { continue }
            let bound = try SQLConstraintCompiler.bindGenerated(expression, column: column, relation: relation,
                physicalSchema: physicalSchema, workMeter: workMeter)
            let evaluated = try await DatabaseContext.evaluateCanonicalScalar(bound,
                on: CanonicalSourceRow(values: values, layout: layout, payloadLeases: leases),
                temporalEnvironment: environment, workMeter: workMeter)
            values[slot] = evaluated.value
            leases.append(contentsOf: evaluated.leases)
        }
        return CanonicalSourceRow(values: values, layout: layout, payloadLeases: leases)
    }

    struct PreparedFields: Sendable {
        let fields: FieldObject
        let leases: DatabaseSharedRetainedArray<DatabaseQueryPayloadLease>
    }

    static func fields(_ row: CanonicalSourceRow, relation: SQLRelation, physicalSchema: Schema,
        preserving fields: FieldObject, workMeter: DatabaseWorkMeter) throws -> PreparedFields {
        guard let entity = physicalSchema.entities.first(where: { SQLConstraintCompiler.same($0.name, relation.entityName) }) else {
            throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "physical entity is missing")
        }
        var payloads = try DatabaseRetainedArrayBuilder<DatabaseQueryPayloadLease>(workMeter: workMeter,
            stage: .mutationPlanning, layout: DatabaseRetainedArrayLayout.forElement(DatabaseQueryPayloadLease.self), expectedCount: relation.columns.count + 1)
        let metadata = try DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<(String, FieldValue)>.stride))
            .multiplied(by: UInt64(fields.count + relation.columns.count))
        let metadataClaim = try workMeter.reserveIntermediate(bytes: metadata.bytes, at: .mutationPlanning)
        try payloads.append(footprint: DatabaseIntermediateFootprint(), make: { DatabaseQueryPayloadLease(retaining: metadataClaim) })
        var entries = fields.fields
        for (slot, column) in relation.columns.enumerated() {
            guard case .generated = column.policy, case .field(let number) = column.backing,
                  let field = entity.fields.first(where: { $0.fieldNumber == number }) else { continue }
            let value: FieldValue
            if let type = field.queryType {
                let maximum = try DatabaseEntityMutationFootprintMeter.encodedValueFootprint(row.values[slot], type: type, workMeter: workMeter)
                let encoded = try DatabaseQueryScopedFieldValue.producing(maximumFootprint: maximum, workMeter: workMeter, stage: .mutationPlanning) {
                    try QueryFieldStorageAdapter.encode(row.values[slot], type: type, limits: limits(workMeter))
                }
                if let reservation = encoded.retainedReservation {
                    try payloads.append(footprint: DatabaseIntermediateFootprint(), make: { DatabaseQueryPayloadLease(retaining: reservation) })
                }
                value = encoded.withValue { copy $0 }
            } else if case .primitive(let scalar) = row.values[slot] { value = scalar }
            else { throw QueryValueError.incompatibleType }
            entries.removeAll { SQLConstraintCompiler.same($0.key, field.name) }
            entries.append((key: field.name, value: value))
        }
        return try PreparedFields(fields: FieldObject(consume entries), leases: payloads.finish().moveToSharedOwnership(at: .mutationPlanning))
    }

    static func key(_ row: CanonicalSourceRow, ids: [String], relation: SQLRelation,
        environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter) throws -> DatabaseSharedRetainedArray<CanonicalSQLValueIdentity> {
        var keys = try DatabaseRetainedArrayBuilder<CanonicalSQLValueIdentity>(workMeter: workMeter,
            stage: .mutationPlanning, layout: DatabaseRetainedArrayLayout.forElement(CanonicalSQLValueIdentity.self), expectedCount: ids.count)
        for id in ids {
            guard let slot = relation.columns.firstIndex(where: { SQLConstraintCompiler.same($0.id, id) }),
                  let type = row.layout.types[slot] else {
                throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "key column is missing")
            }
            let value: QueryValue
            if case .numeric(let numeric) = row.values[slot] {
                // Constraint equality ignores numeric display scale, including
                // equality between different declared integer/decimal widths.
                value = .primitive(.decimal(numeric.value))
            } else { value = row.values[slot] }
            let key = try CanonicalSQLValueIdentity.prepare(value, type: type, environment: environment, workMeter: workMeter)
            try keys.append(footprint: key.payloadFootprint(workMeter: workMeter, stage: .mutationPlanning), make: { key })
        }
        return try keys.finish().moveToSharedOwnership(at: .mutationPlanning)
    }

    static func equal(_ left: DatabaseSharedRetainedArray<CanonicalSQLValueIdentity>,
        _ right: DatabaseSharedRetainedArray<CanonicalSQLValueIdentity>, workMeter: DatabaseWorkMeter) throws -> Bool {
        guard left.count == right.count else { return false }
        for index in left.indices {
            if try !CanonicalSQLValueIdentity.equal(left[index], right[index], workMeter: workMeter, stage: .mutationPlanning) { return false }
        }
        return true
    }

    static func nullCount(_ row: CanonicalSourceRow, ids: [String], relation: SQLRelation) -> Int {
        ids.reduce(0) { count, id in
            guard let slot = relation.columns.firstIndex(where: { SQLConstraintCompiler.same($0.id, id) }) else { return count }
            return count + (row.values[slot].isNull ? 1 : 0)
        }
    }
}
