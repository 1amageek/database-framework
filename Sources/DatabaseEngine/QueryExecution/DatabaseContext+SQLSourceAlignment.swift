import DatabaseKit
import StorageKit

extension DatabaseContext {
    func physicalTableReference(_ table: TableRef, resolution: SQLReadResolution?) throws -> TableRef {
        guard let resolution else { return table }
        let relation = try resolution.resolve(table)
        return TableRef(table: relation.entity.name, alias: table.effectiveName, partitions: table.partitions)
    }

    func physicalTableRelationSchema(_ table: TableRef,
        resolution: SQLReadResolution?) throws -> CanonicalRelationSchema {
        let entity = try resolution?.resolve(table).entity ?? resolveEntity(named: table.table)
        return try CanonicalRelationSchema(scopes: [CanonicalRelationScope(name: table.effectiveName,
            columns: entity.allFields,
            types: entity.allFields.map { name in entity.fieldMapByName[name].map {
                $0.queryType ?? .primitive($0.type, isArray: $0.isArray)
            } }, nullability: entity.allFields.map { entity.fieldMapByName[$0]?.isOptional ?? true })])
    }

    /// The sole physical-to-logical table boundary. The physical input owner
    /// stays retained while destination cells borrow its immutable values.
    func alignCatalogSource(_ relation: CanonicalRelation, table: TableRef,
        resolution: SQLReadResolution?, workMeter: DatabaseWorkMeter,
        authorization: DatabaseReadAuthorization? = nil,
        assigningCandidateOrdinals: Bool = false) throws -> CanonicalRelation {
        guard let resolution else { return relation }
        let resolved = try resolution.resolve(table)
        let allowed = authorization?.admittedFieldNames(forEntityName: resolved.entity.name)
        let footprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
            count: resolved.columns.count, element: Int.self).adding(
                CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
                    fieldNames: resolved.columns.lazy.map { $0.declaration.name },
                    sourceName: table.effectiveName, workMeter: workMeter, stage: .bindingCandidate))
            .adding(DatabaseIntermediateFootprint(bytes: 256))
            .adding(DatabaseIntermediateCollectionMeter.arrayFootprint(count: resolved.columns.count,
                element: SQLCatalogResolvedColumn.self))
        let claim = try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .bindingCandidate)
        var indices: [Int] = []
        var selected: [SQLCatalogResolvedColumn] = []
        indices.reserveCapacity(resolved.columns.count)
        selected.reserveCapacity(resolved.columns.count)
        for column in resolved.columns {
            try workMeter.consume(at: .bindingCandidate)
            if let allowed, !allowed.contains(column.field.name) { continue }
            let matches = relation.schema.layout.slots.indices.filter {
                CanonicalSourceName.equal(relation.schema.layout.slots[$0].name, column.field.name)
            }
            guard matches.count == 1, let index = matches.first else {
                throw SQLReadResolutionError.missingPhysicalColumn(entity: resolved.entity.name, field: column.field.name)
            }
            indices.append(index)
            selected.append(column)
        }
        let schema = try CanonicalRelationSchema(scopes: [CanonicalRelationScope(name: table.effectiveName,
            columns: selected.map { $0.declaration.name },
            types: selected.map { .some($0.field.queryType ?? .primitive($0.field.type, isArray: $0.field.isArray)) },
            nullability: selected.map { $0.field.isOptional })])
        let layout = CanonicalSourceLayout(slots: schema.layout.slots, scopes: schema.layout.scopes,
            reservation: claim, types: schema.slotTypes, nullability: schema.slotNullability,
            payloadLeases: [relation.rows.payloadLease])
        var rows = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(workMeter: workMeter,
            stage: .bindingCandidate, layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: relation.rows.count)
        let cells = try DatabaseIntermediateCollectionMeter.arrayFootprint(count: indices.count, element: QueryValue.self)
        for (ordinal, row) in relation.rows.enumerated() {
            try workMeter.consume(at: .bindingCandidate)
            let footprint = try CanonicalRelationalFootprintMeter.sourceAliasFootprint(of: row,
                workMeter: workMeter).adding(cells)
            try rows.append(footprint: footprint) {
                let values = try indices.map { index -> QueryValue in
                    guard row.values.indices.contains(index) else {
                        throw SQLReadResolutionError.missingPhysicalColumn(entity: resolved.entity.name,
                            field: relation.schema.layout.slots[index].name)
                    }
                    return row.values[index]
                }
                return CanonicalSourceRow(values: values, layout: layout, annotations: row.annotations,
                    version: row.version, payloadLeases: row.payloadLeases + [relation.rows.payloadLease],
                    sourcePresence: row.sourcePresence, candidateOrdinal: assigningCandidateOrdinals ? ordinal : row.candidateOrdinal,
                    candidateIdentity: row.candidateIdentity)
            }
        }
        return CanonicalRelation(schema: schema.retaining(layout: layout),
            rows: try rows.finish().moveToSharedOwnership(at: .bindingCandidate), schemaLayout: layout)
    }
}
