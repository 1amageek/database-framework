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
        assigningCandidateOrdinals: Bool = false,
        resolvedRelation: SQLCatalogResolvedRelation? = nil) async throws -> CanonicalRelation {
        guard let resolution else { return relation }
        let resolved = try resolvedRelation ?? resolution.resolve(table)
        if resolvedRelation == nil, resolution.viewBindings != nil,
           let current = resolution.snapshot.resolvedRelation(id: resolved.declaration.id) {
            let aligned = try await alignCatalogSource(relation, table: table, resolution: resolution,
                workMeter: workMeter, authorization: authorization,
                assigningCandidateOrdinals: assigningCandidateOrdinals, resolvedRelation: current)
            let indices = try resolved.columns.map { column -> Int in
                guard let physical = current.columns.first(where: {
                    SQLCatalogName($0.declaration.id) == SQLCatalogName(column.declaration.id)
                }), let slot = aligned.schema.layout.slots.firstIndex(where: {
                    CanonicalSourceName.equal($0.name, physical.declaration.name)
                }) else {
                    throw SQLViewExecutionError.missingColumn(relation: current.declaration.id, column: column.declaration.id)
                }
                return slot
            }
            let claim = try workMeter.reserveIntermediate(bytes: UInt64(indices.count) * 256 + 256, at: .bindingCandidate)
            let schema = try CanonicalRelationSchema(scopes: [CanonicalRelationScope(name: table.effectiveName,
                columns: resolved.columns.map { $0.declaration.name }, types: resolved.columns.map { .some($0.type) },
                nullability: resolved.columns.map { $0.isNullable })])
            let layout = CanonicalSourceLayout(slots: schema.layout.slots, scopes: schema.layout.scopes,
                reservation: claim, types: schema.slotTypes, nullability: schema.slotNullability,
                payloadLeases: [aligned.rows.payloadLease])
            var rows = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(workMeter: workMeter,
                stage: .bindingCandidate, layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
                expectedCount: aligned.rows.count)
            for row in aligned.rows {
                try workMeter.consume(at: .bindingCandidate)
                try rows.append(footprint: CanonicalRelationalFootprintMeter.sourceAliasFootprint(of: row, workMeter: workMeter)) {
                    CanonicalSourceRow(values: indices.map { row.values[$0] }, layout: layout,
                        annotations: row.annotations, version: row.version,
                        payloadLeases: row.payloadLeases + [aligned.rows.payloadLease], sourcePresence: row.sourcePresence,
                        candidateOrdinal: row.candidateOrdinal, candidateIdentity: row.candidateIdentity)
                }
            }
            return CanonicalRelation(schema: schema.retaining(layout: layout),
                rows: try rows.finish().moveToSharedOwnership(at: .bindingCandidate), schemaLayout: layout)
        }
        let allowed = authorization?.admittedFieldNames(forEntityName: resolved.entity.name)
        let footprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
            count: resolved.columns.count, element: Int?.self)
            .adding(DatabaseIntermediateCollectionMeter.arrayFootprint(count: resolved.columns.count, element: Int.self))
            .adding(DatabaseIntermediateCollectionMeter.arrayFootprint(count: resolved.columns.count,
                element: CanonicalBoundExpression?.self))
            .adding(CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
                fieldNames: resolved.columns.lazy.map { $0.declaration.name },
                sourceName: table.effectiveName, workMeter: workMeter, stage: .bindingCandidate).multiplied(by: 2))
            .adding(DatabaseIntermediateFootprint(bytes: 256))
        let claim = try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .bindingCandidate)
        let physicalSchema = try readPolicy().schema
        var indices = Array<Int?>(repeating: nil, count: resolved.columns.count)
        var generated = Array<CanonicalBoundExpression?>(repeating: nil, count: resolved.columns.count)
        var selected: [Int] = []
        selected.reserveCapacity(resolved.columns.count)
        for (slot, column) in resolved.columns.enumerated() {
            try workMeter.consume(at: .bindingCandidate)
            if let field = column.field {
                if let allowed, !allowed.contains(field.name) { continue }
                let matches = relation.schema.layout.slots.indices.filter {
                    CanonicalSourceName.equal(relation.schema.layout.slots[$0].name, field.name)
                }
                guard matches.count == 1, let index = matches.first else {
                    throw SQLReadResolutionError.missingPhysicalColumn(entity: resolved.entity.name, field: field.name)
                }
                indices[slot] = index
            } else {
                guard case .generated(let expression) = column.declaration.policy else {
                    throw SQLReadResolutionError.missingPhysicalColumn(entity: resolved.entity.name, field: column.declaration.name)
                }
                let bound = try SQLConstraintCompiler.bindGenerated(expression, column: column.declaration,
                    relation: resolved.declaration, physicalSchema: physicalSchema, workMeter: workMeter)
                func dependenciesAreAdmitted(_ value: CanonicalBoundExpression) throws -> Bool {
                    if let dependency = value.columnSlot {
                        guard resolved.columns.indices.contains(dependency), let field = resolved.columns[dependency].field else {
                            throw SQLReadResolutionError.missingPhysicalColumn(entity: resolved.entity.name, field: column.declaration.name)
                        }
                        if let allowed, !allowed.contains(field.name) { return false }
                    }
                    for child in value.children where try !dependenciesAreAdmitted(child) { return false }
                    return true
                }
                guard try dependenciesAreAdmitted(bound) else { continue }
                generated[slot] = bound
            }
            selected.append(slot)
        }
        let fullLayout = CanonicalSourceLayout(slots: resolved.columns.map {
            .init(name: $0.declaration.name, scope: table.effectiveName)
        }, scopes: [table.effectiveName], reservation: claim,
            types: resolved.columns.map { .some($0.type) }, nullability: resolved.columns.map { $0.isNullable })
        let schema = try CanonicalRelationSchema(scopes: [CanonicalRelationScope(name: table.effectiveName,
            columns: selected.map { resolved.columns[$0].declaration.name },
            types: selected.map { .some(resolved.columns[$0].type) },
            nullability: selected.map { resolved.columns[$0].isNullable })])
        let layout = CanonicalSourceLayout(slots: schema.layout.slots, scopes: schema.layout.scopes,
            reservation: claim, types: schema.slotTypes, nullability: schema.slotNullability,
            payloadLeases: [relation.rows.payloadLease])
        var rows = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(workMeter: workMeter,
            stage: .bindingCandidate, layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: relation.rows.count)
        let cells = try DatabaseIntermediateCollectionMeter.arrayFootprint(count: resolved.columns.count, element: QueryValue.self)
        for (ordinal, row) in relation.rows.enumerated() {
            try workMeter.consume(at: .bindingCandidate)
            let temporary = try workMeter.reserveIntermediate(bytes: cells.multiplied(by: 2).bytes, at: .bindingCandidate)
            defer { temporary.release() }
            var values = try indices.map { index -> QueryValue in
                guard let index else { return .primitive(.null) }
                guard row.values.indices.contains(index) else {
                    throw SQLReadResolutionError.missingPhysicalColumn(entity: resolved.entity.name,
                        field: relation.schema.layout.slots[index].name)
                }
                return row.values[index]
            }
            // Generated policies cannot depend on another generated column. A single
            // immutable physical frame therefore excludes stale persisted substitutes.
            let current = CanonicalSourceRow(values: values, layout: fullLayout,
                payloadLeases: row.payloadLeases + [relation.rows.payloadLease])
            var leases = row.payloadLeases + [relation.rows.payloadLease]
            for slot in selected {
                guard let bound = generated[slot] else { continue }
                let result = try await Self.evaluateCanonicalScalar(bound, on: current, workMeter: workMeter)
                values[slot] = result.value
                leases.append(contentsOf: result.leases)
            }
            let footprint = try CanonicalRelationalFootprintMeter.sourceAliasFootprint(of: row,
                workMeter: workMeter).adding(cells)
                .adding(DatabaseIntermediateCollectionMeter.arrayFootprint(count: leases.count,
                    element: DatabaseQueryPayloadLease.self))
            try rows.append(footprint: footprint) {
                CanonicalSourceRow(values: selected.map { values[$0] }, layout: layout, annotations: row.annotations,
                    version: row.version, payloadLeases: leases,
                    sourcePresence: row.sourcePresence, candidateOrdinal: assigningCandidateOrdinals ? ordinal : row.candidateOrdinal,
                    candidateIdentity: row.candidateIdentity)
            }
        }
        return CanonicalRelation(schema: schema.retaining(layout: layout),
            rows: try rows.finish().moveToSharedOwnership(at: .bindingCandidate), schemaLayout: layout)
    }
}
