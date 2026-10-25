import DatabaseKit

/// Readdresses authorized positional cells without changing their payload owner.
enum CanonicalRowReframing {
    static func produce(_ source: SQLRelationRow, to schema: CanonicalRelationSchema,
                        meter: DatabaseWorkMeter) throws -> SQLRelationRow {
        let logical = try positions(from: source.schema, to: schema)
        let row = source.value
        let footprint = try CanonicalRelationalFootprintMeter.sourceAliasFootprint(
            of: row, workMeter: meter, stage: .bindingCandidate)
        return try SQLRelationRow(schema: schema, footprint: footprint, meter: meter, stage: .bindingCandidate) {
            try reframe(row, positions: logical, to: schema, retaining: source.payloadLease, meter: meter)
        }
    }

    static func materialize(_ source: CanonicalRelation, to schema: CanonicalRelationSchema,
                            meter: DatabaseWorkMeter) throws -> CanonicalRelation {
        let logical = try positions(from: source.schema, to: schema)
        var builder = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(workMeter: meter,
            stage: .bindingCandidate, layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: source.rows.count)
        for row in source.rows {
            try builder.append(footprint: CanonicalRelationalFootprintMeter.sourceAliasFootprint(
                of: row, workMeter: meter, stage: .bindingCandidate)) {
                try reframe(row, positions: logical, to: schema, retaining: source.rows.payloadLease, meter: meter)
            }
        }
        return CanonicalRelation(schema: schema, rows: try builder.finish().moveToSharedOwnership(at: .bindingCandidate))
    }

    private static func positions(from source: CanonicalRelationSchema, to target: CanonicalRelationSchema) throws -> [Int] {
        let logical = source.layout.visibleSlots
        guard logical.count == target.layout.slots.count else {
            throw CanonicalReadError.invalidQueryStructure(.columnReference, "Source alias output arity differs")
        }
        return logical
    }

    private static func reframe(_ row: CanonicalSourceRow, positions logical: [Int],
                                to schema: CanonicalRelationSchema, retaining lease: DatabaseQueryPayloadLease,
                                meter: DatabaseWorkMeter) throws -> CanonicalSourceRow {
        if row.layout.bindingSlots.usesIdentityMapping && schema.layout.bindingSlots.usesIdentityMapping
            && logical.elementsEqual(row.values.indices) {
            return CanonicalSourceRow(values: row.values, layout: schema.layout, annotations: row.annotations,
                version: row.version, payloadLeases: row.payloadLeases + [lease])
        }
        let claim = try meter.reserveIntermediate(bytes: DatabaseIntermediateCollectionMeter.arrayFootprint(
            count: logical.count, element: CanonicalSourceLayout.BindingSlot.self).multiplied(by: 8).bytes,
            at: .bindingCandidate)
        var selected: [Int] = [], ordinals: [Int] = []
        var bindings: [CanonicalSourceLayout.BindingSlot] = []
        selected.reserveCapacity(logical.count); ordinals.reserveCapacity(logical.count)
        bindings.reserveCapacity(logical.count)
        for (ordinal, sourceSlot) in logical.enumerated() {
            guard row.layout.bindingSlots.indices.contains(sourceSlot) else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "alias logical source slot")
            }
            let physical = row.layout.bindingSlots[sourceSlot].valueIndex
            bindings.append(.init(address: schema.layout.slots[ordinal], valueIndex: physical == nil ? nil : selected.count))
            if let physical { selected.append(physical); ordinals.append(ordinal) }
        }
        let layout = CanonicalSourceLayout(slots: ordinals.map { schema.layout.slots[$0] },
            scopes: schema.layout.scopes, reservation: claim,
            types: ordinals.map { schema.layout.types[$0] }, nullability: ordinals.map { schema.layout.nullability[$0] },
            payloadLeases: schema.layout.payloadLeases, bindingSlots: bindings)
        return CanonicalSourceRow(values: selected.map { row.values[$0] }, layout: layout,
            annotations: row.annotations, version: row.version, payloadLeases: row.payloadLeases + [lease])
    }
}
