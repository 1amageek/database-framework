import DatabaseKit
import DatabaseTypes

struct CanonicalSourceRow: Sendable {
    let values: [QueryValue]
    let layout: CanonicalSourceLayout
    let annotations: [String: FieldValue]
    let version: PersistableVersionToken?
    let payloadLeases: [DatabaseQueryPayloadLease]
    let sourcePresence: [Bool]
    let candidateOrdinal: Int?
    let candidateIdentity: FieldValue?
    private let ambiguityOverride: CanonicalSourceNames?

    init(
        values: [QueryValue],
        layout: CanonicalSourceLayout,
        annotations: [String: FieldValue] = [:],
        version: PersistableVersionToken? = nil,
        payloadLeases: [DatabaseQueryPayloadLease] = [],
        ambiguityOverride: CanonicalSourceNames? = nil,
        sourcePresence: [Bool]? = nil,
        candidateOrdinal: Int? = nil,
        candidateIdentity: FieldValue? = nil
    ) {
        precondition(values.count == layout.slots.count)
        self.candidateOrdinal = candidateOrdinal
        self.candidateIdentity = candidateIdentity
        self.values = values
        self.layout = layout
        self.annotations = annotations
        self.version = version
        self.payloadLeases = payloadLeases
        self.ambiguityOverride = ambiguityOverride
        self.sourcePresence = sourcePresence ?? Array(repeating: true, count: layout.frameScopes.reduce(0) { $0 + $1.count })
        precondition(self.sourcePresence.count == layout.frameScopes.reduce(0) { $0 + $1.count })
    }

    init(
        fields: [String: FieldValue],
        annotations: [String: FieldValue] = [:],
        version: PersistableVersionToken? = nil
    ) {
        let names = fields.keys.sorted()
        self.init(
            values: names.map { .primitive(fields[$0]!) },
            layout: CanonicalSourceLayout(slots: names.map { .init(name: $0, scope: nil) }),
            annotations: annotations,
            version: version
        )
    }

    init(
        unscopedFields: [String: FieldValue],
        scopedFields: [String: [String: FieldValue]],
        coalescedColumns: CanonicalSourceNames = [],
        annotations: [String: FieldValue] = [:],
        version: PersistableVersionToken? = nil
    ) {
        var slots: [CanonicalSourceLayout.Slot] = []
        var values: [QueryValue] = []
        for name in unscopedFields.keys.sorted() {
            slots.append(.init(name: name, scope: nil))
            values.append(.primitive(unscopedFields[name]!))
        }
        for scope in scopedFields.keys.sorted() {
            for name in scopedFields[scope]!.keys.sorted() {
                slots.append(.init(name: name, scope: scope))
                values.append(.primitive(scopedFields[scope]![name]!))
            }
        }
        self.init(
            values: values,
            layout: CanonicalSourceLayout(
                slots: slots,
                scopes: scopedFields.keys.sorted(),
                coalescedColumns: coalescedColumns,
                visibleSlots: slots.indices.filter {
                    slots[$0].scope == nil || !coalescedColumns.contains(slots[$0].name)
                }
            ),
            annotations: annotations,
            version: version
        )
    }

    static func fromBaseFields(
        _ fields: [String: FieldValue],
        sourceName: String?,
        annotations: [String: FieldValue] = [:],
        version: PersistableVersionToken? = nil
    ) -> CanonicalSourceRow {
        let names = fields.keys.sorted()
        return CanonicalSourceRow(
            values: names.map { .primitive(fields[$0]!) },
            layout: CanonicalSourceLayout(
                slots: names.map { .init(name: $0, scope: sourceName) },
                scopes: sourceName.map { [$0] } ?? []
            ),
            annotations: annotations,
            version: version
        )
    }

    func applyingAlias(_ alias: String?) -> CanonicalSourceRow {
        guard let alias else { return self }
        let selected = layout.visibleSlots
        return CanonicalSourceRow(
            values: selected.map { values[$0] },
            layout: CanonicalSourceLayout(
                slots: selected.map { .init(name: layout.slots[$0].name, scope: alias) },
                scopes: [alias],
                types: selected.map { layout.types[$0] },
                nullability: selected.map { layout.nullability[$0] }
            ),
            annotations: annotations,
            version: version,
            payloadLeases: payloadLeases
        )
    }

    func merged(with other: CanonicalSourceRow) throws -> CanonicalSourceRow {
        guard Set(layout.scopes).isDisjoint(with: other.layout.scopes) else {
            throw CanonicalReadError.invalidQueryStructure(.duplicateAlias, "JOIN inputs require distinct source aliases")
        }
        let leftUnscoped = layout.slots.indices.filter { layout.slots[$0].scope == nil }
        let rightUnscoped = other.layout.slots.indices.filter { other.layout.slots[$0].scope == nil }
        let leftScoped = layout.slots.indices.filter { layout.slots[$0].scope != nil }
        let rightScoped = other.layout.slots.indices.filter { other.layout.slots[$0].scope != nil }
        let addresses = leftUnscoped.map { (false, $0) } + rightUnscoped.map { (true, $0) }
            + leftScoped.map { (false, $0) } + rightScoped.map { (true, $0) }
        func remap(_ slot: CanonicalSourceLayout.BindingSlot, right: Bool) -> CanonicalSourceLayout.BindingSlot {
            CanonicalSourceLayout.BindingSlot(address: slot.address,
                valueIndex: slot.valueIndex.flatMap { value in addresses.firstIndex { $0.0 == right && $0.1 == value } })
        }
        let boundAddresses = layout.bindingSlots.filter { $0.address.scope == nil }.map { remap($0, right: false) }
            + other.layout.bindingSlots.filter { $0.address.scope == nil }.map { remap($0, right: true) }
            + layout.bindingSlots.filter { $0.address.scope != nil }.map { remap($0, right: false) }
            + other.layout.bindingSlots.filter { $0.address.scope != nil }.map { remap($0, right: true) }
        return CanonicalSourceRow(
            values: addresses.map { $0.0 ? other.values[$0.1] : values[$0.1] },
            layout: CanonicalSourceLayout(
                slots: addresses.map { $0.0 ? other.layout.slots[$0.1] : layout.slots[$0.1] },
                scopes: layout.scopes + other.layout.scopes,
                coalescedColumns: layout.coalescedColumns.union(other.layout.coalescedColumns),
                types: addresses.map { $0.0 ? other.layout.types[$0.1] : layout.types[$0.1] },
                nullability: addresses.map { $0.0 ? other.layout.nullability[$0.1] : layout.nullability[$0.1] },
                visibleSlots: addresses.indices.filter {
                    addresses[$0].0 ? other.layout.visibleSlots.contains(addresses[$0].1)
                        : layout.visibleSlots.contains(addresses[$0].1)
                }, bindingSlots: boundAddresses
            ),
            annotations: annotations.merging(other.annotations) { current, _ in current },
            payloadLeases: payloadLeases + other.payloadLeases,
            sourcePresence: sourcePresence + other.sourcePresence
        )
    }

    func replacingAnnotations(_ annotations: [String: FieldValue], retaining lease: DatabaseQueryPayloadLease) -> Self {
        Self(values: values, layout: layout, annotations: annotations, version: version,
            payloadLeases: payloadLeases + [lease], ambiguityOverride: ambiguityOverride, sourcePresence: sourcePresence,
             candidateOrdinal: candidateOrdinal, candidateIdentity: candidateIdentity)
    }

    func sourceIsPresent(at index: Int) throws -> Bool {
        guard sourcePresence.indices.contains(index) else {
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "bound source presence address")
        }
        return sourcePresence[index]
    }

    func retainingPayload(_ lease: DatabaseQueryPayloadLease) -> Self {
        Self(values: values, layout: layout, annotations: annotations, version: version,
             payloadLeases: payloadLeases + [lease], ambiguityOverride: ambiguityOverride, sourcePresence: sourcePresence,
             candidateOrdinal: candidateOrdinal, candidateIdentity: candidateIdentity)
    }

    func queryValue(for column: ColumnRef) -> QueryValue? {
        let matches = layout.matching(column)
        guard matches.count == 1, let index = matches.first else { return nil }
        return values[index]
    }

    func value(for column: ColumnRef) -> FieldValue? {
        guard case .primitive(let value) = queryValue(for: column) else { return nil }
        return value
    }

    var coalescedColumns: CanonicalSourceNames { layout.coalescedColumns }

    var ambiguousUnqualifiedColumns: CanonicalSourceNames {
        if let ambiguityOverride { return ambiguityOverride }
        var counts: [CanonicalSourceName: Int] = [:]
        for index in layout.visibleSlots { counts[CanonicalSourceName(layout.slots[index].name), default: 0] += 1 }
        return CanonicalSourceNames(counts.compactMap { $0.value > 1 ? $0.key.value : nil })
    }

    // These transient adapters serve the primitive evaluator only. Positional
    // cells and the shared layout remain authoritative, including duplicates.
    var unscopedFields: [String: FieldValue] {
        primitiveFields(indices: layout.slots.indices.filter { layout.slots[$0].scope == nil })
    }

    var scopedFields: [String: [String: FieldValue]] {
        var result: [String: [String: FieldValue]] = [:]
        for scope in layout.scopes {
            result[scope] = primitiveFields(indices: layout.slots.indices.filter { CanonicalSourceName.equal(layout.slots[$0].scope, scope) })
        }
        return result
    }

    var fields: [String: FieldValue] {
        var result: [String: FieldValue] = [:]
        for index in layout.slots.indices {
            let slot = layout.slots[index]
            guard case .primitive(let value) = values[index] else { continue }
            if let scope = slot.scope,
               layout.matching(ColumnRef(table: scope, column: slot.name)).count == 1 {
                result["\(scope).\(slot.name)"] = value
            }
            if layout.matching(ColumnRef(column: slot.name)).count == 1 {
                result[slot.name] = value
            }
        }
        return result
    }

    var wildcardFields: [String: FieldValue] {
        primitiveFields(indices: layout.visibleSlots)
    }

    func fields(for sourceName: String) -> [String: FieldValue]? {
        guard layout.scopes.contains(where: { CanonicalSourceName.equal($0, sourceName) }) else { return nil }
        return primitiveFields(indices: layout.slots.indices.filter { CanonicalSourceName.equal(layout.slots[$0].scope, sourceName) })
    }

    private func primitiveFields(indices: [Int]) -> [String: FieldValue] {
        var result: [String: FieldValue] = [:]
        var seen = CanonicalSourceNames()
        var ambiguous = CanonicalSourceNames()
        for index in indices {
            let name = layout.slots[index].name
            guard seen.insert(name).inserted else { ambiguous.insert(name); continue }
            if case .primitive(let value) = values[index] { result[name] = value }
        }
        for name in ambiguous { result.removeValue(forKey: name) }
        return result
    }

    func overlaying(outer: CanonicalSourceRow?) -> CanonicalSourceRow {
        guard let outer else { return self }
        let localNames = CanonicalSourceNames(layout.slots.map { $0.name })
        let localScopes = CanonicalSourceNames(layout.scopes)
        // Keep every frame in binding order. Shadowing affects lookup visibility,
        // never the physical address of an inherited bound column.
        let inheritedVisible = outer.layout.visibleSlots.filter {
            !localNames.contains(outer.layout.slots[$0].name)
                && !(outer.layout.slots[$0].scope.map(localScopes.contains) ?? false)
        }
        return CanonicalSourceRow(
            values: values + outer.values,
            layout: CanonicalSourceLayout(
                slots: layout.slots + outer.layout.slots,
                scopes: layout.scopes + outer.layout.scopes.filter { !localScopes.contains($0) },
                coalescedColumns: coalescedColumns.union(outer.coalescedColumns.subtracting(localNames)),
                types: layout.types + outer.layout.types,
                nullability: layout.nullability + outer.layout.nullability,
                visibleSlots: layout.visibleSlots + inheritedVisible.map { layout.slots.count + $0 },
                frameSizes: layout.frameSizes + outer.layout.frameSizes,
                frameScopes: layout.frameScopes + outer.layout.frameScopes,
                bindingSlots: Array(layout.bindingSlots) + outer.layout.bindingSlots.map {
                    .init(address: $0.address, valueIndex: $0.valueIndex.map { values.count + $0 })
                }
            ),
            annotations: outer.annotations.merging(annotations) { _, local in local },
            version: version,
            payloadLeases: payloadLeases + outer.payloadLeases,
            ambiguityOverride: ambiguousUnqualifiedColumns.union(
                outer.ambiguousUnqualifiedColumns.subtracting(localNames)
            ), sourcePresence: sourcePresence + outer.sourcePresence
        )
    }

    static func flatten(
        unscopedFields: [String: FieldValue] = [:],
        scopedFields: [String: [String: FieldValue]],
        coalescedColumns: CanonicalSourceNames = []
    ) -> [String: FieldValue] {
        var counts: [String: Int] = [:]
        for key in unscopedFields.keys {
            counts[key, default: 0] += 1
        }
        for fields in scopedFields.values {
            for key in fields.keys {
                counts[key, default: 0] += 1
            }
        }

        var flattened: [String: FieldValue] = [:]
        for (key, value) in unscopedFields
            where counts[key] == 1 || coalescedColumns.contains(key) {
            flattened[key] = value
        }
        for (sourceName, sourceFields) in scopedFields {
            for (key, value) in sourceFields {
                flattened["\(sourceName).\(key)"] = value
                if counts[key] == 1 {
                    flattened[key] = value
                }
            }
        }
        return flattened
    }

}
