import DatabaseKit
import DatabaseTypes
import StorageKit

/// Immutable name/scope addressing shared by positional source rows.
final class CanonicalSourceLayout: Sendable {
    struct Slot: Sendable, Hashable {
        let name: String
        let scope: String?
    }

    struct BindingSlot: Sendable {
        let address: Slot
        let valueIndex: Int?
    }

    /// Identity addressing borrows the existing slot owner. Only compressed or
    /// composed frames require a separately admitted sparse mapping array.
    struct BindingSlots: RandomAccessCollection, Sendable {
        private let slots: [Slot]
        private let mapping: [BindingSlot]?

        init(slots: [Slot], mapping: [BindingSlot]?) {
            self.slots = slots
            self.mapping = mapping
        }

        var startIndex: Int { 0 }
        var endIndex: Int { mapping?.count ?? slots.count }
        var usesIdentityMapping: Bool { mapping == nil }

        subscript(index: Int) -> BindingSlot {
            if let mapping { return mapping[index] }
            return BindingSlot(address: slots[index], valueIndex: index)
        }
    }

    let slots: [Slot]
    let visibleSlots: [Int]
    let scopes: [String]
    let coalescedColumns: Set<String>
    let reservation: DatabaseIntermediateReservation?
    let payloadLeases: [DatabaseQueryPayloadLease]
    let types: [QueryColumnType?]
    let nullability: [Bool]
    let frameSizes: [Int]
    let frameScopes: [[String]]
    let bindingSlots: BindingSlots

    init(slots: [Slot], scopes: [String] = [], coalescedColumns: Set<String> = [], reservation: DatabaseIntermediateReservation? = nil, types: [QueryColumnType?]? = nil, nullability: [Bool]? = nil, visibleSlots: [Int]? = nil, payloadLeases: [DatabaseQueryPayloadLease] = [], frameSizes: [Int]? = nil, frameScopes: [[String]]? = nil, bindingSlots: [BindingSlot]? = nil) {
        self.bindingSlots = BindingSlots(slots: slots, mapping: bindingSlots)
        self.frameSizes = frameSizes ?? [slots.count]
        self.frameScopes = frameScopes ?? [scopes]
        precondition(self.frameScopes.count == self.frameSizes.count)
        precondition(self.frameSizes.reduce(0, +) == slots.count)
        self.slots = slots
        self.scopes = scopes
        self.coalescedColumns = coalescedColumns
        self.reservation = reservation
        self.payloadLeases = payloadLeases
        self.types = types ?? Array(repeating: nil, count: slots.count)
        self.nullability = nullability ?? Array(repeating: true, count: slots.count)
        precondition(self.types.count == slots.count && self.nullability.count == slots.count)
        self.visibleSlots = visibleSlots ?? Array(slots.indices)
        precondition(self.visibleSlots.allSatisfy(slots.indices.contains))
    }

    func sourcePresenceIndex(named name: String) -> Int? {
        var offset = 0
        for scopes in frameScopes {
            if let index = scopes.firstIndex(of: name) { return offset + index }
            offset += scopes.count
        }
        return nil
    }

    func matching(_ column: ColumnRef) -> [Int] {
        var offset = 0
        for (frame, count) in frameSizes.enumerated() {
            let indices = offset..<(offset + count)
            let matches: [Int]
            if let scope = column.table {
                matches = indices.filter {
                    (slots[$0].scope == scope && slots[$0].name == column.column)
                        || (slots[$0].scope == nil && slots[$0].name == column.displayName)
                }
                if !matches.isEmpty || frameScopes[frame].contains(scope) {
                    return matches
                }
            } else {
                matches = visibleSlots.filter { indices.contains($0) && slots[$0].name == column.column }
                if !matches.isEmpty { return matches }
            }
            offset += count
        }
        return []
    }

}

struct CanonicalSourceRow: Sendable {
    let values: [QueryValue]
    let layout: CanonicalSourceLayout
    let annotations: [String: FieldValue]
    let version: PersistableVersionToken?
    let payloadLeases: [DatabaseQueryPayloadLease]
    let sourcePresence: [Bool]
    private let ambiguityOverride: Set<String>?

    init(
        values: [QueryValue],
        layout: CanonicalSourceLayout,
        annotations: [String: FieldValue] = [:],
        version: PersistableVersionToken? = nil,
        payloadLeases: [DatabaseQueryPayloadLease] = [],
        ambiguityOverride: Set<String>? = nil,
        sourcePresence: [Bool]? = nil
    ) {
        precondition(values.count == layout.slots.count)
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

    fileprivate init(
        unscopedFields: [String: FieldValue],
        scopedFields: [String: [String: FieldValue]],
        coalescedColumns: Set<String> = [],
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
            throw CanonicalReadError.unsupportedSelectQuery("JOIN inputs require distinct source aliases")
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
            payloadLeases: payloadLeases + [lease], ambiguityOverride: ambiguityOverride, sourcePresence: sourcePresence)
    }

    func sourceIsPresent(at index: Int) throws -> Bool {
        guard sourcePresence.indices.contains(index) else {
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "bound source presence address")
        }
        return sourcePresence[index]
    }

    func retainingPayload(_ lease: DatabaseQueryPayloadLease) -> Self {
        Self(values: values, layout: layout, annotations: annotations, version: version,
             payloadLeases: payloadLeases + [lease], ambiguityOverride: ambiguityOverride, sourcePresence: sourcePresence)
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

    var coalescedColumns: Set<String> { layout.coalescedColumns }

    var ambiguousUnqualifiedColumns: Set<String> {
        if let ambiguityOverride { return ambiguityOverride }
        var counts: [String: Int] = [:]
        for index in layout.visibleSlots { counts[layout.slots[index].name, default: 0] += 1 }
        return Set(counts.compactMap { $0.value > 1 ? $0.key : nil })
    }

    // These transient adapters serve the primitive evaluator only. Positional
    // cells and the shared layout remain authoritative, including duplicates.
    var unscopedFields: [String: FieldValue] {
        primitiveFields(indices: layout.slots.indices.filter { layout.slots[$0].scope == nil })
    }

    var scopedFields: [String: [String: FieldValue]] {
        var result: [String: [String: FieldValue]] = [:]
        for scope in layout.scopes {
            result[scope] = primitiveFields(indices: layout.slots.indices.filter { layout.slots[$0].scope == scope })
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
        guard layout.scopes.contains(sourceName) else { return nil }
        return primitiveFields(indices: layout.slots.indices.filter { layout.slots[$0].scope == sourceName })
    }

    private func primitiveFields(indices: [Int]) -> [String: FieldValue] {
        var result: [String: FieldValue] = [:]
        var seen = Set<String>()
        var ambiguous = Set<String>()
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
        let localNames = Set(layout.slots.map { $0.name })
        let localScopes = Set(layout.scopes)
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
        coalescedColumns: Set<String> = []
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

struct CanonicalRelationScope: Sendable, Equatable {
    let name: String
    let columns: [String]
    let types: [QueryColumnType?]
    let nullability: [Bool]

    init(name: String, columns: [String], types: [QueryColumnType?]? = nil, nullability: [Bool]? = nil) {
        self.name = name
        self.columns = columns
        self.types = types ?? Array(repeating: nil, count: columns.count)
        self.nullability = nullability ?? Array(repeating: true, count: columns.count)
        precondition(self.types.count == columns.count && self.nullability.count == columns.count)
    }
}

struct CanonicalRelationSchema: Sendable, Equatable {
    let unscopedColumns: [String]
    let unscopedTypes: [QueryColumnType?]
    let unscopedNullability: [Bool]
    let scopes: [CanonicalRelationScope]
    let coalescedColumns: Set<String>
    private(set) var layout: CanonicalSourceLayout

    init(
        unscopedColumns: [String] = [],
        scopes: [CanonicalRelationScope] = [],
        coalescedColumns: Set<String> = [],
        types: [QueryColumnType?]? = nil,
        nullability: [Bool]? = nil,
        visibleSlots: [Int]? = nil,
        physicalLayout: CanonicalSourceLayout? = nil
    ) throws {
        guard Set(scopes.map { $0.name }).count == scopes.count else {
            throw CanonicalReadError.unsupportedSelectQuery("A relation contains duplicate source aliases")
        }
        guard coalescedColumns.isSubset(of: Set(unscopedColumns)) else {
            throw CanonicalReadError.unsupportedSelectQuery("Coalesced JOIN columns require a visible merged slot")
        }
        self.unscopedColumns = unscopedColumns
        self.scopes = scopes
        self.coalescedColumns = coalescedColumns
        self.unscopedTypes = types ?? Array(repeating: nil, count: unscopedColumns.count)
        self.unscopedNullability = nullability ?? Array(repeating: true, count: unscopedColumns.count)
        guard self.unscopedTypes.count == unscopedColumns.count,
              self.unscopedNullability.count == unscopedColumns.count else {
            throw CanonicalReadError.unsupportedSelectQuery("A relation schema has inconsistent slot metadata")
        }
        self.layout = physicalLayout ?? CanonicalSourceLayout(
            slots: unscopedColumns.map { .init(name: $0, scope: nil) }
                + scopes.flatMap { scope in scope.columns.map { .init(name: $0, scope: scope.name) } },
            scopes: scopes.map { $0.name }, coalescedColumns: coalescedColumns,
            types: self.unscopedTypes + scopes.flatMap { $0.types },
            nullability: self.unscopedNullability + scopes.flatMap { $0.nullability }, visibleSlots: visibleSlots)
    }

    func retaining(layout: CanonicalSourceLayout) -> Self {
        precondition(layout.slots == self.layout.slots)
        var result = self
        result.layout = layout
        return result
    }

    static func == (left: Self, right: Self) -> Bool {
        left.unscopedColumns == right.unscopedColumns
            && left.unscopedTypes == right.unscopedTypes
            && left.unscopedNullability == right.unscopedNullability
            && left.scopes == right.scopes && left.coalescedColumns == right.coalescedColumns
            && left.layout.visibleSlots == right.layout.visibleSlots
    }

    var visibleColumns: [String] {
        layout.visibleSlots.map { layout.slots[$0].name }
    }

    var slotTypes: [QueryColumnType?] { layout.types }
    var slotNullability: [Bool] { layout.nullability }

    func typedColumns() throws -> [QueryColumn] {
        let addresses = layout.slots
        return try addresses.indices.map { index in
            guard let type = slotTypes[index] else {
                throw CanonicalReadError.unsupportedSelectQuery("A source column has no declared query type")
            }
            return try QueryColumn(number: UInt32(index + 1), name: addresses[index].name,
                                   type: type, isNullable: slotNullability[index])
        }
    }

    func resolve(_ column: ColumnRef) throws -> Int {
        let matches = layout.matching(column)
        guard matches.count == 1, let match = matches.first else {
            if matches.isEmpty { throw DatabaseExpressionEvaluationError.missingColumn(column.displayName) }
            throw DatabaseExpressionEvaluationError.ambiguousColumn(column.displayName)
        }
        return match
    }

    func wildcardSlots(scope: String?) throws -> [Int] {
        let layout = layout
        guard let scope else { return layout.visibleSlots }
        guard layout.scopes.contains(scope) else {
            throw DatabaseExpressionEvaluationError.missingColumn(scope)
        }
        return layout.slots.indices.filter { layout.slots[$0].scope == scope }
    }

    func occurrenceCount(of column: String) -> Int { layout.matching(ColumnRef(column: column)).count }

    func applyingAlias(_ alias: String) throws -> CanonicalRelationSchema {
        let selected = layout.visibleSlots
        return try CanonicalRelationSchema(scopes: [CanonicalRelationScope(
            name: alias, columns: visibleColumns,
            types: selected.map { slotTypes[$0] },
            nullability: selected.map { slotNullability[$0] }
        )])
    }

    func merged(with other: CanonicalRelationSchema) throws -> CanonicalRelationSchema {
        let leftVisible = layout.visibleSlots.map { index in
            index < unscopedColumns.count ? index : index + other.unscopedColumns.count
        }
        let rightVisible = other.layout.visibleSlots.map { index in
            index < other.unscopedColumns.count ? unscopedColumns.count + index
                : layout.slots.count + index
        }
        return try CanonicalRelationSchema(unscopedColumns: unscopedColumns + other.unscopedColumns,
            scopes: scopes + other.scopes,
            coalescedColumns: coalescedColumns.union(other.coalescedColumns),
            types: unscopedTypes + other.unscopedTypes,
            nullability: unscopedNullability + other.unscopedNullability,
            visibleSlots: leftVisible + rightVisible)
    }

    func merged(with other: CanonicalRelationSchema, coalescing columns: [String]) throws -> CanonicalRelationSchema {
        let coalesced = Set(columns)
        let left = unscopedColumns.indices.filter { !coalesced.contains(unscopedColumns[$0]) }
        let right = other.unscopedColumns.indices.filter { !coalesced.contains(other.unscopedColumns[$0]) }
        let unscopedCount = columns.count + left.count + right.count
        let leftVisible = layout.visibleSlots.compactMap { index -> Int? in
            guard !coalesced.contains(layout.slots[index].name) else { return nil }
            if index < unscopedColumns.count {
                return left.firstIndex(of: index).map { columns.count + $0 }
            }
            return unscopedCount + index - unscopedColumns.count
        }
        let rightVisible = other.layout.visibleSlots.compactMap { index -> Int? in
            guard !coalesced.contains(other.layout.slots[index].name) else { return nil }
            if index < other.unscopedColumns.count {
                return right.firstIndex(of: index).map { columns.count + left.count + $0 }
            }
            return unscopedCount + layout.slots.count - unscopedColumns.count
                + index - other.unscopedColumns.count
        }
        let mergedTypes = try columns.map { name -> QueryColumnType? in
            let lhs = try resolve(ColumnRef(column: name))
            let rhs = try other.resolve(ColumnRef(column: name))
            guard let leftType = slotTypes[lhs], let rightType = other.slotTypes[rhs] else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "JOIN USING")
            }
            let binder = CanonicalQueryTypeBinder(columns: [], resolveColumn: { reference in
                throw DatabaseExpressionEvaluationError.missingColumn(reference.displayName)
            }, wildcardSlots: { _ in [] })
            return try binder.commonSourceType([leftType, rightType])
        }
        return try CanonicalRelationSchema(
            unscopedColumns: columns + left.map { unscopedColumns[$0] } + right.map { other.unscopedColumns[$0] },
            scopes: scopes + other.scopes,
            coalescedColumns: coalescedColumns.union(other.coalescedColumns).union(coalesced),
            types: mergedTypes + left.map { unscopedTypes[$0] } + right.map { other.unscopedTypes[$0] },
            nullability: Array(repeating: true, count: columns.count)
                + left.map { unscopedNullability[$0] } + right.map { other.unscopedNullability[$0] },
            visibleSlots: Array(columns.indices) + leftVisible + rightVisible
        )
    }

    func nullExtended() throws -> CanonicalRelationSchema {
        try CanonicalRelationSchema(unscopedColumns: unscopedColumns,
            scopes: scopes.map { CanonicalRelationScope(name: $0.name, columns: $0.columns,
                types: $0.types, nullability: Array(repeating: true, count: $0.columns.count)) },
            coalescedColumns: coalescedColumns, types: unscopedTypes,
            nullability: Array(repeating: true, count: unscopedColumns.count),
            visibleSlots: layout.visibleSlots)
    }

    func nullRow() -> CanonicalSourceRow {
        let layout = layout
        return CanonicalSourceRow(values: Array(repeating: .primitive(.null), count: layout.slots.count), layout: layout,
            sourcePresence: Array(repeating: false, count: layout.frameScopes.reduce(0) { $0 + $1.count }))
    }
}

typealias CanonicalRetainedRows =
    DatabaseSharedRetainedArray<CanonicalSourceRow>
private typealias CanonicalRetainedGroups =
    DatabaseSharedRetainedArray<CanonicalGroupedRow>
typealias CanonicalRetainedQueryRows =
    DatabaseSharedRetainedQueryRows

/// Zero-copy page view that keeps the canonical row owner and its request
/// reservation alive while one row is borrowed. It deliberately does not
/// conform to Collection and cannot expose a row by subscript.
struct CanonicalRetainedQueryRowView: Sendable {
    fileprivate let owner: CanonicalRetainedQueryRows
    private let range: Range<Int>

    init(owner: CanonicalRetainedQueryRows, range: Range<Int>) {
        precondition(
            range.lowerBound >= owner.startIndex
                && range.upperBound <= owner.endIndex
        )
        self.owner = owner
        self.range = range
    }

    var count: Int { range.count }
    var isEmpty: Bool { range.isEmpty }
    var columns: [QueryColumn] { owner.columns }
    var originPayloadLeases: [DatabaseQueryPayloadLease] { owner.originPayloadLeases }
    var schemaLeases: [DatabaseIntermediateReservation] { owner.schemaLeases }
    var workMeter: DatabaseWorkMeter { owner.workMeter }

    func withElement<Failure: Error>(
        at position: Int,
        _ body: (borrowing QueryRow) throws(Failure) -> Void
    ) throws(Failure) {
        precondition(position >= 0 && position < count)
        try owner.withElement(at: range.lowerBound + position, body)
    }

    func withElement<Failure: Error>(
        at position: Int,
        _ body: (borrowing QueryRow) async throws(Failure) -> Void
    ) async throws(Failure) {
        precondition(position >= 0 && position < count)
        try await owner.withElement(at: range.lowerBound + position, body)
    }
}

private func prospectiveJoinRowFootprint(
    left: CanonicalSourceRow,
    right: CanonicalSourceRow,
    condition: JoinCondition?,
    workMeter: DatabaseWorkMeter
) throws -> DatabaseIntermediateFootprint {
    let usingCount: Int
    if case .using(let columns) = condition { usingCount = columns.count } else { usingCount = 0 }
    let (baseCount, overflow) = left.values.count.addingReportingOverflow(right.values.count)
    let (count, countOverflow) = baseCount.addingReportingOverflow(usingCount)
    guard !overflow && !countOverflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
    let (leaseCount, leaseOverflow) = left.payloadLeases.count.addingReportingOverflow(right.payloadLeases.count)
    guard !leaseOverflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
    // JOIN aliases share source payload leases. Only destination structure is new.
    let mapping = try DatabaseIntermediateCollectionMeter.arrayFootprint(
        count: left.layout.bindingSlots.count + right.layout.bindingSlots.count,
        element: CanonicalSourceLayout.BindingSlot.self).multiplied(by: 4)
    return try mapping.adding(CanonicalRelationalFootprintMeter.positionalValueArrayFootprint(
        count: count, valueAt: { _ in .primitive(.null) },
        annotations: left.annotations.merging(right.annotations) { current, _ in current },
        version: nil, workMeter: workMeter, stage: .joinCandidate
    )).adding(DatabaseIntermediateCollectionMeter.arrayFootprint(
        count: leaseCount,
        element: DatabaseQueryPayloadLease.self))
}

private func firstJoinFieldValue(
    named name: String,
    in row: CanonicalSourceRow
) -> FieldValue? {
    if let value = row.unscopedFields[name] { return value }
    for fields in row.scopedFields.values {
        if let value = fields[name] { return value }
    }
    return nil
}

private func reserveQueryScopedValues(
    count: Int,
    workMeter: DatabaseWorkMeter,
    stage: DatabaseWorkStage
) throws -> DatabaseIntermediateReservation {
    let footprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
        count: count,
        element: DatabaseQueryScopedFieldValue.self
    )
    return try workMeter.reserveIntermediate(
        bytes: footprint.bytes,
        at: stage
    )
}

/// Measures a literal's retained FieldValue shape without constructing the
/// converted value. VALUES rows use this bound to admit conversion and row
/// storage before the first converted value or destination slot array exists.
private func prospectiveLiteralValueFootprint(
    _ literal: Literal
) throws -> UInt64 {
    let base = try CanonicalRelationalFootprintMeter.prospectiveValueFootprint().bytes
    switch literal {
    case .null, .bool, .int, .uint, .decimal, .double, .date, .timestamp,
            .uuid:
        return base
    case .string(let value), .sqlUnknownString(let value):
        return try DatabaseIntermediateFootprint(
            bytes: base
        ).adding(
            DatabaseIntermediateFootprint(bytes: UInt64(value.utf8.count))
        ).bytes
    case .binary(let value):
        guard let capacity = value.retainedByteCount, let bytes = UInt64(exactly: capacity) else {
            throw QueryValueError.unknownRetainedByteCount
        }
        return try DatabaseIntermediateFootprint(bytes: base)
            .adding(DatabaseIntermediateFootprint(bytes: bytes)).bytes
    case .array(let values):
        var footprint = try CanonicalRelationalFootprintMeter.prospectiveValueFootprint(
            collectionElementCount: UInt64(values.count)
        )
        for value in values {
            footprint = try footprint.adding(
                DatabaseIntermediateFootprint(
                    bytes: try prospectiveLiteralValueFootprint(value)
                )
            )
        }
        return footprint.bytes
    case .iri(let value), .blankNode(let value):
        return try prospectiveRDFLiteralValueFootprint(
            lexicalBytes: value.utf8.count
        )
    case .typedLiteral(let value, let datatype):
        return try prospectiveRDFLiteralValueFootprint(
            lexicalBytes: value.utf8.count + datatype.utf8.count
        )
    case .langLiteral(let value, let language):
        return try prospectiveRDFLiteralValueFootprint(
            lexicalBytes: value.utf8.count + language.utf8.count
        )
    case .dirLangLiteral(let value, let language, let direction):
        return try prospectiveRDFLiteralValueFootprint(
            lexicalBytes: value.utf8.count
                + language.utf8.count
                + direction.utf8.count
        )
    case .queryValue(let value, _):
        let measured = try QueryValueValidator.measure(value)
        return try DatabaseIntermediateFootprint(bytes: measured.retainedByteCount)
            .adding(DatabaseIntermediateFootprint(bytes: 512).multiplied(by: measured.nodeCount)).bytes
    case .rdfTerm(let term):
        let encoded = try UInt64(RDFTermStorageFormat.encodedByteCount(term))
        return try DatabaseIntermediateFootprint(
            bytes: base + 128
        ).adding(
            try DatabaseIntermediateFootprint(bytes: encoded).multiplied(by: 4)
        ).bytes
    }
}

private func prospectiveRDFLiteralValueFootprint(
    lexicalBytes: Int
) throws -> UInt64 {
    // RDFTermStorageFormat adds framing and term metadata around all lexical
    // components. Reserve a conservative bound before constructing RDFTerm;
    // the retained row owner later keeps the exact converted payload claim.
    let encodedBytes = try DatabaseIntermediateFootprint(
        bytes: UInt64(lexicalBytes)
    ).adding(
        DatabaseIntermediateFootprint(bytes: 64)
    ).bytes
    return try DatabaseIntermediateFootprint(
        bytes: try CanonicalRelationalFootprintMeter.prospectiveValueFootprint().bytes + 128
    ).adding(
        try DatabaseIntermediateFootprint(bytes: encodedBytes).multiplied(by: 4)
    ).bytes
}

private func prospectiveLiteralSourceRowFootprint(
    fieldNames: [String],
    literals: [Literal],
    workMeter: DatabaseWorkMeter,
    stage: DatabaseWorkStage
) throws -> DatabaseIntermediateFootprint {
    precondition(fieldNames.count == literals.count)
    var footprint = try CanonicalRelationalFootprintMeter
        .emptySourceRowFootprint(
            workMeter: workMeter,
            stage: stage
        )
    let slots = try DatabaseRetainedArrayLayout.forElement(QueryValue.self)
    let growth = try slots.growth(from: 0, toFit: literals.count)
    footprint = try footprint.adding(DatabaseIntermediateFootprint(bytes: growth.additionalByteCount))
    for literal in literals {
        let valueBytes = try prospectiveLiteralValueFootprint(literal)
        try DatabaseByteProcessingMeter.consume(byteCount: valueBytes, workMeter: workMeter, stage: stage)
        footprint = try footprint.adding(DatabaseIntermediateFootprint(bytes: valueBytes))
    }
    return footprint
}

private func prospectiveArrayValueFootprint(
    _ values: [FieldValue],
    workMeter: DatabaseWorkMeter,
    stage: DatabaseWorkStage
) throws -> DatabaseIntermediateFootprint {
    var footprint = try CanonicalRelationalFootprintMeter.prospectiveValueFootprint(
        collectionElementCount: UInt64(values.count)
    )
    for value in values {
        footprint = try footprint.adding(
            CanonicalRelationalFootprintMeter.valueFootprint(
                of: value,
                workMeter: workMeter,
                stage: stage
            )
        )
    }
    return footprint
}

private func prospectiveExpressionStringInputBytes(
    _ expression: Expression,
    on row: CanonicalSourceRow
) -> UInt64 {
    switch expression {
    case .literal(.string(let value)), .literal(.sqlUnknownString(let value)),
         .literal(.queryValue(.primitive(.string(let value)), _)):
        return UInt64(value.utf8.count)
    case .column(let column):
        guard let value = row.value(for: column),
              case .string(let string) = value else {
            return 0
        }
        return UInt64(string.utf8.count)
    case .variable(let variable):
        guard let value = row.fields[variable.name],
              case .string(let string) = value else {
            return 0
        }
        return UInt64(string.utf8.count)
    case .function(let function):
        return function.arguments.reduce(into: UInt64(0)) { result, argument in
            result = max(
                result,
                prospectiveExpressionStringInputBytes(argument, on: row)
            )
        }
    case .caseWhen(_, let cases, let elseResult):
        var result: UInt64 = 0
        for pair in cases {
            result = max(
                result,
                prospectiveExpressionStringInputBytes(pair.result, on: row)
            )
        }
        if let elseResult {
            result = max(
                result,
                prospectiveExpressionStringInputBytes(elseResult, on: row)
            )
        }
        return result
    case .coalesce(let values), .array(let values), .row(let values), .greatest(let values), .least(let values):
        return values.reduce(into: UInt64(0)) { result, value in
            result = max(
                result,
                prospectiveExpressionStringInputBytes(value, on: row)
            )
        }
    case .nullIf(let lhs, _), .cast(let lhs, _):
        return prospectiveExpressionStringInputBytes(lhs, on: row)
    default:
        return 0
    }
}

private func prospectiveExpressionResultFootprint(
    _ expression: Expression,
    on row: CanonicalSourceRow,
    workMeter: DatabaseWorkMeter,
    stage: DatabaseWorkStage
) throws -> DatabaseIntermediateFootprint {
    switch expression {
    case .literal(let literal):
        return DatabaseIntermediateFootprint(
            bytes: try prospectiveLiteralValueFootprint(literal)
        )
    case .column(let column):
        guard let value = row.value(for: column) else {
            return try CanonicalRelationalFootprintMeter.prospectiveValueFootprint()
        }
        return try CanonicalRelationalFootprintMeter.valueFootprint(
            of: value,
            workMeter: workMeter,
            stage: stage
        )
    case .variable(let variable):
        guard let value = row.fields[variable.name] else {
            return try CanonicalRelationalFootprintMeter.prospectiveValueFootprint()
        }
        return try CanonicalRelationalFootprintMeter.valueFootprint(
            of: value,
            workMeter: workMeter,
            stage: stage
        )
    case .function(let function):
        let name = function.name.uppercased()
        if name == "LOWER" || name == "UPPER" {
            // Unicode case mappings may expand a scalar. Four times the
            // source UTF-8 size is a bounded admission, and the evaluator
            // still validates the actual argument type before producing it.
            let inputBytes = prospectiveExpressionStringInputBytes(
                function.arguments.first ?? .literal(.null),
                on: row
            )
            let transformedBytes = try DatabaseIntermediateFootprint(
                bytes: inputBytes
            ).multiplied(by: 4)
            return try CanonicalRelationalFootprintMeter.prospectiveValueFootprint()
                .adding(transformedBytes)
        }
        return try CanonicalRelationalFootprintMeter.prospectiveValueFootprint()
    case .caseWhen(_, let cases, let elseResult):
        var result = try CanonicalRelationalFootprintMeter.prospectiveValueFootprint()
        for pair in cases {
            result = maxFootprint(
                result,
                try prospectiveExpressionResultFootprint(
                    pair.result,
                    on: row,
                    workMeter: workMeter,
                    stage: stage
                )
            )
        }
        if let elseResult {
            result = maxFootprint(
                result,
                try prospectiveExpressionResultFootprint(
                    elseResult,
                    on: row,
                    workMeter: workMeter,
                    stage: stage
                )
            )
        }
        return result
    case .coalesce(let values):
        var result = try CanonicalRelationalFootprintMeter.prospectiveValueFootprint()
        for value in values {
            result = maxFootprint(
                result,
                try prospectiveExpressionResultFootprint(
                    value,
                    on: row,
                    workMeter: workMeter,
                    stage: stage
                )
            )
        }
        return result
    case .nullIf(let lhs, _), .cast(let lhs, _):
        return try prospectiveExpressionResultFootprint(
            lhs,
            on: row,
            workMeter: workMeter,
            stage: stage
        )
    case .triple, .subject, .predicate, .object:
        return try CanonicalRelationalFootprintMeter.prospectiveValueFootprint(retainedByteCount: 256)
    default:
        return try CanonicalRelationalFootprintMeter.prospectiveValueFootprint()
    }
}

private func maxFootprint(
    _ lhs: DatabaseIntermediateFootprint,
    _ rhs: DatabaseIntermediateFootprint
) -> DatabaseIntermediateFootprint {
    DatabaseIntermediateFootprint(
        rows: max(lhs.rows, rhs.rows),
        bytes: max(lhs.bytes, rhs.bytes)
    )
}

func canonicalExpressionResultRequiresOwnedPayload(
    _ expression: Expression
) -> Bool {
    switch expression {
    case .literal(.array),
            .literal(.langLiteral),
            .literal(.dirLangLiteral),
            .aggregate,
            .subquery:
        return true
    case .function(let function):
        let name = function.name.uppercased()
        return name == "LOWER" || name == "UPPER"
    case .caseWhen(_, let cases, let elseResult):
        let caseRequires = cases.contains {
            canonicalExpressionResultRequiresOwnedPayload($0.result)
        }
        let elseRequires = elseResult.map {
            canonicalExpressionResultRequiresOwnedPayload($0)
        } ?? false
        return caseRequires || elseRequires
    case .coalesce(let values), .greatest(let values), .least(let values):
        return values.contains {
            canonicalExpressionResultRequiresOwnedPayload($0)
        }
    case .nullIf(let lhs, _), .cast(let lhs, _):
        return canonicalExpressionResultRequiresOwnedPayload(lhs)
    case .triple, .subject, .predicate, .object:
        return true
    default:
        return false
    }
}

private struct CanonicalExpressionPayloadAdmission: Sendable {
    let maximumFootprint: DatabaseIntermediateFootprint
    let reservation: DatabaseIntermediateReservation
}

private func preadmitExpressionPayload(
    _ expression: Expression,
    on row: CanonicalSourceRow,
    workMeter: DatabaseWorkMeter,
    retainedLifetimes: inout DatabaseRetainedArrayBuilder<
        DatabaseIntermediateReservation
    >,
    forceRootOwnership: Bool = false
) throws -> CanonicalExpressionPayloadAdmission? {
    let ownsRoot = forceRootOwnership
        || canonicalExpressionResultRequiresOwnedPayload(expression)
    let rootAdmission: CanonicalExpressionPayloadAdmission?
    if ownsRoot {
        let maximumFootprint = try prospectiveExpressionResultFootprint(
            expression,
            on: row,
            workMeter: workMeter,
            stage: .expressionEvaluation
        )
        rootAdmission = CanonicalExpressionPayloadAdmission(
            maximumFootprint: maximumFootprint,
            reservation: try workMeter.reserveIntermediate(
                rows: maximumFootprint.rows,
                bytes: maximumFootprint.bytes,
                at: .expressionEvaluation
            )
        )
    } else {
        rootAdmission = nil
    }

    func retainChild(_ child: Expression) throws {
        guard let admission = try preadmitExpressionPayload(
            child,
            on: row,
            workMeter: workMeter,
            retainedLifetimes: &retainedLifetimes
        ) else {
            return
        }
        try retainedLifetimes.append(
            footprint: DatabaseIntermediateFootprint(),
            at: .expressionEvaluation,
            make: { admission.reservation }
        )
    }

    switch expression {
    case .currentValue, .literal, .column, .variable, .parameter, .bound,
            .aggregate, .subquery, .arraySubquery, .wholeRow, .exists:
        break
    case .grouping, .window:
        // Selected evaluation owns these results and their input claims before scalar lowering.
        // The primitive evaluator explicitly rejects an analytic node that reaches it directly.
        break
    case .concatenate(let lhs, let rhs), .add(let lhs, let rhs),
            .subtract(let lhs, let rhs),
            .multiply(let lhs, let rhs),
            .divide(let lhs, let rhs),
            .modulo(let lhs, let rhs),
            .equal(let lhs, let rhs),
            .notEqual(let lhs, let rhs),
            .lessThan(let lhs, let rhs),
            .lessThanOrEqual(let lhs, let rhs),
            .greaterThan(let lhs, let rhs),
            .greaterThanOrEqual(let lhs, let rhs),
            .and(let lhs, let rhs),
            .or(let lhs, let rhs),
            .nullIf(let lhs, let rhs), .isDistinctFrom(let lhs, let rhs, _),
            .collectionOperation(_, let lhs, let rhs):
        try retainChild(lhs)
        try retainChild(rhs)
    case .arrayAccess(let base, let subscripts):
        try retainChild(base)
        for item in subscripts {
            switch item {
            case .index(let value): try retainChild(value)
            case .slice(let lower, let upper):
                if let lower { try retainChild(lower) }
                if let upper { try retainChild(upper) }
            }
        }
    case .patternMatch(let subject, let pattern, let escape, _):
        try retainChild(subject)
        try retainChild(pattern)
        if let escape { try retainChild(escape) }
    case .collate(let nested, _), .field(let nested, _), .fieldExpansion(let nested),
            .negate(let nested),
            .not(let nested), .truthTest(let nested, _, _),
            .isNull(let nested),
            .isNotNull(let nested),
                        .regex(let nested, _, _),
            .isTriple(let nested),
            .subject(let nested),
            .predicate(let nested),
            .object(let nested),
            .cast(let nested, _):
        try retainChild(nested)
    case .between(let nested, let low, let high):
        try retainChild(nested)
        try retainChild(low)
        try retainChild(high)
    case .inList(let nested, let values),
            .notInList(let nested, let values):
        try retainChild(nested)
        for value in values {
            try retainChild(value)
        }
    case .quantifiedComparison(let lhs, _, _, let source):
        try retainChild(lhs)
        if case .array(let value) = source { try retainChild(value) }
    case .inSubquery(let value, _):
        try retainChild(value)
    case .function(let function):
        for argument in function.arguments {
            try retainChild(argument)
        }
    case .caseWhen(let operand, let cases, let elseResult):
        if let operand { try retainChild(operand) }
        for pair in cases {
            try retainChild(pair.condition)
            try retainChild(pair.result)
        }
        if let elseResult {
            try retainChild(elseResult)
        }
    case .coalesce(let values), .array(let values), .row(let values), .greatest(let values), .least(let values):
        for value in values {
            try retainChild(value)
        }
    case .triple(let subject, let predicate, let object):
        try retainChild(subject)
        try retainChild(predicate)
        try retainChild(object)
    }
    return rootAdmission
}

/// A canonical query result that is still collecting inside the session,
/// transaction, and lease that produce it.
///
/// It exposes only borrowed views and copyable descriptors. It has no
/// promotion: reaching a caller requires the Collecting-to-Ready transition in
/// `finalizePostClosureResult`, so an active transaction callback cannot hand
/// a promotable result to anyone.
struct CanonicalRetainedQueryResponse: ~Copyable, Sendable {
    let rows: CanonicalRetainedQueryRows
    let visibleRange: Range<Int>
    let continuation: QueryContinuation?
    let metadata: [String: FieldValue]
    let affectedRows: Int?
    let metadataReservation: DatabaseIntermediateReservation?

    init(
        rows: CanonicalRetainedQueryRows,
        visibleRange: Range<Int>,
        continuation: QueryContinuation?,
        metadata: [String: FieldValue],
        affectedRows: Int?,
        metadataReservation: DatabaseIntermediateReservation? = nil
    ) {
        self.rows = rows
        self.visibleRange = visibleRange
        self.continuation = continuation
        self.metadata = metadata
        self.affectedRows = affectedRows
        self.metadataReservation = metadataReservation
    }

    var visibleRows: CanonicalRetainedQueryRowView {
        CanonicalRetainedQueryRowView(owner: rows, range: visibleRange)
    }

    /// Moves this collecting result into its ready form.
    ///
    /// File-private, and gated on an admission that only
    /// `finalizePostClosureResult` can create, so this is the single edge
    /// between the two states.
    fileprivate consuming func admit(
        _ admission: PostClosureResultAdmission
    ) -> CanonicalReadyQueryResponse {
        CanonicalReadyQueryResponse(
            admission: admission,
            rows: rows,
            visibleRange: visibleRange,
            continuation: continuation,
            metadata: metadata,
            affectedRows: affectedRows,
            metadataReservation: metadataReservation
        )
    }
}

/// Proof that every session, transaction, and lease that produced a canonical
/// query result has closed, and that the ownership-scoped post-closure
/// cancellation rule has been applied to it.
///
/// The initializer is file-private, so no caller outside this transition can
/// admit a result.
struct PostClosureResultAdmission: Sendable {
    fileprivate init() {}
}

/// A canonical query result that has left every producing resource scope and
/// passed post-closure admission.
///
/// Promotion to a public response or to caller-owned retained rows exists only
/// on this form.
struct CanonicalReadyQueryResponse: ~Copyable, Sendable {
    let rows: CanonicalRetainedQueryRows
    let visibleRange: Range<Int>
    let continuation: QueryContinuation?
    let metadata: [String: FieldValue]
    let affectedRows: Int?
    let metadataReservation: DatabaseIntermediateReservation?

    fileprivate init(
        admission: PostClosureResultAdmission,
        rows: CanonicalRetainedQueryRows,
        visibleRange: Range<Int>,
        continuation: QueryContinuation?,
        metadata: [String: FieldValue],
        affectedRows: Int?,
        metadataReservation: DatabaseIntermediateReservation?
    ) {
        self.rows = rows
        self.visibleRange = visibleRange
        self.continuation = continuation
        self.metadata = metadata
        self.affectedRows = affectedRows
        self.metadataReservation = metadataReservation
    }

    var visibleRows: CanonicalRetainedQueryRowView {
        CanonicalRetainedQueryRowView(owner: rows, range: visibleRange)
    }

    consuming func promoteToPublicResponse() throws -> QueryResponse {
        let visibleRange = visibleRange
        let continuation = continuation
        let metadata = metadata
        let affectedRows = affectedRows
        let metadataReservation = metadataReservation
        defer { withExtendedLifetime(metadataReservation) {} }
        guard !visibleRange.isEmpty else {
            return try QueryResponse(
                columns: rows.columns,
                rows: [],
                continuation: continuation,
                metadata: metadata,
                affectedRows: affectedRows
            )
        }

        let columns = rows.columns
        var outputRows = rows.promoteToOutput()
        if visibleRange.upperBound < outputRows.count {
            outputRows.removeLast(outputRows.count - visibleRange.upperBound)
        }
        if visibleRange.lowerBound > 0 {
            outputRows.removeFirst(visibleRange.lowerBound)
        }
        return try QueryResponse(
            columns: columns,
            rows: outputRows,
            continuation: continuation,
            metadata: metadata,
            affectedRows: affectedRows
        )
    }

    consuming func retainVisibleRows() -> DatabaseRetainedQueryRows {
        let visibleRange = visibleRange
        let retainedRows = DatabaseRetainedQueryRows(
            sharedStorage: rows.boundedView(visibleRange)
        )
        return retainedRows
    }
}

/// Performs the single Collecting-to-Ready transition.
///
/// `ownsProducingTransaction` states whether the calling scope opened and
/// closed the transaction that produced this result. Only then does the scope
/// own the post-closure cancellation check; on a caller-owned transaction the
/// enclosing owner still holds it, and checking here would answer a cancelled
/// read before that transaction reached its terminal state.
func finalizePostClosureResult(
    _ collecting: consuming CanonicalRetainedQueryResponse,
    ownsProducingTransaction: Bool
) throws -> CanonicalReadyQueryResponse {
    if ownsProducingTransaction {
        try ensureDatabaseTaskIsActive()
    }
    return collecting.admit(PostClosureResultAdmission())
}

/// Runs a producing read scope and admits its result only after the scope has
/// closed.
///
/// `body` returns the collecting result inside the read result box, so the
/// noncopyable value crosses the closure boundary without being promotable
/// inside it. Every session, transaction, and lease opened by `body` is closed
/// once it returns, so the value is taken out of the box and finalized here.
func withPostClosureReadSnapshot(
    ownsProducingTransaction: Bool,
    _ body: () async throws
        -> DatabaseReadResultBox<CanonicalRetainedQueryResponse>
) async throws -> CanonicalReadyQueryResponse {
    let boxed = try await body()
    return try finalizePostClosureResult(
        boxed.take(),
        ownsProducingTransaction: ownsProducingTransaction
    )
}

struct CanonicalRelation: Sendable {
    let schema: CanonicalRelationSchema
    let rows: CanonicalRetainedRows

    init(schema: CanonicalRelationSchema, rows: CanonicalRetainedRows, schemaLayout: CanonicalSourceLayout? = nil) {
        self.schema = schemaLayout.map { schema.retaining(layout: $0) } ?? schema
        self.rows = rows
    }
}

private struct CanonicalGroupKey: Sendable {
    let values: [QueryValue]
    let identity: [CanonicalSQLValueIdentity]
    let selectedLeaves: [CanonicalBoundExpression]
    let activeLeaves: [Int]
}

private struct CanonicalGroupedRow: Sendable {
    let key: CanonicalGroupKey
    let representative: CanonicalSourceRow
    let rows: CanonicalRetainedRows
}

private struct CanonicalRowValueIdentity: Sendable {
    let values: [CanonicalSQLValueIdentity]
    let reservation: DatabaseIntermediateReservation
}

@inline(__always)
func canonicalHashLookupSlot(hashValue: Int, mask: Int) -> Int {
    precondition(mask >= 0)
    return Int(UInt(bitPattern: hashValue) & UInt(mask))
}

func canonicalVisitAggregateExpressions(_ call: SQLAggregateCall, _ visit: (Expression) throws -> Void) rethrows {
    for value in call.arguments { try visit(value) }
    for key in call.orderBy { try visit(key.expression) }
    if let filter = call.filter { try visit(filter) }
    for key in call.withinGroup ?? [] { try visit(key.expression) }
}

func canonicalVisitWindowExpressions(_ specification: WindowSpecification, _ visit: (Expression) throws -> Void) rethrows {
    for value in specification.partitionBy { try visit(value) }
    for key in specification.orderBy { try visit(key.expression) }
    if let frame = specification.frame {
        func bound(_ value: WindowFrameBound) throws {
            switch value {
            case .preceding(let expression), .following(let expression): try visit(expression)
            default: break
            }
        }
        try bound(frame.start)
        try bound(frame.end)
    }
}

func canonicalVisitWindowExpressions(_ call: WindowCall, _ visit: (Expression) throws -> Void) rethrows {
    switch call.function {
    case .aggregate(let value): try canonicalVisitAggregateExpressions(value, visit)
    case .analytic(_, let arguments): for value in arguments { try visit(value) }
    }
    if case .specification(let value) = call.reference { try canonicalVisitWindowExpressions(value, visit) }
}

func canonicalVisitGroupingExpressions(_ elements: [GroupingElement], _ visit: (Expression) throws -> Void) rethrows {
    for element in elements {
        switch element {
        case .expression(let value): try visit(value)
        case .rollup(let units), .cube(let units): for unit in units { for value in unit { try visit(value) } }
        case .groupingSets(let sets): for set in sets { try canonicalVisitGroupingExpressions(set, visit) }
        }
    }
}

private func canonicalGroupingExpressions(_ elements: [GroupingElement]) -> [Expression] {
    var result: [Expression] = []
    canonicalVisitGroupingExpressions(elements) { result.append($0) }
    return result
}

func canonicalQueryRequiresWindows(_ query: SelectQuery) -> Bool {
    var found = false
    func visit(_ value: Expression) {
        if case .window = value { found = true; return }
        canonicalVisitScalarChildren(value, visit)
    }
    for item in query.projection { if case .expression(let value, _) = item { visit(value) } }
    for key in query.orderBy ?? [] { visit(key.expression) }
    return found
}

/// Visits current SELECT-level children without entering nested SELECT scopes.
private func canonicalVisitScalarChildren(_ expression: Expression, _ visit: (Expression) -> Void) {
    switch expression {
    case .window(let call): canonicalVisitWindowExpressions(call, visit)
    case .grouping(let arguments): for argument in arguments { visit(argument) }
    case .aggregate(.sql(let call)): canonicalVisitAggregateExpressions(call, visit)
    case .aggregate(let aggregate):
        switch aggregate {
        case .count(let value, _): if let value { visit(value) }
        case .sum(let value, _), .avg(let value, _), .min(let value), .max(let value), .sample(let value), .groupConcat(let value, _, _): visit(value)
        case .arrayAgg(let value, let keys, _): visit(value); for key in keys ?? [] { visit(key.expression) }
        case .sql: break
        }
    case .add(let lhs, let rhs), .subtract(let lhs, let rhs), .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
         .modulo(let lhs, let rhs), .concatenate(let lhs, let rhs), .equal(let lhs, let rhs), .notEqual(let lhs, let rhs),
         .lessThan(let lhs, let rhs), .lessThanOrEqual(let lhs, let rhs), .greaterThan(let lhs, let rhs), .greaterThanOrEqual(let lhs, let rhs),
         .and(let lhs, let rhs), .or(let lhs, let rhs), .nullIf(let lhs, let rhs), .isDistinctFrom(let lhs, let rhs, _), .collectionOperation(_, let lhs, let rhs): visit(lhs); visit(rhs)
    case .collate(let value, _), .field(let value, _), .fieldExpansion(let value), .negate(let value), .not(let value),
         .truthTest(let value, _, _), .isNull(let value), .isNotNull(let value), .regex(let value, _, _), .cast(let value, _),
         .isTriple(let value), .subject(let value), .predicate(let value), .object(let value): visit(value)
    case .coalesce(let values), .array(let values), .row(let values), .greatest(let values), .least(let values): for value in values { visit(value) }
    case .between(let value, let low, let high): visit(value); visit(low); visit(high)
    case .inList(let value, let values), .notInList(let value, let values): visit(value); for value in values { visit(value) }
    case .function(let call): for value in call.arguments { visit(value) }
    case .caseWhen(let operand, let pairs, let fallback):
        if let operand { visit(operand) }; for pair in pairs { visit(pair.condition); visit(pair.result) }; if let fallback { visit(fallback) }
    case .arrayAccess(let value, let subscripts):
        visit(value); for item in subscripts { switch item { case .index(let value): visit(value); case .slice(let low, let high): if let low { visit(low) }; if let high { visit(high) } } }
    case .patternMatch(let value, let pattern, let escape, _): visit(value); visit(pattern); if let escape { visit(escape) }
    case .quantifiedComparison(let lhs, _, _, let source): visit(lhs); if case .array(let value) = source { visit(value) }
    case .inSubquery(let value, _): visit(value)
    case .triple(let s, let p, let o): visit(s); visit(p); visit(o)
    default: break
    }
}

func canonicalQueryRequiresAggregation(_ query: SelectQuery) -> Bool {
    if query.groupBy != nil { return true }
    if query.having != nil { return true }
    for definition in query.windows {
        var aggregate = false
        canonicalVisitWindowExpressions(definition.specification) { if canonicalExpressionContainsAggregate($0) { aggregate = true } }
        if aggregate { return true }
    }
    if query.orderBy?.contains(
        where: { canonicalExpressionContainsAggregate($0.expression) }
    ) == true {
        return true
    }
    return query.projection.contains { item in
        if case .expression(let expression, _) = item { return canonicalExpressionContainsAggregate(expression) }
        return false
    }
}

private func canonicalExpressionContainsAggregate(
    _ expression: Expression
) -> Bool {
    switch expression {
    case .aggregate:
        return true
    case .window(let call):
        var found = false
        canonicalVisitWindowExpressions(call) { if canonicalExpressionContainsAggregate($0) { found = true } }
        return found
    case .grouping: return false
    case .concatenate(let lhs, let rhs), .add(let lhs, let rhs), .subtract(let lhs, let rhs),
            .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
            .modulo(let lhs, let rhs), .equal(let lhs, let rhs),
            .notEqual(let lhs, let rhs), .lessThan(let lhs, let rhs),
            .lessThanOrEqual(let lhs, let rhs),
            .greaterThan(let lhs, let rhs),
            .greaterThanOrEqual(let lhs, let rhs), .and(let lhs, let rhs),
            .or(let lhs, let rhs), .nullIf(let lhs, let rhs), .isDistinctFrom(let lhs, let rhs, _),
            .collectionOperation(_, let lhs, let rhs):
        return canonicalExpressionContainsAggregate(lhs)
            || canonicalExpressionContainsAggregate(rhs)
    case .arrayAccess(let base, let subscripts):
        if canonicalExpressionContainsAggregate(base) { return true }
        for item in subscripts {
            switch item {
            case .index(let value):
                if canonicalExpressionContainsAggregate(value) { return true }
            case .slice(let lower, let upper):
                if lower.map(canonicalExpressionContainsAggregate) == true
                    || upper.map(canonicalExpressionContainsAggregate) == true { return true }
            }
        }
        return false
    case .patternMatch(let subject, let pattern, let escape, _):
        return canonicalExpressionContainsAggregate(subject)
            || canonicalExpressionContainsAggregate(pattern)
            || (escape.map(canonicalExpressionContainsAggregate) ?? false)
    case .collate(let nested, _), .field(let nested, _), .fieldExpansion(let nested),
            .negate(let nested), .not(let nested), .truthTest(let nested, _, _), .isNull(let nested),
            .isNotNull(let nested),             .regex(let nested, _, _), .cast(let nested, _),
            .isTriple(let nested), .subject(let nested),
            .predicate(let nested), .object(let nested):
        return canonicalExpressionContainsAggregate(nested)
    case .between(let value, let low, let high):
        return canonicalExpressionContainsAggregate(value)
            || canonicalExpressionContainsAggregate(low)
            || canonicalExpressionContainsAggregate(high)
    case .inList(let value, let values), .notInList(let value, let values):
        return canonicalExpressionContainsAggregate(value)
            || values.contains(where: canonicalExpressionContainsAggregate)
    case .quantifiedComparison(let lhs, _, _, let source):
        return canonicalExpressionContainsAggregate(lhs)
            || { if case .array(let value) = source { return canonicalExpressionContainsAggregate(value) }; return false }()
    case .inSubquery(let value, _):
        return canonicalExpressionContainsAggregate(value)
    case .function(let function):
        return function.arguments.contains(
            where: canonicalExpressionContainsAggregate
        )
    case .caseWhen(let operand, let pairs, let fallback):
        return operand.map(canonicalExpressionContainsAggregate) == true || pairs.contains {
            canonicalExpressionContainsAggregate($0.condition)
                || canonicalExpressionContainsAggregate($0.result)
        } || fallback.map(canonicalExpressionContainsAggregate) == true
    case .coalesce(let values), .array(let values), .row(let values), .greatest(let values), .least(let values):
        return values.contains(where: canonicalExpressionContainsAggregate)
    case .triple(let subject, let predicate, let object):
        return canonicalExpressionContainsAggregate(subject)
            || canonicalExpressionContainsAggregate(predicate)
            || canonicalExpressionContainsAggregate(object)
    case .wholeRow, .arraySubquery, .currentValue, .literal, .column, .variable, .parameter, .bound, .subquery,
            .exists:
        return false
    }
}

private func canonicalComparisonReadError(
    _ failure: FieldValueComparisonError,
    operation: String
) -> CanonicalReadError {
    switch failure {
    case .incomparable:
        return .expressionEvaluation(.typeMismatch(operation: operation))
    case .unorderedFloatingPoint:
        return .expressionEvaluation(.numericOverflow)
    }
}

private enum CanonicalPartitionRoutingMode: Sendable {
    case strict
    case routed
}

/// An immutable preflighted source tree consumed by relational materialization.
final class CanonicalSourceBindingPlan: Sendable {
    fileprivate let source: DataSource
    fileprivate let schema: CanonicalRelationSchema
    fileprivate let children: [CanonicalSourceBindingPlan]
    fileprivate let queryPlan: CanonicalBoundSelect?
    fileprivate let condition: CanonicalBoundExpression?
    fileprivate let valueBindings: [[CanonicalBoundExpression]]
    fileprivate let query: SelectQuery?
    fileprivate let binding: CanonicalQueryBindingEnvironment
    fileprivate let namedSubqueries: [NamedSubquery]
    fileprivate let reservation: DatabaseIntermediateReservation
    fileprivate let bindingFrames: [CanonicalSourceLayout]

    fileprivate init(source: DataSource, schema: CanonicalRelationSchema,
        children: [CanonicalSourceBindingPlan] = [], queryPlan: CanonicalBoundSelect? = nil,
        condition: CanonicalBoundExpression? = nil, query: SelectQuery? = nil, valueBindings: [[CanonicalBoundExpression]] = [],
        binding: CanonicalQueryBindingEnvironment, namedSubqueries: [NamedSubquery],
        reservation: DatabaseIntermediateReservation, bindingFrames: [CanonicalSourceLayout]? = nil) {
        self.bindingFrames = bindingFrames ?? [schema.layout]
        self.source = source; self.schema = schema; self.children = children
        self.queryPlan = queryPlan; self.condition = condition; self.query = query; self.valueBindings = valueBindings
        self.binding = binding; self.namedSubqueries = namedSubqueries; self.reservation = reservation
    }

    fileprivate func owning(_ query: SelectQuery) -> CanonicalSourceBindingPlan {
        CanonicalSourceBindingPlan(source: source, schema: schema, children: children,
            queryPlan: queryPlan, condition: condition, query: query, valueBindings: valueBindings, binding: binding,
            namedSubqueries: namedSubqueries, reservation: reservation, bindingFrames: bindingFrames)
    }
}

fileprivate final class CanonicalNamedBindingScope: Sendable {
    private let parent: CanonicalNamedBindingScope?
    private let name: String?
    private let plan: CanonicalBoundSelect?
    init(parent: CanonicalNamedBindingScope? = nil, name: String? = nil,
         plan: CanonicalBoundSelect? = nil) {
        self.parent = parent; self.name = name; self.plan = plan
    }

    func resolve(_ name: String, workMeter: DatabaseWorkMeter) throws -> CanonicalBoundSelect? {
        var scope: CanonicalNamedBindingScope? = self
        while let current = scope {
            try workMeter.consume(at: .bindingCandidate)
            if current.name == name { return current.plan }
            scope = current.parent
        }
        return nil
    }
}

fileprivate struct CanonicalQueryBindingEnvironment: Sendable {
    let parameters: CanonicalParameterBindingState
    let workMeter: DatabaseWorkMeter
    let occurrences: QueryOccurrenceArena?
    let occurrence: QueryOccurrenceID?
    let allowingSPARQLFunctions: Bool
    var namedPlans = CanonicalNamedBindingScope()
    var root: QueryOccurrenceID? = nil
    var planReservation: DatabaseIntermediateReservation? = nil
    var groupingOuterLayouts: [CanonicalSourceLayout] = []

    func child(_ field: QueryOccurrenceField, index: UInt32? = nil) -> Self {
        Self(parameters: parameters, workMeter: workMeter, occurrences: occurrences,
             occurrence: occurrence.flatMap { occurrences?.child(of: $0, field: field, index: index) },
             allowingSPARQLFunctions: allowingSPARQLFunctions, namedPlans: namedPlans, root: root ?? occurrence, planReservation: planReservation, groupingOuterLayouts: groupingOuterLayouts)
    }
}

typealias CanonicalSourceEvaluationDispatcher = @Sendable (
    [String: FieldValue],
    @escaping @Sendable (DatabaseContext, DatabaseReadSession) async throws -> DatabaseContext.ProjectedQueryValue
) async throws -> DatabaseContext.ProjectedQueryValue

private struct CanonicalWindowEvaluation: Sendable {
    let plans: [CanonicalBoundWindow]
    let results: [SQLWindowResults]
    let row: Int
}

private struct CanonicalQueryEvaluationContext: Sendable {
    let options: ReadExecutionContext
    let transaction: DatabaseReadTransaction
    let partitionValues: FieldObject?
    let partitionMode: CanonicalPartitionRoutingMode
    let namedSubqueries: [NamedSubquery]
    let queryBinding: CanonicalQueryBindingEnvironment?
    let boundPlan: CanonicalBoundSelect?
    let cteEnvironment: SQLCTEEnvironment
    let sourceEvaluation: CanonicalSourceEvaluationDispatcher?
    let sourceQuery: SelectQuery?
    let outerRow: CanonicalSourceRow?
    let preparedFusionGraph: FusionPreparedQueryGraph
    let fusionSession: DatabaseReadSession
    let windowValues: CanonicalWindowEvaluation?

    init(options: ReadExecutionContext, transaction: DatabaseReadTransaction,
         partitionValues: FieldObject?, partitionMode: CanonicalPartitionRoutingMode,
         namedSubqueries: [NamedSubquery], queryBinding: CanonicalQueryBindingEnvironment? = nil,
         boundPlan: CanonicalBoundSelect? = nil, cteEnvironment: SQLCTEEnvironment = SQLCTEEnvironment(), outerRow: CanonicalSourceRow?,
         preparedFusionGraph: FusionPreparedQueryGraph, fusionSession: DatabaseReadSession,
         sourceEvaluation: CanonicalSourceEvaluationDispatcher? = nil, sourceQuery: SelectQuery? = nil,
         windowValues: CanonicalWindowEvaluation? = nil) {
        self.options = options; self.transaction = transaction; self.partitionValues = partitionValues
        self.partitionMode = partitionMode; self.namedSubqueries = namedSubqueries
        self.queryBinding = queryBinding; self.boundPlan = boundPlan; self.outerRow = outerRow
        self.preparedFusionGraph = preparedFusionGraph; self.fusionSession = fusionSession; self.cteEnvironment = cteEnvironment
        self.sourceEvaluation = sourceEvaluation; self.sourceQuery = sourceQuery; self.windowValues = windowValues
    }

    func usingWindows(_ values: CanonicalWindowEvaluation?) -> Self {
        Self(options: options, transaction: transaction, partitionValues: partitionValues,
            partitionMode: partitionMode, namedSubqueries: namedSubqueries, queryBinding: queryBinding,
            boundPlan: boundPlan, cteEnvironment: cteEnvironment, outerRow: outerRow,
            preparedFusionGraph: preparedFusionGraph, fusionSession: fusionSession,
            sourceEvaluation: sourceEvaluation, sourceQuery: sourceQuery, windowValues: values)
    }
}

extension DatabaseContext {
    public func query(
        _ selectQuery: SelectQuery,
        options: ReadExecutionOptions = .default,
        graphPartitions: FieldObject = FieldObject()
    ) async throws -> QueryResponse {
        let execution = ReadExecutionContext(
            options: options,
            monotonicClock: container.monotonicClock,
            temporalEnvironment: inheritedSQLTemporalEnvironment ?? makeSQLTemporalEnvironment()
        )
        let response = try await query(
            selectQuery,
            execution: execution,
            graphPartitions: graphPartitions
        )
        guard let rowCount = UInt32(exactly: response.rows.count) else {
            throw DatabaseWorkLimitError.maximumRows(
                stage: .resultMaterialization,
                consumed: execution.workMeter.consumedRows,
                requested: UInt32.max,
                maximum: execution.workMeter.budget.maximumRows
            )
        }
        try execution.workMeter.recordOutputRows(rowCount)
        return response
    }

    package func query(
        _ selectQuery: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject = FieldObject(),
        parameters: [QueryParameter] = [],
        occurrences: QueryOccurrenceArena? = nil,
        occurrence: QueryOccurrenceID? = nil
    ) async throws -> QueryResponse {
        let response = try await queryRetained(
            selectQuery,
            execution: execution,
            graphPartitions: graphPartitions, parameters: parameters, occurrences: occurrences, occurrence: occurrence
        )
        return try response.promoteToPublicResponse()
    }

    func queryRetained(
        _ selectQuery: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject = FieldObject(),
        parameters: [QueryParameter] = [],
        occurrences: QueryOccurrenceArena? = nil,
        occurrence: QueryOccurrenceID? = nil
    ) async throws -> CanonicalReadyQueryResponse {
        let temporalEnvironment = execution.temporalEnvironment ?? inheritedSQLTemporalEnvironment ?? makeSQLTemporalEnvironment()
        let execution = execution.withTemporalEnvironment(temporalEnvironment)
        return try await withSQLTemporalStatement(environment: temporalEnvironment) {
        do {
            return try await queryRetainedUnmapped(
                selectQuery,
                execution: execution,
                graphPartitions: graphPartitions, parameters: parameters, occurrences: occurrences, occurrence: occurrence
            )
        } catch {
            throw sanitizedFusionExecutionError(error)
        }
        }
    }

    private func queryRetainedUnmapped(
        _ selectQuery: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject,
        parameters: [QueryParameter] = [],
        occurrences: QueryOccurrenceArena? = nil,
        occurrence: QueryOccurrenceID? = nil
    ) async throws -> CanonicalReadyQueryResponse {
        try QueryStructuralValidator.validate(
            selectQuery,
            limits: execution.queryStructuralLimits
        )
        let queryBinding = CanonicalQueryBindingEnvironment(
            parameters: try CanonicalParameterBindingState(parameters: parameters, workMeter: execution.workMeter), workMeter: execution.workMeter,
            occurrences: occurrences, occurrence: occurrence, allowingSPARQLFunctions: false)


        // This family owns its own Collecting-to-Ready transition on the path
        // where it opens the producing transaction: the storage access below
        // closes that transaction and releases its PartitionLease before
        // returning, and the read session drains inside it, so nothing
        // transaction-bound survives the scope. The read is read-only by
        // construction, so no durable outcome is reported as cancelled. When a
        // caller-owned transaction is already bound on entry, that same access
        // runs on it and closes nothing, so the enclosing owner still holds
        // the transition and this scope admits without checking.
        let ownsProducingTransaction = ActiveDatabaseTransactionContext
            .binding == nil
        return try await withPostClosureReadSnapshot(
            ownsProducingTransaction: ownsProducingTransaction
        ) { [self] in
            try await withDataOperation { [self] in
                let resolvedFusionGraph = try FusionPreflight.resolveGraph(
                    selectQuery,
                    context: self,
                    workMeter: execution.workMeter
                )
                let readExecution = CanonicalReadExecution.resolve(
                    requested: execution.consistency,
                    default: .serializable
                )
                let authorizationPlan = resolvedFusionGraph.authorizationPlan
                return try await withFieldReadAuthorization(
                    authorizationPlan,
                    listRequirements: resolvedFusionGraph
                        .listAuthorizationRequirements
                ) { authorization in
                    let preparedFusionGraph = try FusionPreflight.prepareGraph(
                        resolvedFusionGraph,
                        authorization: authorization,
                        context: self,
                        workMeter: execution.workMeter
                    )
                    return try await withStorageAccess(
                        requiredAccess: .read,
                        configuration: readExecution.transactionConfiguration
                    ) { [self] _ in
                        let response = try await DatabaseReadSession.withSession(
                            context: self,
                            workMeter: execution.workMeter
                        ) { session in
                            let authorizedSession = try session
                                .authorizedSession(authorization)
                            return try await queryCanonical(
                                selectQuery,
                                options: execution,
                                partitionValues: graphPartitions,
                                partitionMode: .strict,
                                transaction: authorizedSession.transaction,
                                queryBinding: queryBinding,
                                preparedFusionGraph: preparedFusionGraph,
                                fusionSession: authorizedSession
                            )
                        }
                        return DatabaseReadResultBox(consume response)
                    }
                }
            }
        }
    }

    func boundFusionRelationalQueries(
        _ queries: [SelectQuery],
        parameters: [QueryParameter],
        options: ReadExecutionContext
    ) throws -> [CanonicalBoundSelect] {
        let arrayFootprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
            count: queries.count, element: CanonicalBoundSelect.self)
        let claim = try options.workMeter.reserveIntermediate(bytes: arrayFootprint.bytes, at: .bindingCandidate)
        let state = try CanonicalParameterBindingState(parameters: parameters, workMeter: options.workMeter)
        var binding = CanonicalQueryBindingEnvironment(parameters: state, workMeter: options.workMeter,
            occurrences: nil, occurrence: nil, allowingSPARQLFunctions: false)
        binding.planReservation = claim
        var plans: [CanonicalBoundSelect] = []
        plans.reserveCapacity(queries.count)
        for query in queries {
            try QueryStructuralValidator.validate(query, limits: options.queryStructuralLimits)
            try claim.reserveAdditional(bytes: prospectiveBindingPlanBytes(query, workMeter: options.workMeter),
                at: .bindingCandidate)
            plans.append(try bindCanonicalQuery(query, inherited: [], outerSchemas: [], binding: binding))
        }
        try state.finalize()
        return plans
    }

    private func fusionProjection(
        _ plan: CanonicalBoundSelect,
        columns: [QueryColumn]
    ) -> CanonicalBoundSelect {
        CanonicalBoundSelect(columns: columns, projection: plan.projection, filter: plan.filter,
            groupBy: plan.groupBy, having: plan.having, orderBy: plan.orderBy, sourcePlan: plan.sourcePlan,
            distinctOn: plan.distinctOn, pagination: plan.pagination, ctes: plan.ctes,
            metadataLease: plan.metadataLease, bindingMetadata: plan.bindingMetadata, grouping: plan.grouping, windows: plan.windows)
    }

    /// Runs a preflighted relational Fusion input on the caller-owned
    /// transaction without promoting its retained rows to a public response.
    func executeFusionRelationalRows(
        _ selectQuery: SelectQuery,
        options: ReadExecutionContext,
        preparedFusionGraph: FusionPreparedQueryGraph,
        session: DatabaseReadSession,
        authorization: DatabaseReadAuthorization,
        listAuthorizationRequirement:
            DatabaseListReadAuthorizationRequirement,
        parameters: [QueryParameter] = [],
        boundPlan suppliedPlan: CanonicalBoundSelect? = nil
    ) async throws -> CanonicalRetainedQueryResponse {
        let internalOptions = executionContextWithoutExternalPageWindow(options)
        let authorizedSession = try session.authorizedSession(authorization)
        let selected = try suppliedPlan ?? boundFusionRelationalQueries([selectQuery], parameters: parameters,
            options: internalOptions)[0]
        let boundPlan: CanonicalBoundSelect
        if selectQuery.projection == [.wildcard()], case .table(let table) = selectQuery.source {
            let entity = try resolveEntity(named: table.table)
            guard let allowed = authorization.admittedFieldNames(forEntityName: entity.name) else {
                throw DatabaseReadSessionError.authorizationMismatch
            }
            let columns = try entity.fields.filter { allowed.contains($0.name) }.enumerated().map { index, field in
                try QueryColumn(number: UInt32(index + 1), name: field.name,
                    type: field.queryType ?? .primitive(field.type, isArray: field.isArray), isNullable: field.isOptional)
            }
            boundPlan = fusionProjection(selected, columns: columns)
        } else { boundPlan = selected }
        return try await queryCanonical(
            selectQuery,
            options: internalOptions,
            partitionValues: FieldObject(),
            partitionMode: .strict,
            transaction: authorizedSession.transaction,
            queryBinding: boundPlan.sourcePlan?.binding,
            boundPlan: boundPlan,
            preparedFusionGraph: preparedFusionGraph,
            fusionSession: authorizedSession,
            admittedListAuthorizationRequirement:
                listAuthorizationRequirement
        )
    }

    /// Applies canonical relational semantics to Engine-owned candidates. The
    /// materialized row domain never crosses into a feature module.
    func executeFusionCandidateRelationalRows(
        _ candidates: FusionCandidateDomain,
        query selectQuery: SelectQuery,
        options: ReadExecutionContext,
        preparedFusionGraph: FusionPreparedQueryGraph,
        session: DatabaseReadSession,
        authorization: DatabaseReadAuthorization,
        parameters: [QueryParameter] = [],
        boundPlan suppliedPlan: CanonicalBoundSelect? = nil
    ) async throws -> CanonicalRetainedQueryResponse {
        guard case .table(let tableRef) = selectQuery.source else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "A Fusion candidate input must use a table source"
            )
        }
        let internalOptions = executionContextWithoutExternalPageWindow(options)
        let sourceName = tableRef.alias ?? tableRef.effectiveName
        let declared = candidates.columns
        let schema = try CanonicalRelationSchema(scopes: [CanonicalRelationScope(name: sourceName,
            columns: declared.map { $0.name }, types: declared.map { $0.type }, nullability: declared.map { $0.isNullable })])
        let selected = try suppliedPlan ?? boundFusionRelationalQueries([selectQuery], parameters: parameters,
            options: internalOptions)[0]
        let boundPlan: CanonicalBoundSelect
        if selectQuery.projection == [.wildcard()] { boundPlan = fusionProjection(selected, columns: declared) }
        else { boundPlan = selected }
        let queryBinding = boundPlan.sourcePlan?.binding
        let layoutFootprint = try CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
            fieldNames: declared.lazy.map { $0.name }, sourceName: sourceName,
            workMeter: internalOptions.workMeter, stage: .bindingCandidate)
        let layoutClaim = try internalOptions.workMeter.reserveIntermediate(bytes: layoutFootprint.bytes, at: .bindingCandidate)
        let layout = CanonicalSourceLayout(slots: schema.layout.slots, scopes: schema.layout.scopes,
            reservation: layoutClaim, types: schema.slotTypes, nullability: schema.slotNullability)
        let origins = candidates.originPayloadLeases
        var builder = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: internalOptions.workMeter, stage: .bindingCandidate,
            layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self), expectedCount: candidates.count)
        try candidates.forEachEntry { entry in
            try internalOptions.workMeter.consume(at: .bindingCandidate)
            let footprint = try CanonicalRelationalFootprintMeter.positionalValueArrayFootprint(
                count: declared.count, valueAt: { _ in .primitive(.null) },
                annotations: entry.row.annotations, version: entry.row.version,
                workMeter: internalOptions.workMeter, stage: .bindingCandidate)
                .adding(DatabaseIntermediateCollectionMeter.arrayFootprint(
                    count: origins.count + entry.payloadLeases.count, element: DatabaseQueryPayloadLease.self))
            try builder.append(footprint: footprint) {
                CanonicalSourceRow(values: entry.row.values, layout: layout,
                    annotations: entry.row.annotations, version: entry.row.version,
                    payloadLeases: origins + entry.payloadLeases)
            }
        }
        let sourceRows = try builder.finish().moveToSharedOwnership(
            at: .bindingCandidate
        )
        let authorizedSession = try session.authorizedSession(authorization)
        return try await finalizeRelationalRows(
            selectQuery,
            sourceRows: sourceRows,
            sourceSchema: schema,
            residualFilter: selectQuery.filter,
            residualOrderBy: selectQuery.orderBy,
            options: internalOptions,
            evaluationContext: CanonicalQueryEvaluationContext(
                options: internalOptions,
                transaction: authorizedSession.transaction,
                partitionValues: FieldObject(),
                partitionMode: .strict,
                namedSubqueries: try mergeNamedSubqueries(
                    local: selectQuery.subqueries ?? [],
                    inherited: []
                ),
                queryBinding: queryBinding,
                boundPlan: boundPlan,
                outerRow: nil,
                preparedFusionGraph: preparedFusionGraph,
                fusionSession: authorizedSession
            )
        )
    }

    @_spi(DatabaseExecution)
    public func executeCanonicalQuery(
        _ selectQuery: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject = FieldObject()
    ) async throws -> QueryResponse {
        try await query(
            selectQuery,
            execution: execution,
            graphPartitions: graphPartitions
        )
    }

    /// Executes a Base-local canonical read through one validated read session.
    package func querySessionBound(
        _ selectQuery: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject = FieldObject(),
        session: DatabaseReadSession,
        parameters: [QueryParameter] = [],
        occurrences: QueryOccurrenceArena? = nil,
        occurrence: QueryOccurrenceID? = nil
    ) async throws -> QueryResponse {
        let temporalEnvironment = execution.temporalEnvironment ?? session.temporalEnvironment
        let execution = execution.withTemporalEnvironment(temporalEnvironment)
        return try await withSQLTemporalStatement(environment: temporalEnvironment) {
        do {
            let collecting = try await querySessionBoundRetainedUnmapped(
                selectQuery,
                execution: execution,
                graphPartitions: graphPartitions,
                session: session, parameters: parameters, occurrences: occurrences, occurrence: occurrence
            )
            // The caller owns the session and its transaction, which are both
            // still open here, so the enclosing owner holds the check.
            let response = try finalizePostClosureResult(
                consume collecting,
                ownsProducingTransaction: false
            )
            return try response.promoteToPublicResponse()
        } catch {
            throw sanitizedFusionExecutionError(error)
        }
        }
    }

    package func retainedSessionBoundPage(
        _ selectQuery: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject = FieldObject(),
        session: DatabaseReadSession,
        parameters: [QueryParameter] = [],
        occurrences: QueryOccurrenceArena? = nil,
        occurrence: QueryOccurrenceID? = nil
    ) async throws -> DatabaseRetainedQueryPage {
        let temporalEnvironment = execution.temporalEnvironment ?? session.temporalEnvironment
        let execution = execution.withTemporalEnvironment(temporalEnvironment)
        return try await withSQLTemporalStatement(environment: temporalEnvironment) {
        do {
            let collecting = try await querySessionBoundRetainedUnmapped(
                selectQuery,
                execution: execution,
                graphPartitions: graphPartitions,
                session: session, parameters: parameters, occurrences: occurrences, occurrence: occurrence
            )
            // The caller owns the session and its transaction, which are both
            // still open here, so the enclosing owner holds the check.
            let response = try finalizePostClosureResult(
                consume collecting,
                ownsProducingTransaction: false
            )
            let continuation = response.continuation
            let rows = response.retainVisibleRows()
            return DatabaseRetainedQueryPage(
                rows: consume rows,
                continuation: continuation
            )
        } catch {
            throw sanitizedFusionExecutionError(error)
        }
        }
    }

    private func querySessionBoundRetainedUnmapped(
        _ selectQuery: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject,
        session: DatabaseReadSession,
        parameters: [QueryParameter] = [],
        occurrences: QueryOccurrenceArena? = nil,
        occurrence: QueryOccurrenceID? = nil
    ) async throws -> CanonicalRetainedQueryResponse {
        try QueryStructuralValidator.validate(
            selectQuery,
            limits: execution.queryStructuralLimits
        )
        let queryBinding = CanonicalQueryBindingEnvironment(
            parameters: try CanonicalParameterBindingState(parameters: parameters, workMeter: execution.workMeter), workMeter: execution.workMeter,
            occurrences: occurrences, occurrence: occurrence, allowingSPARQLFunctions: false)


        guard let binding = ActiveDatabaseTransactionContext.binding else {
            throw DatabaseTransactionError.invalidOperationContext
        }
        try binding.validate(for: self)
        #if DATABASE_MULTI_BASE
        guard binding.resource == self.resource,
              binding.authorization == self.authorization,
              binding.grantedAccess.isSuperset(of: .read) else {
            throw DatabaseGrantAuthorizationError.denied(
                resource: self.resource,
                required: .read
            )
        }
        #endif
        guard session.transaction.storageAccess.matches(binding.transaction)
        else {
            throw DatabaseTransactionError.invalidOperationContext
        }
        let output = try await session.withCanonicalExecution(
            workMeter: execution.workMeter
        ) { _ in
            return try await withDataOperation { [self] in
                let resolvedFusionGraph = try FusionPreflight.resolveGraph(
                    selectQuery,
                    context: self,
                    workMeter: execution.workMeter
                )
                let authorizationPlan = resolvedFusionGraph.authorizationPlan
                return try await withFieldReadAuthorization(
                    authorizationPlan,
                    listRequirements: resolvedFusionGraph
                        .listAuthorizationRequirements
                ) { authorization in
                    let preparedFusionGraph = try FusionPreflight.prepareGraph(
                        resolvedFusionGraph,
                        authorization: authorization,
                        context: self,
                        workMeter: execution.workMeter
                    )
                    let authorizedSession = try session
                        .authorizedSession(authorization)
                    let response = try await queryCanonical(
                        selectQuery,
                        options: execution,
                        partitionValues: graphPartitions,
                        partitionMode: .strict,
                        transaction: authorizedSession.transaction,
                                queryBinding: queryBinding,
                        preparedFusionGraph: preparedFusionGraph,
                        fusionSession: authorizedSession
                    )
                    return DatabaseReadResultBox(consume response)
                }
            }
        }
        return output.take()
    }

    private func mergeNamedSubqueries(
        local: [NamedSubquery],
        inherited: [NamedSubquery]
    ) throws -> [NamedSubquery] {
        guard Set(local.map { $0.name }).count == local.count else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "A WITH clause contains duplicate common table expression names"
            )
        }
        let localNames = Set(local.map { $0.name })
        // A local definition becomes visible after its body binds. Before that
        // point an inherited CTE or physical table with the same name remains visible.
        var available = Set(inherited.map { $0.name })
        available.formUnion(localNames.filter { container.schema.entity(named: $0) != nil })
        for definition in local {
            let unavailable = referencedTableNames(in: definition.query, among: localNames)
                .subtracting(available)
            guard unavailable.isEmpty else {
                throw CanonicalReadError.unsupportedSelectQuery("A nonrecursive CTE may reference only earlier definitions")
            }
            available.insert(definition.name)
        }
        return local + inherited.filter { !localNames.contains($0.name) }
    }

    private func referencedTableNames(
        in query: SelectQuery,
        among candidateNames: Set<String>
    ) -> Set<String> {
        var names = Set<String>()
        let localNames = Set(query.subqueries?.map { $0.name } ?? [])
        let visibleCandidates = candidateNames.subtracting(localNames)

        func collect(_ aggregate: AggregateFunction) {
            switch aggregate {
            case .sql(let call): canonicalVisitAggregateExpressions(call, collect)
            case .count(let expression, _):
                if let expression { collect(expression) }
            case .sum(let expression, _), .avg(let expression, _),
                    .min(let expression), .max(let expression),
                    .groupConcat(let expression, _, _),
                    .sample(let expression):
                collect(expression)
            case .arrayAgg(let expression, let orderBy, _):
                collect(expression)
                for sortKey in orderBy ?? [] {
                    collect(sortKey.expression)
                }
            }
        }

        func collect(_ expression: Expression) {
            switch expression {
            case .window(let call): canonicalVisitWindowExpressions(call, collect)
            case .grouping(let arguments): arguments.forEach(collect)
            case .concatenate(let lhs, let rhs), .add(let lhs, let rhs), .subtract(let lhs, let rhs),
                    .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
                    .modulo(let lhs, let rhs), .equal(let lhs, let rhs),
                    .notEqual(let lhs, let rhs), .lessThan(let lhs, let rhs),
                    .lessThanOrEqual(let lhs, let rhs),
                    .greaterThan(let lhs, let rhs),
                    .greaterThanOrEqual(let lhs, let rhs),
                    .and(let lhs, let rhs), .or(let lhs, let rhs),
                    .nullIf(let lhs, let rhs), .isDistinctFrom(let lhs, let rhs, _),
                    .collectionOperation(_, let lhs, let rhs):
                collect(lhs)
                collect(rhs)
            case .arrayAccess(let base, let subscripts):
                collect(base)
                for item in subscripts {
                    switch item {
                    case .index(let value): collect(value)
                    case .slice(let lower, let upper):
                        if let lower { collect(lower) }
                        if let upper { collect(upper) }
                    }
                }
            case .patternMatch(let subject, let pattern, let escape, _):
                collect(subject); collect(pattern)
                if let escape { collect(escape) }
            case .collate(let operand, _), .field(let operand, _), .fieldExpansion(let operand),
         .negate(let operand), .not(let operand), .truthTest(let operand, _, _),
                    .isNull(let operand), .isNotNull(let operand),
                    .regex(let operand, _, _),
                    .cast(let operand, _), .isTriple(let operand),
                    .subject(let operand), .predicate(let operand),
                    .object(let operand):
                collect(operand)
            case .between(let operand, let lower, let upper):
                collect(operand)
                collect(lower)
                collect(upper)
            case .inList(let operand, let values),
                    .notInList(let operand, let values):
                collect(operand)
                values.forEach(collect)
            case .quantifiedComparison(let lhs, _, _, let source):
                collect(lhs)
                switch source {
                case .array(let value): collect(value)
                case .subquery(let query): names.formUnion(referencedTableNames(in: query, among: visibleCandidates))
                }
            case .inSubquery(let operand, let subquery):
                collect(operand)
                names.formUnion(
                    referencedTableNames(
                        in: subquery,
                        among: visibleCandidates
                    )
                )
            case .aggregate(let aggregate):
                collect(aggregate)
            case .function(let function):
                function.arguments.forEach(collect)
            case .caseWhen(let operand, let cases, let elseResult):
                if let operand { collect(operand) }
                for pair in cases {
                    collect(pair.condition)
                    collect(pair.result)
                }
                if let elseResult { collect(elseResult) }
            case .coalesce(let expressions), .array(let expressions), .row(let expressions), .greatest(let expressions), .least(let expressions):
                expressions.forEach(collect)
            case .triple(let subject, let predicate, let object):
                collect(subject)
                collect(predicate)
                collect(object)
            case .arraySubquery(let subquery), .subquery(let subquery), .exists(let subquery):
                names.formUnion(
                    referencedTableNames(
                        in: subquery,
                        among: visibleCandidates
                    )
                )
            case .wholeRow, .currentValue, .literal, .column, .variable, .parameter, .bound:
                break
            }
        }

        func collect(_ path: PathPattern) {
            for element in path.elements {
                switch element {
                case .node(let node):
                    for property in node.properties ?? [] {
                        collect(property.value)
                    }
                case .edge(let edge):
                    for property in edge.properties ?? [] {
                        collect(property.value)
                    }
                case .quantified(let nested, _):
                    collect(nested)
                case .alternation(let alternatives):
                    alternatives.forEach(collect)
                }
            }
        }

        func collect(_ pattern: GraphPattern) {
            switch pattern {
            case .join(let lhs, let rhs), .optional(let lhs, let rhs),
                    .union(let lhs, let rhs), .minus(let lhs, let rhs),
                    .lateral(let lhs, let rhs):
                collect(lhs)
                collect(rhs)
            case .filter(let pattern, let expression):
                collect(pattern)
                collect(expression)
            case .graph(_, let pattern), .service(_, let pattern, _):
                collect(pattern)
            case .bind(let pattern, _, let expression):
                collect(pattern)
                collect(expression)
            case .subquery(let subquery):
                names.formUnion(
                    referencedTableNames(
                        in: subquery,
                        among: visibleCandidates
                    )
                )
            case .groupBy(let pattern, let expressions, let aggregates):
                collect(pattern)
                expressions.forEach(collect)
                aggregates.forEach { collect($0.aggregate) }
            case .basic, .values:
                break
            }
        }

        func collect(_ source: DataSource) {
            switch source {
            case .table(let table):
                if visibleCandidates.contains(table.table) {
                    names.insert(table.table)
                }
            case .subquery(let subquery):
                names.formUnion(
                    referencedTableNames(
                        in: subquery,
                        among: visibleCandidates
                    )
                )
            case .join(let join):
                collect(join.left)
                collect(join.right)
                if case .on(let expression) = join.condition {
                    collect(expression)
                }
            case .aliased(let nested, _, _): collect(nested)
            case .setOperation(_, _, let lhs, let rhs):
                names.formUnion(referencedTableNames(in: lhs, among: visibleCandidates))
                names.formUnion(referencedTableNames(in: rhs, among: visibleCandidates))
            case .values(let rows):
                for row in rows { for expression in row { collect(expression) } }
            #if DATABASE_MULTI_BASE
            case .base(_, let source):
                collect(source)
            #endif
            case .graphTable(let graphTable):
                graphTable.matchPattern.paths.forEach(collect)
                if let filter = graphTable.matchPattern.where {
                    collect(filter)
                }
                for column in graphTable.columns ?? [] {
                    collect(column.expression)
                }
            case .graphPattern(let pattern),
                    .namedGraph(_, let pattern),
                    .service(_, let pattern, _):
                collect(pattern)
            case .logical:
                break
            }
        }

        for subquery in query.subqueries ?? [] {
            names.formUnion(
                referencedTableNames(
                    in: subquery.query,
                    among: visibleCandidates
                )
            )
        }
        collect(query.source)
        for item in query.projection { if case .expression(let expression, _) = item { collect(expression) } }
        if let filter = query.filter { collect(filter) }
        canonicalVisitGroupingExpressions(query.groupBy ?? [], collect)
        for window in query.windows { canonicalVisitWindowExpressions(window.specification, collect) }
        if let having = query.having { collect(having) }
        for sortKey in query.orderBy ?? [] { collect(sortKey.expression) }
        return names
    }

    private func withFieldReadAuthorization<Result: Sendable>(
        _ plan: DatabaseFieldReadAuthorizationPlan,
        listRequirements: [DatabaseListReadAuthorizationRequirement],
        _ operation: @Sendable (
            DatabaseReadAuthorization
        ) async throws -> Result
    ) async throws -> Result {
        let authorization = try readPolicy().authorizeRead(
            listRequirements: listRequirements,
            fields: plan
        )
        return try await operation(authorization)
    }

    private func sourceRow(
        _ row: CanonicalSourceRow,
        containsColumnNamed name: String
    ) -> Bool {
        row.layout.slots.contains { slot in
            slot.name == name || slot.scope.map { "\($0).\(slot.name)" == name } == true
        }
    }

    private func groupedOuterScope(
        sourceRow: CanonicalSourceRow,
        groupBy: [Expression]
    ) -> CanonicalSourceRow {
        var grouped = Set<Int>()
        for expression in groupBy {
            if case .column(let column) = expression {
                let matches = sourceRow.layout.matching(column)
                if matches.count == 1, let slot = matches.first { grouped.insert(slot) }
            }
        }
        let selected = sourceRow.layout.slots.indices.filter { grouped.contains($0) }
        let names = Set(selected.map { sourceRow.layout.slots[$0].name })
        let layout = CanonicalSourceLayout(slots: selected.map { sourceRow.layout.slots[$0] },
            scopes: sourceRow.layout.scopes,
            coalescedColumns: sourceRow.coalescedColumns.intersection(names),
            types: selected.map { sourceRow.layout.types[$0] },
            nullability: selected.map { sourceRow.layout.nullability[$0] })
        return CanonicalSourceRow(values: selected.map { sourceRow.values[$0] }, layout: layout,
            payloadLeases: sourceRow.payloadLeases)
    }





    private func finalizeRelationalRows(
        _ selectQuery: SelectQuery,
        sourceRows: CanonicalRetainedRows,
        sourceSchema: CanonicalRelationSchema,
        residualFilter: Expression?,
        residualOrderBy: [SortKey]?,
        sourceRowsAlreadyOrdered: Bool = false,
        paginationQuery: SelectQuery? = nil,
        rowsAreContinuationRelative: Bool = false,
        continuationPosition: ByteString? = nil,
        prevalidatedQueryFingerprint: ByteString? = nil,
        metadata: [String: FieldValue] = [:],
        metadataReservation: DatabaseIntermediateReservation? = nil,
        options: ReadExecutionContext,
        evaluationContext: CanonicalQueryEvaluationContext? = nil,
        boundPlan suppliedPlan: CanonicalBoundSelect? = nil
    ) async throws -> CanonicalRetainedQueryResponse {
        let selectQuery = try resolvedQueryReferences(selectQuery, sourceSchema: sourceSchema)
        guard let boundPlan = suppliedPlan ?? evaluationContext?.boundPlan else {
            throw CanonicalReadError.unsupportedSelectQuery("A canonical relation has no executable SELECT binding")
        }
        let addressedRows: CanonicalRetainedRows
        if let expected = boundPlan.sourcePlan?.schema {
            addressedRows = try addressingBoundSource(CanonicalRelation(schema: sourceSchema, rows: sourceRows),
                schema: expected, workMeter: options.workMeter).rows
        } else { addressedRows = sourceRows }
        let outputColumns = boundPlan.columns
        let filteredRows = try await applyFilter(
            residualFilter,
            bound: residualFilter == nil ? nil : boundPlan.filter,
            to: addressedRows,
            workMeter: options.workMeter,
            evaluationContext: evaluationContext
        )

        if !selectQuery.source.usesSPARQLSemantics,
           canonicalQueryRequiresAggregation(selectQuery) || canonicalQueryRequiresWindows(selectQuery) {
            guard let context = evaluationContext else {
                throw CanonicalReadError.unsupportedSelectQuery("Analytic execution requires an admitted query context")
            }
            let pagination = try await evaluatePagination(boundPlan.pagination, context: context)
            let result = try await finalizeSelectedRows(selectQuery.replacing(filter: nil),
                relation: CanonicalRelation(schema: sourceSchema, rows: filteredRows), plan: boundPlan,
                pagination: pagination, context: context)
            let rows = try outputQueryRows(result, columns: boundPlan.columns, meter: options.workMeter,
                metadataLeases: retainedBindingClaims(boundPlan, workMeter: options.workMeter))
            let page = try CanonicalQueryPagination.retainedWindow(rows: rows,
                selectQuery: paginationQuery ?? selectQuery, options: options, rowsAreLogicalQueryRelative: true)
            return CanonicalRetainedQueryResponse(rows: rows, visibleRange: page.range,
                continuation: page.continuation, metadata: metadata, affectedRows: nil,
                metadataReservation: metadataReservation)
        }
        let projectedRows: CanonicalRetainedQueryRows
        if canonicalQueryRequiresAggregation(selectQuery) {
            try validateGroupedWildcardProjection(
                selectQuery.projection,
                sourceSchema: sourceSchema,
                groupBy: canonicalGroupingExpressions(selectQuery.groupBy ?? [])
            )
            let groups = try await makeCanonicalGroups(
                filteredRows,
                boundGroupBy: boundPlan.groupBy,
                workMeter: options.workMeter,
                evaluationContext: evaluationContext
            )
            let havingGroups = try await applyHaving(
                selectQuery.having,
                bound: boundPlan.having,
                to: groups,
                workMeter: options.workMeter,
                evaluationContext: evaluationContext
            )
            let orderedGroups = try await applyGroupedOrder(
                residualOrderBy,
                boundOrder: boundPlan.orderBy.map { $0.expression },
                to: havingGroups,
                projection: selectQuery.projection,
                workMeter: options.workMeter,
                evaluationContext: evaluationContext
            )
            projectedRows = try await projectGroupedRows(
                orderedGroups,
                projection: selectQuery.projection,
                columns: outputColumns,
                sourceSchema: sourceSchema,
                workMeter: options.workMeter,
                evaluationContext: evaluationContext,
                boundPlan: boundPlan
            )
        } else {
            let orderedRows: CanonicalRetainedRows
            if sourceRowsAlreadyOrdered,
               residualOrderBy?.isEmpty ?? true {
                orderedRows = filteredRows
            } else {
                orderedRows = try await applyOrder(
                    try resolvedOrderBy(
                        residualOrderBy,
                        projection: selectQuery.projection
                    ),
                    boundOrder: boundPlan.orderBy.map { $0.expression },
                    to: filteredRows,
                    workMeter: options.workMeter,
                    evaluationContext: evaluationContext
                )
            }
            projectedRows = try await projectRows(
                orderedRows,
                projection: selectQuery.projection,
                columns: outputColumns,
                sourceSchema: sourceSchema,
                workMeter: options.workMeter,
                evaluationContext: evaluationContext, boundPlan: boundPlan
            )
        }

        let distinctRows: CanonicalRetainedQueryRows
        if selectQuery.quantifier == .distinct {
            distinctRows = try canonicalUniqueRows(
                projectedRows,
                workMeter: options.workMeter
            )
        } else {
            distinctRows = projectedRows
        }

        let page = try CanonicalQueryPagination.retainedWindow(
            rows: distinctRows,
            selectQuery: paginationQuery ?? selectQuery,
            options: options,
            rowsAreContinuationRelative: rowsAreContinuationRelative,
            continuationPosition: continuationPosition,
            prevalidatedQueryFingerprint: prevalidatedQueryFingerprint
        )
        return CanonicalRetainedQueryResponse(
            rows: distinctRows,
            visibleRange: page.range,
            continuation: page.continuation,
            metadata: metadata,
            affectedRows: nil,
            metadataReservation: metadataReservation
        )
    }

    #if DATABASE_MULTI_BASE
    /// Applies the canonical relational pipeline to two already-authorized
    /// Base-local table inputs. Only the Composition planner may call this
    /// boundary; ordinary Base execution rejects Base-qualified sources.
    func executeCompositionCrossBaseJoin(
        _ selectQuery: SelectQuery,
        join: JoinClause,
        leftRows: consuming DatabaseRetainedQueryRows,
        leftTable: TableRef,
        rightRows: consuming DatabaseRetainedQueryRows,
        rightTable: TableRef,
        options: ReadExecutionContext,
        session: DatabaseReadSession, parameters: [QueryParameter] = [],
        resolvedPagination: SQLResolvedPagination? = nil
    ) async throws -> DatabaseSharedRetainedQueryRows {
        guard join.type == .inner else {
            throw CompositionQueryError.unsupportedPlan(
                "bounded cross-Base execution currently requires INNER JOIN"
            )
        }
        guard leftRows.workMeter === options.workMeter,
              rightRows.workMeter === options.workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        let leftColumns = leftRows.columns
        let rightColumns = rightRows.columns
        try QueryColumn.validate(leftColumns)
        try QueryColumn.validate(rightColumns)
        let leftName = leftTable.alias ?? leftTable.effectiveName
        let rightName = rightTable.alias ?? rightTable.effectiveName
        let leftFootprint = try CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
            fieldNames: leftColumns.lazy.map { $0.name }, sourceName: leftName,
            workMeter: options.workMeter, stage: .joinCandidate
        )
        let rightFootprint = try CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
            fieldNames: rightColumns.lazy.map { $0.name }, sourceName: rightName,
            workMeter: options.workMeter, stage: .joinCandidate
        )
        let leftClaim = try options.workMeter.reserveIntermediate(bytes: leftFootprint.bytes, at: .joinCandidate)
        let rightClaim = try options.workMeter.reserveIntermediate(bytes: rightFootprint.bytes, at: .joinCandidate)
        let leftLayout = CanonicalSourceLayout(
            slots: leftColumns.map { .init(name: $0.name, scope: leftName) }, scopes: [leftName],
            reservation: leftClaim, types: leftColumns.map { $0.type }, nullability: leftColumns.map { $0.isNullable }
        )
        let rightLayout = CanonicalSourceLayout(
            slots: rightColumns.map { .init(name: $0.name, scope: rightName) }, scopes: [rightName],
            reservation: rightClaim, types: rightColumns.map { $0.type }, nullability: rightColumns.map { $0.isNullable }
        )
        // Metadata must remain admitted even when either source or the join is empty.
        defer {
            withExtendedLifetime(leftLayout) {}
            withExtendedLifetime(rightLayout) {}
        }
        let leftSchema = try CanonicalRelationSchema(scopes: [CanonicalRelationScope(
            name: leftName, columns: leftColumns.map { $0.name },
            types: leftColumns.map { $0.type }, nullability: leftColumns.map { $0.isNullable }
        )]).retaining(layout: leftLayout)
        let rightSchema = try CanonicalRelationSchema(scopes: [CanonicalRelationScope(
            name: rightName, columns: rightColumns.map { $0.name },
            types: rightColumns.map { $0.type }, nullability: rightColumns.map { $0.isNullable }
        )]).retaining(layout: rightLayout)
        let left = try materializeLogicalSourceRows(consume leftRows, sourceName: leftName,
                                                   workMeter: options.workMeter, stage: .joinCandidate)
        let right = try materializeLogicalSourceRows(consume rightRows, sourceName: rightName,
                                                    workMeter: options.workMeter, stage: .joinCandidate)
        let inputSchema = try leftSchema.merged(with: rightSchema)
        let binding = CanonicalQueryBindingEnvironment(
            parameters: try CanonicalParameterBindingState(parameters: parameters, workMeter: options.workMeter),
            workMeter: options.workMeter, occurrences: nil, occurrence: nil, allowingSPARQLFunctions: false)
        let binder = try typeBinder(schema: inputSchema, namedSubqueries: [], queryBinding: binding)
        let boundQuery = try bindCanonicalMaterializedSelect(selectQuery, schema: inputSchema, binding: binding)
        let boundCondition: CanonicalBoundExpression?
        if case .on(let expression) = join.condition {
            boundCondition = try binder.bindPredicate(expression)
        } else { boundCondition = nil }
        try binding.parameters.finalize()
        let joined = try await performJoin(
            left: CanonicalRelation(schema: leftSchema, rows: left, schemaLayout: leftLayout),
            right: CanonicalRelation(schema: rightSchema, rows: right, schemaLayout: rightLayout),
            type: join.type, condition: join.condition, boundCondition: boundCondition, workMeter: options.workMeter
        )
        let context = try compositionEvaluationContext(selectQuery, plan: boundQuery, binding: binding,
            execution: options, session: session)
        return try await withSQLTemporalStatement(environment: context.options.temporalEnvironment ?? session.temporalEnvironment) {
            let pagination: SQLResolvedPagination
            if let resolvedPagination { pagination = resolvedPagination }
            else { pagination = try await evaluatePagination(boundQuery.pagination, context: context) }
            let result = try await finalizeSelectedRows(selectQuery, relation: joined, plan: boundQuery,
                pagination: pagination, context: context)
            return try outputQueryRows(result, columns: boundQuery.columns, meter: options.workMeter,
                metadataLeases: retainedBindingClaims(boundQuery, workMeter: options.workMeter))
        }
    }
    #endif

    #if DATABASE_MULTI_BASE
    func materializeCompositionSource(_ query: SelectQuery, execution: ReadExecutionContext,
        session: DatabaseReadSession, parameters: [QueryParameter]
    ) async throws -> CanonicalRelation {
        let binding = CanonicalQueryBindingEnvironment(parameters: try CanonicalParameterBindingState(
            parameters: parameters, workMeter: execution.workMeter), workMeter: execution.workMeter,
            occurrences: nil, occurrence: nil, allowingSPARQLFunctions: false)
        let plan = try bindCanonicalQuery(query, inherited: [], outerSchemas: [], binding: binding)
        try binding.parameters.finalize()
        guard let source = plan.sourcePlan else { throw CanonicalReadError.unsupportedSelectQuery("Composition source has no executable binding") }
        let context = try compositionEvaluationContext(query, plan: plan, binding: binding, execution: execution, session: session)
        return try await withSQLTemporalStatement(environment: context.options.temporalEnvironment ?? session.temporalEnvironment) {
            try await drainSQLProducer(makeSourceProducer(query.source, plan: source, context: context,
                authorizationQuery: query), meter: execution.workMeter)
        }
    }

    func finalizeCompositionRelation(_ query: SelectQuery, relation: CanonicalRelation,
        execution: ReadExecutionContext, session: DatabaseReadSession, parameters: [QueryParameter],
        mergeDuplicateAnnotations: @escaping @Sendable ([String: FieldValue], [String: FieldValue]) throws -> [String: FieldValue],
        sourceEvaluation: CanonicalSourceEvaluationDispatcher? = nil,
        resolvedPagination: SQLResolvedPagination? = nil
    ) async throws -> DatabaseSharedRetainedQueryRows {
        guard relation.rows.workMeter === execution.workMeter else { throw DatabaseIntermediateReservationError.workMeterMismatch }
        let binding = CanonicalQueryBindingEnvironment(parameters: try CanonicalParameterBindingState(
            parameters: parameters, workMeter: execution.workMeter), workMeter: execution.workMeter,
            occurrences: nil, occurrence: nil, allowingSPARQLFunctions: false)
        let plan = try bindCanonicalMaterializedSelect(query, schema: relation.schema, binding: binding)
        try binding.parameters.finalize()
        let context = try compositionEvaluationContext(query, plan: plan, binding: binding, execution: execution, session: session, sourceEvaluation: sourceEvaluation)
        return try await withSQLTemporalStatement(environment: context.options.temporalEnvironment ?? session.temporalEnvironment) {
            let pagination: SQLResolvedPagination
            if let resolvedPagination { pagination = resolvedPagination }
            else { pagination = try await evaluatePagination(plan.pagination, context: context) }
            let result = try await finalizeSelectedRows(query, relation: relation, plan: plan,
                pagination: pagination, context: context, mergeDuplicateAnnotations: mergeDuplicateAnnotations)
            return try outputQueryRows(result, columns: plan.columns, meter: execution.workMeter,
                metadataLeases: retainedBindingClaims(plan, workMeter: execution.workMeter))
        }
    }

    func resolveCompositionPagination(_ query: SelectQuery, parameters: [QueryParameter],
        execution: ReadExecutionContext, session: DatabaseReadSession) async throws -> SQLResolvedPagination {
        try validateCompositionPagination(query, parameters: parameters, execution: execution)
        let state = try CanonicalParameterBindingState(parameters: parameters, workMeter: execution.workMeter)
        let binding = CanonicalQueryBindingEnvironment(parameters: state, workMeter: execution.workMeter,
            occurrences: nil, occurrence: nil, allowingSPARQLFunctions: false)
        let binder = try typeBinder(schema: CanonicalRelationSchema(), namedSubqueries: [], queryBinding: binding)
        let pagination = try binder.bindPagination(query.pagination, localColumnCount: 0)
        let plan = CanonicalBoundSelect(columns: [], projection: [], filter: nil, groupBy: [], having: nil,
            orderBy: [], pagination: pagination)
        let context = try compositionEvaluationContext(query, plan: plan, binding: binding,
            execution: execution, session: session)
        return try await withSQLTemporalStatement(environment: context.options.temporalEnvironment ?? session.temporalEnvironment) {
            try await evaluatePagination(pagination, context: context)
        }
    }

    func validateCompositionPagination(_ query: SelectQuery, parameters: [QueryParameter],
        execution: ReadExecutionContext) throws {
        var counts: [QueryCount] = []
        if let offset = query.pagination.offset { counts.append(offset) }
        if case .count(let limit, _) = query.pagination.limit { counts.append(limit) }
        for count in counts {
            if case .expression(let expression) = count,
               try FusionSelectQueryGraphWalker.containsNestedQuery(in: expression, workMeter: execution.workMeter) {
                throw CanonicalReadError.unsupportedSelectQuery("Composition pagination subqueries require an explicit global evaluation scope")
            }
        }
        let state = try CanonicalParameterBindingState(parameters: parameters, workMeter: execution.workMeter)
        let binding = CanonicalQueryBindingEnvironment(parameters: state, workMeter: execution.workMeter,
            occurrences: nil, occurrence: nil, allowingSPARQLFunctions: false)
        let binder = try typeBinder(schema: CanonicalRelationSchema(), namedSubqueries: [], queryBinding: binding)
        let pagination = try binder.bindPagination(query.pagination, localColumnCount: 0)
        func validate(_ count: CanonicalBoundCount?) throws {
            guard case .expression(let root) = count else { return }
            var pending = [root]
            while let expression = pending.popLast() {
                try execution.workMeter.consume(at: .bindingCandidate)
                guard expression.subqueryBinding == nil else {
                    throw CanonicalReadError.unsupportedSelectQuery("Composition pagination subqueries require an explicit global evaluation scope")
                }
                pending.append(contentsOf: expression.children)
            }
        }
        try validate(pagination.offset); try validate(pagination.limit)
    }

    private func compositionEvaluationContext(_ query: SelectQuery, plan: CanonicalBoundSelect,
        binding: CanonicalQueryBindingEnvironment, execution: ReadExecutionContext, session: DatabaseReadSession,
        sourceEvaluation: CanonicalSourceEvaluationDispatcher? = nil
    ) throws -> CanonicalQueryEvaluationContext {
        let resolved = try FusionPreflight.resolveGraph(query, context: self, workMeter: execution.workMeter)
        let authorization = try readPolicy().authorizeRead(listRequirements: resolved.listAuthorizationRequirements,
            fields: resolved.authorizationPlan)
        let prepared = try FusionPreflight.prepareGraph(resolved, authorization: authorization,
            context: self, workMeter: execution.workMeter)
        let authorized = try session.authorizedSession(authorization)
        return CanonicalQueryEvaluationContext(options: execution.withTemporalEnvironment(execution.temporalEnvironment ?? session.temporalEnvironment),
            transaction: authorized.transaction, partitionValues: nil, partitionMode: .strict,
            namedSubqueries: [], queryBinding: binding, boundPlan: plan, outerRow: nil,
            preparedFusionGraph: prepared, fusionSession: authorized, sourceEvaluation: sourceEvaluation, sourceQuery: query)
    }
    #endif

    private func runtimeContext(_ context: CanonicalQueryEvaluationContext,
        environment: SQLCTEEnvironment, outerRow: CanonicalSourceRow? = nil
    ) -> CanonicalQueryEvaluationContext {
        CanonicalQueryEvaluationContext(options: context.options, transaction: context.transaction,
            partitionValues: context.partitionValues, partitionMode: context.partitionMode,
            namedSubqueries: context.namedSubqueries, queryBinding: context.queryBinding,
            boundPlan: context.boundPlan, cteEnvironment: environment,
            outerRow: outerRow ?? context.outerRow, preparedFusionGraph: context.preparedFusionGraph,
            fusionSession: context.fusionSession)
    }

    private func executeSQLRelation(_ query: SelectQuery, plan: CanonicalBoundSelect,
        context: CanonicalQueryEvaluationContext
    ) async throws -> CanonicalRetainedQueryResponse {
        let producer = try await makeSelectProducer(query, plan: plan, context: context)
        let relation = try await drainSQLProducer(producer, meter: context.options.workMeter)
        let rows = try outputQueryRows(relation, columns: plan.columns, meter: context.options.workMeter,
            metadataLeases: retainedBindingClaims(plan, workMeter: context.options.workMeter))
        let page = try CanonicalQueryPagination.retainedWindow(rows: rows, selectQuery: query,
            options: context.options, rowsAreLogicalQueryRelative: true)
        return CanonicalRetainedQueryResponse(rows: rows, visibleRange: page.range,
            continuation: page.continuation, metadata: [:], affectedRows: nil)
    }

    private func makeSelectProducer(_ query: SelectQuery, plan: CanonicalBoundSelect,
        context: CanonicalQueryEvaluationContext
    ) async throws -> any SQLRelationProducer {
        guard let sourcePlan = plan.sourcePlan else { throw CanonicalReadError.unsupportedSelectQuery("SELECT has no source binding") }
        let context = CanonicalQueryEvaluationContext(options: context.options, transaction: context.transaction,
            partitionValues: context.partitionValues, partitionMode: context.partitionMode,
            namedSubqueries: sourcePlan.namedSubqueries, queryBinding: sourcePlan.binding, boundPlan: plan,
            cteEnvironment: context.cteEnvironment, outerRow: context.outerRow,
            preparedFusionGraph: context.preparedFusionGraph, fusionSession: context.fusionSession)
        var environment = context.cteEnvironment
        var caches: [SQLCTECache] = []
        let meter = context.options.workMeter
        do {
            for definition in plan.ctes {
                let visible = environment
                let body = definition.plan
                let names = try aliasedColumnNames(body.columns.map { $0.name }, overrides: definition.definition.columns)
                let schema = try CanonicalRelationSchema(unscopedColumns: names, types: body.columns.map { $0.type },
                    nullability: body.columns.map { $0.isNullable })
                let childContext = runtimeContext(context, environment: visible)
                let factory: @Sendable () async throws -> any SQLRelationProducer = { [self] in
                    let producer = try await makeSelectProducer(definition.definition.query, plan: body, context: childContext)
                    return reframedProducer(producer, schema: schema, meter: meter)
                }
                if definition.definition.materialized == .notMaterialized && !definition.isVolatile {
                    environment = environment.adding(name: definition.definition.name, factory: factory)
                } else {
                    let cache = try SQLCTECache(schema: schema, meter: meter, factory: factory)
                    environment = environment.adding(name: definition.definition.name, cache: cache)
                    caches.append(cache)
                }
            }
            let active = runtimeContext(context, environment: environment)
            let pagination = try await evaluatePagination(plan.pagination, context: active)
            if pagination.limit == 0 {
                let localCaches = caches
                return SQLRelationPullProducer(schema: try outputSchema(plan.columns), meter: meter,
                    operation: { _ in nil }, cleanup: {
                        var failure: (any Error)?
                        for cache in localCaches {
                            do { try await cache.close() } catch { if failure == nil { failure = error } }
                        }
                        if let failure { throw failure }
                    })
            }
            let source = try await makeSourceProducer(query.source, plan: sourcePlan, context: active,
                authorizationQuery: query, allowsOuterReferences: true)
            let outputSchema = try self.outputSchema(plan.columns)
            if canonicalQueryRequiresAggregation(query) || canonicalQueryRequiresWindows(query) || !(query.orderBy?.isEmpty ?? true) || query.quantifier != .all {
                let localCaches = caches
                let materialized = SQLDeferredRowProducer(schema: outputSchema, meter: meter) { [self] in
                    let relation = try await drainSQLProducer(source, meter: meter)
                    return try await prepareSelectedRows(query, relation: relation, plan: plan,
                        pagination: pagination, context: active)
                }
                return SQLRelationPullProducer(schema: outputSchema, meter: meter,
                    operation: { _ in try await materialized.next() }, cleanup: {
                        var failure: (any Error)?
                        do { try await materialized.close() } catch { failure = error }
                        do { try await source.close() } catch { if failure == nil { failure = error } }
                        for cache in localCaches { do { try await cache.close() } catch { if failure == nil { failure = error } } }
                        if let failure { throw failure }
                    })
            }
            return SQLSelectRowProducer(schema: outputSchema, meter: meter, source: source,
                pagination: pagination, caches: caches, predicate: { [self] row in
                    guard let filter = plan.filter else { return true }
                    return try await evaluateBoundBoolean(filter, on: row, context: active, workMeter: meter)
                }, projection: { [self] row in
                    let relation = try sqlSingleRow(row, schema: sourcePlan.schema, meter: meter)
                    let projected = try await projectRows(relation.rows, projection: query.projection,
                        columns: plan.columns, sourceSchema: sourcePlan.schema, workMeter: meter,
                        evaluationContext: active, boundPlan: plan)
                    return try relationFromOutput(projected, schema: outputSchema, meter: meter)
                })
        } catch {
            let operationError = error
            var cleanupError: (any Error)?
            for cache in caches {
                do { try await cache.close() } catch { if cleanupError == nil { cleanupError = error } }
            }
            if let cleanupError {
                throw SQLRelationCleanupError(operationError: operationError, cleanupError: cleanupError)
            }
            throw operationError
        }
    }

    private func makeSourceProducer(_ source: DataSource, plan: CanonicalSourceBindingPlan,
        context: CanonicalQueryEvaluationContext, authorizationQuery: SelectQuery,
        allowsOuterReferences: Bool = false
    ) async throws -> any SQLRelationProducer {
        let meter = context.options.workMeter
        switch source {
        case .table(let table):
            if table.schema == nil, let producer = try await context.cteEnvironment.consumer(named: table.table) {
                return reframedProducer(producer, schema: plan.schema, meter: meter)
            }
        case .aliased(let nested, _, _):
            let child = try await makeSourceProducer(nested, plan: plan.children[0], context: context,
                authorizationQuery: authorizationQuery, allowsOuterReferences: allowsOuterReferences)
            return reframedProducer(child, schema: plan.schema, meter: meter)
        case .subquery(let query):
            guard let body = plan.queryPlan else { throw CanonicalReadError.unsupportedSelectQuery("A derived relation has no executable binding") }
            let child = try await makeSelectProducer(query, plan: body, context: context)
            return reframedProducer(child, schema: plan.schema, meter: meter)
        case .values:
            let empty = try CanonicalRelationSchema().nullRow()
            return SQLRelationPullProducer(schema: plan.schema, meter: meter) { [self] index in
                guard index < plan.valueBindings.count else { return nil }
                let row = try await evaluateValuesRow(plan.valueBindings[index], schema: plan.schema, on: empty, context: context)
                return try sqlSingleRow(row, schema: plan.schema, meter: meter)
            }
        case .join(let clause):
            let left = try await makeSourceProducer(clause.left, plan: plan.children[0], context: context,
                authorizationQuery: authorizationQuery, allowsOuterReferences: allowsOuterReferences)
            let lateral = clause.type == .lateral || clause.type == .leftLateral
            let type = lateral ? (clause.type == .leftLateral ? JoinType.left : .inner) : naturalJoinBaseType(clause.type)
            let condition: JoinCondition?
            switch clause.type {
            case .natural, .naturalLeft, .naturalRight, .naturalFull:
                condition = .using(inferNaturalJoinColumns(leftSchema: plan.children[0].schema, rightSchema: plan.children[1].schema))
            default: condition = clause.condition
            }
            return try SQLJoinRowProducer(schema: plan.schema, left: left, rightSchema: plan.children[1].schema,
                type: type, lateral: lateral, meter: meter,
                hashKey: try selectedJoinHashKey(clause: clause, plan: plan, meter: meter),
                rightFactory: { [self] leftRow in
                    let active = runtimeContext(context, environment: context.cteEnvironment,
                        outerRow: leftRow.map { $0.overlaying(outer: context.outerRow) })
                    return try await makeSourceProducer(clause.right, plan: plan.children[1], context: active,
                        authorizationQuery: authorizationQuery, allowsOuterReferences: lateral)
                }, matches: { [self] left, right in
                    try await joinMatches(left: left, right: right, condition: condition, boundCondition: plan.condition,
                        joinType: type, evaluationContext: context, workMeter: meter, usingLayout: plan.schema.layout)
                }, merge: { [self] left, right in try mergeSelectedJoinRows(left, right, condition: condition, schema: plan.schema, meter: meter, temporalEnvironment: context.options.temporalEnvironment) })
        default: break
        }
        return SQLMaterializedRelationProducer(schema: plan.schema, meter: meter) { [self] in
            try await materializeRows(for: source, sourcePlan: plan, namedSubqueries: context.namedSubqueries,
                queryBinding: context.queryBinding, options: context.options, partitionValues: context.partitionValues,
                partitionMode: context.partitionMode, transaction: context.transaction,
                preparedFusionGraph: context.preparedFusionGraph, fusionSession: context.fusionSession,
                authorizationQuery: authorizationQuery, outerRow: context.outerRow,
                allowsOuterReferences: allowsOuterReferences, cteEnvironment: context.cteEnvironment)
        }
    }

    private struct CanonicalJoinColumnPair: Sendable {
        let left: ColumnRef
        let right: ColumnRef
    }

    private func selectedJoinHashKey(clause: JoinClause, plan: CanonicalSourceBindingPlan,
        meter: DatabaseWorkMeter
    ) throws -> (@Sendable (CanonicalSourceRow, Bool) throws -> SQLJoinRowProducer.HashKey?)? {
        switch clause.type {
        case .inner, .left, .right, .full: break
        default: return nil
        }
        guard isPhysicalJoinInput(plan.children[1]), case .on(let expression) = clause.condition,
              let selected = plan.condition, try selected.loweredExpression == expression else { return nil }
        var pairs = try DatabaseRetainedArrayBuilder<CanonicalJoinColumnPair>(workMeter: meter,
            stage: .joinCandidate, layout: DatabaseRetainedArrayLayout.forElement(CanonicalJoinColumnPair.self))
        guard try collectSelectedJoinKeys(expression, left: plan.children[0].schema,
            right: plan.children[1].schema, into: &pairs, meter: meter) else { return nil }
        let keys = try pairs.finish().moveToSharedOwnership(at: .joinCandidate)
        guard !keys.isEmpty else { return nil }
        return { [self] row, left in
            let claim = try meter.reserveIntermediate(bytes: DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: keys.count, element: CanonicalSQLValueIdentity.self).bytes, at: .joinCandidate)
            var values: [CanonicalSQLValueIdentity] = []
            values.reserveCapacity(keys.count)
            if values.capacity > keys.count {
                try claim.reserveAdditional(bytes: DatabaseIntermediateFootprint(
                    bytes: UInt64(MemoryLayout<CanonicalSQLValueIdentity>.stride))
                    .multiplied(by: UInt64(values.capacity - keys.count)).bytes, at: .joinCandidate)
            }
            for pair in keys {
                let reference = left ? pair.left : pair.right
                let matches = row.layout.matching(reference)
                guard matches.count == 1, let slot = matches.first,
                      row.values.indices.contains(slot), let type = row.layout.types[slot] else {
                    throw CanonicalReadError.unsupportedSelectQuery("A selected JOIN hash key lost its admitted field")
                }
                let value = row.values[slot]
                if value.isNull { return nil }
                try claim.reserveAdditional(bytes: CanonicalRelationalFootprintMeter.valueFootprint(
                    of: value, workMeter: meter, stage: .joinCandidate).bytes, at: .joinCandidate)
                values.append(try canonicalQueryValueIdentity(value, operation: "hash JOIN", type: type, workMeter: meter))
            }
            return SQLJoinRowProducer.HashKey(values: values, lease: DatabaseQueryPayloadLease(retaining: claim))
        }
    }

    private func isPhysicalJoinInput(_ plan: CanonicalSourceBindingPlan) -> Bool {
        switch plan.source {
        case .table: return plan.queryPlan == nil
        case .aliased: return plan.children.count == 1 && isPhysicalJoinInput(plan.children[0])
        default: return false
        }
    }

    private func collectSelectedJoinKeys(_ expression: Expression,
        left: CanonicalRelationSchema, right: CanonicalRelationSchema,
        into pairs: inout DatabaseRetainedArrayBuilder<CanonicalJoinColumnPair>, meter: DatabaseWorkMeter
    ) throws -> Bool {
        try meter.consume(at: .joinCandidate)
        switch expression {
        case .and(let lhs, let rhs):
            return try collectSelectedJoinKeys(lhs, left: left, right: right, into: &pairs, meter: meter)
                && collectSelectedJoinKeys(rhs, left: left, right: right, into: &pairs, meter: meter)
        case .equal(.column(let lhs), .column(let rhs)) where lhs.table != nil && rhs.table != nil:
            let forward = left.layout.matching(lhs).count == 1 && right.layout.matching(rhs).count == 1
                && left.layout.matching(rhs).isEmpty && right.layout.matching(lhs).isEmpty
            let reverse = left.layout.matching(rhs).count == 1 && right.layout.matching(lhs).count == 1
                && left.layout.matching(lhs).isEmpty && right.layout.matching(rhs).isEmpty
            guard forward != reverse else { return false }
            let pair = CanonicalJoinColumnPair(left: forward ? lhs : rhs, right: forward ? rhs : lhs)
            guard let leftSlot = left.layout.matching(pair.left).first,
                  let rightSlot = right.layout.matching(pair.right).first,
                  let type = left.slotTypes[leftSlot], type == right.slotTypes[rightSlot],
                  case .primitive(let scalar, isArray: false) = type else { return false }
            switch scalar {
            case .bool, .int8, .int16, .int32, .int64, .uint8, .uint16, .uint32, .uint64,
                 .float32, .float64, .decimal, .string, .bytes, .uuid: break
            default: return false
            }
            try pairs.append(footprint: DatabaseIntermediateFootprint()) { pair }
            return true
        default: return false
        }
    }

    private func reframedProducer(_ source: any SQLRelationProducer, schema: CanonicalRelationSchema,
        meter: DatabaseWorkMeter
    ) -> any SQLRelationProducer {
        SQLRelationPullProducer(schema: schema, meter: meter, operation: { [self] _ in
            guard let relation = try await source.next() else { return nil }
            return try reframeRelation(relation, to: schema, workMeter: meter)
        }, cleanup: { try await source.close() })
    }

    private func drainSQLProducer(_ producer: any SQLRelationProducer,
        meter: DatabaseWorkMeter
    ) async throws -> CanonicalRelation {
        var rows = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(workMeter: meter,
            stage: .bindingCandidate, layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self))
        do {
            while let next = try await producer.next() {
                let row = next.rows[0].retainingPayload(next.rows.payloadLease)
                try rows.append(footprint: CanonicalRelationalFootprintMeter.sourceAliasFootprint(of: row,
                    workMeter: meter, stage: .bindingCandidate), make: { row })
            }
            try await producer.close()
            return CanonicalRelation(schema: producer.schema, rows: try rows.finish().moveToSharedOwnership(at: .bindingCandidate))
        } catch {
            let operationError = error
            do { try await producer.close() } catch { throw SQLRelationCleanupError(operationError: operationError, cleanupError: error) }
            throw operationError
        }
    }

    private func reframeRelation(_ source: CanonicalRelation, to schema: CanonicalRelationSchema,
        workMeter: DatabaseWorkMeter
    ) throws -> CanonicalRelation {
        let logical = source.schema.layout.visibleSlots
        guard logical.count == schema.layout.slots.count else {
            throw CanonicalReadError.unsupportedSelectQuery("Source alias output arity differs")
        }
        var builder = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(workMeter: workMeter,
            stage: .bindingCandidate, layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self), expectedCount: source.rows.count)
        for row in source.rows {
            let claim = try workMeter.reserveIntermediate(bytes:
                DatabaseIntermediateCollectionMeter.arrayFootprint(count: logical.count,
                    element: CanonicalSourceLayout.BindingSlot.self).multiplied(by: 8).bytes,
                at: .bindingCandidate)
            var selected: [Int] = []
            var ordinals: [Int] = []
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
                types: ordinals.map { schema.layout.types[$0] },
                nullability: ordinals.map { schema.layout.nullability[$0] },
                payloadLeases: schema.layout.payloadLeases, bindingSlots: bindings)
            let footprint = try CanonicalRelationalFootprintMeter.sourceAliasFootprint(of: row, workMeter: workMeter, stage: .bindingCandidate)
            try builder.append(footprint: footprint) {
                // Alias projection reorders only the authorized positional vector; payload owners remain shared.
                CanonicalSourceRow(values: selected.map { row.values[$0] }, layout: layout,
                    annotations: row.annotations, version: row.version,
                    payloadLeases: row.payloadLeases + [source.rows.payloadLease])
            }
        }
        return CanonicalRelation(schema: schema, rows: try builder.finish().moveToSharedOwnership(at: .bindingCandidate))
    }

    private func mergeSelectedJoinRows(_ left: CanonicalSourceRow, _ right: CanonicalSourceRow,
        condition: JoinCondition?, schema: CanonicalRelationSchema, meter: DatabaseWorkMeter,
        temporalEnvironment: SQLTemporalEnvironment?
    ) throws -> CanonicalSourceRow {
        let footprint = try prospectiveJoinRowFootprint(left: left, right: right,
            condition: condition, workMeter: meter)
        // The merged payload owns one row before allocation; publication owns its separate row container.
        let claim = try meter.reserveIntermediate(rows: 1, bytes: footprint.multiplied(by: 4).bytes, at: .joinCandidate)
        let lhs = try relationSchema(for: left.layout)
        let rhs = try relationSchema(for: right.layout)
        let actual: CanonicalRelationSchema
        if case .using(let columns) = condition { actual = try lhs.merged(with: rhs, coalescing: columns) }
        else { actual = try lhs.merged(with: rhs) }
        var consumed = Array(repeating: false, count: actual.layout.slots.count)
        let bindings = try schema.layout.slots.map { address -> CanonicalSourceLayout.BindingSlot in
            try meter.consume(at: .joinCandidate)
            let index = actual.layout.slots.indices.first { !consumed[$0] && actual.layout.slots[$0] == address }
            if let index { consumed[index] = true }
            return .init(address: address, valueIndex: index)
        }
        let layout = CanonicalSourceLayout(slots: actual.layout.slots, scopes: actual.layout.scopes,
            coalescedColumns: actual.layout.coalescedColumns, reservation: claim,
            types: actual.layout.slots.indices.map { index in
                let logical = bindings.firstIndex { $0.valueIndex == index }
                return logical.flatMap { schema.layout.types[$0] }
            }, nullability: actual.layout.slots.indices.map { index in
                let address = actual.layout.slots[index]
                return schema.layout.slots.firstIndex(of: address).map { schema.layout.nullability[$0] } ?? true
            }, visibleSlots: actual.layout.visibleSlots, bindingSlots: bindings)
        return try mergeJoinRows(left, right, condition: condition, layout: layout, workMeter: meter,
            temporalEnvironment: temporalEnvironment ?? inheritedSQLTemporalEnvironment)
            .retainingPayload(DatabaseQueryPayloadLease(retaining: claim))
    }

    private func evaluateValuesRow(_ bindings: [CanonicalBoundExpression], schema: CanonicalRelationSchema,
        on row: CanonicalSourceRow, context: CanonicalQueryEvaluationContext
    ) async throws -> CanonicalSourceRow {
        let meter = context.options.workMeter
        let claim = try meter.reserveIntermediate(bytes: UInt64(bindings.count) * 128 + 64, at: .bindingCandidate)
        var values: [QueryValue] = []; var leases: [DatabaseQueryPayloadLease] = []
        values.reserveCapacity(bindings.count); leases.reserveCapacity(bindings.count + 1)
        for bound in bindings {
            let value = try await evaluateBoundValue(bound, on: row, context: context, workMeter: meter)
            try claim.reserveAdditional(bytes: UInt64(value.leases.count) * 64, at: .bindingCandidate)
            values.append(value.value); leases.append(contentsOf: value.leases)
        }
        leases.append(DatabaseQueryPayloadLease(retaining: claim))
        return CanonicalSourceRow(values: values, layout: schema.layout, payloadLeases: leases)
    }

    private func outputQueryRows(_ relation: CanonicalRelation, columns: [QueryColumn],
        meter: DatabaseWorkMeter, metadataLeases: [DatabaseQueryPayloadLease] = []
    ) throws -> DatabaseSharedRetainedQueryRows {
        var builder = try DatabaseRetainedQueryRowsBuilder(columns: columns, workMeter: meter,
            stage: .resultMaterialization, expectedCount: relation.rows.count,
            payloadLeases: [relation.rows.payloadLease] + metadataLeases, schemaLeases: [])
        for row in relation.rows {
            try builder.append(QueryRow(values: row.values, columns: columns, annotations: row.annotations, version: row.version))
        }
        return try builder.finish().moveToSharedOwnership(at: .resultMaterialization)
    }

    private func relationFromOutput(_ rows: DatabaseSharedRetainedQueryRows, schema: CanonicalRelationSchema,
        meter: DatabaseWorkMeter
    ) throws -> CanonicalRelation {
        var builder = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(workMeter: meter,
            stage: .bindingCandidate, layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self), expectedCount: rows.count)
        for index in 0..<rows.count {
            try rows.withElement(at: index) { row in
                try builder.append(footprint: CanonicalRelationalFootprintMeter.positionalValueArrayFootprint(
                    count: row.values.count, valueAt: { _ in .primitive(.null) }, annotations: row.annotations,
                    version: row.version, workMeter: meter, stage: .bindingCandidate)) {
                    CanonicalSourceRow(values: row.values, layout: schema.layout, annotations: row.annotations,
                        version: row.version, payloadLeases: rows.originPayloadLeases)
                }
            }
        }
        return CanonicalRelation(schema: schema, rows: try builder.finish().moveToSharedOwnership(at: .bindingCandidate))
    }

    private func evaluatePagination(_ pagination: CanonicalBoundPagination,
        context: CanonicalQueryEvaluationContext
    ) async throws -> SQLResolvedPagination {
        let row = try CanonicalRelationSchema().nullRow()
        func count(_ bound: CanonicalBoundCount?, name: String) async throws -> UInt64? {
            guard let bound else { return nil }
            switch bound {
            case .fixed(let value): return value
            case .expression(let expression):
                let value = try await evaluateBoundValue(expression, on: row, context: context, workMeter: context.options.workMeter)
                if value.value.isNull { return nil }
                guard case .primitive(let scalar) = value.value,
                      let integer = scalar.int64Value, integer >= 0 else {
                    throw CanonicalReadError.unsupportedSelectQuery("SQL \(name) must be a nonnegative bigint")
                }
                return UInt64(integer)
            }
        }
        let offset = try await count(pagination.offset, name: "OFFSET") ?? 0
        let limit = try await count(pagination.limit, name: "LIMIT")
        if pagination.rejectsUnknownNullLimit, limit == nil {
            throw CanonicalReadError.unsupportedSelectQuery("FETCH WITH TIES row count cannot be unknown NULL")
        }
        return SQLResolvedPagination(offset: offset, limit: limit, withTies: pagination.withTies)
    }

    private struct CanonicalSelectedOutputFrame: Sendable {
        var source: CanonicalSourceRow
        let group: CanonicalGroupedRow?
        var projected: [ProjectedQueryValue?]
        var ordering: [ProjectedQueryValue]
        var distinct: [ProjectedQueryValue]
        let ordinal: Int
    }

    private func selectedProjection(_ index: Int, frame: inout CanonicalSelectedOutputFrame,
        plan: CanonicalBoundSelect, context: CanonicalQueryEvaluationContext
    ) async throws -> ProjectedQueryValue {
        if let value = frame.projected[index] { return value }
        let active = context.windowValues.map { context.usingWindows(CanonicalWindowEvaluation(plans: $0.plans, results: $0.results, row: frame.ordinal)) } ?? context
        let value = try await evaluateBoundValue(plan.projection[index], on: frame.source,
            group: frame.group, context: active, workMeter: context.options.workMeter)
        frame.projected[index] = value
        return value
    }

    private func selectedKey(_ key: CanonicalBoundKey, frame: inout CanonicalSelectedOutputFrame,
        plan: CanonicalBoundSelect, context: CanonicalQueryEvaluationContext
    ) async throws -> ProjectedQueryValue {
        if let index = key.projectionIndex { return try await selectedProjection(index, frame: &frame, plan: plan, context: context) }
        let active = context.windowValues.map { context.usingWindows(CanonicalWindowEvaluation(plans: $0.plans, results: $0.results, row: frame.ordinal)) } ?? context
        return try await evaluateBoundValue(key.expression, on: frame.source, group: frame.group,
            context: active, workMeter: context.options.workMeter)
    }

    private func finalizeSelectedRows(_ query: SelectQuery, relation: CanonicalRelation,
        plan: CanonicalBoundSelect, pagination: SQLResolvedPagination,
        context: CanonicalQueryEvaluationContext,
        mergeDuplicateAnnotations: (@Sendable ([String: FieldValue], [String: FieldValue]) throws -> [String: FieldValue])? = nil
    ) async throws -> CanonicalRelation {
        let producer = try await prepareSelectedRows(query, relation: relation, plan: plan,
            pagination: pagination, context: context, mergeDuplicateAnnotations: mergeDuplicateAnnotations)
        return try await drainSQLProducer(producer, meter: context.options.workMeter)
    }

    private func prepareSelectedRows(_ query: SelectQuery, relation: CanonicalRelation,
        plan: CanonicalBoundSelect, pagination: SQLResolvedPagination,
        context: CanonicalQueryEvaluationContext,
        mergeDuplicateAnnotations: (@Sendable ([String: FieldValue], [String: FieldValue]) throws -> [String: FieldValue])? = nil
    ) async throws -> any SQLRelationProducer {
        let meter = context.options.workMeter
        let filtered = try await applyFilter(query.filter, bound: plan.filter, to: relation.rows,
            workMeter: meter, evaluationContext: context)
        let grouped: CanonicalRetainedGroups?
        if canonicalQueryRequiresAggregation(query) {
            let groups = try await makeCanonicalGroups(filtered,
                boundGroupBy: plan.groupBy, workMeter: meter, evaluationContext: context,
                groupingPlan: plan.grouping, mergeAnnotations: mergeDuplicateAnnotations)
            grouped = try await applyHaving(query.having, bound: plan.having, to: groups,
                workMeter: meter, evaluationContext: context)
        } else { grouped = nil }
        let count = grouped?.count ?? filtered.count
        let hasWindows = canonicalQueryRequiresWindows(query)
        let windowClaim = try hasWindows ? meter.reserveIntermediate(bytes: 128, at: .projection) : nil
        var windowPlans: [CanonicalBoundWindow] = []
        func collectWindows(_ value: CanonicalBoundExpression) throws {
            try meter.consume(at: .bindingCandidate)
            if let window = value.window {
                if !windowPlans.contains(where: { $0 === window }) {
                    try CanonicalBindingStorage.append(window, to: &windowPlans, reservation: windowClaim)
                }
                return
            }
            for child in value.children { try collectWindows(child) }
        }
        if hasWindows {
            for expression in plan.projection { try collectWindows(expression) }
            for key in plan.orderBy { try collectWindows(key.expression) }
        }
        var windowResults: [SQLWindowResults] = []
        for window in windowPlans {
            let results = try await SQLWindowEvaluator.evaluate(plan: window, rowCount: count,
                evaluate: { [self] index, selected in
                    let group = grouped.map { $0[index] }
                    let row = group?.representative ?? filtered[index]
                    let value = try await evaluateBoundValue(selected, on: row, group: group,
                        context: context, workMeter: meter)
                    return ProjectedQueryValue(value: value.value,
                        leases: value.leases + row.payloadLeases + [grouped?.payloadLease ?? filtered.payloadLease],
                        payloadIsRetained: value.payloadIsRetained)
                }, direct: { [self] selected in
                    let group = grouped.flatMap { $0.isEmpty ? nil : $0[0] }
                    let row: CanonicalSourceRow
                    if let group { row = group.representative }
                    else if !filtered.isEmpty { row = filtered[0] }
                    else { row = relation.schema.nullRow() }
                    return try await evaluateBoundValue(selected, on: row, group: group,
                        context: context, workMeter: meter)
                }, workMeter: meter, temporalEnvironment: context.options.temporalEnvironment,
                limits: Self.selectedValueLimits(context: context, meter: meter))
            try CanonicalBindingStorage.append(results, to: &windowResults, reservation: windowClaim)
        }
        let analyticContext = hasWindows ? context.usingWindows(CanonicalWindowEvaluation(plans: windowPlans, results: windowResults, row: 0)) : context
        let windowMetadataLease = windowClaim.map { DatabaseQueryPayloadLease(retaining: $0) }
        let keyCount = plan.orderBy.count + plan.distinctOn.count + plan.columns.count
        let frameBytes = try DatabaseIntermediateCollectionMeter.arrayFootprint(count: count, element: CanonicalSelectedOutputFrame.self)
            .multiplied(by: 2)
            .adding(DatabaseIntermediateFootprint(bytes: UInt64(count) * UInt64(keyCount + 1) * 192))
        let scratch = try meter.reserveIntermediate(rows: UInt64(count), bytes: frameBytes.bytes, at: .sortInput)
        var frames: [CanonicalSelectedOutputFrame] = []; frames.reserveCapacity(count)
        for index in 0..<count {
            let group = grouped.map { $0[index] }
            let row = group?.representative ?? filtered[index]
            var frame = CanonicalSelectedOutputFrame(source: row,
                group: group, projected: Array(repeating: nil, count: plan.columns.count),
                ordering: [], distinct: [], ordinal: index)
            frame.ordering.reserveCapacity(plan.orderBy.count)
            for key in plan.orderBy { let value = try await selectedKey(key, frame: &frame, plan: plan, context: analyticContext); frame.ordering.append(value) }
            frame.distinct.reserveCapacity(plan.distinctOn.count)
            for key in plan.distinctOn { let value = try await selectedKey(key, frame: &frame, plan: plan, context: analyticContext); frame.distinct.append(value) }
            if query.quantifier == .distinct {
                for output in plan.columns.indices { _ = try await selectedProjection(output, frame: &frame, plan: plan, context: analyticContext) }
            }
            frames.append(frame)
        }
        if !plan.orderBy.isEmpty {
            frames = try frames.sorted { left, right in
                for (index, key) in plan.orderBy.enumerated() {
                    guard let sort = key.sortKey else { throw CanonicalReadError.unsupportedSelectQuery("An ORDER key lost its ordering declaration") }
                    let comparison = try Self.selectedOrdering(left.ordering[index].value, right.ordering[index].value,
                        bound: key.expression, using: sort, environment: context.options.temporalEnvironment, workMeter: meter)
                    if comparison != .equal { return comparison == .lessThan }
                }
                return left.ordinal < right.ordinal
            }
        }
        var uniqueIndices: [Int] = []; uniqueIndices.reserveCapacity(frames.count)
        var keyRows: [[CanonicalSQLValueIdentity]] = []; keyRows.reserveCapacity(frames.count)
        var buckets: [Int: [Int]] = [:]
        let identityClaim = try meter.reserveIntermediate(bytes: UInt64(frames.count) * 192 + 64, at: .deduplication)
        defer { identityClaim.release() }
        for index in frames.indices {
            try meter.consume(at: .deduplication)
            if query.quantifier == .all { uniqueIndices.append(index); continue }
            let values: [QueryValue]
            let types: [QueryColumnType]
            if query.quantifier == .distinct {
                values = frames[index].projected.map { $0!.value }; types = plan.columns.map { $0.type }
            } else {
                values = frames[index].distinct.map { $0.value }; types = plan.distinctOn.map { $0.expression.type }
            }
            try identityClaim.reserveAdditional(bytes: UInt64(values.count) * 128, at: .deduplication)
            let keys = try values.enumerated().map { try canonicalQueryValueIdentity($0.element,
                operation: "DISTINCT", type: types[$0.offset], workMeter: meter) }
            let hash = try CanonicalSQLValueIdentity.hash(keys, workMeter: meter, stage: .deduplication)
            var existing: Int?
            for candidate in buckets[hash] ?? [] {
                if try CanonicalSQLValueIdentity.equal(keyRows[candidate], keys, workMeter: meter, stage: .deduplication) {
                    existing = candidate; break
                }
            }
            if let existing {
                if query.quantifier == .distinct, let mergeDuplicateAnnotations {
                    let representative = uniqueIndices[existing]
                    let original = frames[representative].source
                    let annotations = try mergeDuplicateAnnotations(original.annotations, frames[index].source.annotations)
                    frames[representative].source = CanonicalSourceRow(values: original.values, layout: original.layout,
                        annotations: annotations, version: original.version, payloadLeases: original.payloadLeases,
                        sourcePresence: original.sourcePresence)
                }
            } else {
                buckets[hash, default: []].append(keyRows.count)
                keyRows.append(keys); uniqueIndices.append(index)
            }
        }
        let lower = pagination.offset >= UInt64(uniqueIndices.count) ? uniqueIndices.count : Int(pagination.offset)
        var upper = pagination.limit.map { value in value >= UInt64(uniqueIndices.count - lower) ? uniqueIndices.count : lower + Int(value) } ?? uniqueIndices.count
        if pagination.withTies, upper > lower, upper < uniqueIndices.count {
            let peer = frames[uniqueIndices[upper - 1]]
            while upper < uniqueIndices.count {
                let candidate = frames[uniqueIndices[upper]]
                var equal = true
                for (index, key) in plan.orderBy.enumerated() {
                    guard let sort = key.sortKey else { throw CanonicalReadError.unsupportedSelectQuery("Missing peer ORDER key") }
                    if try Self.selectedOrdering(peer.ordering[index].value, candidate.ordering[index].value,
                        bound: key.expression, using: sort, environment: context.options.temporalEnvironment, workMeter: meter) != .equal {
                        equal = false; break
                    }
                }
                if !equal { break }
                upper += 1
            }
        }
        let schema = try outputSchema(plan.columns)
        let selectedFrames = frames
        let positions = uniqueIndices
        let outputLower = lower
        let outputUpper = upper
        return SQLRelationPullProducer(schema: schema, meter: meter, operation: { [self] index in
            guard index < outputUpper - outputLower else { return nil }
            var frame = selectedFrames[positions[outputLower + index]]
            var values: [QueryValue] = []; var leases = frame.source.payloadLeases + [filtered.payloadLease]
            if let windowMetadataLease { leases.append(windowMetadataLease) }
            if let grouped { leases.append(grouped.payloadLease) }
            let payloadClaim = try meter.reserveIntermediate(bytes:
                DatabaseIntermediateCollectionMeter.arrayFootprint(count: plan.columns.count,
                    element: QueryValue.self).multiplied(by: 4).bytes + 64, at: .projection)
            values.reserveCapacity(plan.columns.count)
            for column in plan.columns.indices {
                let cell = try await selectedProjection(column, frame: &frame, plan: plan, context: analyticContext)
                try payloadClaim.reserveAdditional(bytes: UInt64(cell.leases.count) * 64, at: .projection)
                values.append(try CanonicalQueryTypeBinder.normalize(cell.value, to: plan.columns[column].type))
                leases.append(contentsOf: cell.leases)
            }
            leases.append(DatabaseQueryPayloadLease(retaining: payloadClaim))
            let row = CanonicalSourceRow(values: values, layout: schema.layout, annotations: frame.source.annotations,
                version: frame.source.version, payloadLeases: leases)
            return try sqlSingleRow(row, schema: schema, meter: meter)
        }, cleanup: { scratch.release() })
    }

    private func validateDistinctOrderPrefix(distinct: [CanonicalBoundKey], ordering: [CanonicalBoundKey],
        meter: DatabaseWorkMeter) throws {
        var remaining = Set(distinct.indices)
        for key in ordering {
            if let index = try remaining.first(where: { index in
                let candidate = distinct[index]
                return try candidate.isSemanticallyEquivalent(to: key, workMeter: meter,
                    isVolatileSubquery: selectedPlanIsVolatile)
            }) { remaining.remove(index) }
            else if !remaining.isEmpty {
                throw CanonicalReadError.unsupportedSelectQuery("DISTINCT ON expressions must match initial ORDER BY expressions")
            }
        }
    }

    private func selectedPlanIsVolatile(_ plan: CanonicalBoundSelect) -> Bool {
        func expression(_ value: CanonicalBoundExpression) -> Bool {
            if case .function(let call) = value.expression,
               call.name.lowercased() == "clock_timestamp" { return true }
            if let nested = value.subqueryBinding, selectedPlanIsVolatile(nested) { return true }
            return value.children.contains(where: expression)
        }
        func source(_ value: CanonicalSourceBindingPlan) -> Bool {
            if let query = value.queryPlan, selectedPlanIsVolatile(query) { return true }
            if let condition = value.condition, expression(condition) { return true }
            return value.valueBindings.contains { $0.contains(where: expression) }
                || value.children.contains(where: source)
        }
        if plan.projection.contains(where: expression) || plan.groupBy.contains(where: expression)
            || plan.orderBy.contains(where: { expression($0.expression) })
            || plan.distinctOn.contains(where: { expression($0.expression) }) { return true }
        if let filter = plan.filter, expression(filter) { return true }
        if let having = plan.having, expression(having) { return true }
        for window in plan.windows {
            if window.partitionBy.contains(where: expression) || window.orderBy.contains(where: { expression($0.expression) }) { return true }
            if let cell = window.frame?.startOffset, expression(cell) { return true }
            if let cell = window.frame?.endOffset, expression(cell) { return true }
        }
        for count in [plan.pagination.offset, plan.pagination.limit] {
            if case .expression(let value) = count, expression(value) { return true }
        }
        if let root = plan.sourcePlan, source(root) { return true }
        return plan.ctes.contains { $0.isVolatile }
    }


    private func queryCanonical(
        _ selectQuery: SelectQuery,
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
        partitionMode: CanonicalPartitionRoutingMode,
        transaction: DatabaseReadTransaction,
        inheritedSubqueries: [NamedSubquery] = [],
        queryBinding: CanonicalQueryBindingEnvironment? = nil,
        boundPlan suppliedPlan: CanonicalBoundSelect? = nil,
        outerRow: CanonicalSourceRow? = nil,
        cteEnvironment: SQLCTEEnvironment = SQLCTEEnvironment(),
        preparedFusionGraph: FusionPreparedQueryGraph,
        fusionSession: DatabaseReadSession,
        admittedListAuthorizationRequirement:
            DatabaseListReadAuthorizationRequirement? = nil
    ) async throws -> CanonicalRetainedQueryResponse {
        let initialBinding = try queryBinding ?? CanonicalQueryBindingEnvironment(
            parameters: try CanonicalParameterBindingState(parameters: [], workMeter: options.workMeter), workMeter: options.workMeter,
            occurrences: nil, occurrence: nil, allowingSPARQLFunctions: false)
        let boundPlan = try suppliedPlan ?? bindCanonicalQuery(selectQuery,
            inherited: inheritedSubqueries,
            outerSchemas: outerRow.map { [try relationSchema(for: $0.layout)] } ?? [], binding: initialBinding)
        if suppliedPlan == nil { try initialBinding.parameters.finalize() }
        guard let sourcePlan = boundPlan.sourcePlan, let resolvedQuery = sourcePlan.query else {
            throw CanonicalReadError.unsupportedSelectQuery("A canonical SELECT has no bound source plan")
        }
        // Authorization describes the caller's logical window and ordering.
        // Resolving an output alias may produce a computed ORDER expression;
        // it must not replace the requirement already admitted for that request.
        var listAuthorizationRequirement = admittedListAuthorizationRequirement
        if listAuthorizationRequirement == nil, sourcePlan.queryPlan == nil,
           case .table(let table) = selectQuery.source {
            listAuthorizationRequirement = try DatabaseReadPolicy.listRequirement(
                entityName: table.table, selectQuery: selectQuery, schema: try readPolicy().schema)
        }
        let selectQuery = resolvedQuery
        let queryBinding = sourcePlan.binding
        let namedSubqueries = sourcePlan.namedSubqueries
        let evaluationContext = CanonicalQueryEvaluationContext(
            options: options,
            transaction: transaction,
            partitionValues: partitionValues,
            partitionMode: partitionMode,
            namedSubqueries: namedSubqueries, queryBinding: queryBinding,
            boundPlan: boundPlan,
            cteEnvironment: cteEnvironment,
            outerRow: outerRow,
            preparedFusionGraph: preparedFusionGraph,
            fusionSession: fusionSession
        )
        let fixedPagination: Bool
        switch (selectQuery.pagination.offset, selectQuery.pagination.limit) {
        case (.expression?, _), (_, .count(.expression, _)?): fixedPagination = false
        case (_, .count(_, withTies: true)?): fixedPagination = false
        default: fixedPagination = true
        }
        let directTable: Bool
        if case .table = selectQuery.source, sourcePlan.queryPlan == nil,
           selectQuery.subqueries == nil, selectQuery.groupBy == nil, selectQuery.having == nil,
           selectQuery.quantifier == .all, !canonicalQueryRequiresWindows(selectQuery), fixedPagination, !selectedPlanIsVolatile(boundPlan),
           !boundPlan.columns.contains(where: { column in
               if case .sql(.composite) = column.type { return true }; return false
           }) {
            directTable = true
        } else { directTable = false }
        if !selectQuery.source.usesSPARQLSemantics, selectQuery.accessPath == nil, !directTable {
            return try await executeSQLRelation(selectQuery, plan: boundPlan, context: evaluationContext)
        }

        if let accessPath = selectQuery.accessPath {
            return try await executeAccessPathRows(
                selectQuery,
                accessPath: accessPath,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                evaluationContext: evaluationContext,
                preparedFusionGraph: preparedFusionGraph,
                admittedListAuthorizationRequirement: listAuthorizationRequirement
            )
        }

        if case .logical(let logicalSource) = selectQuery.source,
           logicalSource.kindIdentifier == LogicalSourceKind.polymorphic,
           selectQuery.subqueries == nil,
           selectQuery.groupBy == nil,
           selectQuery.having == nil,
           selectQuery.dataset == .implicit,
           selectQuery.reduced == false {
            return try await executePolymorphicRows(
                selectQuery,
                logicalSource: logicalSource,
                options: options,
                transaction: transaction,
                evaluationContext: evaluationContext
            )
        }

        if selectQuery.source.usesSPARQLSemantics {
            guard let executor = try readPolicy().sparqlSourceExecutor else {
                throw CanonicalReadError.unsupportedSource("SPARQL source executor is not registered")
            }
            let sourceSession = try fusionSession.admittingRDFDatasetRead()
            let retainedRows = try await executor.executeInTransaction(
                session: sourceSession,
                selectQuery: selectQuery,
                options: options,
                partitions: partitionValues ?? FieldObject()
            )
            let rows = try retainLogicalQueryRows(
                consume retainedRows,
                workMeter: options.workMeter,
                stage: .resultMaterialization
            )
            let page = try CanonicalQueryPagination.retainedWindow(
                rows: rows,
                selectQuery: selectQuery,
                options: options,
                rowsAreLogicalQueryRelative: true
            )
            return CanonicalRetainedQueryResponse(
                rows: rows,
                visibleRange: page.range,
                continuation: page.continuation,
                metadata: [:],
                affectedRows: nil
            )
        }

        if case .table = selectQuery.source,
           sourcePlan.queryPlan == nil,
           selectQuery.subqueries == nil,
           selectQuery.groupBy == nil,
           selectQuery.having == nil,
           selectQuery.dataset == .implicit,
           selectQuery.reduced == false {
            guard partitionValues?.isEmpty != false else {
                throw CanonicalReadError.invalidPartition(
                    entity: "graph",
                    reason: "graph partitions cannot be applied to a table source"
                )
            }
            return try await executeSingleTableRows(
                selectQuery,
                options: options,
                transaction: transaction,
                evaluationContext: evaluationContext,
                admittedListAuthorizationRequirement:
                    listAuthorizationRequirement
            )
        }

        guard selectQuery.dataset == .implicit,
              selectQuery.reduced == false else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Canonical relational execution does not support SPARQL dataset clauses"
            )
        }

        let sourceOptions = executionContextWithoutExternalPageWindow(options)
        let sourceRelation = try await materializeRows(
            for: selectQuery.source,
            sourcePlan: sourcePlan,
            namedSubqueries: namedSubqueries, queryBinding: queryBinding,
            options: sourceOptions,
            partitionValues: partitionValues,
            partitionMode: .routed,
            transaction: transaction,
            preparedFusionGraph: preparedFusionGraph,
            fusionSession: fusionSession,
            authorizationQuery: selectQuery,
            outerRow: outerRow
        )

        return try await finalizeRelationalRows(
            selectQuery,
            sourceRows: sourceRelation.rows,
            sourceSchema: sourceRelation.schema,
            residualFilter: selectQuery.filter,
            residualOrderBy: selectQuery.orderBy,
            options: options,
            evaluationContext: evaluationContext
        )
    }

    private func executeAccessPathRows(
        _ selectQuery: SelectQuery,
        accessPath: AccessPath,
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
        partitionMode: CanonicalPartitionRoutingMode,
        transaction: DatabaseReadTransaction,
        evaluationContext: CanonicalQueryEvaluationContext,
        preparedFusionGraph: FusionPreparedQueryGraph,
        admittedListAuthorizationRequirement: DatabaseListReadAuthorizationRequirement?
    ) async throws -> CanonicalRetainedQueryResponse {
        switch selectQuery.source {
        case .table(let tableRef):
            guard partitionValues?.isEmpty != false else {
                throw CanonicalReadError.invalidPartition(
                    entity: "graph",
                    reason: "graph partitions cannot be applied to a table source"
                )
            }
            if case .fusion(let fusionSource) = accessPath {
                let listAuthorizationRequirement = try admittedListAuthorizationRequirement
                    ?? DatabaseReadPolicy.listRequirement(
                        entityName: tableRef.table, selectQuery: selectQuery, schema: try readPolicy().schema)
                let preparedEntry = try preparedFusionGraph.entry(
                    tableRef: tableRef,
                    source: fusionSource,
                    listAuthorizationRequirement:
                        listAuthorizationRequirement,
                    workMeter: options.workMeter
                )
                let execution = try FusionExecution.make(
                    query: selectQuery,
                    entry: preparedEntry,
                    graph: preparedFusionGraph,
                    session: evaluationContext.fusionSession,
                    options: options,
                    parameters: evaluationContext.queryBinding?.parameters.resolvedParameters ?? []
                )
                let rowSet = try await FusionExecutor.execute(execution)
                let sourceName = tableRef.alias ?? tableRef.effectiveName
                return try await finalizeIndexReadResult(
                    rowSet,
                    sourceName: sourceName,
                    sourceSchema: try tableRelationSchema(tableRef),
                    selectQuery: selectQuery,
                    options: options,
                    evaluationContext: evaluationContext
                )
            }
            // Non-scalar index access paths (fulltext, vector, rank, etc.) are
            // handled by kind-specific executors registered in ReadExecutorRegistry.
            // Only scalar index access is routed through SelectQueryPlanner, because
            // it maps cleanly onto Query<T>.forcedIndex + typed fetch.
            if case .index(let indexScan) = accessPath,
                indexScan.indexType != .ordered
            {
                let rowSet = try await dispatchTableIndexExecutor(
                    tableRef: tableRef,
                    selectQuery: selectQuery,
                    indexScan: indexScan,
                    options: options,
                    session: evaluationContext.fusionSession
                )
                let sourceName = tableRef.alias ?? tableRef.effectiveName
                return try await finalizeIndexReadResult(
                    rowSet,
                    sourceName: sourceName,
                    sourceSchema: try tableRelationSchema(tableRef),
                    selectQuery: selectQuery,
                    options: options,
                    evaluationContext: evaluationContext
                )
            }
            return try await executeSingleTableRows(
                selectQuery,
                options: options,
                transaction: transaction,
                evaluationContext: evaluationContext,
                admittedListAuthorizationRequirement: admittedListAuthorizationRequirement
            )

        case .logical(let logicalSource):
            guard logicalSource.kindIdentifier == LogicalSourceKind.polymorphic else {
                throw CanonicalReadError.unsupportedAccessPath(
                    "accessPath queries do not support logical source '\(logicalSource.kindIdentifier)'"
                )
            }
            guard case .index(let indexScan) = accessPath else {
                throw CanonicalReadError.unsupportedAccessPath(
                    "Polymorphic logical sources currently support only index access paths"
                )
            }
            let group = try container.polymorphicGroup(identifier: logicalSource.identifier)
            guard let index = group.indexes.first(
                where: { $0.name == indexScan.indexName }
            ) else {
                throw CanonicalReadError.indexHintNotFound(
                    "Index '\(indexScan.indexName)' is not declared by polymorphic group '\(group.identifier)'"
                )
            }
            guard index.type == indexScan.indexType else {
                throw CanonicalReadError.unsupportedAccessPath(
                    "Index '\(index.name)' has type '\(index.type.diagnosticName)', not '\(indexScan.indexType.diagnosticName)'"
                )
            }
            guard let executor = try readPolicy().polymorphicIndexExecutor(
                for: index.type
            )
            else {
                throw CanonicalReadError.executorNotRegistered(
                    index.type
                )
            }
            let rowSet = try await executor.executeRows(
                session: evaluationContext.fusionSession,
                selectQuery: selectQuery,
                index: index,
                indexScan: indexScan,
                group: group,
                options: options,
                partitions: partitionValues ?? FieldObject()
            )
            return try await finalizeIndexReadResult(
                rowSet,
                sourceName: logicalSource.effectiveName,
                sourceSchema: try polymorphicRelationSchema(
                    group,
                    sourceName: logicalSource.effectiveName
                ),
                selectQuery: selectQuery,
                options: options,
                evaluationContext: evaluationContext
            )

        default:
            throw CanonicalReadError.unsupportedAccessPath("accessPath queries require a table or logical source")
        }
    }

    /// Apply the common relational pipeline on top of an index executor's row
    /// set. Index-defined ordering is preserved only when the outer `SELECT`
    /// has no explicit ordering.
    private func finalizeIndexReadResult(
        _ rowSet: IndexReadResult,
        sourceName: String?,
        sourceSchema: CanonicalRelationSchema,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        evaluationContext: CanonicalQueryEvaluationContext
    ) async throws -> CanonicalRetainedQueryResponse {
        let workMeter = options.workMeter
        if let sourceMeter = rowSet.workMeter, sourceMeter !== workMeter {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        guard let authorization = evaluationContext.fusionSession.transaction.authorization else {
            throw DatabaseReadSessionError.authorizationMismatch
        }
        let memberNames: [String]
        switch selectQuery.source {
        case .table(let table): memberNames = [table.table]
        case .logical(let logical) where logical.kindIdentifier == LogicalSourceKind.polymorphic:
            memberNames = try container.polymorphicGroup(identifier: logical.identifier).memberTypeNames
        default:
            throw CanonicalReadError.unsupportedSource("Index rows require a declared table or polymorphic source")
        }
        var schemaFootprint = try CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
            fieldNames: sourceSchema.unscopedColumns, sourceName: nil, workMeter: workMeter, stage: .projection
        )
        for scope in sourceSchema.scopes {
            schemaFootprint = try schemaFootprint.adding(CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
                fieldNames: scope.columns, sourceName: scope.name, workMeter: workMeter, stage: .projection
            ))
        }
        schemaFootprint = try schemaFootprint.adding(
            DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<Schema.Entity>.stride) + 128)
                .multiplied(by: UInt64(memberNames.count))
        ).adding(DatabaseIntermediateFootprint(bytes: 128))
        for name in memberNames {
            schemaFootprint = try schemaFootprint.adding(DatabaseIntermediateFootprint(bytes: UInt64(name.utf8.count)))
        }
        let layoutClaim = try workMeter.reserveIntermediate(bytes: schemaFootprint.bytes, at: .projection)
        var declarations: [String: Schema.Entity] = [:]
        var admittedFields: [String: Set<String>] = [:]
        declarations.reserveCapacity(memberNames.count)
        admittedFields.reserveCapacity(memberNames.count)
        for name in memberNames {
            let entity = try resolveEntity(named: name)
            guard let fields = authorization.admittedFieldNames(forEntityName: name) else {
                throw DatabaseReadSessionError.authorizationMismatch
            }
            declarations[name] = entity
            admittedFields[name] = fields
        }
        func admittedSlots(_ names: [String]) throws -> [Int] {
            try names.indices.filter { index in
                let name = names[index]
                guard declarations.values.contains(where: { $0.fieldMapByName[name] != nil }) else {
                    throw CanonicalReadError.unsupportedSource("Index source column '\(name)' has no declared entity field")
                }
                return admittedFields.values.contains { $0.contains(name) }
            }
        }
        let unscoped = try admittedSlots(sourceSchema.unscopedColumns)
        let scopes = try sourceSchema.scopes.map { scope -> CanonicalRelationScope in
            let selected = try admittedSlots(scope.columns)
            return CanonicalRelationScope(name: scope.name, columns: selected.map { scope.columns[$0] },
                types: selected.map { scope.types[$0] }, nullability: selected.map { scope.nullability[$0] })
        }
        let admittedSchema = try CanonicalRelationSchema(
            unscopedColumns: unscoped.map { sourceSchema.unscopedColumns[$0] }, scopes: scopes,
            coalescedColumns: Set(sourceSchema.coalescedColumns.filter { name in
                admittedFields.values.contains { $0.contains(name) }
            }), types: unscoped.map { sourceSchema.unscopedTypes[$0] },
            nullability: unscoped.map { sourceSchema.unscopedNullability[$0] }
        )
        let declaredLayout = admittedSchema.layout
        let layout = CanonicalSourceLayout(slots: declaredLayout.slots, scopes: declaredLayout.scopes,
            coalescedColumns: declaredLayout.coalescedColumns, reservation: layoutClaim,
            types: declaredLayout.types, nullability: declaredLayout.nullability,
            visibleSlots: declaredLayout.visibleSlots)
        let payloadLeases = rowSet.originPayloadLeases
        let limits = QueryValueLimits(maximumRetainedBytes: workMeter.budget.maximumIntermediateBytes,
            maximumTotalNodes: workMeter.budget.maximumWorkUnits,
            maximumNestingDepth: workMeter.budget.maximumWorkUnits,
            maximumCollectionElements: workMeter.budget.maximumWorkUnits)
        let valueLayout = try DatabaseRetainedArrayLayout.forElement(QueryValue.self)
        let valueGrowth = try valueLayout.growth(from: 0, toFit: layout.slots.count)
        var retainedRows = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter, stage: .projection,
            layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self), expectedCount: rowSet.count
        )
        // The shared layout also owns the empty source's metadata until final
        // output columns receive their independently admitted ownership.
        defer { withExtendedLifetime(layout) {} }
        for index in 0..<rowSet.count {
            try workMeter.consume(at: .projection)
            try rowSet.withRow(at: index) { indexRow in
                let entity: Schema.Entity
                switch selectQuery.source {
                case .table(let table):
                    guard let declared = declarations[table.table] else { throw DatabaseReadSessionError.authorizationMismatch }
                    entity = declared
                default:
                    guard let typeName = indexRow.annotations[PolymorphicRowAnnotation.typeName]?.stringValue,
                          let declared = declarations[typeName] else {
                        throw CanonicalReadError.missingAnnotation(PolymorphicRowAnnotation.typeName)
                    }
                    entity = declared
                }
                guard let fields = admittedFields[entity.name] else { throw DatabaseReadSessionError.authorizationMismatch }
                for name in indexRow.fields.keys where entity.fieldMapByName[name] == nil {
                    throw SchemaDrivenEntityRuntimeError.unknownField(entity: entity.name, field: name)
                }
                func payload(at slot: Int) throws -> FieldValue {
                    let name = layout.slots[slot].name
                    guard let field = entity.fieldMapByName[name] else {
                        guard layout.nullability[slot] else {
                            throw SchemaDrivenEntityRuntimeError.unknownField(entity: entity.name, field: name)
                        }
                        return .null
                    }
                    guard fields.contains(name) else { throw DatabaseReadSessionError.authorizationMismatch }
                    guard let value = indexRow.fields[name] else {
                        guard field.isOptional else {
                            throw SchemaDrivenEntityRuntimeError.missingRequiredField(entity: entity.name, field: name)
                        }
                        return .null
                    }
                    guard field.isOptional || value != .null else {
                        throw SchemaDrivenEntityRuntimeError.nullRequiredField(entity: entity.name, field: name)
                    }
                    return value
                }
                var footprint = try CanonicalRelationalFootprintMeter.queryRowBaseFootprint()
                    .adding(DatabaseIntermediateFootprint(bytes: valueGrowth.additionalByteCount))
                for slot in layout.slots.indices {
                    let value = try payload(at: slot)
                    guard let field = entity.fieldMapByName[layout.slots[slot].name] else { continue }
                    guard let declaredType = layout.types[slot] else { throw QueryValueError.incompatibleType }
                    if let descriptor = field.queryType {
                        guard declaredType == descriptor else { throw QueryValueError.incompatibleType }
                        try QueryFieldStorageAdapter.validateDescriptor(descriptor, fieldType: field.type, isArray: field.isArray)
                        let bounds = try QueryFieldStorageAdapter.measureDecoded(value, type: descriptor)
                        let nodeStride = UInt64(max(MemoryLayout<QueryValue>.stride, MemoryLayout<QueryJSONBMember>.stride) + 32)
                        let nodes = try DatabaseIntermediateFootprint(bytes: nodeStride).multiplied(by: bounds.nodeCount)
                        let elements = try DatabaseIntermediateFootprint(bytes: nodeStride)
                            .multiplied(by: bounds.collectionElementCount).multiplied(by: 2)
                        footprint = try footprint.adding(nodes).adding(elements)
                            .adding(DatabaseIntermediateFootprint(bytes: bounds.minimumJSONBStorageByteCount))
                            .adding(DatabaseIntermediateFootprint(bytes:
                                CanonicalRelationalFootprintMeter.decodedQueryValueStructureAllowance(bounds)))
                        try workMeter.consume(bounds.nodeCount, at: .projection)
                    } else {
                        try QueryFieldStorageAdapter.validateDescriptor(declaredType, fieldType: field.type, isArray: field.isArray)
                        guard QueryValueValidator.accepts(.primitive(value), type: .primitive(field.type, isArray: field.isArray)) else {
                            throw SchemaDrivenEntityRuntimeError.invalidFieldValue(entity: entity.name, field: field.name, expected: field.type)
                        }
                    }
                }
                // Primitive payload, annotations and version share the proven
                // index origin; only destination arrays and rich structure are new.
                let prospective = footprint
                try retainedRows.append(footprint: prospective) {
                    var values: [QueryValue] = []
                    values.reserveCapacity(layout.slots.count)
                    for slot in layout.slots.indices {
                        let persisted = try payload(at: slot)
                        let value: QueryValue
                        if let field = entity.fieldMapByName[layout.slots[slot].name], let descriptor = field.queryType {
                            value = try QueryFieldStorageAdapter.decode(persisted, type: descriptor, limits: limits)
                        } else {
                            value = .primitive(persisted)
                        }
                        guard let type = layout.types[slot], QueryValueValidator.accepts(value, type: type),
                              layout.nullability[slot] || !value.isNull else { throw QueryValueError.incompatibleType }
                        values.append(value)
                    }
                    var retainedPayloadLeases = payloadLeases
                    let actual = try CanonicalRelationalFootprintMeter.decodedRowStructureFootprint(
                        values: values, annotations: indexRow.annotations, version: indexRow.version,
                        payloadLeaseCount: payloadLeases.count + 1, workMeter: workMeter, stage: .projection
                    )
                    if actual.bytes > prospective.bytes {
                        let delta = try workMeter.reserveIntermediate(
                            bytes: actual.bytes - prospective.bytes, at: .projection
                        )
                        retainedPayloadLeases.append(DatabaseQueryPayloadLease(retaining: delta))
                    }
                    return CanonicalSourceRow(values: values, layout: layout, annotations: indexRow.annotations,
                                              version: indexRow.version, payloadLeases: retainedPayloadLeases)
                }
            }
        }
        let canonicalRows = try retainedRows.finish().moveToSharedOwnership(at: .projection)
        let hasExplicitOrder = selectQuery.orderBy?.isEmpty == false
        return try await finalizeRelationalRows(
            selectQuery, sourceRows: canonicalRows, sourceSchema: admittedSchema.retaining(layout: layout),
            residualFilter: selectQuery.filter, residualOrderBy: selectQuery.orderBy,
            sourceRowsAlreadyOrdered: rowSet.ordering == .orderedByIndex && !hasExplicitOrder,
            metadata: rowSet.metadata, metadataReservation: rowSet.retainedMetadataReservation,
            options: options, evaluationContext: evaluationContext
        )
    }

    /// Builds source bindings from the already admitted, immutable row layout.
    private func addressingBoundSource(
        _ relation: CanonicalRelation,
        schema boundSchema: CanonicalRelationSchema,
        workMeter: DatabaseWorkMeter
    ) throws -> CanonicalRelation {
        let actual = relation.schema.layout
        if actual.bindingSlots.count == boundSchema.layout.slots.count,
           actual.bindingSlots.elementsEqual(boundSchema.layout.slots, by: { $0.address == $1 }) {
            return relation
        }
        let footprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
            count: boundSchema.layout.slots.count, element: CanonicalSourceLayout.BindingSlot.self)
            .adding(DatabaseIntermediateCollectionMeter.arrayFootprint(count: actual.slots.count,
                element: Bool.self))
            .adding(DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<CanonicalSourceLayout>.stride) + 128))
        let claim = try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .bindingCandidate)
        var consumed = Array(repeating: false, count: actual.slots.count)
        let bindings = try boundSchema.layout.slots.map { address -> CanonicalSourceLayout.BindingSlot in
            try workMeter.consume(at: .bindingCandidate)
            let index = actual.slots.indices.first { !consumed[$0] && actual.slots[$0] == address }
            if let index { consumed[index] = true }
            return .init(address: address, valueIndex: index)
        }
        let layout = CanonicalSourceLayout(slots: actual.slots, scopes: actual.scopes,
            coalescedColumns: actual.coalescedColumns, reservation: claim, types: actual.types,
            nullability: actual.nullability, visibleSlots: actual.visibleSlots,
            payloadLeases: actual.payloadLeases + [relation.rows.payloadLease],
            frameSizes: actual.frameSizes, frameScopes: actual.frameScopes, bindingSlots: bindings)
        var rows = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(workMeter: workMeter,
            stage: .bindingCandidate, layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: relation.rows.count)
        for row in relation.rows {
            try rows.append(footprint: CanonicalRelationalFootprintMeter.sourceAliasFootprint(of: row,
                workMeter: workMeter), make: {
                    CanonicalSourceRow(values: row.values, layout: layout, annotations: row.annotations,
                        version: row.version, payloadLeases: row.payloadLeases + [relation.rows.payloadLease],
                        sourcePresence: row.sourcePresence)
                })
        }
        return CanonicalRelation(schema: relation.schema.retaining(layout: layout),
            rows: try rows.finish().moveToSharedOwnership(at: .bindingCandidate))
    }

    private func admittedRelationSchema(for layout: CanonicalSourceLayout) throws -> CanonicalRelationSchema {
        guard let claim = layout.reservation else {
            throw CanonicalReadError.unsupportedSource("Source layout has no admitted metadata owner")
        }
        let footprint = try CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
            fieldNames: layout.slots.lazy.map { $0.name }, sourceName: nil,
            workMeter: claim.workMeter, stage: .bindingCandidate
        )
        try claim.reserveAdditional(bytes: footprint.bytes, at: .bindingCandidate)
        let unscoped = layout.slots.indices.filter { layout.slots[$0].scope == nil }
        let scopes = layout.scopes.map { name -> CanonicalRelationScope in
            let selected = layout.slots.indices.filter { layout.slots[$0].scope == name }
            return CanonicalRelationScope(name: name, columns: selected.map { layout.slots[$0].name },
                types: selected.map { layout.types[$0] }, nullability: selected.map { layout.nullability[$0] })
        }
        return try CanonicalRelationSchema(unscopedColumns: unscoped.map { layout.slots[$0].name },
            scopes: scopes, coalescedColumns: layout.coalescedColumns,
            types: unscoped.map { layout.types[$0] }, nullability: unscoped.map { layout.nullability[$0] })
            .retaining(layout: layout)
    }

    private func permitsPrimitivePushdown(
        _ bound: CanonicalBoundExpression,
        original: Expression
    ) throws -> Bool {
        guard CanonicalSQLValueSemantics.permitsStoredKey(bound) else { return false }
        if try bound.loweredExpression == original { return true }
        if case .cast = bound.expression, let child = bound.children.first {
            let identity: Bool
            switch (child.type, bound.type) {
            case (.primitive(.string, false), .sql(.text)),
                 (.primitive(.bool, false), .sql(.boolean)),
                 (.primitive(.int16, false), .sql(.smallint)),
                 (.primitive(.int32, false), .sql(.integer)),
                 (.primitive(.int64, false), .sql(.bigint)),
                 (.primitive(.float32, false), .sql(.real)),
                 (.primitive(.float64, false), .sql(.doublePrecision)),
                 (.primitive(.bytes, false), .sql(.blob)): identity = true
            default: identity = false
            }
            if identity { return try permitsPrimitivePushdown(child, original: original) }
        }
        let operands: (Expression, Expression)
        switch original {
        case .equal(let left, let right), .notEqual(let left, let right),
             .lessThan(let left, let right), .lessThanOrEqual(let left, let right),
             .greaterThan(let left, let right), .greaterThanOrEqual(let left, let right),
             .and(let left, let right), .or(let left, let right): operands = (left, right)
        default: return false
        }
        guard bound.expression == original, bound.children.count == 2 else { return false }
        return try permitsPrimitivePushdown(bound.children[0], original: operands.0)
            && permitsPrimitivePushdown(bound.children[1], original: operands.1)
    }

    /// Converts only checked comparisons whose operands retain the same
    /// primitive carrier and comparator. Typed literals must round-trip exactly;
    /// numeric widening and value-changing CASTs remain canonical residual work.
    private func primitiveBoundPredicate(_ bound: CanonicalBoundExpression) throws -> Expression? {
        func operand(_ value: CanonicalBoundExpression) throws -> Expression? {
            if case .sql(.char) = value.type { return nil }
            if case .column = value.expression, value.columnSlot != nil { return value.expression }
            if case .cast = value.expression, let child = value.children.first,
               try permitsPrimitivePushdown(value, original: child.expression) {
                return try operand(child)
            }
            guard case .literal(let literal) = value.expression else { return nil }
            if case .queryValue(.primitive(let field), _) = literal {
                let converted: Literal
                switch field {
                case .bool, .int8, .int16, .int32, .int64, .uint8, .uint16, .uint32, .uint64,
                     .decimal, .float32, .float64, .string, .bytes, .date, .timestamp, .uuid:
                    converted = try field.toLiteral()
                default: return nil
                }
                guard try converted.toFieldValue() == field else { return nil }
                return .literal(converted)
            }
            guard try value.loweredExpression == value.expression else { return nil }
            return value.expression
        }
        guard CanonicalSQLValueSemantics.permitsStoredKey(bound), bound.children.count == 2 else { return nil }
        switch bound.expression {
        case .and, .or:
            guard let left = try primitiveBoundPredicate(bound.children[0]),
                  let right = try primitiveBoundPredicate(bound.children[1]) else { return nil }
            if case .and = bound.expression { return .and(left, right) }
            return .or(left, right)
        default: break
        }
        guard bound.children[0].type == bound.children[1].type,
              let left = try operand(bound.children[0]), let right = try operand(bound.children[1]) else { return nil }
        switch bound.expression {
        case .equal: return .equal(left, right)
        case .notEqual: return .notEqual(left, right)
        case .lessThan: return .lessThan(left, right)
        case .lessThanOrEqual: return .lessThanOrEqual(left, right)
        case .greaterThan: return .greaterThan(left, right)
        case .greaterThanOrEqual: return .greaterThanOrEqual(left, right)
        default: return nil
        }
    }

    private func executeSingleTableRows(
        _ selectQuery: SelectQuery,
        options: ReadExecutionContext,
        transaction: DatabaseReadTransaction,
        evaluationContext: CanonicalQueryEvaluationContext? = nil,
        admittedListAuthorizationRequirement:
            DatabaseListReadAuthorizationRequirement? = nil
    ) async throws -> CanonicalRetainedQueryResponse {
        guard case .table(let tableRef) = selectQuery.source else {
            throw CanonicalReadError.unsupportedSource("Expected table source")
        }

        let entity = try resolveEntity(named: tableRef.table)
        guard let runtime = try readPolicy().entityRuntime(
            named: entity.name
        ) else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Entity '\(tableRef.table)' has no registered runtime type"
            )
        }

        let sourceName = tableRef.alias ?? tableRef.effectiveName
        let authorizationRequirement = try admittedListAuthorizationRequirement
            ?? DatabaseReadPolicy.listRequirement(
                entityName: entity.name,
                selectQuery: selectQuery, schema: try readPolicy().schema
            )
        guard let boundPlan = evaluationContext?.boundPlan else {
            throw CanonicalReadError.unsupportedSelectQuery("Table planning has no executable SELECT binding")
        }
        let selectedPagination: SQLResolvedPagination?
        let expressionCount: Bool
        switch (boundPlan.pagination.offset, boundPlan.pagination.limit) {
        case (.expression?, _), (_, .expression?): expressionCount = true
        default: expressionCount = false
        }
        let distinctOn: Bool
        if case .distinctOn = selectQuery.quantifier { distinctOn = true } else { distinctOn = false }
        let compositeOutput = boundPlan.columns.contains { column in
            if case .sql(.composite) = column.type { return true }; return false
        }
        if selectQuery.accessPath != nil,
           expressionCount || boundPlan.pagination.withTies || distinctOn || compositeOutput || selectedPlanIsVolatile(boundPlan) {
            guard let evaluationContext else {
                throw CanonicalReadError.unsupportedSelectQuery("Selected index operators require an admitted invocation scope")
            }
            selectedPagination = try await evaluatePagination(boundPlan.pagination, context: evaluationContext)
        } else { selectedPagination = nil }
        let physicalFilter: Expression?
        if let bound = boundPlan.filter {
            if let converted = try primitiveBoundPredicate(bound) { physicalFilter = converted }
            else if let original = selectQuery.filter, try permitsPrimitivePushdown(bound, original: original) {
                physicalFilter = original
            } else { physicalFilter = nil }
        } else { physicalFilter = nil }
        let filterEquivalent = selectQuery.filter == nil || physicalFilter != nil
        let orderEquivalent = try (selectQuery.orderBy ?? []).enumerated().allSatisfy { index, key in
            guard boundPlan.orderBy.indices.contains(index) else { return false }
            return try permitsPrimitivePushdown(boundPlan.orderBy[index].expression, original: key.expression)
        }
        let permitsPushdown = filterEquivalent && orderEquivalent
        // Filter eligibility is independent of ORDER eligibility. A checked
        // index predicate still narrows the source when computed ordering must
        // run afterward; LIMIT/OFFSET wait for both residual stages.
        var physicalQuery = selectQuery.replacing(filter: physicalFilter)
            .replacing(orderBy: orderEquivalent ? selectQuery.orderBy : nil)
        if !permitsPushdown { physicalQuery = physicalQuery.replacing(pagination: QueryPagination()) }
        if let selectedPagination {
            physicalQuery = physicalQuery.replacing(pagination: QueryPagination()).replacing(quantifier: .all)
            if selectedPagination.limit == 0 {
                // Typed planning still validates the explicit path and source metadata, then fetches no candidates.
                physicalQuery = physicalQuery.replacing(filter: nil).replacing(orderBy: nil)
                    .replacing(pagination: QueryPagination(limit: .count(.fixed(0), withTies: false)))
            }
        }
        let pushdown = try await fetchTableSourceRows(
            runtime: runtime,
            sourceName: sourceName,
            selectQuery: physicalQuery,
            authorizationRequirement: authorizationRequirement,
            options: permitsPushdown && selectedPagination == nil ? options : executionContextWithoutExternalPageWindow(options),
            transaction: transaction
        )

        if let selectedPagination, let evaluationContext {
            let actualSchema = try admittedRelationSchema(for: pushdown.sourceLayout)
            let input = CanonicalRelation(schema: actualSchema, rows: pushdown.rows)
            let addressed = try addressingBoundSource(input,
                schema: boundPlan.sourcePlan?.schema ?? actualSchema, workMeter: options.workMeter)
            let result: CanonicalRelation
            if selectedPagination.limit == 0 {
                let empty = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(workMeter: options.workMeter,
                    stage: .projection, layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self), expectedCount: 0)
                result = CanonicalRelation(schema: try outputSchema(boundPlan.columns),
                    rows: try empty.finish().moveToSharedOwnership(at: .projection))
            } else {
                result = try await finalizeSelectedRows(selectQuery, relation: addressed,
                    plan: boundPlan, pagination: selectedPagination, context: evaluationContext)
            }
            let rows = try outputQueryRows(result, columns: boundPlan.columns, meter: options.workMeter,
                metadataLeases: retainedBindingClaims(boundPlan, workMeter: options.workMeter))
            let page = try CanonicalQueryPagination.retainedWindow(rows: rows, selectQuery: selectQuery,
                options: options, rowsAreLogicalQueryRelative: true)
            return CanonicalRetainedQueryResponse(rows: rows, visibleRange: page.range,
                continuation: page.continuation, metadata: [:], affectedRows: nil)
        }

        // When LIMIT/OFFSET are pushed down to the typed fetch, strip them from the
        // pagination input so pagination doesn't re-apply them.
        let paginationQuery: SelectQuery
        if pushdown.limitPushed || pushdown.offsetPushed {
            var modified = selectQuery
            if pushdown.limitPushed { modified = modified.replacing(pagination: QueryPagination(offset: modified.pagination.offset)) }
            if pushdown.offsetPushed { modified = modified.replacing(pagination: QueryPagination(limit: modified.pagination.limit)) }
            paginationQuery = modified
        } else {
            paginationQuery = selectQuery
        }

        return try await finalizeRelationalRows(
            selectQuery,
            sourceRows: pushdown.rows,
            sourceSchema: try admittedRelationSchema(for: pushdown.sourceLayout),
            residualFilter: filterEquivalent ? pushdown.residualFilter : selectQuery.filter,
            residualOrderBy: orderEquivalent ? pushdown.residualOrderBy : selectQuery.orderBy,
            paginationQuery: paginationQuery,
            rowsAreContinuationRelative: pushdown.pageWindowPushed,
            continuationPosition: pushdown.continuationPosition,
            prevalidatedQueryFingerprint:
                pushdown.stableSnapshotQueryFingerprint,
            options: options,
            evaluationContext: evaluationContext
        )
    }

    private func dispatchTableIndexExecutor(
        tableRef: TableRef,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        options: ReadExecutionContext,
        session: DatabaseReadSession
    ) async throws -> IndexReadResult {
        let entity = try resolveEntity(named: tableRef.table)
        guard let index = entity.indexDescriptors.first(
            where: { $0.name == indexScan.indexName }
        ) else {
            throw CanonicalReadError.indexHintNotFound(
                "Index '\(indexScan.indexName)' is not declared by entity '\(entity.name)'"
            )
        }
        guard index.type == indexScan.indexType else {
            throw CanonicalReadError.unsupportedAccessPath(
                "Index '\(index.name)' has type '\(index.type.diagnosticName)', not '\(indexScan.indexType.diagnosticName)'"
            )
        }
        guard let runtime = try readPolicy().entityRuntime(
            named: entity.name
        ) else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Entity '\(tableRef.table)' has no registered runtime type"
            )
        }
        let result = try await runtime.executeIndexRows(
            index: index,
            session: session,
            selectQuery: selectQuery,
            indexScan: indexScan,
            options: options,
            partitions: tableRef.partitions
        )
        guard let result else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Entity '\(entity.name)' has no registered '\(indexScan.indexType.diagnosticName)' index reader"
            )
        }
        return result
    }

    /// Executes physical table planning with the opaque list requirement
    /// admitted by the canonical query boundary. A nested source may
    /// intentionally use an unwindowed physical query, but it must never
    /// authorize that query's synthetic window.
    private func fetchTableSourceRows(
        runtime: EntityRuntimeRegistration,
        sourceName: String,
        selectQuery: SelectQuery,
        authorizationRequirement: DatabaseListReadAuthorizationRequirement,
        options: ReadExecutionContext,
        transaction: DatabaseReadTransaction
    ) async throws -> EntityTableRows {
        try await runtime.fetchTableRows(
            context: self,
            sourceName: sourceName,
            selectQuery: selectQuery,
            authorizationRequirement: authorizationRequirement,
            options: options,
            transaction: transaction
        )
    }

    private func materializeUnwindowedTableSourceRows(
        _ tableRef: TableRef,
        options: ReadExecutionContext,
        transaction: DatabaseReadTransaction,
        authorizationQuery: SelectQuery
    ) async throws -> CanonicalRelation {
        let entity = try resolveEntity(named: tableRef.table)
        guard let runtime = try readPolicy().entityRuntime(
            named: entity.name
        ) else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Entity '\(tableRef.table)' has no registered runtime type"
            )
        }
        let authorizationRequirement = try DatabaseReadPolicy.listRequirement(
            entityName: entity.name,
            selectQuery: authorizationQuery, schema: try readPolicy().schema
        )
        let sourceName = tableRef.alias ?? tableRef.effectiveName
        let select = SelectQuery(
            projection: [.wildcard()],
            source: .table(tableRef)
        )
        let sourceOptions = executionContextWithoutExternalPageWindow(options)
        let rows = try await fetchTableSourceRows(
            runtime: runtime,
            sourceName: sourceName,
            selectQuery: select,
            authorizationRequirement: authorizationRequirement,
            options: sourceOptions,
            transaction: transaction
        )
        guard rows.residualFilter == nil,
              rows.residualOrderBy == nil,
              !rows.limitPushed,
              !rows.offsetPushed,
              !rows.pageWindowPushed else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "A join source unexpectedly applied top-level query pushdown"
            )
        }
        return CanonicalRelation(schema: try admittedRelationSchema(for: rows.sourceLayout),
                                 rows: rows.rows, schemaLayout: rows.sourceLayout)
    }

    private func executionContextWithoutExternalPageWindow(
        _ options: ReadExecutionContext
    ) -> ReadExecutionContext {
        ReadExecutionContext(
            options: options.options.withoutExternalPageWindow(),
            monotonicClock: container.monotonicClock,
            workMeter: options.workMeter,
            queryStructuralLimits: options.queryStructuralLimits,
            temporalEnvironment: options.temporalEnvironment
        )
    }

    func resolveEntity(named name: String) throws -> Schema.Entity {
        guard let entity = container.schema.entity(named: name) else {
            throw CanonicalReadError.unsupportedSource(
                "Entity '\(name)' not found in schema"
            )
        }
        return entity
    }

    private func materializePolymorphicSourceRows(
        _ entities: borrowing DatabaseRetainedPolymorphicEntities,
        group: PolymorphicGroup,
        sourceName: String,
        authorization: DatabaseReadAuthorization,
        stage: DatabaseWorkStage
    ) throws -> CanonicalRelation {
        let workMeter = entities.workMeter
        var footprint = DatabaseIntermediateFootprint(bytes: 128)
        for name in group.memberTypeNames {
            let entity = try resolveEntity(named: name)
            guard let admitted = authorization.admittedFieldNames(forEntityName: name),
                  admitted.allSatisfy({ entity.fieldMapByName[$0] != nil }) else {
                throw DatabaseReadSessionError.authorizationMismatch
            }
            footprint = try footprint.adding(CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
                fieldNames: entity.fields.lazy.filter { admitted.contains($0.name) }.map { $0.name },
                sourceName: sourceName, workMeter: workMeter, stage: stage
            )).adding(DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<Schema.Entity>.stride) + 128))
        }
        let claim = try workMeter.reserveIntermediate(bytes: footprint.bytes, at: stage)
        var declarations: [String: Schema.Entity] = [:]
        var allowed: [String: Set<String>] = [:]
        declarations.reserveCapacity(group.memberTypeNames.count)
        allowed.reserveCapacity(group.memberTypeNames.count)
        var names: [String] = []
        var seen = Set<String>()
        for name in group.memberTypeNames {
            let entity = try resolveEntity(named: name)
            guard let admitted = authorization.admittedFieldNames(forEntityName: name) else {
                throw DatabaseReadSessionError.authorizationMismatch
            }
            declarations[name] = entity
            allowed[name] = admitted
            for field in entity.fields where admitted.contains(field.name) {
                if seen.insert(field.name).inserted { names.append(field.name) }
            }
        }
        let binder = CanonicalQueryTypeBinder(columns: [], resolveColumn: { reference in
            throw DatabaseExpressionEvaluationError.missingColumn(reference.displayName)
        }, wildcardSlots: { _ in [] }, workMeter: workMeter)
        let types = try names.map { name -> QueryColumnType? in
            var candidates: [QueryColumnType] = []
            for member in group.memberTypeNames {
                guard let entity = declarations[member] else { throw DatabaseReadSessionError.authorizationMismatch }
                guard let field = entity.fieldMapByName[name] else { continue }
                guard allowed[member]?.contains(name) == true else { throw DatabaseReadSessionError.authorizationMismatch }
                candidates.append(field.queryType ?? .primitive(field.type, isArray: field.isArray))
            }
            return try binder.commonSourceType(candidates)
        }
        let nullability = names.map { name in
            group.memberTypeNames.contains { declarations[$0]?.fieldMapByName[name]?.isOptional != false }
        }
        let layout = CanonicalSourceLayout(slots: names.map { .init(name: $0, scope: sourceName) },
            scopes: [sourceName], reservation: claim, types: types, nullability: nullability)
        let schema = try admittedRelationSchema(for: layout)
        var sourceRows = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter, stage: stage,
            layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self), expectedCount: entities.count
        )
        for index in 0..<entities.count {
            _ = try entities.appendCanonicalSourceRow(at: index, entities: declarations,
                                                      layout: layout, to: &sourceRows, stage: stage)
        }
        return CanonicalRelation(schema: schema,
            rows: try sourceRows.finish().moveToSharedOwnership(at: stage), schemaLayout: layout)
    }

    private func executePolymorphicRows(
        _ selectQuery: SelectQuery,
        logicalSource: LogicalSourceRef,
        options: ReadExecutionContext,
        transaction: DatabaseReadTransaction,
        evaluationContext: CanonicalQueryEvaluationContext
    ) async throws -> CanonicalRetainedQueryResponse {
        guard let authorization = evaluationContext.fusionSession.transaction.authorization else {
            throw DatabaseReadSessionError.authorizationMismatch
        }
        let group = try container.polymorphicGroup(identifier: logicalSource.identifier)
        let entities = try await evaluationContext.fusionSession
            .scanRetainedPolymorphicItems(
            group: group,
            selectQuery: selectQuery
        )
        let sourceName = logicalSource.effectiveName
        let sourceRows = try materializePolymorphicSourceRows(
            entities,
            group: group,
            sourceName: sourceName,
            authorization: authorization,
            stage: .resultMaterialization
        )

        return try await finalizeRelationalRows(
            selectQuery,
            sourceRows: sourceRows.rows,
            sourceSchema: sourceRows.schema,
            residualFilter: selectQuery.filter,
            residualOrderBy: selectQuery.orderBy,
            options: options,
            evaluationContext: evaluationContext
        )
    }

    private func materializeRows(
        for source: DataSource,
        sourcePlan: CanonicalSourceBindingPlan,
        namedSubqueries: [NamedSubquery],
        queryBinding: CanonicalQueryBindingEnvironment? = nil,
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
        partitionMode: CanonicalPartitionRoutingMode,
        transaction: DatabaseReadTransaction,
        preparedFusionGraph: FusionPreparedQueryGraph,
        fusionSession: DatabaseReadSession,
        authorizationQuery: SelectQuery,
        outerRow: CanonicalSourceRow? = nil,
        allowsOuterReferences: Bool = false,
        cteEnvironment: SQLCTEEnvironment = SQLCTEEnvironment()
    ) async throws -> CanonicalRelation {
        switch source {
        case .table(let tableRef):
            if let named = namedSubqueries.first(where: { $0.name == tableRef.table }) {
                guard tableRef.partitions.isEmpty else {
                    throw CanonicalReadError.invalidPartition(
                        entity: tableRef.table,
                        reason: "common table expressions cannot have storage partitions"
                    )
                }
                let response = try await queryCanonical(
                    named.query,
                    options: options,
                    partitionValues: partitionValues,
                    partitionMode: partitionMode,
                    transaction: transaction,
                    inheritedSubqueries: namedSubqueries, queryBinding: queryBinding,
                    boundPlan: sourcePlan.queryPlan,
                    outerRow: allowsOuterReferences ? outerRow : nil,
                    preparedFusionGraph: preparedFusionGraph,
                    fusionSession: fusionSession
                )
                let alias = tableRef.alias ?? named.name
                return try materializeQueryRelation(
                    response.visibleRows,
                    query: named.query,
                    explicitColumns: named.columns,
                    alias: alias,
                    namedSubqueries: namedSubqueries, queryBinding: queryBinding,
                    workMeter: options.workMeter
                )
            }

            let relation = try await materializeUnwindowedTableSourceRows(
                tableRef, options: options, transaction: transaction, authorizationQuery: authorizationQuery)
            return try addressingBoundSource(relation, schema: sourcePlan.schema, workMeter: options.workMeter)

        case .logical(let logicalSource):
            guard logicalSource.kindIdentifier == LogicalSourceKind.polymorphic else {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "Logical source '\(logicalSource.kindIdentifier)' is not supported"
                )
            }
            guard let authorization = fusionSession.transaction.authorization else {
                throw DatabaseReadSessionError.authorizationMismatch
            }
            let group = try container.polymorphicGroup(identifier: logicalSource.identifier)
            let entities = try await fusionSession
                .scanRetainedPolymorphicItems(
                group: group,
                selectQuery: authorizationQuery
            )
            let sourceName = logicalSource.effectiveName
            let relation = try materializePolymorphicSourceRows(
                entities,
                group: group,
                sourceName: sourceName,
                authorization: authorization,
                stage: .bindingCandidate
            )
            return try addressingBoundSource(relation, schema: sourcePlan.schema, workMeter: options.workMeter)

        case .subquery(let query):
            let response = try await queryCanonical(
                query,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                inheritedSubqueries: namedSubqueries, queryBinding: queryBinding,
                boundPlan: sourcePlan.queryPlan,
                outerRow: allowsOuterReferences ? outerRow : nil,
                preparedFusionGraph: preparedFusionGraph,
                fusionSession: fusionSession
            )
            return try materializeQueryRelation(
                response.visibleRows,
                query: query,
                explicitColumns: nil,
                alias: nil,
                namedSubqueries: namedSubqueries, queryBinding: queryBinding,
                workMeter: options.workMeter
            )

        case .join(let clause):
            let relation = try await materializeJoinRows(
                clause,
                sourcePlan: sourcePlan,
                namedSubqueries: namedSubqueries, queryBinding: queryBinding,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                preparedFusionGraph: preparedFusionGraph,
                fusionSession: fusionSession,
                authorizationQuery: authorizationQuery,
                outerRow: outerRow,
                allowsOuterReferences: allowsOuterReferences
            )
            return try addressingBoundSource(relation, schema: sourcePlan.schema, workMeter: options.workMeter)

        case .setOperation(let operation, let quantifier, let left, let right):
            return try await materializeBinarySet(operation: operation, quantifier: quantifier,
                left: left, right: right, sourcePlan: sourcePlan, namedSubqueries: namedSubqueries,
                queryBinding: queryBinding, options: options, partitionValues: partitionValues,
                partitionMode: partitionMode, transaction: transaction, preparedFusionGraph: preparedFusionGraph,
                fusionSession: fusionSession, outerRow: outerRow, cteEnvironment: cteEnvironment)
        case .aliased(let nested, _, _):
            let relation = try await materializeRows(for: nested, sourcePlan: sourcePlan.children[0],
                namedSubqueries: namedSubqueries, queryBinding: queryBinding, options: options,
                partitionValues: partitionValues, partitionMode: partitionMode, transaction: transaction,
                preparedFusionGraph: preparedFusionGraph, fusionSession: fusionSession,
                authorizationQuery: authorizationQuery, outerRow: outerRow, allowsOuterReferences: allowsOuterReferences)
            return try reframeRelation(relation, to: sourcePlan.schema, workMeter: options.workMeter)

        case .values(let rows):
            let schema = sourcePlan.schema
            let empty = try CanonicalRelationSchema().nullRow()
            let context = CanonicalQueryEvaluationContext(options: options, transaction: transaction,
                partitionValues: partitionValues, partitionMode: partitionMode, namedSubqueries: namedSubqueries,
                queryBinding: queryBinding, outerRow: outerRow, preparedFusionGraph: preparedFusionGraph, fusionSession: fusionSession)
            var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(workMeter: options.workMeter,
                stage: .bindingCandidate, layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self), expectedCount: rows.count)
            for bindings in sourcePlan.valueBindings {
                let row = try await evaluateValuesRow(bindings, schema: schema, on: empty, context: context)
                try retained.append(footprint: CanonicalRelationalFootprintMeter.sourceAliasFootprint(of: row,
                    workMeter: options.workMeter, stage: .bindingCandidate), make: { row })
            }
            return CanonicalRelation(schema: schema, rows: try retained.finish().moveToSharedOwnership(at: .bindingCandidate))

        case .graphTable(let graphTableSource):
            guard let executor = try readPolicy().graphTableSourceExecutor else {
                throw CanonicalReadError.unsupportedSource("GRAPH_TABLE executor is not registered")
            }
            let sourceRows = try await executor.executeInTransaction(
                session: fusionSession, graphTableSource: graphTableSource,
                options: options, partitions: partitionValues ?? FieldObject())
            let rawColumns = sourceRows.columns
            let rawSchema = try CanonicalRelationSchema(unscopedColumns: rawColumns.map { $0.name },
                types: rawColumns.map { $0.type }, nullability: rawColumns.map { $0.isNullable })
            let graphRows = try materializeLogicalSourceRows(consume sourceRows, sourceName: nil,
                workMeter: options.workMeter, stage: .bindingCandidate)
            let projection: [ProjectionItem]
            let outputColumns: [QueryColumn]
            if let columns = graphTableSource.columns, !columns.isEmpty {
                projection = columns.map { ProjectionItem($0.expression, alias: $0.alias) }
                guard let plan = sourcePlan.queryPlan else {
                    throw CanonicalReadError.unsupportedSelectQuery("GRAPH_TABLE projection has no executable binding")
                }
                outputColumns = plan.columns
            } else { projection = [.wildcard()]; outputColumns = rawColumns }
            let projected = try await projectRows(graphRows, projection: projection, columns: outputColumns,
                sourceSchema: rawSchema, workMeter: options.workMeter,
                evaluationContext: CanonicalQueryEvaluationContext(options: options, transaction: transaction,
                    partitionValues: partitionValues, partitionMode: partitionMode, namedSubqueries: namedSubqueries, queryBinding: queryBinding,
                    boundPlan: sourcePlan.queryPlan,
                    outerRow: allowsOuterReferences ? outerRow : nil,
                    preparedFusionGraph: preparedFusionGraph, fusionSession: fusionSession))
            let view = CanonicalRetainedQueryRowView(owner: projected, range: 0..<projected.count)
            return try materializeQueryRelation(view, query: authorizationQuery,
                explicitColumns: nil, alias: nil,
                namedSubqueries: namedSubqueries, queryBinding: queryBinding, workMeter: options.workMeter)

        case .graphPattern:
            guard let executor = try readPolicy().sparqlSourceExecutor else {
                throw CanonicalReadError.unsupportedSource("SPARQL source executor is not registered")
            }
            let sourceSession = try fusionSession.admittingRDFDatasetRead()
            let sourceRows = try await executor.executeInTransaction(
                session: sourceSession,
                selectQuery: SelectQuery(
                    projection: [.wildcard()],
                    source: source
                ),
                options: options,
                partitions: partitionValues ?? FieldObject()
            )
            let declaredColumns = sourceRows.columns
            let rows = try materializeLogicalSourceRows(
                consume sourceRows,
                sourceName: nil,
                workMeter: options.workMeter,
                stage: .bindingCandidate
            )
            return CanonicalRelation(
                schema: try CanonicalRelationSchema(
                    unscopedColumns: declaredColumns.map { $0.name },
                    types: declaredColumns.map { $0.type }, nullability: declaredColumns.map { $0.isNullable }
                ),
                rows: rows
            )

        case .namedGraph:
            guard let executor = try readPolicy().sparqlSourceExecutor else {
                throw CanonicalReadError.unsupportedSource("SPARQL source executor is not registered")
            }
            let sourceSession = try fusionSession.admittingRDFDatasetRead()
            let sourceRows = try await executor.executeInTransaction(
                session: sourceSession,
                selectQuery: SelectQuery(
                    projection: [.wildcard()],
                    source: source
                ),
                options: options,
                partitions: partitionValues ?? FieldObject()
            )
            let declaredColumns = sourceRows.columns
            let rows = try materializeLogicalSourceRows(
                consume sourceRows,
                sourceName: nil,
                workMeter: options.workMeter,
                stage: .bindingCandidate
            )
            return CanonicalRelation(
                schema: try CanonicalRelationSchema(
                    unscopedColumns: declaredColumns.map { $0.name },
                    types: declaredColumns.map { $0.type }, nullability: declaredColumns.map { $0.isNullable }
                ),
                rows: rows
            )

        case .service(let endpoint, _, _):
            throw CanonicalReadError.unsupportedSource(
                "SERVICE source '\(endpoint)' is not supported on the canonical RPC"
            )
        #if DATABASE_MULTI_BASE
        case .base:
            throw CanonicalReadError.unsupportedSource(
                "Base-qualified sources require a Composition planner"
            )
        #endif
        }
    }

    private func materializeJoinRows(
        _ clause: JoinClause,
        sourcePlan: CanonicalSourceBindingPlan,
        namedSubqueries: [NamedSubquery],
        queryBinding: CanonicalQueryBindingEnvironment? = nil,
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
        partitionMode: CanonicalPartitionRoutingMode,
        transaction: DatabaseReadTransaction,
        preparedFusionGraph: FusionPreparedQueryGraph,
        fusionSession: DatabaseReadSession,
        authorizationQuery: SelectQuery,
        outerRow: CanonicalSourceRow?,
        allowsOuterReferences: Bool
    ) async throws -> CanonicalRelation {
        try validateJoinDeclaration(clause)
        if case .using(let columns) = clause.condition,
           columns.isEmpty {
            throw CanonicalReadError.unsupportedSelectQuery(
                "JOIN USING requires at least one column"
            )
        }
        let evaluationContext = CanonicalQueryEvaluationContext(
            options: options,
            transaction: transaction,
            partitionValues: partitionValues,
            partitionMode: partitionMode,
            namedSubqueries: namedSubqueries, queryBinding: queryBinding,
            outerRow: outerRow,
            preparedFusionGraph: preparedFusionGraph,
            fusionSession: fusionSession
        )
        switch clause.type {
        case .lateral, .leftLateral:
            guard !sourceRequiresRuntimeInferredSchema(
                clause.right,
                namedSubqueries: namedSubqueries, queryBinding: queryBinding
            ) else {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "A LATERAL source must declare a stable output schema"
                )
            }
            let left = try await materializeRows(
                for: clause.left, sourcePlan: sourcePlan.children[0],
                namedSubqueries: namedSubqueries, queryBinding: queryBinding,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                preparedFusionGraph: preparedFusionGraph,
                fusionSession: fusionSession,
                authorizationQuery: authorizationQuery,
                outerRow: outerRow,
                allowsOuterReferences: allowsOuterReferences
            )
            let rightSchema = sourcePlan.children[1].schema
            try validateJoinCondition(
                clause.condition,
                leftSchema: left.schema,
                rightSchema: rightSchema,
                type: clause.type
            )
            let outputSchema = try joinOutputSchema(
                left.schema,
                clause.type == .leftLateral ? rightSchema.nullExtended() : rightSchema,
                condition: clause.condition
            )
            var outputRows = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
                workMeter: options.workMeter,
                stage: .joinCandidate,
                layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self)
            )
            for leftRow in left.rows {
                let lateralOuterRows = try retainedConstructedSourceRow(
                    prospectiveFootprint: try Self.prospectiveSourceCompositionFootprint(
                        leftRow,
                        outer: outerRow,
                        workMeter: options.workMeter
                    ),
                    workMeter: options.workMeter,
                    stage: .joinCandidate,
                    make: {
                        leftRow.overlaying(outer: outerRow)
                    }
                )
                let right = try await lateralOuterRows.withElement(at: 0) {
                    lateralOuter in
                    try await materializeRows(
                        for: clause.right, sourcePlan: sourcePlan.children[1],
                        namedSubqueries: namedSubqueries, queryBinding: queryBinding,
                        options: options,
                        partitionValues: partitionValues,
                        partitionMode: partitionMode,
                        transaction: transaction,
                        preparedFusionGraph: preparedFusionGraph,
                        fusionSession: fusionSession,
                        authorizationQuery: authorizationQuery,
                        outerRow: lateralOuter,
                        allowsOuterReferences: true
                    )
                }
                guard right.schema == rightSchema else {
                    throw CanonicalReadError.unsupportedSelectQuery(
                        "A LATERAL source changed its schema between outer rows"
                    )
                }
                let retainedLeftRow = try retainedCanonicalRow(
                    leftRow,
                    workMeter: options.workMeter,
                    stage: .joinCandidate
                )
                let joined = try await performJoin(
                    left: CanonicalRelation(
                        schema: left.schema,
                        rows: retainedLeftRow
                    ),
                    right: right,
                    type: clause.type == .leftLateral ? .left : .inner,
                    condition: clause.condition,
                    boundCondition: sourcePlan.condition,
                    workMeter: options.workMeter,
                    evaluationContext: evaluationContext
                )
                for row in joined.rows {
                    try outputRows.append(
                        footprint: try CanonicalRelationalFootprintMeter
                            .sourceAliasFootprint(
                                of: row,
                                workMeter: options.workMeter
                            ),
                        make: { row.retainingPayload(joined.rows.payloadLease) }
                    )
                }
            }
            return CanonicalRelation(
                schema: outputSchema,
                rows: try outputRows.finish().moveToSharedOwnership(
                    at: .bindingCandidate
                )
            )
        case .natural, .naturalLeft, .naturalRight, .naturalFull:
            let left = try await materializeRows(
                for: clause.left, sourcePlan: sourcePlan.children[0],
                namedSubqueries: namedSubqueries, queryBinding: queryBinding,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                preparedFusionGraph: preparedFusionGraph,
                fusionSession: fusionSession,
                authorizationQuery: authorizationQuery,
                outerRow: outerRow,
                allowsOuterReferences: allowsOuterReferences
            )
            let right = try await materializeRows(
                for: clause.right, sourcePlan: sourcePlan.children[1],
                namedSubqueries: namedSubqueries, queryBinding: queryBinding,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                preparedFusionGraph: preparedFusionGraph,
                fusionSession: fusionSession,
                authorizationQuery: authorizationQuery,
                outerRow: outerRow,
                allowsOuterReferences: allowsOuterReferences
            )
            let columns = inferNaturalJoinColumns(
                leftSchema: left.schema,
                rightSchema: right.schema
            )
            return try await performJoin(
                left: left,
                right: right,
                type: naturalJoinBaseType(clause.type),
                condition: .using(columns),
                boundCondition: nil,
                workMeter: options.workMeter,
                evaluationContext: evaluationContext
            )
        default:
            let left = try await materializeRows(
                for: clause.left, sourcePlan: sourcePlan.children[0],
                namedSubqueries: namedSubqueries, queryBinding: queryBinding,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                preparedFusionGraph: preparedFusionGraph,
                fusionSession: fusionSession,
                authorizationQuery: authorizationQuery,
                outerRow: outerRow,
                allowsOuterReferences: allowsOuterReferences
            )
            let right = try await materializeRows(
                for: clause.right, sourcePlan: sourcePlan.children[1],
                namedSubqueries: namedSubqueries, queryBinding: queryBinding,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                preparedFusionGraph: preparedFusionGraph,
                fusionSession: fusionSession,
                authorizationQuery: authorizationQuery,
                outerRow: outerRow,
                allowsOuterReferences: allowsOuterReferences
            )
            return try await performJoin(
                left: left,
                right: right,
                type: clause.type,
                condition: clause.condition,
                boundCondition: sourcePlan.condition,
                workMeter: options.workMeter,
                evaluationContext: evaluationContext
            )
        }
    }

    private func performJoin(
        left: CanonicalRelation,
        right: CanonicalRelation,
        type: JoinType,
        condition: JoinCondition?,
        boundCondition: CanonicalBoundExpression? = nil,
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext? = nil
    ) async throws -> CanonicalRelation {
        try validateJoinCondition(
            condition,
            leftSchema: left.schema,
            rightSchema: right.schema,
            type: type
        )
        let outputSchema = try joinOutputSchema(
            (type == .right || type == .full) ? left.schema.nullExtended() : left.schema,
            (type == .left || type == .full) ? right.schema.nullExtended() : right.schema,
            condition: condition
        )
        let layoutFootprint = try CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
            fieldNames: outputSchema.layout.slots.lazy.map { $0.name }, sourceName: nil,
            workMeter: workMeter, stage: .joinCandidate)
        let layoutClaim = try workMeter.reserveIntermediate(bytes: layoutFootprint.bytes, at: .joinCandidate)
        let declaredLayout = outputSchema.layout
        let outputLayout = CanonicalSourceLayout(slots: declaredLayout.slots, scopes: declaredLayout.scopes,
            coalescedColumns: declaredLayout.coalescedColumns, reservation: layoutClaim,
            types: outputSchema.slotTypes, nullability: outputSchema.slotNullability,
            visibleSlots: declaredLayout.visibleSlots,
            payloadLeases: [left.rows.payloadLease, right.rows.payloadLease])
        let leftRows = left.rows
        let rightRows = right.rows
        if type == .cross {
            var rows = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
                workMeter: workMeter,
                stage: .joinCandidate,
                layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self)
            )
            for left in leftRows {
                for right in rightRows {
                    try workMeter.consume(at: .joinCandidate)
                    try rows.append(
                        footprint: try prospectiveJoinRowFootprint(
                            left: left,
                            right: right,
                            condition: condition,
                            workMeter: workMeter
                        ),
                        make: {
                            try mergeJoinRows(
                                left,
                                right,
                                condition: condition, layout: outputLayout, workMeter: workMeter,
                                temporalEnvironment: evaluationContext?.options.temporalEnvironment ?? inheritedSQLTemporalEnvironment
                            )
                        }
                    )
                }
            }
            return CanonicalRelation(
                schema: outputSchema.retaining(layout: outputLayout),
                rows: try rows.finish().moveToSharedOwnership(
                    at: .joinCandidate
                )
            )
        }

        let emptyLeft = left.schema.nullRow()
        let emptyRight = right.schema.nullRow()
        if let hashJoined = try await performHashJoin(
            leftRows: leftRows,
            rightRows: rightRows,
            type: type,
            condition: condition,
            boundCondition: boundCondition,
            emptyLeft: emptyLeft,
            emptyRight: emptyRight,
            outputLayout: outputLayout,
            workMeter: workMeter,
            evaluationContext: evaluationContext
        ) {
            return CanonicalRelation(schema: outputSchema.retaining(layout: outputLayout), rows: hashJoined)
        }

        let matchedSetBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(max(1, MemoryLayout<Int>.stride + 16))
        ).multiplied(by: UInt64(rightRows.count)).bytes
        let matchedSetReservation = try workMeter.reserveIntermediate(
            bytes: matchedSetBytes,
            at: .joinCandidate
        )
        defer { matchedSetReservation.release() }
        var matchedRightIndexes = Set<Int>()
        matchedRightIndexes.reserveCapacity(rightRows.count)
        var results = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .joinCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self)
        )

        for leftRow in leftRows {
            var matched = false
            for (rightIndex, rightRow) in rightRows.enumerated() {
                try workMeter.consume(at: .joinCandidate)
                if try await joinMatches(
                    left: leftRow,
                    right: rightRow,
                    condition: condition,
                    boundCondition: boundCondition,
                    joinType: type,
                    evaluationContext: evaluationContext,
                    workMeter: workMeter,
                    usingLayout: outputLayout
                ) {
                    matched = true
                    matchedRightIndexes.insert(rightIndex)
                    try results.append(
                        footprint: try prospectiveJoinRowFootprint(
                            left: leftRow,
                            right: rightRow,
                            condition: condition,
                            workMeter: workMeter
                        ),
                        make: {
                            try mergeJoinRows(
                                leftRow,
                                rightRow,
                                condition: condition, layout: outputLayout, workMeter: workMeter,
                                temporalEnvironment: evaluationContext?.options.temporalEnvironment ?? inheritedSQLTemporalEnvironment
                            )
                        }
                    )
                }
            }

            if !matched, type == .left || type == .full {
                try results.append(
                    footprint: try prospectiveJoinRowFootprint(
                        left: leftRow,
                        right: emptyRight,
                        condition: condition,
                        workMeter: workMeter
                    ),
                    make: {
                        try mergeJoinRows(
                            leftRow,
                            emptyRight,
                            condition: condition, layout: outputLayout, workMeter: workMeter,
                                temporalEnvironment: evaluationContext?.options.temporalEnvironment ?? inheritedSQLTemporalEnvironment
                        )
                    }
                )
            }
        }

        if type == .right || type == .full {
            for (rightIndex, rightRow) in rightRows.enumerated() where !matchedRightIndexes.contains(rightIndex) {
                try results.append(
                    footprint: try prospectiveJoinRowFootprint(
                        left: emptyLeft,
                        right: rightRow,
                        condition: condition,
                        workMeter: workMeter
                    ),
                    make: {
                        try mergeJoinRows(
                            emptyLeft,
                            rightRow,
                            condition: condition, layout: outputLayout, workMeter: workMeter,
                                temporalEnvironment: evaluationContext?.options.temporalEnvironment ?? inheritedSQLTemporalEnvironment
                        )
                    }
                )
            }
        }

        return CanonicalRelation(
            schema: outputSchema.retaining(layout: outputLayout),
            rows: try results.finish().moveToSharedOwnership(
                at: .joinCandidate
            )
        )
    }

    private func validateJoinCondition(
        _ condition: JoinCondition?,
        leftSchema: CanonicalRelationSchema,
        rightSchema: CanonicalRelationSchema,
        type: JoinType
    ) throws {
        guard type != .cross, case .using(let columns) = condition else {
            return
        }
        guard Set(columns).count == columns.count else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "JOIN USING contains duplicate column names"
            )
        }
        for column in columns {
            guard leftSchema.occurrenceCount(of: column) == 1,
                  rightSchema.occurrenceCount(of: column) == 1 else {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "JOIN USING column '\(column)' must resolve exactly once in each input"
                )
            }
        }
    }

    private func validateJoinDeclaration(
        _ clause: JoinClause
    ) throws {
        guard clause.condition != nil else { return }
        switch clause.type {
        case .cross:
            throw CanonicalReadError.unsupportedSelectQuery(
                "CROSS JOIN cannot declare ON or USING"
            )
        case .natural, .naturalLeft, .naturalRight, .naturalFull:
            throw CanonicalReadError.unsupportedSelectQuery(
                "NATURAL JOIN cannot declare ON or USING"
            )
        default:
            return
        }
    }

    private func joinOutputSchema(
        _ left: CanonicalRelationSchema,
        _ right: CanonicalRelationSchema,
        condition: JoinCondition?
    ) throws -> CanonicalRelationSchema {
        if case .using(let columns) = condition {
            return try left.merged(with: right, coalescing: columns)
        }
        return try left.merged(with: right)
    }

    private func mergeJoinRows(
        _ left: CanonicalSourceRow,
        _ right: CanonicalSourceRow,
        condition: JoinCondition?,
        layout: CanonicalSourceLayout,
        workMeter: DatabaseWorkMeter,
        temporalEnvironment: SQLTemporalEnvironment?
    ) throws -> CanonicalSourceRow {
        let coalescing: [String]
        if case .using(let columns) = condition { coalescing = columns } else { coalescing = [] }
        let coalesced = Set(coalescing)
        let conversionClaim = coalescing.isEmpty ? nil : try workMeter.reserveIntermediate(bytes: 128, at: .joinCandidate)
        var conversionLeases: [DatabaseQueryPayloadLease] = []
        var values: [QueryValue] = []
        values.reserveCapacity(layout.slots.count)
        for name in coalescing {
            let reference = ColumnRef(column: name)
            guard let lhs = left.queryValue(for: reference), let rhs = right.queryValue(for: reference) else {
                throw DatabaseExpressionEvaluationError.ambiguousColumn(name)
            }
            guard let type = layout.types[values.count] else {
                throw CanonicalReadError.unsupportedSelectQuery("JOIN USING output type is missing")
            }
            let source = lhs.isNull ? right : left
            guard let slot = source.layout.matching(reference).first, let sourceType = source.layout.types[slot] else {
                throw CanonicalReadError.unsupportedSelectQuery("JOIN USING input type is missing")
            }
            let converted = try CanonicalSQLScalarCast.alignSource(lhs.isNull ? rhs : lhs, from: sourceType, to: type,
                temporalEnvironment: temporalEnvironment, workMeter: workMeter,
                limits: Self.selectedValueLimits(context: nil, meter: workMeter))
            values.append(converted.value)
            if let lease = converted.lease {
                try CanonicalBindingStorage.append(lease, to: &conversionLeases, reservation: conversionClaim)
            }
        }
        for source in [left, right] {
            for index in source.layout.slots.indices
                where source.layout.slots[index].scope == nil && !coalesced.contains(source.layout.slots[index].name) {
                values.append(source.values[index])
            }
        }
        for source in [left, right] {
            for scope in source.layout.scopes {
                for index in source.layout.slots.indices where source.layout.slots[index].scope == scope {
                    values.append(source.values[index])
                }
            }
        }
        guard values.count == layout.slots.count else {
            throw CanonicalReadError.unsupportedSelectQuery("JOIN positional schema and row differ")
        }
        if let conversionClaim {
            try CanonicalBindingStorage.append(DatabaseQueryPayloadLease(retaining: conversionClaim),
                to: &conversionLeases, reservation: conversionClaim)
        }
        return CanonicalSourceRow(values: values, layout: layout,
            annotations: left.annotations.merging(right.annotations) { current, _ in current },
            payloadLeases: left.payloadLeases + right.payloadLeases + conversionLeases,
            sourcePresence: left.sourcePresence + right.sourcePresence)
    }

    private enum CanonicalJoinKeySource: Hashable {
        case column(ColumnRef)
        case unqualified(String)
    }

    private struct CanonicalHashJoinPlan {
        let left: [CanonicalJoinKeySource]
        let right: [CanonicalJoinKeySource]
        let validatesFullCondition: Bool
    }

    private struct CanonicalJoinKey: Hashable {
        let values: [CanonicalSQLValueIdentity]
        let reservation: DatabaseIntermediateReservation
        static func == (left: Self, right: Self) -> Bool { left.values == right.values }
        func hash(into hasher: inout Hasher) { hasher.combine(values) }
    }

    private func performHashJoin(
        leftRows: CanonicalRetainedRows,
        rightRows: CanonicalRetainedRows,
        type: JoinType,
        condition: JoinCondition?,
        boundCondition: CanonicalBoundExpression? = nil,
        emptyLeft: CanonicalSourceRow,
        emptyRight: CanonicalSourceRow,
        outputLayout: CanonicalSourceLayout,
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedRows? {
        if case .on(let expression) = condition {
            guard let boundCondition, try boundCondition.loweredExpression == expression else { return nil }
        }
        guard let plan = canonicalHashJoinPlan(
            condition: condition,
            leftRows: leftRows,
            rightRows: rightRows
        ) else {
            return nil
        }

        if let left = leftRows.first, let right = rightRows.first {
            func declaration(_ source: CanonicalJoinKeySource, row: CanonicalSourceRow) -> QueryColumnType? {
                let column: ColumnRef
                switch source {
                case .column(let reference): column = reference
                case .unqualified(let name): column = ColumnRef(name)
                }
                return row.layout.matching(column).first.flatMap { row.layout.types[$0] }
            }
            for (leftSource, rightSource) in zip(plan.left, plan.right) {
                let leftType = declaration(leftSource, row: left), rightType = declaration(rightSource, row: right)
                if Self.selectedCollectionType(leftType) || Self.selectedCollectionType(rightType) { return nil }
                if CanonicalSQLValueSemantics.isTemporal(leftType) || CanonicalSQLValueSemantics.isTemporal(rightType) {
                    guard leftType.flatMap(CanonicalQueryTypeBinder.temporalType) == rightType.flatMap(CanonicalQueryTypeBinder.temporalType) else { return nil }
                }
            }
        }

        let keySlotBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(max(1, MemoryLayout<CanonicalSQLValueIdentity?>.stride + 16))
        ).multiplied(by: UInt64(plan.left.count)).bytes
        let hashEntryBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(
                max(1, MemoryLayout<CanonicalSourceRow>.stride + 64)
            )
        ).adding(
            DatabaseIntermediateFootprint(bytes: keySlotBytes)
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: UInt64(max(1, MemoryLayout<Int>.stride + 16))
            )
        ).bytes
        let hashBytes = try DatabaseIntermediateFootprint(
            bytes: hashEntryBytes
        ).multiplied(by: UInt64(rightRows.count)).bytes
        let hashReservation = try workMeter.reserveIntermediate(
            rows: UInt64(rightRows.count),
            bytes: hashBytes,
            at: .joinCandidate
        )
        defer { hashReservation.release() }
        var buckets: [CanonicalJoinKey: [(Int, CanonicalSourceRow)]] = [:]
        buckets.reserveCapacity(rightRows.count)
        for (index, row) in rightRows.enumerated() {
            try workMeter.consume(at: .joinCandidate)
            guard let key = try canonicalJoinKey(
                sources: plan.right,
                row: row, workMeter: workMeter
            ) else { continue }
            buckets[key, default: []].append((index, row))
        }

        let matchedSetBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(max(1, MemoryLayout<Int>.stride + 16))
        ).multiplied(by: UInt64(rightRows.count)).bytes
        let matchedSetReservation = try workMeter.reserveIntermediate(
            bytes: matchedSetBytes,
            at: .joinCandidate
        )
        defer { matchedSetReservation.release() }
        var matchedRight = Set<Int>()
        matchedRight.reserveCapacity(rightRows.count)
        var results = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .joinCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self)
        )
        for left in leftRows {
            try workMeter.consume(at: .joinCandidate)
            let matches: [(Int, CanonicalSourceRow)]
            if let key = try canonicalJoinKey(sources: plan.left, row: left, workMeter: workMeter) {
                matches = buckets[key] ?? []
            } else {
                matches = []
            }
            var matchedLeft = false
            for (rightIndex, right) in matches {
                try workMeter.consume(at: .joinCandidate)
                if plan.validatesFullCondition,
                   try await joinMatches(
                       left: left,
                       right: right,
                       condition: condition,
                       boundCondition: boundCondition,
                       joinType: type,
                       evaluationContext: evaluationContext,
                       workMeter: workMeter,
                       usingLayout: outputLayout
                   ) == false {
                    continue
                }
                matchedLeft = true
                matchedRight.insert(rightIndex)
                try results.append(
                    footprint: try prospectiveJoinRowFootprint(
                        left: left,
                        right: right,
                        condition: condition,
                        workMeter: workMeter
                    ),
                    make: {
                        try mergeJoinRows(
                            left,
                            right,
                            condition: condition, layout: outputLayout, workMeter: workMeter,
                                temporalEnvironment: evaluationContext?.options.temporalEnvironment ?? inheritedSQLTemporalEnvironment
                        )
                    }
                )
            }
            if !matchedLeft, type == .left || type == .full {
                try results.append(
                    footprint: try prospectiveJoinRowFootprint(
                        left: left,
                        right: emptyRight,
                        condition: condition,
                        workMeter: workMeter
                    ),
                    make: {
                        try mergeJoinRows(
                            left,
                            emptyRight,
                            condition: condition, layout: outputLayout, workMeter: workMeter,
                                temporalEnvironment: evaluationContext?.options.temporalEnvironment ?? inheritedSQLTemporalEnvironment
                        )
                    }
                )
            }
        }
        if type == .right || type == .full {
            for (index, right) in rightRows.enumerated()
                where !matchedRight.contains(index) {
                try results.append(
                    footprint: try prospectiveJoinRowFootprint(
                        left: emptyLeft,
                        right: right,
                        condition: condition,
                        workMeter: workMeter
                    ),
                    make: {
                        try mergeJoinRows(
                            emptyLeft,
                            right,
                            condition: condition, layout: outputLayout, workMeter: workMeter,
                                temporalEnvironment: evaluationContext?.options.temporalEnvironment ?? inheritedSQLTemporalEnvironment
                        )
                    }
                )
            }
        }
        return try results.finish().moveToSharedOwnership(
            at: .joinCandidate
        )
    }

    private func canonicalHashJoinPlan(
        condition: JoinCondition?,
        leftRows: CanonicalRetainedRows,
        rightRows: CanonicalRetainedRows
    ) -> CanonicalHashJoinPlan? {
        switch condition {
        case .using(let columns) where !columns.isEmpty:
            let sources = columns.map(CanonicalJoinKeySource.unqualified)
            return CanonicalHashJoinPlan(
                left: sources,
                right: sources,
                validatesFullCondition: false
            )
        case .on(let expression):
            var leftIterator = leftRows.makeIterator()
            var rightIterator = rightRows.makeIterator()
            guard let leftSample = leftIterator.next(),
                  let rightSample = rightIterator.next() else {
                return nil
            }
            var pairs: [(ColumnRef, ColumnRef)] = []
            collectHashJoinColumnPairs(
                from: expression,
                leftSample: leftSample,
                rightSample: rightSample,
                into: &pairs
            )
            guard !pairs.isEmpty else { return nil }
            return CanonicalHashJoinPlan(
                left: pairs.map { .column($0.0) },
                right: pairs.map { .column($0.1) },
                validatesFullCondition: true
            )
        default:
            return nil
        }
    }

    private func collectHashJoinColumnPairs(
        from expression: Expression,
        leftSample: CanonicalSourceRow,
        rightSample: CanonicalSourceRow,
        into pairs: inout [(ColumnRef, ColumnRef)]
    ) {
        switch expression {
        case .equal(.column(let lhs), .column(let rhs)):
            let forward = leftSample.value(for: lhs) != nil
                && rightSample.value(for: rhs) != nil
            let reverse = leftSample.value(for: rhs) != nil
                && rightSample.value(for: lhs) != nil
            if forward != reverse {
                pairs.append(forward ? (lhs, rhs) : (rhs, lhs))
            }
        case .and(let lhs, let rhs):
            collectHashJoinColumnPairs(
                from: lhs,
                leftSample: leftSample,
                rightSample: rightSample,
                into: &pairs
            )
            collectHashJoinColumnPairs(
                from: rhs,
                leftSample: leftSample,
                rightSample: rightSample,
                into: &pairs
            )
        default:
            break
        }
    }

    private func canonicalJoinKey(
        sources: [CanonicalJoinKeySource], row: CanonicalSourceRow, workMeter: DatabaseWorkMeter
    ) throws -> CanonicalJoinKey? {
        let footprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(count: sources.count, element: CanonicalSQLValueIdentity.self)
        let reservation = try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .joinCandidate)
        var values: [CanonicalSQLValueIdentity] = []
        values.reserveCapacity(sources.count)
        for source in sources {
            let column: ColumnRef
            switch source {
            case .column(let reference): column = reference
            case .unqualified(let name): column = ColumnRef(name)
            }
            let indices = row.layout.matching(column)
            guard let slot = indices.first, !row.values[slot].isNull else { return nil }
            let value = row.values[slot]
            let payload = try CanonicalRelationalFootprintMeter.valueFootprint(of: value,
                workMeter: workMeter, stage: .joinCandidate)
            try reservation.reserveAdditional(bytes: payload.bytes, at: .joinCandidate)
            values.append(try canonicalQueryValueIdentity(value, operation: "hash JOIN", type: row.layout.types[slot], workMeter: workMeter))
        }
        return CanonicalJoinKey(values: values, reservation: reservation)
    }

    private func joinMatches(
        left: CanonicalSourceRow,
        right: CanonicalSourceRow,
        condition: JoinCondition?,
        boundCondition: CanonicalBoundExpression? = nil,
        joinType: JoinType,
        evaluationContext: CanonicalQueryEvaluationContext?,
        workMeter: DatabaseWorkMeter,
        usingLayout: CanonicalSourceLayout
    ) async throws -> Bool {
        if joinType == .cross {
            return true
        }

        guard let condition else { return true }
        switch condition {
        case .using(let columns):
            for (index, column) in columns.enumerated() {
                guard let leftSlot = left.layout.matching(ColumnRef(column)).first,
                      let rightSlot = right.layout.matching(ColumnRef(column)).first else { return false }
                guard let leftType = left.layout.types[leftSlot], let rightType = right.layout.types[rightSlot],
                      usingLayout.types.indices.contains(index), let selectedType = usingLayout.types[index] else {
                    throw CanonicalReadError.unsupportedSelectQuery("JOIN USING has no selected key type")
                }
                let environment = evaluationContext?.options.temporalEnvironment ?? inheritedSQLTemporalEnvironment
                let lhs = try CanonicalSQLScalarCast.alignSource(left.values[leftSlot], from: leftType, to: selectedType,
                    temporalEnvironment: environment, workMeter: workMeter,
                    limits: Self.selectedValueLimits(context: evaluationContext, meter: workMeter))
                let rhs = try CanonicalSQLScalarCast.alignSource(right.values[rightSlot], from: rightType, to: selectedType,
                    temporalEnvironment: environment, workMeter: workMeter,
                    limits: Self.selectedValueLimits(context: evaluationContext, meter: workMeter))
                defer { withExtendedLifetime(lhs) {}; withExtendedLifetime(rhs) {} }
                if try Self.selectedComparison(lhs.value, rhs.value, comparison: .equal,
                    leftType: selectedType, rightType: selectedType, temporalEnvironment: environment,
                    workMeter: workMeter) != true { return false }
            }
            return true
        case .on(let expression):
            guard let boundCondition else {
                throw CanonicalReadError.unsupportedSelectQuery("JOIN ON has no executable binding")
            }
            let mergedRows = try retainedConstructedSourceRow(
                prospectiveFootprint: try prospectiveJoinRowFootprint(
                    left: left,
                    right: right,
                    condition: .on(expression),
                    workMeter: workMeter
                ),
                workMeter: workMeter,
                stage: .joinCandidate,
                make: { try left.merged(with: right) }
            )
            return try await mergedRows.withElement(at: 0) { merged in
                try await evaluateBoundBoolean(
                    boundCondition,
                    on: merged,
                    context: evaluationContext,
                    workMeter: workMeter
                )
            }
        }
    }

    private func inferNaturalJoinColumns(
        leftSchema: CanonicalRelationSchema,
        rightSchema: CanonicalRelationSchema
    ) -> [String] {
        let rightNames = Set(rightSchema.visibleColumns)
        return leftSchema.visibleColumns.filter { rightNames.contains($0) }
    }

    private func naturalJoinBaseType(_ type: JoinType) -> JoinType {
        switch type {
        case .naturalLeft:
            return .left
        case .naturalRight:
            return .right
        case .naturalFull:
            return .full
        case .natural:
            return .inner
        default:
            return type
        }
    }

    private func firstScopedFieldValue(
        named column: String,
        in row: CanonicalSourceRow
    ) -> FieldValue? {
        if let value = row.unscopedFields[column] {
            return value
        }
        for fields in row.scopedFields.values {
            if let value = fields[column] {
                return value
            }
        }
        return nil
    }

    private func materializeBinarySet(operation: SetOperator, quantifier: SetQuantifier,
        left: SelectQuery, right: SelectQuery, sourcePlan: CanonicalSourceBindingPlan,
        namedSubqueries: [NamedSubquery], queryBinding: CanonicalQueryBindingEnvironment?,
        options: ReadExecutionContext, partitionValues: FieldObject?, partitionMode: CanonicalPartitionRoutingMode,
        transaction: DatabaseReadTransaction, preparedFusionGraph: FusionPreparedQueryGraph,
        fusionSession: DatabaseReadSession, outerRow: CanonicalSourceRow?,
        cteEnvironment: SQLCTEEnvironment = SQLCTEEnvironment()
    ) async throws -> CanonicalRelation {
        let meter = options.workMeter
        guard let leftPlan = sourcePlan.children[0].queryPlan, let rightPlan = sourcePlan.children[1].queryPlan else {
            throw CanonicalReadError.unsupportedSelectQuery("A set operand has no executable query binding")
        }
        func operand(_ query: SelectQuery, _ plan: CanonicalBoundSelect) async throws -> CanonicalRelation {
            let context = CanonicalQueryEvaluationContext(options: executionContextWithoutExternalPageWindow(options),
                transaction: transaction, partitionValues: partitionValues, partitionMode: partitionMode,
                namedSubqueries: namedSubqueries, queryBinding: queryBinding, boundPlan: plan,
                cteEnvironment: cteEnvironment, outerRow: outerRow,
                preparedFusionGraph: preparedFusionGraph, fusionSession: fusionSession)
            return try await drainSQLProducer(makeSelectProducer(query, plan: plan, context: context), meter: meter)
        }
        let lhs = try await operand(left, leftPlan)
        let rhs = try await operand(right, rightPlan)
        let layout = try admittedSetOperationLayout(sourcePlan.schema, retaining: lhs.rows.payloadLease,
            and: rhs.rows.payloadLease, workMeter: meter)
        let environment = options.temporalEnvironment ?? fusionSession.temporalEnvironment
        let leftRows = try alignSetOperationRows(lhs, to: sourcePlan.schema, layout: layout,
            workMeter: meter, temporalEnvironment: environment)
        let rightRows = try alignSetOperationRows(rhs, to: sourcePlan.schema, layout: layout,
            workMeter: meter, temporalEnvironment: environment)
        let rightKeyCount = operation == .union ? 0 : rightRows.count
        let state = try meter.reserveIntermediate(bytes: UInt64(rightKeyCount) * 256 + 128, at: .deduplication)
        defer { state.release() }
        var keys: [(key: CanonicalRowValueIdentity, count: Int)] = []
        var buckets: [Int: [Int]] = [:]
        keys.reserveCapacity(rightKeyCount); buckets.reserveCapacity(rightKeyCount)
        if operation != .union {
            for row in rightRows {
                let key = try identityRow(row, operation: "set operation", workMeter: meter)
                let hash = try CanonicalSQLValueIdentity.hash(key.values, workMeter: meter, stage: .deduplication)
                var matching: Int?
                for index in buckets[hash] ?? [] {
                    if try CanonicalSQLValueIdentity.equal(keys[index].key.values, key.values, workMeter: meter, stage: .deduplication) { matching = index; break }
                }
                if let matching { keys[matching].count += 1 }
                else { buckets[hash, default: []].append(keys.count); keys.append((key, 1)) }
            }
        }
        var output = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(workMeter: meter,
            stage: .deduplication, layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self))
        var seen: CanonicalSQLIdentitySet?
        if quantifier == .distinct { seen = try CanonicalSQLIdentitySet(workMeter: meter, stage: .deduplication) }
        for row in leftRows {
            try meter.consume(at: .deduplication)
            if operation == .union && quantifier == .all {
                try output.append(footprint: CanonicalRelationalFootprintMeter.sourceAliasFootprint(of: row,
                    workMeter: meter, stage: .deduplication), make: { row.retainingPayload(leftRows.payloadLease) })
                continue
            }
            let key = try identityRow(row, operation: "set operation", workMeter: meter)
            var include = operation == .union
            if operation != .union {
                let hash = try CanonicalSQLValueIdentity.hash(key.values, workMeter: meter, stage: .deduplication)
                var matching: Int?
                for index in buckets[hash] ?? [] {
                    if try CanonicalSQLValueIdentity.equal(keys[index].key.values, key.values, workMeter: meter, stage: .deduplication) { matching = index; break }
                }
                let available = matching.map { keys[$0].count > 0 } ?? false
                if available, quantifier == .all, let matching { keys[matching].count -= 1 }
                include = operation == .intersect ? available : !available
            }
            if include, let unique = try seen?.insert(key.values) { include = unique }
            if include {
                try output.append(footprint: CanonicalRelationalFootprintMeter.sourceAliasFootprint(of: row,
                    workMeter: meter, stage: .deduplication), make: { row.retainingPayload(leftRows.payloadLease) })
            }
        }
        if operation == .union {
            for row in rightRows {
                let include = try quantifier == .all || seen?.insert(identityRow(row, operation: "UNION", workMeter: meter).values) == true
                if include {
                    try output.append(footprint: CanonicalRelationalFootprintMeter.sourceAliasFootprint(of: row,
                        workMeter: meter, stage: .deduplication), make: { row.retainingPayload(rightRows.payloadLease) })
                }
            }
        }
        return CanonicalRelation(schema: sourcePlan.schema.retaining(layout: layout),
            rows: try output.finish().moveToSharedOwnership(at: .deduplication))
    }

    private func materializeUnionRows(
        _ sources: [DataSource],
        deduplicate: Bool,
        sourcePlan: CanonicalSourceBindingPlan,
        namedSubqueries: [NamedSubquery],
        queryBinding: CanonicalQueryBindingEnvironment? = nil,
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
        partitionMode: CanonicalPartitionRoutingMode,
        transaction: DatabaseReadTransaction,
        preparedFusionGraph: FusionPreparedQueryGraph,
        fusionSession: DatabaseReadSession,
        authorizationQuery: SelectQuery,
        outerRow: CanonicalSourceRow? = nil,
        allowsOuterReferences: Bool = false
    ) async throws -> CanonicalRelation {
        guard let firstSource = sources.first else {
            return CanonicalRelation(
                schema: try CanonicalRelationSchema(),
                rows: try emptyCanonicalRows(
                    workMeter: options.workMeter,
                    stage: .bindingCandidate
                )
            )
        }
        let outputSchema = sourcePlan.schema
        let first = try await materializeRows(
            for: firstSource, sourcePlan: sourcePlan.children[0],
            namedSubqueries: namedSubqueries, queryBinding: queryBinding,
            options: options,
            partitionValues: partitionValues,
            partitionMode: partitionMode,
            transaction: transaction,
            preparedFusionGraph: preparedFusionGraph,
            fusionSession: fusionSession,
            authorizationQuery: authorizationQuery,
            outerRow: outerRow,
            allowsOuterReferences: allowsOuterReferences
        )
        let outputLayout = try admittedSetOperationLayout(
            outputSchema, retaining: first.rows.payloadLease, workMeter: options.workMeter
        )
        var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: options.workMeter,
            stage: .bindingCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self)
        )
        try appendAlignedSetOperationRows(
            first,
            to: outputSchema,
            layout: outputLayout,
            into: &retained,
            workMeter: options.workMeter
        )
        for (offset, source) in sources.dropFirst().enumerated() {
            let relation = try await materializeRows(
                for: source, sourcePlan: sourcePlan.children[offset + 1],
                namedSubqueries: namedSubqueries, queryBinding: queryBinding,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                preparedFusionGraph: preparedFusionGraph,
                fusionSession: fusionSession,
                authorizationQuery: authorizationQuery,
                outerRow: outerRow,
                allowsOuterReferences: allowsOuterReferences
            )
            try appendAlignedSetOperationRows(
                relation,
                to: outputSchema,
                layout: outputLayout,
                into: &retained,
                workMeter: options.workMeter
            )
        }
        let rows = try retained.finish().moveToSharedOwnership(
            at: .bindingCandidate
        )
        if deduplicate {
            return CanonicalRelation(
                schema: outputSchema,
                rows: try uniqueSourceRows(rows, workMeter: options.workMeter),
                schemaLayout: outputLayout
            )
        }
        return CanonicalRelation(schema: outputSchema, rows: rows, schemaLayout: outputLayout)
    }

    private func materializeIntersectRows(
        _ sources: [DataSource],
        sourcePlan: CanonicalSourceBindingPlan,
        namedSubqueries: [NamedSubquery],
        queryBinding: CanonicalQueryBindingEnvironment? = nil,
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
        partitionMode: CanonicalPartitionRoutingMode,
        transaction: DatabaseReadTransaction,
        preparedFusionGraph: FusionPreparedQueryGraph,
        fusionSession: DatabaseReadSession,
        authorizationQuery: SelectQuery,
        outerRow: CanonicalSourceRow? = nil,
        allowsOuterReferences: Bool = false
    ) async throws -> CanonicalRelation {
        guard let first = sources.first else {
            return CanonicalRelation(
                schema: try CanonicalRelationSchema(),
                rows: try emptyCanonicalRows(
                    workMeter: options.workMeter,
                    stage: .bindingCandidate
                )
            )
        }
        let outputSchema = sourcePlan.schema
        let firstRelation = try await materializeRows(
            for: first, sourcePlan: sourcePlan.children[0],
            namedSubqueries: namedSubqueries, queryBinding: queryBinding,
            options: options,
            partitionValues: partitionValues,
            partitionMode: partitionMode,
            transaction: transaction,
            preparedFusionGraph: preparedFusionGraph,
            fusionSession: fusionSession,
            authorizationQuery: authorizationQuery,
            outerRow: outerRow,
            allowsOuterReferences: allowsOuterReferences
        )
        let outputLayout = try admittedSetOperationLayout(
            outputSchema, retaining: firstRelation.rows.payloadLease, workMeter: options.workMeter
        )
        var accumulator = try alignSetOperationRows(
            firstRelation,
            to: outputSchema,
            layout: outputLayout,
            workMeter: options.workMeter
        )
        for (offset, source) in sources.dropFirst().enumerated() {
            let nextRelation = try await materializeRows(
                for: source, sourcePlan: sourcePlan.children[offset + 1],
                namedSubqueries: namedSubqueries, queryBinding: queryBinding,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                preparedFusionGraph: preparedFusionGraph,
                fusionSession: fusionSession,
                authorizationQuery: authorizationQuery,
                outerRow: outerRow,
                allowsOuterReferences: allowsOuterReferences
            )
            let next = try alignSetOperationRows(
                nextRelation,
                to: outputSchema,
                layout: outputLayout,
                workMeter: options.workMeter
            )
            let setBytes = try DatabaseIntermediateFootprint(
                bytes: UInt64(
                    max(1, MemoryLayout<CanonicalRowValueIdentity>.stride + 32)
                )
            ).multiplied(by: UInt64(next.count)).bytes
            let setReservation = try options.workMeter.reserveIntermediate(
                rows: UInt64(next.count),
                bytes: setBytes,
                at: .deduplication
            )
            defer { setReservation.release() }
            var nextKeys = try CanonicalSQLIdentitySet(workMeter: options.workMeter,
                stage: .deduplication, expectedCount: next.count)
            for row in next {
                try options.workMeter.consume(at: .deduplication)
                let key = try identityRow(row, operation: "INTERSECT", workMeter: options.workMeter)
                _ = try nextKeys.insert(key.values)
            }
            var intersected = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
                workMeter: options.workMeter,
                stage: .joinCandidate,
                layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
                expectedCount: accumulator.count
            )
            for row in accumulator {
                try options.workMeter.consume(at: .joinCandidate)
                guard try nextKeys.contains(
                    identityRow(row, operation: "INTERSECT", workMeter: options.workMeter).values
                ) else { continue }
                try intersected.append(
                    footprint: try CanonicalRelationalFootprintMeter.sourceAliasFootprint(
                        of: row,
                        workMeter: options.workMeter
                    ),
                    make: { row.retainingPayload(accumulator.payloadLease) }
                )
            }
            accumulator = try intersected.finish().moveToSharedOwnership(
                at: .joinCandidate
            )
        }
        return CanonicalRelation(
            schema: outputSchema,
            rows: try uniqueSourceRows(
                accumulator,
                workMeter: options.workMeter
            ),
            schemaLayout: outputLayout
        )
    }

    private func materializeExceptRows(
        _ lhs: DataSource,
        _ rhs: DataSource,
        sourcePlan: CanonicalSourceBindingPlan,
        namedSubqueries: [NamedSubquery],
        queryBinding: CanonicalQueryBindingEnvironment? = nil,
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
        partitionMode: CanonicalPartitionRoutingMode,
        transaction: DatabaseReadTransaction,
        preparedFusionGraph: FusionPreparedQueryGraph,
        fusionSession: DatabaseReadSession,
        authorizationQuery: SelectQuery,
        outerRow: CanonicalSourceRow? = nil,
        allowsOuterReferences: Bool = false
    ) async throws -> CanonicalRelation {
        let outputSchema = sourcePlan.schema
        let left = try await materializeRows(
            for: lhs, sourcePlan: sourcePlan.children[0],
            namedSubqueries: namedSubqueries, queryBinding: queryBinding,
            options: options,
            partitionValues: partitionValues,
            partitionMode: partitionMode,
            transaction: transaction,
            preparedFusionGraph: preparedFusionGraph,
            fusionSession: fusionSession,
            authorizationQuery: authorizationQuery,
            outerRow: outerRow,
            allowsOuterReferences: allowsOuterReferences
        )
        let right = try await materializeRows(
            for: rhs, sourcePlan: sourcePlan.children[1],
            namedSubqueries: namedSubqueries, queryBinding: queryBinding,
            options: options,
            partitionValues: partitionValues,
            partitionMode: partitionMode,
            transaction: transaction,
            preparedFusionGraph: preparedFusionGraph,
            fusionSession: fusionSession,
            authorizationQuery: authorizationQuery,
            outerRow: outerRow,
            allowsOuterReferences: allowsOuterReferences
        )
        let outputLayout = try admittedSetOperationLayout(
            outputSchema, retaining: left.rows.payloadLease,
            and: right.rows.payloadLease, workMeter: options.workMeter
        )
        let leftRows = try alignSetOperationRows(
            left,
            to: outputSchema,
            layout: outputLayout,
            workMeter: options.workMeter
        )
        let rightRows = try alignSetOperationRows(
            right,
            to: outputSchema,
            layout: outputLayout,
            workMeter: options.workMeter
        )
        let setBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(
                max(1, MemoryLayout<CanonicalRowValueIdentity>.stride + 32)
            )
        ).multiplied(by: UInt64(rightRows.count)).bytes
        let setReservation = try options.workMeter.reserveIntermediate(
            rows: UInt64(rightRows.count),
            bytes: setBytes,
            at: .deduplication
        )
        defer { setReservation.release() }
        var rightKeys = try CanonicalSQLIdentitySet(workMeter: options.workMeter,
            stage: .deduplication, expectedCount: rightRows.count)
        for row in rightRows {
            try options.workMeter.consume(at: .deduplication)
            let key = try identityRow(row, operation: "EXCEPT", workMeter: options.workMeter)
            _ = try rightKeys.insert(key.values)
        }
        var difference = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: options.workMeter,
            stage: .joinCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: leftRows.count
        )
        for row in leftRows {
            try options.workMeter.consume(at: .joinCandidate)
            guard try !rightKeys.contains(
                identityRow(row, operation: "EXCEPT", workMeter: options.workMeter).values
            ) else { continue }
            try difference.append(
                footprint: try CanonicalRelationalFootprintMeter.sourceAliasFootprint(
                    of: row,
                    workMeter: options.workMeter
                ),
                make: { row.retainingPayload(leftRows.payloadLease) }
            )
        }
        return CanonicalRelation(
            schema: outputSchema,
            rows: try uniqueSourceRows(
                try difference.finish().moveToSharedOwnership(
                    at: .joinCandidate
                ),
                workMeter: options.workMeter
            ),
            schemaLayout: outputLayout
        )
    }

    private func uniqueSourceRows(
        _ rows: CanonicalRetainedRows,
        workMeter: DatabaseWorkMeter
    ) throws -> CanonicalRetainedRows {
        let setBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(
                max(1, MemoryLayout<CanonicalRowValueIdentity>.stride + 32)
            )
        ).multiplied(by: UInt64(rows.count)).bytes
        let setReservation = try workMeter.reserveIntermediate(
            rows: UInt64(rows.count),
            bytes: setBytes,
            at: .deduplication
        )
        defer { setReservation.release() }
        var seen = try CanonicalSQLIdentitySet(workMeter: workMeter,
            stage: .deduplication, expectedCount: rows.count)
        var unique = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .deduplication,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: rows.count
        )
        for row in rows {
            try workMeter.consume(at: .deduplication)
            let key = try identityRow(
                row,
                operation: "relational DISTINCT",
                workMeter: workMeter
            )
            if try seen.insert(key.values) {
                try unique.append(
                    footprint: try CanonicalRelationalFootprintMeter.sourceAliasFootprint(
                        of: row,
                        workMeter: workMeter
                    ),
                    make: { row.retainingPayload(rows.payloadLease) }
                )
            }
        }
        return try unique.finish().moveToSharedOwnership(at: .deduplication)
    }

    private func identityRow(
        _ row: CanonicalSourceRow,
        operation: String,
        workMeter: DatabaseWorkMeter
    ) throws -> CanonicalRowValueIdentity {
        guard row.layout.reservation.map({ $0.workMeter === workMeter }) ?? true,
              row.layout.payloadLeases.allSatisfy({ $0.workMeter === workMeter }),
              row.payloadLeases.allSatisfy({ $0.workMeter === workMeter }) else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        guard row.layout.visibleSlots.allSatisfy({ row.values.indices.contains($0) }) else {
            throw CanonicalReadError.unsupportedSelectQuery("Relational identity references a missing source slot")
        }
        let footprint = try CanonicalRelationalFootprintMeter.positionalValueArrayFootprint(
            count: row.layout.visibleSlots.count,
            valueAt: { row.values[row.layout.visibleSlots[$0]] },
            annotations: [:], version: nil, workMeter: workMeter, stage: .deduplication
        ).adding(identityArrayAdditionalFootprint(count: row.layout.visibleSlots.count))
        let reservation = try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .deduplication)
        return try CanonicalRowValueIdentity(
            values: canonicalIdentityValues(row, operation: operation, workMeter: workMeter),
            reservation: reservation
        )
    }

    private func identityArrayAdditionalFootprint(count: Int) throws -> DatabaseIntermediateFootprint {
        let selected = try DatabaseIntermediateCollectionMeter.arrayFootprint(count: count, element: CanonicalSQLValueIdentity.self)
        let source = try DatabaseIntermediateCollectionMeter.arrayFootprint(count: count, element: QueryValue.self)
        return DatabaseIntermediateFootprint(bytes: selected.bytes > source.bytes ? selected.bytes - source.bytes : 0)
    }

    private func canonicalIdentityValues(
        _ row: CanonicalSourceRow,
        operation: String, workMeter: DatabaseWorkMeter
    ) throws -> [CanonicalSQLValueIdentity] {
        var values: [CanonicalSQLValueIdentity] = []
        values.reserveCapacity(row.layout.visibleSlots.count)
        for slot in row.layout.visibleSlots {
            let value = row.values[slot]
            values.append(try canonicalQueryValueIdentity(value, operation: operation, type: row.layout.types[slot], workMeter: workMeter))
        }
        return values
    }

    private func canonicalValueIdentity(
        _ value: FieldValue,
        operation: String, type: QueryColumnType? = nil, workMeter: DatabaseWorkMeter
    ) throws -> FieldValue {
        do {
            let selected = try CanonicalSQLValueSemantics.identity(value, type: type, workMeter: workMeter)
            return try RelationalValueIdentity.canonicalize(selected).value
        } catch RelationalValueIdentityError.nonFiniteNumericValue {
            throw CanonicalReadError.expressionEvaluation(
                .typeMismatch(
                    operation: "\(operation) with a non-finite numeric value"
                )
            )
        } catch RelationalValueIdentityError.invalidObject {
            throw CanonicalReadError.expressionEvaluation(
                .typeMismatch(
                    operation: "\(operation) with an invalid object value"
                )
            )
        }
    }

    private func canonicalQueryValueIdentity(_ value: QueryValue, operation: String, type: QueryColumnType? = nil, workMeter: DatabaseWorkMeter) throws -> CanonicalSQLValueIdentity {
        if let identity = try CanonicalSQLValueIdentity.prepareCollection(value, type: type,
            environment: inheritedSQLTemporalEnvironment, workMeter: workMeter) { return identity }
        if let identity = try CanonicalSQLValueSemantics.temporalIdentity(value, type: type,
            environment: inheritedSQLTemporalEnvironment, workMeter: workMeter) { return .temporal(identity) }
        switch value {
        case .primitive(let primitive):
            return .value(.primitive(try canonicalValueIdentity(primitive, operation: operation, type: type, workMeter: workMeter)))
        case .numeric(let numeric):
            return .value(.primitive(try canonicalValueIdentity(.decimal(numeric.value), operation: operation, type: type, workMeter: workMeter)))
        default: return .value(value)
        }
    }

    private func applyFilter(
        _ filter: DatabaseKit.Expression?,
        bound: CanonicalBoundExpression?,
        to rows: CanonicalRetainedRows,
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedRows {
        guard filter != nil else { return rows }
        guard let bound else { throw CanonicalReadError.unsupportedSelectQuery("A filter has no executable binding") }
        var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .filterEvaluation,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: rows.count
        )
        for row in rows {
            try workMeter.consume(at: .filterEvaluation)
            guard try await evaluateBoundBoolean(
                bound,
                on: row,
                context: evaluationContext,
                workMeter: workMeter
            ) else { continue }
            try retained.append(
                footprint: try CanonicalRelationalFootprintMeter.sourceAliasFootprint(
                    of: row,
                    workMeter: workMeter
                ),
                make: { row.retainingPayload(rows.payloadLease) }
            )
        }
        return try retained.finish().moveToSharedOwnership(
            at: .filterEvaluation
        )
    }

    private func applyOrder(
        _ orderBy: [SortKey]?,
        boundOrder: [CanonicalBoundExpression],
        to rows: CanonicalRetainedRows,
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedRows {
        guard let orderBy, !orderBy.isEmpty else { return rows }
        guard orderBy.count == boundOrder.count else {
            throw CanonicalReadError.unsupportedSelectQuery("Sort keys differ from their executable bindings")
        }
        let outerArrayFootprint = try DatabaseIntermediateCollectionMeter
            .arrayFootprint(
                count: rows.count,
                element: (
                    CanonicalSourceRow,
                    [ProjectedQueryValue],
                    ByteString
                ).self
            )
        let nestedValuesFootprint = try DatabaseIntermediateCollectionMeter
            .arrayFootprint(
                count: orderBy.count,
                element: ProjectedQueryValue.self
            )
            .multiplied(by: UInt64(rows.count))
        let decorationFootprint = try outerArrayFootprint
            .multiplied(by: 2)
            .adding(nestedValuesFootprint)
            .adding(
                try DatabaseIntermediateFootprint(bytes: 32)
                    .multiplied(by: UInt64(rows.count))
            )
        let decorationRows = try DatabaseIntermediateFootprint(
            rows: UInt64(rows.count)
        ).multiplied(by: 2).rows
        let decorationReservation = try workMeter.reserveIntermediate(
            rows: decorationRows,
            bytes: decorationFootprint.bytes,
            at: .sortInput
        )
        defer { decorationReservation.release() }
        try workMeter.consume(UInt64(rows.count), at: .sortInput)
        var decorated: [(
            CanonicalSourceRow,
            [ProjectedQueryValue],
            ByteString
        )] = []
        decorated.reserveCapacity(rows.count)
        for row in rows {
            var values: [ProjectedQueryValue] = []
            values.reserveCapacity(orderBy.count)
            for (index, _) in orderBy.enumerated() {
                values.append(
                    try await evaluateBoundValue(
                        boundOrder[index],
                        on: row,
                        context: evaluationContext,
                        workMeter: workMeter
                    )
                )
            }
            let sourceColumns = try relationSchema(for: row.layout).typedColumns()
            let fingerprint = try CanonicalRowFingerprint.compute(
                QueryRow(validatedValues: row.values, annotations: row.annotations, version: row.version),
                columns: sourceColumns, workMeter: workMeter)
            decorated.append((row, values, fingerprint))
        }
        let sorted: [(
            CanonicalSourceRow,
            [ProjectedQueryValue],
            ByteString
        )]
        do {
            sorted = try decorated.sorted { lhs, rhs in
                for (index, sortKey) in orderBy.enumerated() {
                    try workMeter.consume(2, at: .sortComparison)
                    let comparison = try Self.selectedOrdering(lhs.1[index].value, rhs.1[index].value,
                        bound: boundOrder[index], using: sortKey,
                        environment: evaluationContext?.options.temporalEnvironment ?? inheritedSQLTemporalEnvironment, workMeter: workMeter)
                    guard comparison != .equal else { continue }
                    return comparison == .lessThan
                }
                try workMeter.consume(2, at: .sortComparison)
                return lhs.2.lexicographicallyPrecedes(rhs.2)
            }
        } catch let failure as FieldValueComparisonError {
            throw canonicalComparisonReadError(
                failure,
                operation: "ordering"
            )
        }
        var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .sortInput,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: sorted.count
        )
        for item in sorted {
            let row = item.0
            try retained.append(
                footprint: try CanonicalRelationalFootprintMeter.sourceAliasFootprint(
                    of: row,
                    workMeter: workMeter
                ),
                make: { row.retainingPayload(rows.payloadLease) }
            )
        }
        return try retained.finish().moveToSharedOwnership(at: .sortInput)
    }

    /// Mutation supplies an admitted positional frame and retains every returned
    /// payload lease through its persistence boundary. Scalar/lazy semantics
    /// remain the same selected path used by SELECT.
    static func evaluateCanonicalScalar(
        _ bound: CanonicalBoundExpression, on row: CanonicalSourceRow,
        temporalEnvironment: SQLTemporalEnvironment? = nil, workMeter: DatabaseWorkMeter
    ) async throws -> (value: QueryValue, leases: [DatabaseQueryPayloadLease]) {
        guard row.layout.reservation.map({ $0.workMeter === workMeter }) ?? true,
              row.layout.payloadLeases.allSatisfy({ $0.workMeter === workMeter }),
              row.payloadLeases.allSatisfy({ $0.workMeter === workMeter }) else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        func requiresRelation(_ expression: CanonicalBoundExpression) -> Bool {
            switch expression.expression {
            case .aggregate, .subquery, .arraySubquery, .exists, .inSubquery: return true
            case .quantifiedComparison(_, _, _, .subquery): return true
            default: return expression.children.contains(where: requiresRelation)
            }
        }
        guard !requiresRelation(bound) else {
            throw DatabaseExpressionEvaluationError.unsupportedExpression("scalar execution requires a context-free expression")
        }
        let result = try await evaluateSelectedValue(bound, on: row, context: nil, databaseContext: nil, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
        return (result.value, result.leases)
    }

    struct ProjectedQueryValue: Sendable {
        let value: QueryValue
        let leases: [DatabaseQueryPayloadLease]
        let payloadIsRetained: Bool
    }

    private func evaluateBoundBoolean(
        _ bound: CanonicalBoundExpression,
        on row: CanonicalSourceRow,
        context: CanonicalQueryEvaluationContext?,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        let cell = try await evaluateBoundValue(bound, on: row, context: context, workMeter: workMeter)
        switch cell.value {
        case .primitive(.bool(let value)): return value
        case .primitive(.null): return false
        default: throw CanonicalReadError.expressionEvaluation(.typeMismatch(operation: "bound predicate"))
        }
    }

    private func evaluateBoundScopedValue(
        _ bound: CanonicalBoundExpression,
        on row: CanonicalSourceRow,
        group: CanonicalGroupedRow? = nil,
        context: CanonicalQueryEvaluationContext?,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseQueryScopedFieldValue {
        let cell = try await evaluateBoundValue(bound, on: row, group: group,
            context: context, workMeter: workMeter)
        let value: FieldValue
        switch cell.value {
        case .primitive(let primitive): value = primitive
        case .numeric(let numeric): value = .decimal(numeric.value)
        default:
            throw CanonicalReadError.unsupportedSelectQuery("This bound scalar kernel does not accept rich values")
        }
        // The destination scalar owns its claim after a nested result releases
        // its original owner; source and derived COW payloads remain immutable.
        return try .retaining(value, workMeter: workMeter, stage: .expressionEvaluation)
    }

    private func evaluateBoundValue(
        _ bound: CanonicalBoundExpression,
        on row: CanonicalSourceRow,
        group: CanonicalGroupedRow? = nil,
        effectiveRow suppliedRow: CanonicalSourceRow? = nil,
        context: CanonicalQueryEvaluationContext?,
        workMeter: DatabaseWorkMeter
    ) async throws -> ProjectedQueryValue {
        return try await Self.evaluateSelectedValue(bound, on: row, group: group,
            effectiveRow: suppliedRow, context: context, databaseContext: self, workMeter: workMeter)
    }

    private static func evaluateSelectedValue(
        _ bound: CanonicalBoundExpression,
        on row: CanonicalSourceRow,
        group: CanonicalGroupedRow? = nil,
        effectiveRow suppliedRow: CanonicalSourceRow? = nil,
        context: CanonicalQueryEvaluationContext?,
        databaseContext: DatabaseContext?,
        temporalEnvironment suppliedTemporalEnvironment: SQLTemporalEnvironment? = nil,
        workMeter: DatabaseWorkMeter
    ) async throws -> ProjectedQueryValue {
        // Fast-path async resumes can accumulate across child nodes and projection columns.
        await Task.yield()
        try workMeter.checkpoint(at: .expressionEvaluation)
        let value = try await evaluateSelectedNode(bound, on: row, group: group,
            effectiveRow: suppliedRow, context: context, databaseContext: databaseContext,
            temporalEnvironment: suppliedTemporalEnvironment, workMeter: workMeter)
        await Task.yield()
        try workMeter.checkpoint(at: .expressionEvaluation)
        return value
    }

    private static func evaluateSelectedNode(
        _ bound: CanonicalBoundExpression,
        on row: CanonicalSourceRow,
        group: CanonicalGroupedRow? = nil,
        effectiveRow suppliedRow: CanonicalSourceRow? = nil,
        context: CanonicalQueryEvaluationContext?,
        databaseContext: DatabaseContext?,
        temporalEnvironment suppliedTemporalEnvironment: SQLTemporalEnvironment? = nil,
        workMeter: DatabaseWorkMeter
    ) async throws -> ProjectedQueryValue {
        let temporalEnvironment = suppliedTemporalEnvironment ?? context?.options.temporalEnvironment ?? databaseContext?.inheritedSQLTemporalEnvironment
        do {
        try workMeter.consume(at: .expressionEvaluation)
        let claim = try workMeter.reserveIntermediate(
            bytes: UInt64(bound.children.count) * 512 + 256, at: .expressionEvaluation)
        defer { claim.release() }
        let overlayClaim: DatabaseIntermediateReservation?
        let effectiveRow: CanonicalSourceRow
        if let suppliedRow { effectiveRow = suppliedRow; overlayClaim = nil }
        else {
            overlayClaim = try workMeter.reserveIntermediate(
                bytes: prospectiveSourceCompositionFootprint(row, outer: context?.outerRow,
                    workMeter: workMeter).bytes, at: .expressionEvaluation)
            effectiveRow = row.overlaying(outer: context?.outerRow)
        }
        defer { overlayClaim?.release() }
        func child(_ index: Int) async throws -> ProjectedQueryValue {
            guard bound.children.indices.contains(index) else {
                throw CanonicalReadError.unsupportedSelectQuery("An executable expression has no selected child")
            }
            return try await evaluateSelectedValue(bound.children[index], on: row, group: group,
                effectiveRow: effectiveRow, context: context, databaseContext: databaseContext, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
        }
        func normalized(_ cell: ProjectedQueryValue) throws -> ProjectedQueryValue {
            ProjectedQueryValue(value: try (group != nil && cell.value.isNull
                ? CanonicalQueryTypeBinder.normalize(cell.value, to: bound.type) : bound.normalize(cell.value)), leases: cell.leases,
                payloadIsRetained: cell.payloadIsRetained)
        }
        if let group {
            for index in group.key.selectedLeaves.indices {
                let leaf = group.key.selectedLeaves[index]
                if try CanonicalBoundKey(expression: bound).isSemanticallyEquivalent(to: CanonicalBoundKey(expression: leaf),
                    workMeter: workMeter, isVolatileSubquery: { databaseContext?.selectedPlanIsVolatile($0) ?? true }) {
                    guard let ordinal = group.key.activeLeaves.firstIndex(of: index) else {
                        return try normalized(ProjectedQueryValue(value: .primitive(.null), leases: [], payloadIsRetained: false))
                    }
                    return try normalized(ProjectedQueryValue(value: group.key.values[ordinal],
                        leases: [group.rows.payloadLease], payloadIsRetained: true))
                }
            }
        }
        if case .grouping = bound.expression {
            guard let group else { throw CanonicalReadError.aggregateEvaluation(.invalidGroupedExpression("GROUPING requires a grouped query")) }
            var bits: Int32 = 0
            for child in bound.children {
                var index: Int?
                for candidate in group.key.selectedLeaves.indices {
                    if try CanonicalBoundKey(expression: child).isSemanticallyEquivalent(to: CanonicalBoundKey(expression: group.key.selectedLeaves[candidate]),
                        workMeter: workMeter, isVolatileSubquery: { databaseContext?.selectedPlanIsVolatile($0) ?? true }) { index = candidate; break }
                }
                guard let index else { throw CanonicalReadError.aggregateEvaluation(.invalidGroupedExpression("GROUPING argument is not a grouping expression")) }
                bits = (bits << 1) | (group.key.activeLeaves.contains(index) ? 0 : 1)
            }
            return try normalized(ProjectedQueryValue(value: .primitive(.int64(Int64(bits))), leases: [], payloadIsRetained: false))
        }
        if let window = bound.window {
            guard let cache = context?.windowValues,
                  let slot = cache.plans.firstIndex(where: { $0 === window }),
                  cache.results[slot].values.indices.contains(cache.row) else {
                throw CanonicalReadError.unsupportedSelectQuery("A window expression has no analytic evaluation frame")
            }
            let value = cache.results[slot].values[cache.row]
            return try normalized(ProjectedQueryValue(value: value.value,
                leases: value.leases + [cache.results[slot].payloadLease], payloadIsRetained: true))
        }
        if let slot = bound.columnSlot {
            guard effectiveRow.layout.bindingSlots.indices.contains(slot),
                  let physical = effectiveRow.layout.bindingSlots[slot].valueIndex,
                  effectiveRow.values.indices.contains(physical) else {
                throw CanonicalReadError.unsupportedSelectQuery("A bound frame column is outside its admitted row")
            }
            // Projection and analytic callback owners retain the source once; a borrowed column adds no lease vector.
            return try normalized(ProjectedQueryValue(value: effectiveRow.values[physical], leases: [], payloadIsRetained: true))
        }
        if let whole = bound.wholeRow {
            guard try effectiveRow.sourceIsPresent(at: whole.presenceIndex) else {
                return try normalized(ProjectedQueryValue(value: .primitive(.null), leases: [], payloadIsRetained: false))
            }
            let limits = selectedValueLimits(context: context, meter: workMeter)
            let fieldClaim = try workMeter.reserveIntermediate(bytes:
                DatabaseIntermediateCollectionMeter.arrayFootprint(count: whole.fieldSlots.count,
                    element: QueryValue.self).bytes, at: .expressionEvaluation)
            defer { fieldClaim.release() }
            let fields = try whole.fieldSlots.map { slot -> QueryValue in
                guard effectiveRow.layout.bindingSlots.indices.contains(slot),
                      let physical = effectiveRow.layout.bindingSlots[slot].valueIndex,
                      effectiveRow.values.indices.contains(physical) else {
                    throw DatabaseExpressionEvaluationError.typeMismatch(operation: "whole-row field address")
                }
                return effectiveRow.values[physical]
            }
            let result = try CanonicalSQLCompositeEvaluator.construct(type: whole.type, fields: fields, workMeter: workMeter, limits: limits)
            let leases = effectiveRow.payloadLeases + effectiveRow.layout.payloadLeases + (result.lease.map { [$0] } ?? [])
            return try normalized(ProjectedQueryValue(value: result.value, leases: leases, payloadIsRetained: true))
        }
        #if DATABASE_MULTI_BASE
        if bound.subqueryBinding != nil, let context, let dispatch = context.sourceEvaluation {
            guard let query = context.sourceQuery, let plan = context.boundPlan, let binding = context.queryBinding else {
                throw CanonicalReadError.unsupportedSelectQuery("A Composition source read lost its selected query scope")
            }
            // Provenance selects authority only when a selected nested query actually reads a source.
            // Global scalar, grouping and window evaluation keeps the dispatcher for each input row.
            return try await dispatch(row.annotations) { origin, session in
                let active = try origin.compositionEvaluationContext(query, plan: plan, binding: binding,
                    execution: context.options, session: session).usingWindows(context.windowValues)
                return try await Self.evaluateSelectedValue(bound, on: row, group: group,
                    effectiveRow: effectiveRow, context: active, databaseContext: origin, workMeter: workMeter)
            }
        }
        #endif
        switch bound.expression {
        case .truthTest(_, let target, let negated):
            let value = try await child(0).value
            let matches: Bool
            switch (value, target) {
            case (.primitive(.bool(true)), .true), (.primitive(.bool(false)), .false),
                 (.primitive(.null), .unknown): matches = true
            case (.primitive(.bool), _), (.primitive(.null), _): matches = false
            default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "SQL truth test")
            }
            return ProjectedQueryValue(value: .primitive(.bool(negated ? !matches : matches)),
                leases: [], payloadIsRetained: false)
        case .isDistinctFrom(_, _, let negated):
            let left = try await child(0)
            let right = try await child(1)
            let equal: Bool
            if left.value.isNull || right.value.isNull { equal = left.value.isNull && right.value.isNull }
            else { equal = try selectedComparison(left.value, right.value, comparison: .equal, leftType: bound.children[0].type, rightType: bound.children[1].type, temporalEnvironment: temporalEnvironment, workMeter: workMeter) == true }
            return ProjectedQueryValue(value: .primitive(.bool(negated ? equal : !equal)),
                leases: [], payloadIsRetained: false)
        case .greatest, .least:
            let greatest: Bool
            if case .greatest = bound.expression { greatest = true } else { greatest = false }
            var selected: ProjectedQueryValue?
            for index in bound.children.indices {
                let value = try await child(index)
                guard !value.value.isNull else { continue }
                if let current = selected {
                    if try selectedComparison(value.value, current.value,
                        comparison: greatest ? .greaterThan : .lessThan, leftType: bound.type, rightType: bound.type, temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter) == true { selected = value }
                } else { selected = value }
            }
            return try normalized(selected ?? ProjectedQueryValue(value: .primitive(.null),
                leases: [], payloadIsRetained: false))
        case .nullIf:
            let left = try await child(0)
            let right = try await child(1)
            if try selectedComparison(left.value, right.value, comparison: .equal, leftType: bound.children[0].type, rightType: bound.children[1].type, temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter) == true {
                return try normalized(ProjectedQueryValue(value: .primitive(.null), leases: [], payloadIsRetained: false))
            }
            return try normalized(left)
        case .quantifiedComparison(_, let comparison, let quantifier, let source):
            let left = try await child(0)
            var sawNull = false
            func examine(_ member: QueryValue, sourceType: QueryColumnType) throws -> Bool? {
                try workMeter.consume(at: .expressionEvaluation)
                let selected = try coerceSelectedComparisonValue(member, from: sourceType,
                    to: bound.children[0].type, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
                defer { withExtendedLifetime(selected) {} }
                guard let value = try selectedComparison(left.value, selected.value, comparison: comparison, leftType: bound.children[0].type, rightType: bound.children[0].type, temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter) else {
                    sawNull = true
                    return nil
                }
                return value
            }
            switch source {
            case .array:
                let array = try await child(1)
                if array.value.isNull {
                    return ProjectedQueryValue(value: .primitive(.null), leases: [], payloadIsRetained: false)
                }
                guard case .array(let value) = array.value else {
                    throw DatabaseExpressionEvaluationError.typeMismatch(operation: "ANY/ALL array")
                }
                for member in value.elements {
                    if let result = try examine(member, sourceType: .sql(value.elementType)),
                       (quantifier == .any && result) || (quantifier == .all && !result) {
                        return ProjectedQueryValue(value: .primitive(.bool(result)), leases: [], payloadIsRetained: false)
                    }
                }
            case .subquery(let query):
                guard let plan = bound.subqueryBinding, !plan.columns.isEmpty else {
                    throw CanonicalReadError.invalidMembershipSubquery(columnCount: bound.subqueryBinding?.columns.count ?? 0)
                }
                guard let databaseContext else {
                    throw DatabaseExpressionEvaluationError.unsupportedExpression("scalar subquery requires a query context")
                }
                let response = try await databaseContext.executeNestedQuery(query, boundPlan: plan, outerRow: effectiveRow, context: context)
                let rows = response.visibleRows
                for index in 0..<rows.count {
                    var fields: [QueryValue] = []
                    rows.withElement(at: index) { fields = $0.values }
                    let member = try selectedMembershipValue(fields, columns: plan.columns, bound: bound,
                        context: context, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
                    let result = try selectedComparison(left.value, member.value, comparison: comparison,
                        leftType: bound.children[0].type, rightType: bound.children[0].type,
                        temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter)
                    if result == nil { sawNull = true }
                    if let result, (quantifier == .any && result) || (quantifier == .all && !result) {
                        return ProjectedQueryValue(value: .primitive(.bool(result)), leases: [], payloadIsRetained: false)
                    }
                }
            }
            return ProjectedQueryValue(value: .primitive(sawNull ? .null : .bool(quantifier == .all)),
                leases: [], payloadIsRetained: false)
        case .literal(let literal):
            let value: QueryValue
            if case .queryValue(let rich, _) = literal { value = rich }
            else { value = .primitive(try literal.toFieldValue()) }
            let footprint = try CanonicalRelationalFootprintMeter.valueFootprint(of: value,
                workMeter: workMeter, stage: .expressionEvaluation)
            let reservation = try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .expressionEvaluation)
            return try normalized(ProjectedQueryValue(value: value,
                leases: [DatabaseQueryPayloadLease(retaining: reservation)], payloadIsRetained: true))
        case .caseWhen(let operand, let pairs, let fallback):
            let selectedOperand: ProjectedQueryValue?
            if operand != nil { selectedOperand = try await child(0) } else { selectedOperand = nil }
            let offset = selectedOperand == nil ? 0 : 1
            for index in pairs.indices {
                let matches: Bool
                if let selectedOperand {
                    let comparison = bound.children[offset + index]
                    guard case .equal = comparison.expression, comparison.children.count == 2 else {
                        throw DatabaseExpressionEvaluationError.typeMismatch(operation: "simple CASE selected equality")
                    }
                    do {
                        // Coerce the saved original value independently for this WHEN.
                        let left = try coerceSelectedComparisonValue(selectedOperand.value,
                            from: bound.children[0].type, to: comparison.children[0].type, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
                        defer { withExtendedLifetime(left) {} }
                        let right = try await evaluateSelectedValue(comparison.children[1], on: row, group: group,
                            effectiveRow: effectiveRow, context: context, databaseContext: databaseContext, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
                        matches = try selectedComparison(left.value, right.value, comparison: .equal, leftType: comparison.children[0].type, rightType: comparison.children[1].type, temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter) == true
                    } catch let cause as DatabaseExpressionEvaluationError {
                        throw CanonicalReadError.expressionEvaluation(cause, occurrence: comparison.occurrence)
                    }
                } else {
                    let condition = try await child(offset + index)
                    switch condition.value {
                    case .primitive(.bool(true)): matches = true
                    case .primitive(.bool(false)), .primitive(.null): matches = false
                    default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "CASE condition")
                    }
                }
                if matches { return try normalized(await child(offset + pairs.count + index)) }
            }
            if fallback != nil { return try normalized(await child(offset + pairs.count * 2)) }
            return try normalized(ProjectedQueryValue(value: .primitive(.null), leases: [], payloadIsRetained: false))
        case .coalesce:
            for index in bound.children.indices {
                let value = try await child(index)
                if !value.value.isNull { return try normalized(value) }
            }
            return try normalized(ProjectedQueryValue(value: .primitive(.null), leases: [], payloadIsRetained: false))
        case .and, .or:
            let left = try await child(0)
            if case .and = bound.expression, left.value == .primitive(.bool(false)) { return try normalized(left) }
            if case .or = bound.expression, left.value == .primitive(.bool(true)) { return try normalized(left) }
            let right = try await child(1)
            return try await evaluateBoundKernel(bound, children: [left, right], on: effectiveRow,
                context: context, temporalEnvironment: temporalEnvironment, workMeter: workMeter, groupingNulls: group != nil)
        case .isNull, .isNotNull:
            let value = try await child(0)
            if case .sql(.composite) = bound.children[0].type {
                return try await evaluateBoundKernel(bound, children: [value], on: effectiveRow,
                    context: context, temporalEnvironment: temporalEnvironment, workMeter: workMeter, groupingNulls: group != nil)
            }
            let result: Bool
            if case .isNull = bound.expression { result = value.value.isNull } else { result = !value.value.isNull }
            return ProjectedQueryValue(value: .primitive(.bool(result)), leases: [], payloadIsRetained: false)
        case .arraySubquery(let query):
            guard let plan = bound.subqueryBinding, plan.columns.count == 1, let databaseContext else {
                throw CanonicalReadError.invalidScalarSubquery(rowCount: nil, columnCount: bound.subqueryBinding?.columns.count)
            }
            let response = try await databaseContext.executeNestedQuery(query, boundPlan: plan,
                outerRow: effectiveRow, context: context)
            let rows = response.visibleRows
            let scratch = try workMeter.reserveIntermediate(bytes: DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: rows.count, element: QueryValue.self).bytes, at: .expressionEvaluation)
            defer { scratch.release() }
            var values: [QueryValue] = []; values.reserveCapacity(rows.count)
            for index in 0..<rows.count { rows.withElement(at: index) { values.append($0.values[0]) } }
            let result = try CanonicalSQLArrayEvaluator.assembleSubquery(values, columnType: plan.columns[0].type,
                resultType: bound.type, temporalEnvironment: temporalEnvironment, workMeter: workMeter,
                limits: selectedValueLimits(context: context, meter: workMeter))
            return try normalized(ProjectedQueryValue(value: result.value,
                leases: response.rows.originPayloadLeases + (result.lease.map { [$0] } ?? []), payloadIsRetained: true))
        case .subquery(let query), .exists(let query), .inSubquery(_, let query):
            guard let plan = bound.subqueryBinding else {
                throw CanonicalReadError.unsupportedSelectQuery("A nested expression has no executable SELECT binding")
            }
            let candidate: ProjectedQueryValue?
            if case .inSubquery = bound.expression { candidate = try await child(0) } else { candidate = nil }
            guard let databaseContext else {
                throw DatabaseExpressionEvaluationError.unsupportedExpression("scalar subquery requires a query context")
            }
            if case .exists = bound.expression, query.quantifier == .all,
               !canonicalQueryRequiresAggregation(query) {
                let exists = try await databaseContext.executeExists(query, plan: plan,
                    outerRow: effectiveRow, context: context)
                return ProjectedQueryValue(value: .primitive(.bool(exists)), leases: [], payloadIsRetained: false)
            }
            let response = try await databaseContext.executeNestedQuery(query, boundPlan: plan,
                outerRow: effectiveRow, context: context)
            let rows = response.visibleRows
            if case .exists = bound.expression {
                return ProjectedQueryValue(value: .primitive(.bool(!rows.isEmpty)), leases: [], payloadIsRetained: false)
            }
            if case .sql(.composite(let type)) = bound.type, plan.columns.count != 1 {
                guard rows.count <= 1 else { throw CanonicalReadError.invalidScalarSubquery(rowCount: rows.count, columnCount: nil) }
                if rows.isEmpty { return try normalized(ProjectedQueryValue(value: .primitive(.null), leases: [], payloadIsRetained: false)) }
                var fields: [QueryValue] = []
                rows.withElement(at: 0) { fields = $0.values }
                let fieldClaim = try workMeter.reserveIntermediate(bytes:
                    DatabaseIntermediateCollectionMeter.arrayFootprint(count: fields.count,
                        element: ProjectedQueryValue.self).multiplied(by: 2).bytes, at: .expressionEvaluation)
                defer { fieldClaim.release() }
                var selected: [ProjectedQueryValue] = []; selected.reserveCapacity(fields.count)
                for index in fields.indices {
                    selected.append(try coerceSelectedComparisonValue(fields[index], from: plan.columns[index].type,
                        to: type.columns[index].type, temporalEnvironment: temporalEnvironment, workMeter: workMeter))
                }
                let result = try CanonicalSQLCompositeEvaluator.construct(type: type, fields: selected.map { $0.value },
                    workMeter: workMeter, limits: selectedValueLimits(context: context, meter: workMeter))
                return try normalized(ProjectedQueryValue(value: result.value,
                    leases: response.rows.originPayloadLeases + selected.flatMap { $0.leases }
                        + (result.lease.map { [$0] } ?? []), payloadIsRetained: true))
            }
            if let candidate {
                var sawNull = false
                for index in 0..<rows.count {
                    var fields: [QueryValue] = []
                    rows.withElement(at: index) { fields = $0.values }
                    let member = try selectedMembershipValue(fields, columns: plan.columns, bound: bound,
                        context: context, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
                    let result = try selectedComparison(candidate.value, member.value, comparison: .equal,
                        leftType: bound.children[0].type, rightType: bound.children[0].type,
                        temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter)
                    if result == true { return ProjectedQueryValue(value: .primitive(.bool(true)), leases: [], payloadIsRetained: false) }
                    if result == nil { sawNull = true }
                }
                return ProjectedQueryValue(value: .primitive(sawNull ? .null : .bool(false)), leases: [], payloadIsRetained: false)
            }
            guard plan.columns.count == 1 else {
                throw CanonicalReadError.invalidScalarSubquery(rowCount: nil, columnCount: plan.columns.count)
            }
            guard rows.count <= 1 else {
                throw CanonicalReadError.invalidScalarSubquery(rowCount: rows.count, columnCount: nil)
            }
            if rows.isEmpty {
                return try normalized(ProjectedQueryValue(value: .primitive(.null), leases: [], payloadIsRetained: false))
            }
            var value: QueryValue?
            rows.withElement(at: 0) { value = $0.values[0] }
            guard let value else { throw CanonicalReadError.invalidScalarSubquery(rowCount: 0, columnCount: 1) }
            return try normalized(ProjectedQueryValue(value: value, leases: response.rows.originPayloadLeases,
                payloadIsRetained: true))
        case .aggregate(let aggregate):
            guard let databaseContext else {
                throw DatabaseExpressionEvaluationError.unsupportedExpression("scalar aggregate requires a query context")
            }
            guard let group else {
                throw CanonicalReadError.aggregateEvaluation(.invalidGroupedExpression("An aggregate requires a group"))
            }
            if bound.aggregate != nil {
                return try await SQLAggregateEvaluator.evaluate(bound: bound, rowCount: group.rows.count,
                    evaluate: { index, selected in
                        let member = group.rows[index]
                        let value = try await evaluateSelectedValue(selected, on: member, context: context,
                            databaseContext: databaseContext, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
                        return ProjectedQueryValue(value: value.value,
                            leases: value.leases + member.payloadLeases + [group.rows.payloadLease], payloadIsRetained: value.payloadIsRetained)
                    }, direct: { selected in
                        try await evaluateSelectedValue(selected, on: row, group: group, context: context,
                            databaseContext: databaseContext, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
                    }, temporalEnvironment: temporalEnvironment, workMeter: workMeter,
                    limits: selectedValueLimits(context: context, meter: workMeter))
            }
            if case .sum = aggregate {
                return try await databaseContext.evaluateNumericAggregate(aggregate, bound: bound, rows: group.rows,
                    workMeter: workMeter, evaluationContext: context)
            }
            if case .avg = aggregate {
                return try await databaseContext.evaluateNumericAggregate(aggregate, bound: bound, rows: group.rows,
                    workMeter: workMeter, evaluationContext: context)
            }
            if case .count(_, let distinct) = aggregate, let input = bound.children.first,
               CanonicalSQLValueSemantics.isTemporal(input.type) || selectedCollectionType(input.type) {
                let scratch = try DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<CanonicalSQLValueIdentity>.stride + 32))
                    .multiplied(by: UInt64(distinct ? group.rows.count : 0))
                let reservation = try workMeter.reserveIntermediate(bytes: scratch.bytes, at: .aggregateInput)
                defer { reservation.release() }
                var seen = try CanonicalSQLIdentitySet(workMeter: workMeter,
                    stage: .aggregateInput, expectedCount: distinct ? group.rows.count : 0)
                var count: Int64 = 0
                for member in group.rows {
                    let cell = try await evaluateSelectedValue(input, on: member, context: context,
                        databaseContext: databaseContext, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
                    guard !cell.value.isNull else { continue }
                    if distinct {
                        let key = try databaseContext.canonicalQueryValueIdentity(cell.value, operation: "COUNT(DISTINCT)",
                            type: input.type, workMeter: workMeter)
                        guard try seen.insert(key) else { continue }
                    }
                    let increment = count.addingReportingOverflow(1)
                    guard !increment.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
                    count = increment.partialValue
                }
                return try normalized(ProjectedQueryValue(value: .primitive(.int64(count)), leases: [], payloadIsRetained: false))
            }
            switch aggregate {
            case .min, .max, .sample:
                guard case .sql = bound.type else { break }
                guard let input = bound.children.first else {
                    throw CanonicalReadError.unsupportedSelectQuery("A scalar aggregate has no selected input")
                }
                var selected: ProjectedQueryValue?
                for row in group.rows {
                    try workMeter.consume(at: .aggregateInput)
                    let cell = try await evaluateSelectedValue(input, on: row, context: context, databaseContext: databaseContext, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
                    guard !cell.value.isNull else { continue }
                    if let current = selected {
                        switch aggregate {
                        case .min:
                            if try selectedComparison(cell.value, current.value, comparison: .lessThan, leftType: input.type, rightType: input.type, temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter) == true { selected = cell }
                        case .max:
                            if try selectedComparison(cell.value, current.value, comparison: .greaterThan, leftType: input.type, rightType: input.type, temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter) == true { selected = cell }
                        default: break
                        }
                    } else { selected = cell }
                }
                return try normalized(selected ?? ProjectedQueryValue(value: .primitive(.null), leases: [], payloadIsRetained: false))
            default: break
            }
            let value = try await databaseContext.evaluateAggregate(aggregate, bound: bound, rows: group.rows,
                workMeter: workMeter, evaluationContext: context)
            let result = try value.withValue { try bound.normalize(.primitive(copy $0)) }
            return ProjectedQueryValue(value: result,
                leases: value.retainedReservation.map { [DatabaseQueryPayloadLease(retaining: $0)] } ?? [],
                payloadIsRetained: value.retainedReservation != nil)
        default:
            var values: [ProjectedQueryValue] = []
            values.reserveCapacity(bound.children.count)
            for index in bound.children.indices { values.append(try await child(index)) }
            return try await evaluateBoundKernel(bound, children: values, on: effectiveRow, context: context, temporalEnvironment: temporalEnvironment, workMeter: workMeter, groupingNulls: group != nil)
        }
        } catch let cause as SQLTimeZoneError {
            throw CanonicalReadError.expressionEvaluation(.timeZone(cause), occurrence: bound.occurrence)
        } catch let cause as DatabaseExpressionEvaluationError {
            throw CanonicalReadError.expressionEvaluation(cause, occurrence: bound.occurrence)
        } catch let cause as QueryValueError {
            throw CanonicalReadError.expressionEvaluation(.queryValue(cause), occurrence: bound.occurrence)
        } catch CanonicalReadError.expressionEvaluation(let cause, let occurrence) {
            throw CanonicalReadError.expressionEvaluation(cause, occurrence: occurrence ?? bound.occurrence)
        }
    }

    private static func selectedValueLimits(context: CanonicalQueryEvaluationContext?, meter: DatabaseWorkMeter) -> QueryValueLimits {
        let structural = context?.options.queryStructuralLimits ?? .default
        return QueryValueLimits(maximumRetainedBytes: min(structural.maximumRetainedBytes, meter.budget.maximumIntermediateBytes),
            maximumTotalNodes: min(structural.maximumTotalNodes, meter.budget.maximumWorkUnits),
            maximumNestingDepth: min(structural.maximumNestingDepth, meter.budget.maximumWorkUnits),
            maximumCollectionElements: min(structural.maximumCollectionElements, meter.budget.maximumWorkUnits))
    }

    private static func selectedCollectionType(_ type: QueryColumnType?) -> Bool {
        switch type {
        case .sql(.array)?, .sql(.json)?, .sql(.jsonb)?, .sql(.composite)?: return true
        default: return false
        }
    }

    private static func selectedCollectionCast(_ bound: CanonicalBoundExpression) -> Bool {
        if selectedCollectionType(bound.type) { return true }
        return bound.children.first?.type == .sql(.jsonb) && !CanonicalSQLScalarCast.usesPayload(bound.type)
    }

    private static func selectedArrayFunction(_ name: String) -> Bool {
        switch name {
        case "array_length", "array_lower", "array_upper", "array_ndims", "array_dims", "cardinality",
             "array_append", "array_prepend", "array_cat", "array_position", "array_positions",
             "array_remove", "array_replace", "array_to_string", "string_to_array": return true
        default: return false
        }
    }

    private static func selectedJSONFunction(_ name: String) -> Bool {
        switch name {
        case "to_json", "to_jsonb", "array_to_json", "row_to_json", "json_build_array", "jsonb_build_array",
             "json_build_object", "jsonb_build_object", "json_typeof", "jsonb_typeof",
             "json_array_length", "jsonb_array_length", "json_extract_path", "jsonb_extract_path",
             "json_extract_path_text", "jsonb_extract_path_text", "jsonb_set", "jsonb_insert",
             "jsonb_strip_nulls": return true
        default: return false
        }
    }

    private static func selectedNumericCastTarget(_ target: DataType) -> Bool {
        switch target {
        case .smallint, .integer, .bigint, .real, .doublePrecision, .decimal, .boolean: return true
        default: return false
        }
    }

    private static func selectedNumericCastSource(_ source: QueryColumnType) -> Bool {
        switch source {
        case .sql(.smallint), .sql(.integer), .sql(.bigint), .sql(.real), .sql(.doublePrecision),
             .sql(.decimal), .sql(.boolean): return true
        case .primitive(let kind, false):
            switch kind {
            case .bool, .int8, .int16, .int32, .int64, .uint8, .uint16, .uint32, .uint64,
                 .float32, .float64, .decimal: return true
            default: return false
            }
        default: return false
        }
    }

    private static func selectedScalarCastTarget(_ target: QueryColumnType) -> Bool {
        if CanonicalQueryTypeBinder.temporalType(target) != nil { return true }
        if CanonicalSQLScalarCast.usesPayload(target) { return true }
        switch target {
        case .sql(let type): return type == .uuid || selectedNumericCastTarget(type)
        case .primitive(let kind, false):
            switch kind {
            case .uuid, .bool, .int8, .int16, .int32, .int64, .uint8, .uint16, .uint32, .uint64,
                 .float32, .float64, .decimal: return true
            default: return false
            }
        default: return false
        }
    }

    private static func selectedNumericFunction(_ name: String) -> Bool {
        switch name.lowercased() {
        case "abs", "sign", "ceil", "ceiling", "floor", "round", "trunc", "mod", "div", "gcd", "lcm",
             "sqrt", "cbrt", "power", "exp", "ln", "log", "log10", "scale", "min_scale", "trim_scale",
             "pi", "degrees", "radians", "sin", "cos", "tan", "sind", "cosd", "tand", "cot", "cotd",
             "asin", "acos", "atan", "atan2", "asind", "acosd", "atand", "atan2d",
             "sinh", "cosh", "tanh", "asinh", "acosh", "atanh", "width_bucket": return true
        default: return false
        }
    }

    private static func selectedOrdering(_ left: QueryValue, _ right: QueryValue,
        bound: CanonicalBoundExpression, using key: SortKey,
        environment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter) throws -> QueryComparison {
        do {
            let selected = SortKey(key.expression, direction: key.direction,
                nulls: key.nulls ?? (key.direction == .ascending ? .last : .first))
            return try CanonicalSQLValueSemantics.compareValues(left, right, type: bound.type,
                using: selected, environment: environment, workMeter: workMeter)
        } catch let cause as SQLTimeZoneError {
            throw CanonicalReadError.expressionEvaluation(.timeZone(cause), occurrence: bound.occurrence)
        } catch let cause as DatabaseExpressionEvaluationError {
            throw CanonicalReadError.expressionEvaluation(cause, occurrence: bound.occurrence)
        }
    }

    private static func selectedMembershipValue(_ fields: [QueryValue], columns: [QueryColumn],
        bound: CanonicalBoundExpression, context: CanonicalQueryEvaluationContext?,
        temporalEnvironment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter
    ) throws -> ProjectedQueryValue {
        guard fields.count == columns.count else {
            throw CanonicalReadError.invalidMembershipSubquery(columnCount: fields.count)
        }
        if columns.count == 1 {
            return try coerceSelectedComparisonValue(fields[0], from: columns[0].type,
                to: bound.children[0].type, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
        }
        guard case .sql(.composite(let type)) = bound.children[0].type,
              bound.subqueryComparisonTypes.count == fields.count else {
            throw CanonicalReadError.invalidMembershipSubquery(columnCount: columns.count)
        }
        let claim = try workMeter.reserveIntermediate(bytes:
            DatabaseIntermediateCollectionMeter.arrayFootprint(count: fields.count,
                element: ProjectedQueryValue.self).multiplied(by: 2).bytes, at: .expressionEvaluation)
        defer { claim.release() }
        var selected: [ProjectedQueryValue] = []
        selected.reserveCapacity(fields.count)
        for index in fields.indices {
            selected.append(try coerceSelectedComparisonValue(fields[index], from: columns[index].type,
                to: bound.subqueryComparisonTypes[index], temporalEnvironment: temporalEnvironment, workMeter: workMeter))
        }
        let result = try CanonicalSQLCompositeEvaluator.construct(type: type, fields: selected.map { $0.value },
            workMeter: workMeter, limits: selectedValueLimits(context: context, meter: workMeter))
        return ProjectedQueryValue(value: result.value,
            leases: selected.flatMap { $0.leases } + (result.lease.map { [$0] } ?? []), payloadIsRetained: true)
    }

    private static func selectedComparison(
        _ left: QueryValue, _ right: QueryValue, comparison: DatabaseKit.ComparisonOperator,
        leftType: QueryColumnType, rightType: QueryColumnType,
        temporalEnvironment: SQLTemporalEnvironment? = nil, mode: CanonicalSQLCompositeComparisonMode = .record,
        workMeter: DatabaseWorkMeter
    ) throws -> Bool? {
        try workMeter.consume(at: .expressionEvaluation)
        if left.isNull || right.isNull { return nil }
        if case .sql(.composite) = leftType {
            return try CanonicalSQLCompositeEvaluator.comparePredicate(left, right,
                leftType: leftType, rightType: rightType, comparison: comparison,
                mode: mode, environment: temporalEnvironment, workMeter: workMeter)
        }
        do {
            if comparison == .equal || comparison == .notEqual {
                let equal = try CanonicalSQLValueSemantics.equalValues(left, right,
                    leftType: leftType, rightType: rightType, environment: temporalEnvironment, workMeter: workMeter)
                return comparison == .equal ? equal : !equal
            }
            let result = try CanonicalSQLValueSemantics.compareValues(left, right,
                leftType: leftType, rightType: rightType, environment: temporalEnvironment, workMeter: workMeter)
            switch comparison {
            case .lessThan: return result == .lessThan
            case .lessThanOrEqual: return result != .greaterThan
            case .greaterThan: return result == .greaterThan
            case .greaterThanOrEqual: return result != .lessThan
            case .equal, .notEqual: preconditionFailure("Equality is handled before ordering")
            }
        } catch let failure as FieldValueComparisonError {
            throw canonicalComparisonReadError(failure, operation: "selected scalar comparison")
        }
    }

    private static func coerceSelectedComparisonValue(
        _ value: QueryValue, from source: QueryColumnType, to target: QueryColumnType,
        temporalEnvironment: SQLTemporalEnvironment? = nil, workMeter: DatabaseWorkMeter
    ) throws -> ProjectedQueryValue {
        let result = try CanonicalSQLScalarCast.coerce(value, from: source, to: target,
            temporalEnvironment: temporalEnvironment, workMeter: workMeter,
            limits: selectedValueLimits(context: nil, meter: workMeter))
        return ProjectedQueryValue(value: result.value, leases: result.lease.map { [$0] } ?? [],
            payloadIsRetained: result.lease != nil || QueryValueValidator.accepts(value, type: target))
    }

    private static func evaluateBoundKernel(
        _ bound: CanonicalBoundExpression,
        children: [ProjectedQueryValue],
        on row: CanonicalSourceRow,
        context: CanonicalQueryEvaluationContext?,
        temporalEnvironment: SQLTemporalEnvironment? = nil, workMeter: DatabaseWorkMeter, groupingNulls: Bool = false
    ) async throws -> ProjectedQueryValue {
        let claim = try workMeter.reserveIntermediate(bytes: UInt64(children.count) * 512 + 256,
            at: .expressionEvaluation)
        defer { claim.release() }
        let structural = context?.options.queryStructuralLimits ?? .default
        let limits = QueryValueLimits(
            maximumRetainedBytes: min(structural.maximumRetainedBytes, workMeter.budget.maximumIntermediateBytes),
            maximumTotalNodes: min(structural.maximumTotalNodes, workMeter.budget.maximumWorkUnits),
            maximumNestingDepth: min(structural.maximumNestingDepth, workMeter.budget.maximumWorkUnits),
            maximumCollectionElements: min(structural.maximumCollectionElements, workMeter.budget.maximumWorkUnits))
        func normalize(_ value: QueryValue) throws -> QueryValue {
            if groupingNulls, value.isNull {
                return try CanonicalQueryTypeBinder.normalize(value, to: bound.type)
            }
            return try bound.normalize(value)
        }
        func booleanResult(_ value: Bool?) throws -> ProjectedQueryValue {
            let result = value.map { QueryValue.primitive(.bool($0)) } ?? .primitive(.null)
            return ProjectedQueryValue(value: try normalize(result), leases: [], payloadIsRetained: false)
        }
        func payloadResult(_ scoped: consuming DatabaseQueryScopedFieldValue) throws -> ProjectedQueryValue {
            let result = try scoped.withValue { try normalize(.primitive(copy $0)) }
            var leases = children.flatMap { $0.leases }
            if let reservation = scoped.retainedReservation { leases.append(DatabaseQueryPayloadLease(retaining: reservation)) }
            return ProjectedQueryValue(value: result, leases: leases, payloadIsRetained: !leases.isEmpty)
        }
        func patternResult() throws -> ProjectedQueryValue {
            let result = try CanonicalSQLPatternEvaluator.evaluate(bound,
                operands: children.map { $0.value }, workMeter: workMeter, limits: limits)
            var leases = children.flatMap { $0.leases }
            if let lease = result.lease { leases.append(lease) }
            return ProjectedQueryValue(value: try normalize(result.value), leases: leases, payloadIsRetained: !leases.isEmpty)
        }
        func collectionResult(array: Bool) throws -> ProjectedQueryValue {
            let result: (value: QueryValue, lease: DatabaseQueryPayloadLease?)
            if case .cast = bound.expression {
                guard let input = bound.children.first, let value = children.first?.value else {
                    throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected collection CAST arity")
                }
                result = try CanonicalSQLScalarCast.evaluateCollection(value, from: input.type, to: bound.type,
                    context: bound.castContext, temporalEnvironment: temporalEnvironment,
                    workMeter: workMeter, limits: limits)
            } else if array {
                result = try CanonicalSQLArrayEvaluator.evaluate(bound, operands: children.map { $0.value },
                    temporalEnvironment: temporalEnvironment, workMeter: workMeter, limits: limits)
            } else {
                result = try CanonicalSQLJSONEvaluator.evaluate(bound, operands: children.map { $0.value },
                    temporalEnvironment: temporalEnvironment, workMeter: workMeter, limits: limits)
            }
            var leases = children.flatMap { $0.leases }
            if let lease = result.lease { leases.append(lease) }
            return ProjectedQueryValue(value: try normalize(result.value), leases: leases,
                payloadIsRetained: !leases.isEmpty)
        }
        func temporalResult() throws -> ProjectedQueryValue {
            guard let temporalEnvironment else { throw DatabaseExpressionEvaluationError.missingTemporalEnvironment }
            let value = try CanonicalSQLTemporalEvaluator.evaluate(bound, operands: children.map { $0.value },
                environment: temporalEnvironment, workMeter: workMeter)
            return ProjectedQueryValue(value: try normalize(value), leases: children.flatMap { $0.leases }, payloadIsRetained: false)
        }
        if case .cast = bound.expression, bound.sourceAlignment {
            guard let input = bound.children.first, let value = children.first?.value else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected source alignment arity")
            }
            let result = try CanonicalSQLScalarCast.alignSource(value, from: input.type, to: bound.type,
                temporalEnvironment: temporalEnvironment, workMeter: workMeter, limits: limits)
            var leases = children.flatMap { $0.leases }
            if let lease = result.lease { leases.append(lease) }
            return ProjectedQueryValue(value: try normalize(result.value), leases: leases,
                payloadIsRetained: !leases.isEmpty)
        }
        let compositeOperation: Bool
        switch bound.expression {
        case .row, .field, .fieldExpansion, .collate: compositeOperation = true
        case .equal, .notEqual, .lessThan, .lessThanOrEqual, .greaterThan, .greaterThanOrEqual, .isNull, .isNotNull:
            compositeOperation = bound.children.contains { if case .sql(.composite) = $0.type { return true }; return false }
        default: compositeOperation = false
        }
        if compositeOperation {
            let result = try CanonicalSQLCompositeEvaluator.evaluate(bound, operands: children.map { $0.value },
                temporalEnvironment: temporalEnvironment, workMeter: workMeter, limits: limits)
            let leases = children.flatMap { $0.leases } + (result.lease.map { [$0] } ?? [])
            return ProjectedQueryValue(value: try normalize(result.value), leases: leases, payloadIsRetained: !leases.isEmpty)
        }
        switch bound.expression {
        case .array, .arrayAccess: return try collectionResult(array: true)
        case .collectionOperation:
            let array: Bool
            if case .sql(.array)? = bound.children.first?.type { array = true }
            else { array = false }
            return try collectionResult(array: array)
        case .cast where selectedCollectionCast(bound): return try collectionResult(array: false)
        case .currentValue: return try temporalResult()
        case .equal, .notEqual, .lessThan, .lessThanOrEqual, .greaterThan, .greaterThanOrEqual:
            guard children.count == 2 else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected comparison arity")
            }
            let comparison: DatabaseKit.ComparisonOperator
            switch bound.expression {
            case .equal: comparison = .equal
            case .notEqual: comparison = .notEqual
            case .lessThan: comparison = .lessThan
            case .lessThanOrEqual: comparison = .lessThanOrEqual
            case .greaterThan: comparison = .greaterThan
            case .greaterThanOrEqual: comparison = .greaterThanOrEqual
            default: preconditionFailure("The selected expression is a scalar comparison")
            }
            return try booleanResult(selectedComparison(children[0].value, children[1].value,
                comparison: comparison, leftType: bound.children[0].type, rightType: bound.children[1].type, temporalEnvironment: temporalEnvironment, workMeter: workMeter))
        case .between:
            guard children.count == 3 else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected BETWEEN arity")
            }
            let lower = try selectedComparison(children[0].value, children[1].value,
                comparison: .greaterThanOrEqual, leftType: bound.children[0].type, rightType: bound.children[1].type, temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter)
            let upper = try selectedComparison(children[0].value, children[2].value,
                comparison: .lessThanOrEqual, leftType: bound.children[0].type, rightType: bound.children[2].type, temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter)
            if lower == false || upper == false { return try booleanResult(false) }
            if lower == nil || upper == nil { return try booleanResult(nil) }
            return try booleanResult(true)
        case .inList, .notInList:
            guard let candidate = children.first else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected IN arity")
            }
            let negated: Bool
            if case .notInList = bound.expression { negated = true } else { negated = false }
            var sawNull = false
            for (index, member) in children.enumerated().dropFirst() {
                let result = try selectedComparison(candidate.value, member.value,
                    comparison: .equal, leftType: bound.children[0].type, rightType: bound.children[index].type, temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter)
                if result == true { return try booleanResult(!negated) }
                if result == nil { sawNull = true }
            }
            return try booleanResult(sawNull ? nil : negated)
        case .add, .subtract, .multiply, .divide, .modulo, .negate:
            if case .subtract = bound.expression, bound.children.first?.type == .sql(.jsonb) {
                return try collectionResult(array: false)
            }
            if bound.children.contains(where: { CanonicalSQLValueSemantics.isTemporal($0.type) }) { return try temporalResult() }
            let result = try CanonicalSQLNumericEvaluator.evaluate(bound,
                operands: children.map { $0.value }, workMeter: workMeter)
            return ProjectedQueryValue(value: try normalize(result),
                leases: children.flatMap { $0.leases }, payloadIsRetained: false)
        case .cast(_, let target) where selectedNumericCastTarget(target)
            && bound.type == .sql(target)
            && bound.children.first.map({ selectedNumericCastSource($0.type) }) == true:
            let result = try CanonicalSQLNumericEvaluator.evaluate(bound,
                operands: children.map { $0.value }, workMeter: workMeter)
            return ProjectedQueryValue(value: try normalize(result),
                leases: children.flatMap { $0.leases }, payloadIsRetained: false)
        case .function(let function) where selectedNumericFunction(function.name):
            let result = try CanonicalSQLNumericEvaluator.evaluate(bound,
                operands: children.map { $0.value }, workMeter: workMeter)
            return ProjectedQueryValue(value: try normalize(result),
                leases: children.flatMap { $0.leases }, payloadIsRetained: false)
        case .cast where selectedScalarCastTarget(bound.type):
            guard let input = bound.children.first, let value = children.first?.value else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected CAST arity")
            }
            if CanonicalSQLScalarCast.usesPayload(bound.type) {
                return try payloadResult(CanonicalSQLScalarCast.evaluatePayload(value, from: input.type,
                    to: bound.type, context: bound.castContext, temporalEnvironment: temporalEnvironment,
                    limits: limits, workMeter: workMeter))
            }
            let result = try CanonicalSQLScalarCast.evaluateValue(value, from: input.type,
                to: bound.type, context: bound.castContext, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
            return ProjectedQueryValue(value: try normalize(result), leases: children.flatMap { $0.leases }, payloadIsRetained: false)
        case .patternMatch: return try patternResult()
        case .concatenate:
            if case .sql(.array) = bound.type { return try collectionResult(array: true) }
            if bound.type == .sql(.jsonb) { return try collectionResult(array: false) }
            if bound.type == .sql(.blob) {
                return try payloadResult(CanonicalSQLBinaryEvaluator.evaluate(bound, operands: children.map { $0.value }, workMeter: workMeter))
            }
            return try payloadResult(CanonicalSQLStringEvaluator.evaluate(bound, operands: children.map { $0.value }, temporalEnvironment: temporalEnvironment, workMeter: workMeter))
        case .function(let function):
            let name = function.name
            if selectedArrayFunction(name) { return try collectionResult(array: true) }
            if selectedJSONFunction(name) { return try collectionResult(array: false) }
            if ["to_char", "to_date"].contains(name) || (name == "to_timestamp" && children.count == 2) {
                guard let temporalEnvironment else { throw DatabaseExpressionEvaluationError.missingTemporalEnvironment }
                let result = try SQLTemporalFormatting.evaluate(bound, operands: children.map { $0.value },
                    environment: temporalEnvironment, workMeter: workMeter, limits: limits)
                var leases = children.flatMap { $0.leases }
                if let lease = result.lease { leases.append(lease) }
                return ProjectedQueryValue(value: try normalize(result.value), leases: leases, payloadIsRetained: !leases.isEmpty)
            }
            if ["now", "transaction_timestamp", "statement_timestamp", "clock_timestamp", "extract", "date_part", "date_trunc", "date_bin", "age",
                "make_date", "make_time", "make_timestamp", "make_timestamptz", "make_interval", "timezone", "to_timestamp"].contains(name) {
                return try temporalResult()
            }
            if ["regexp_like", "regexp_match", "regexp_replace", "regexp_split_to_array"].contains(name)
                || (name == "substring" && bound.children.count > 1 && bound.children[1].type == .sql(.text)) {
                return try patternResult()
            }
            if ["encode", "decode", "get_byte", "set_byte", "get_bit", "set_bit"].contains(name)
                || (["length", "octet_length", "bit_length", "substr", "substring"].contains(name)
                    && bound.children.first?.type == .sql(.blob)) {
                return try payloadResult(CanonicalSQLBinaryEvaluator.evaluate(bound, operands: children.map { $0.value }, workMeter: workMeter))
            }
            return try payloadResult(CanonicalSQLStringEvaluator.evaluate(bound, operands: children.map { $0.value }, temporalEnvironment: temporalEnvironment, workMeter: workMeter))
        default: break
        }
        let operands = children.enumerated().map { index, value in
            CanonicalBoundExpression(expression: .literal(.queryValue(value.value, type: bound.children[index].type)),
                type: bound.children[index].type, isNullable: bound.children[index].isNullable)
        }
        let executable = CanonicalBoundExpression(expression: bound.expression, type: bound.type,
            isNullable: bound.isNullable, children: operands, castContext: bound.castContext,
            sourceAlignment: bound.sourceAlignment)
        let expression = try executable.materializing(in: row.values)
        // Only selected, already-evaluated operands reach the primitive kernel.
        // This call performs no column, parameter, aggregate, or subquery lookup.
        let value = try evaluateSelectedKernel(expression, on: row, workMeter: workMeter)
        let result = try value.withValue { try normalize(.primitive(copy $0)) }
        var leases = children.flatMap { $0.leases }
        if let reservation = value.retainedReservation { leases.append(DatabaseQueryPayloadLease(retaining: reservation)) }
        return ProjectedQueryValue(value: result, leases: leases, payloadIsRetained: !leases.isEmpty)
    }

    private func projectRows(
        _ rows: CanonicalRetainedRows,
        projection: [ProjectionItem],
        columns: [QueryColumn],
        sourceSchema: CanonicalRelationSchema,
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?,
        boundPlan suppliedPlan: CanonicalBoundSelect? = nil
    ) async throws -> CanonicalRetainedQueryRows {
        guard let boundPlan = suppliedPlan ?? evaluationContext?.boundPlan else {
            throw CanonicalReadError.unsupportedSelectQuery("Projection has no executable SELECT binding")
        }
        var leaseCount = 1
        for row in rows {
            let (next, overflow) = leaseCount.addingReportingOverflow(row.payloadLeases.count)
            guard !overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
            leaseCount = next
        }
        var schemaFootprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(count: columns.count, element: QueryColumn.self)
        schemaFootprint = try schemaFootprint.adding(DatabaseIntermediateCollectionMeter.arrayFootprint(
            count: leaseCount, element: DatabaseQueryPayloadLease.self))
        for column in columns {
            schemaFootprint = try schemaFootprint.adding(DatabaseIntermediateFootprint(bytes: UInt64(column.name.utf8.count) + 64))
        }
        schemaFootprint = try schemaFootprint.adding(DatabaseIntermediateFootprint(
            bytes: UInt64(MemoryLayout<DatabaseSharedRetainedQueryRows>.stride)))
        let schemaClaim = try workMeter.reserveIntermediate(bytes: schemaFootprint.bytes, at: .projection)
        var leases: [DatabaseQueryPayloadLease] = []
        let leaseLayout = try DatabaseRetainedArrayLayout.forElement(DatabaseQueryPayloadLease.self)
        var accountedLeaseCapacity = try leaseLayout.growth(from: 0, toFit: leaseCount).capacity
        leases.reserveCapacity(accountedLeaseCapacity)
        leases.append(rows.payloadLease)
        for row in rows { leases.append(contentsOf: row.payloadLeases) }
        func retainDerived(_ incoming: [DatabaseQueryPayloadLease]) throws {
            let (count, overflow) = leases.count.addingReportingOverflow(incoming.count)
            guard !overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
            let growth = try leaseLayout.growth(from: accountedLeaseCapacity, toFit: count)
            if growth.additionalByteCount > 0 {
                try schemaClaim.reserveAdditional(bytes: growth.additionalByteCount, at: .projection)
                leases.reserveCapacity(growth.capacity)
                accountedLeaseCapacity = growth.capacity
            }
            leases.append(contentsOf: incoming)
        }
        var retained = try DatabaseRetainedArrayBuilder<QueryRow>(
            workMeter: workMeter, stage: .projection,
            layout: DatabaseRetainedArrayLayout.forElement(QueryRow.self), expectedCount: rows.count)
        let selectedSlots: [Int]? = nil
        let expressions = boundPlan.projection.map { $0.expression }
        for row in rows {
            try workMeter.consume(at: .projection)
            let scratchFootprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(count: columns.count, element: QueryValue.self)
                .adding(DatabaseIntermediateCollectionMeter.arrayFootprint(count: columns.count, element: Bool.self))
            let scratchClaim = try workMeter.reserveIntermediate(bytes: scratchFootprint.bytes, at: .projection)
            defer { scratchClaim.release() }
            let cells: [QueryValue]
            var retainedPayload: [Bool] = []
            if let slots = selectedSlots {
                retainedPayload = Array(repeating: true, count: slots.count)
                // Source layouts are constructed in the declared schema order.
                cells = try slots.enumerated().map { ordinal, index in
                    guard index < row.values.count else {
                        throw CanonicalReadError.unsupportedSelectQuery("Source row slot is missing")
                    }
                    return try CanonicalQueryTypeBinder.normalize(row.values[index], to: columns[ordinal].type)
                }
            } else {
                retainedPayload.reserveCapacity(columns.count)
                var values: [QueryValue] = []
                values.reserveCapacity(columns.count)
                for (index, _) in expressions.enumerated() {
                    let cell = try await evaluateBoundValue(boundPlan.projection[index], on: row,
                        context: evaluationContext, workMeter: workMeter)
                    try retainDerived(cell.leases)
                    retainedPayload.append(cell.payloadIsRetained)
                    values.append(try CanonicalQueryTypeBinder.normalize(cell.value, to: columns[index].type))
                }
                cells = values
            }
            try retained.append(
                footprint: CanonicalRelationalFootprintMeter.positionalValueArrayFootprint(
                    count: cells.count,
                    valueAt: { index in
                        retainedPayload[index] ? .primitive(.null) : cells[index]
                    }, annotations: row.annotations, version: row.version,
                    workMeter: workMeter, stage: .projection),
                make: { QueryRow(validatedValues: cells, annotations: row.annotations, version: row.version) })
        }
        let result = DatabaseSharedRetainedQueryRows(
            storage: try retained.finish().moveToSharedOwnership(at: .projection), columns: columns,
            payloadLeases: leases, schemaLeases: [schemaClaim])
        return result
    }

    private func makeCanonicalGroups(_ rows: CanonicalRetainedRows,
        boundGroupBy: [CanonicalBoundExpression], workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?, groupingPlan: CanonicalBoundGrouping? = nil,
        mergeAnnotations: (@Sendable ([String: FieldValue], [String: FieldValue]) throws -> [String: FieldValue])? = nil
    ) async throws -> CanonicalRetainedGroups {
        let grouping = groupingPlan ?? evaluationContext?.boundPlan?.grouping
        let leaves = grouping?.leaves ?? boundGroupBy
        let scratch = try workMeter.reserveIntermediate(bytes: UInt64(leaves.count) * 128 + 64, at: .aggregateInput)
        defer { scratch.release() }
        var sets = grouping?.sets ?? []
        if grouping == nil {
            var set: [Int] = []
            for index in leaves.indices { try CanonicalBindingStorage.append(index, to: &set, reservation: scratch) }
            try CanonicalBindingStorage.append(set, to: &sets, reservation: scratch)
        }
        var values: [[ProjectedQueryValue]]? = nil
        if sets.count > 1, !leaves.isEmpty {
            var cache: [[ProjectedQueryValue]] = []
            for row in rows {
                var cells: [ProjectedQueryValue] = []
                for leaf in leaves {
                    let value = try await evaluateBoundValue(leaf, on: row, context: evaluationContext, workMeter: workMeter)
                    if !value.payloadIsRetained {
                        let footprint = try CanonicalRelationalFootprintMeter.valueFootprint(of: value.value,
                            workMeter: workMeter, stage: .aggregateInput)
                        try scratch.reserveAdditional(bytes: footprint.bytes, at: .aggregateInput)
                    }
                    try CanonicalBindingStorage.append(value, to: &cells, reservation: scratch)
                }
                try CanonicalBindingStorage.append(cells, to: &cache, reservation: scratch)
            }
            values = cache
        }
        if sets.count == 1, let set = sets.first {
            let selected = set.map { leaves[$0] }
            return try await makeCanonicalGroupSet(rows, groupBy: selected.map { $0.expression },
                boundGroupBy: selected, workMeter: workMeter, evaluationContext: evaluationContext,
                selectedLeaves: leaves, activeLeaves: set, selectedValues: nil, mergeAnnotations: mergeAnnotations)
        }
        var result = try DatabaseRetainedArrayBuilder<CanonicalGroupedRow>(workMeter: workMeter,
            stage: .aggregateInput, layout: try DatabaseRetainedArrayLayout.forElement(CanonicalGroupedRow.self))
        for set in sets {
            try workMeter.checkpoint(at: .aggregateInput)
            let selected = set.map { leaves[$0] }
            let groups = try await makeCanonicalGroupSet(rows, groupBy: selected.map { $0.expression },
                boundGroupBy: selected, workMeter: workMeter, evaluationContext: evaluationContext,
                selectedLeaves: leaves, activeLeaves: set, selectedValues: values, mergeAnnotations: mergeAnnotations)
            for group in groups {
                try result.append(footprint: canonicalGroupedRowFootprint(group, workMeter: workMeter), make: { group })
            }
        }
        return try result.finish().moveToSharedOwnership(at: .aggregateInput)
    }

    private func makeCanonicalGroupSet(
        _ rows: CanonicalRetainedRows,
        groupBy: [Expression],
        boundGroupBy: [CanonicalBoundExpression],
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?,
        selectedLeaves: [CanonicalBoundExpression], activeLeaves: [Int], selectedValues: [[ProjectedQueryValue]]?,
        mergeAnnotations: (@Sendable ([String: FieldValue], [String: FieldValue]) throws -> [String: FieldValue])?
    ) async throws -> CanonicalRetainedGroups {
        let maximumGroupCount = max(1, rows.count)
        let doubledGroupCount = maximumGroupCount.multipliedReportingOverflow(
            by: 2
        )
        guard !doubledGroupCount.overflow else {
            throw DatabaseIntermediateFootprintError.rowMultiplicationOverflow(
                value: UInt64(maximumGroupCount),
                multiplier: 2
            )
        }
        var lookupCapacity = 1
        while lookupCapacity < doubledGroupCount.partialValue {
            let doubled = lookupCapacity.multipliedReportingOverflow(by: 2)
            guard !doubled.overflow else {
                throw DatabaseIntermediateFootprintError
                    .rowMultiplicationOverflow(
                        value: UInt64(lookupCapacity),
                        multiplier: 2
                    )
            }
            lookupCapacity = doubled.partialValue
        }
        var stateFootprint = try DatabaseIntermediateCollectionMeter
            .arrayFootprint(
                count: maximumGroupCount,
                element: CanonicalGroupKey.self
            )
        stateFootprint = try stateFootprint.adding(
            DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: maximumGroupCount,
                element: CanonicalSourceRow.self
            )
        ).adding(
            DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: maximumGroupCount,
                element: Int.self
            )
        ).adding(
            DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: maximumGroupCount,
                element: Int.self
            )
        ).adding(
            DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: rows.count,
                element: Int?.self
            )
        ).adding(
            DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: lookupCapacity,
                element: Int?.self
            )
        )
        let stateReservation = try workMeter.reserveIntermediate(
            rows: UInt64(maximumGroupCount),
            bytes: stateFootprint.bytes,
            at: .aggregateInput
        )
        defer { stateReservation.release() }

        var keys: [CanonicalGroupKey] = []
        keys.reserveCapacity(maximumGroupCount)
        var representatives: [CanonicalSourceRow] = []
        representatives.reserveCapacity(maximumGroupCount)
        var groupHeads: [Int] = []
        groupHeads.reserveCapacity(maximumGroupCount)
        var groupTails: [Int] = []
        groupTails.reserveCapacity(maximumGroupCount)
        var nextRow = Array<Int?>(repeating: nil, count: rows.count)
        var groupLookup = Array<Int?>(
            repeating: nil,
            count: lookupCapacity
        )

        func lookupSlot(for identity: [CanonicalSQLValueIdentity]) throws -> (Int, Int?) {
            let mask = lookupCapacity - 1
            var slot = canonicalHashLookupSlot(
                hashValue: try CanonicalSQLValueIdentity.hash(identity, workMeter: workMeter, stage: .aggregateInput),
                mask: mask
            )
            while let groupIndex = groupLookup[slot] {
                if try CanonicalSQLValueIdentity.equal(keys[groupIndex].identity, identity,
                    workMeter: workMeter, stage: .aggregateInput) {
                    return (slot, groupIndex)
                }
                slot = (slot + 1) & mask
            }
            return (slot, nil)
        }

        for position in 0..<rows.count {
            let row = rows[position]
            try workMeter.consume(at: .aggregateInput)
            let scopedValueReservation = try reserveQueryScopedValues(
                count: groupBy.count,
                workMeter: workMeter,
                stage: .aggregateInput
            )
            defer { scopedValueReservation.release() }
            var scopedValues: [ProjectedQueryValue] = []
            scopedValues.reserveCapacity(groupBy.count)
            if let selectedValues {
                for leaf in activeLeaves { scopedValues.append(selectedValues[position][leaf]) }
            } else {
                for bound in boundGroupBy {
                    scopedValues.append(try await evaluateBoundValue(bound, on: row,
                        context: evaluationContext, workMeter: workMeter))
                }
            }
            let keyArrays = try DatabaseIntermediateCollectionMeter
                .arrayFootprint(
                    count: groupBy.count,
                    element: QueryValue.self
                ).adding(
                    DatabaseIntermediateCollectionMeter.arrayFootprint(
                        count: groupBy.count,
                        element: CanonicalSQLValueIdentity.self
                    )
                )
            let keyReservation = try workMeter.reserveIntermediate(
                bytes: keyArrays.bytes,
                at: .aggregateInput
            )
            defer { keyReservation.release() }
            var values: [QueryValue] = []
            values.reserveCapacity(groupBy.count)
            var identities: [CanonicalSQLValueIdentity] = []
            identities.reserveCapacity(groupBy.count)
            for (index, scopedValue) in scopedValues.enumerated() {
                let value = scopedValue.value
                let valueFootprint = try CanonicalRelationalFootprintMeter.valueFootprint(of: value, workMeter: workMeter, stage: .aggregateInput)
                try keyReservation.reserveAdditional(bytes: valueFootprint.bytes, at: .aggregateInput)
                values.append(value)
                let identity = try canonicalQueryValueIdentity(value, operation: "GROUP BY", type: boundGroupBy[index].type, workMeter: workMeter)
                try keyReservation.reserveAdditional(bytes: identity.payloadFootprint(workMeter: workMeter, stage: .aggregateInput).bytes, at: .aggregateInput)
                identities.append(identity)
            }
            let (slot, existingGroupIndex) = try lookupSlot(for: identities)
            if let groupIndex = existingGroupIndex {
                nextRow[groupTails[groupIndex]] = position
                groupTails[groupIndex] = position
            } else {
                try stateReservation.absorbAll(from: keyReservation)
                let groupIndex = keys.count
                keys.append(
                    CanonicalGroupKey(
                        values: values,
                        identity: identities, selectedLeaves: selectedLeaves, activeLeaves: activeLeaves
                    )
                )
                representatives.append(row)
                groupHeads.append(position)
                groupTails.append(position)
                groupLookup[slot] = groupIndex
            }
        }

        if rows.isEmpty, groupBy.isEmpty {
            keys.append(CanonicalGroupKey(values: [], identity: [], selectedLeaves: selectedLeaves, activeLeaves: activeLeaves))
            representatives.append(CanonicalSourceRow(fields: [:]))
            groupHeads.append(-1)
            groupTails.append(-1)
        }

        var retained = try DatabaseRetainedArrayBuilder<CanonicalGroupedRow>(
            workMeter: workMeter,
            stage: .aggregateInput,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalGroupedRow.self),
            expectedCount: keys.count
        )
        for index in keys.indices {
            var groupRows = try DatabaseRetainedArrayBuilder<
                CanonicalSourceRow
            >(
                workMeter: workMeter,
                stage: .aggregateInput,
                layout: try DatabaseRetainedArrayLayout.forElement(
                    CanonicalSourceRow.self
                )
            )
            var position = groupHeads[index]
            while position >= 0 {
                let row = rows[position]
                try groupRows.append(
                    footprint: try CanonicalRelationalFootprintMeter.sourceAliasFootprint(
                        of: row,
                        workMeter: workMeter
                    ),
                    make: { row.retainingPayload(rows.payloadLease) }
                )
                position = nextRow[position] ?? -1
            }
            let retainedGroupRows = try groupRows.finish()
                .moveToSharedOwnership(at: .aggregateInput)
            let original = representatives[index]
            var annotations = original.annotations
            if let mergeAnnotations {
                for member in retainedGroupRows {
                    try workMeter.consume(at: .aggregateInput)
                    let footprint = try CanonicalRelationalFootprintMeter.sourceAliasFootprint(of: member, workMeter: workMeter)
                    try stateReservation.reserveAdditional(bytes: footprint.bytes, at: .aggregateInput)
                    annotations = try mergeAnnotations(annotations, member.annotations)
                }
            }
            var representativeValues = original.values
            if activeLeaves.count < selectedLeaves.count {
                let footprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(count: original.values.count, element: QueryValue.self)
                try stateReservation.reserveAdditional(bytes: footprint.bytes, at: .aggregateInput)
                for index in selectedLeaves.indices where !activeLeaves.contains(index) {
                    if let slot = selectedLeaves[index].columnSlot,
                       original.layout.bindingSlots.indices.contains(slot),
                       let physical = original.layout.bindingSlots[slot].valueIndex,
                       representativeValues.indices.contains(physical) { representativeValues[physical] = .primitive(.null) }
                }
            }
            let representative = CanonicalSourceRow(values: representativeValues, layout: original.layout,
                annotations: annotations, version: original.version, payloadLeases: original.payloadLeases,
                sourcePresence: original.sourcePresence)
            let group = CanonicalGroupedRow(
                key: keys[index], representative: representative, rows: retainedGroupRows
            )
            try retained.append(
                footprint: try canonicalGroupedRowFootprint(
                    group,
                    workMeter: workMeter
                ),
                make: { group }
            )
        }
        return try retained.finish().moveToSharedOwnership(at: .aggregateInput)
    }

    private func applyHaving(
        _ having: Expression?,
        bound: CanonicalBoundExpression?,
        to groups: CanonicalRetainedGroups,
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedGroups {
        guard having != nil else { return groups }
        guard let bound else { throw CanonicalReadError.unsupportedSelectQuery("HAVING has no executable binding") }
        var result = try DatabaseRetainedArrayBuilder<CanonicalGroupedRow>(
            workMeter: workMeter,
            stage: .aggregateInput,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalGroupedRow.self),
            expectedCount: groups.count
        )
        for group in groups {
            try workMeter.consume(at: .aggregateInput)
            let value = try await evaluateBoundScopedValue(
                bound,
                on: group.representative,
                group: group,
                context: evaluationContext,
                workMeter: workMeter
            )
            do {
                var isIncluded = false
                try value.withValue { borrowed in
                    isIncluded = try DatabaseExpressionEvaluator(
                        fields: ["value": borrowed]
                    ).predicate(.column(ColumnRef("value")))
                }
                if isIncluded {
                    try result.append(
                        footprint: try canonicalGroupedRowFootprint(
                            group,
                            workMeter: workMeter
                        ),
                        make: { group }
                    )
                }
            } catch let error as DatabaseExpressionEvaluationError {
                throw CanonicalReadError.expressionEvaluation(error)
            }
        }
        return try result.finish().moveToSharedOwnership(at: .aggregateInput)
    }

    private func applyGroupedOrder(
        _ orderBy: [SortKey]?,
        boundOrder: [CanonicalBoundExpression],
        to groups: CanonicalRetainedGroups,
        projection: [ProjectionItem],
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedGroups {
        guard let orderBy, !orderBy.isEmpty else { return groups }
        guard orderBy.count == boundOrder.count else {
            throw CanonicalReadError.unsupportedSelectQuery("Grouped sort keys differ from their executable bindings")
        }
        let outerArrayFootprint = try DatabaseIntermediateCollectionMeter
            .arrayFootprint(
                count: groups.count,
                element: (
                    CanonicalGroupedRow,
                    [ProjectedQueryValue]
                ).self
            )
        let nestedValuesFootprint = try DatabaseIntermediateCollectionMeter
            .arrayFootprint(
                count: orderBy.count,
                element: ProjectedQueryValue.self
            )
            .multiplied(by: UInt64(groups.count))
        let decorationFootprint = try outerArrayFootprint
            .multiplied(by: 2)
            .adding(nestedValuesFootprint)
        let decorationRows = try DatabaseIntermediateFootprint(
            rows: UInt64(groups.count)
        ).multiplied(by: 2).rows
        let decorationReservation = try workMeter.reserveIntermediate(
            rows: decorationRows,
            bytes: decorationFootprint.bytes,
            at: .sortInput
        )
        defer { decorationReservation.release() }
        try workMeter.consume(UInt64(groups.count), at: .sortInput)
        var decorated: [(
            CanonicalGroupedRow,
            [ProjectedQueryValue]
        )] = []
        decorated.reserveCapacity(groups.count)
        for group in groups {
            var values: [ProjectedQueryValue] = []
            values.reserveCapacity(orderBy.count)
            for (index, _) in orderBy.enumerated() {
                values.append(
                    try await evaluateBoundValue(
                        boundOrder[index],
                        on: group.representative,
                        group: group,
                        context: evaluationContext,
                        workMeter: workMeter
                    )
                )
            }
            decorated.append((group, values))
        }
        let sorted: [(
            CanonicalGroupedRow,
            [ProjectedQueryValue]
        )]
        do {
            sorted = try decorated.sorted { lhs, rhs in
                for (index, sortKey) in orderBy.enumerated() {
                    try workMeter.consume(2, at: .sortComparison)
                    let comparison = try Self.selectedOrdering(lhs.1[index].value, rhs.1[index].value,
                        bound: boundOrder[index], using: sortKey,
                        environment: evaluationContext?.options.temporalEnvironment ?? inheritedSQLTemporalEnvironment, workMeter: workMeter)
                    guard comparison != .equal else { continue }
                    return comparison == .lessThan
                }
                try workMeter.consume(2, at: .sortComparison)
                return try lhs.0.key.identity.lexicographicallyPrecedes(
                    rhs.0.key.identity, by: { left, right in
                        try CanonicalSQLValueIdentity.precedes(left, right,
                            environment: evaluationContext?.options.temporalEnvironment ?? inheritedSQLTemporalEnvironment,
                            workMeter: workMeter)
                    }
                )
            }
        } catch let failure as FieldValueComparisonError {
            throw canonicalComparisonReadError(
                failure,
                operation: "aggregate ordering"
            )
        }
        var retained = try DatabaseRetainedArrayBuilder<CanonicalGroupedRow>(
            workMeter: workMeter,
            stage: .sortInput,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalGroupedRow.self),
            expectedCount: sorted.count
        )
        for item in sorted {
            let group = item.0
            try retained.append(
                footprint: try canonicalGroupedRowFootprint(
                    group,
                    workMeter: workMeter
                ),
                make: { group }
            )
        }
        return try retained.finish().moveToSharedOwnership(at: .sortInput)
    }

    private func projectionReferenceIndex(
        _ expression: Expression, projection: [ProjectionItem],
        sourceSchema: CanonicalRelationSchema, prefersInput: Bool
    ) throws -> Int? {
        let items = try expandedProjectionItems(projection, sourceSchema: sourceSchema)
        if case .literal(.int(let position)) = expression {
            guard position > 0, let ordinal = Int(exactly: position), ordinal <= items.count else {
                throw CanonicalReadError.unsupportedSelectQuery("Output ordinal is outside the projection")
            }
            return ordinal - 1
        }
        guard case .column(let reference) = expression, reference.table == nil else { return nil }
        if prefersInput, !sourceSchema.layout.matching(reference).isEmpty { return nil }
        let indices = items.indices.filter { (items[$0].alias ?? canonicalProjectionName(for: items[$0].expression, index: $0)) == reference.column }
        guard let index = indices.first else { return nil }
        guard indices.allSatisfy({ candidate in
            let expression = items[candidate].expression
            let selected = items[index].expression
            if expression == selected { return true }
            guard case .column(let left) = expression, case .column(let right) = selected else { return false }
            let leftSlots = sourceSchema.layout.matching(left)
            let rightSlots = sourceSchema.layout.matching(right)
            return leftSlots.count == 1 && rightSlots.count == 1 && leftSlots[0] == rightSlots[0]
        }) else {
            throw CanonicalReadError.expressionEvaluation(.ambiguousColumn(reference.displayName))
        }
        return index
    }

    private func expandedProjectionItems(
        _ projection: [ProjectionItem], sourceSchema: CanonicalRelationSchema
    ) throws -> [(expression: Expression, alias: String?)] {
        var result: [(expression: Expression, alias: String?)] = []
        for item in projection {
            switch item {
            case .expression(let expression, let alias): result.append((expression, alias))
            case .wildcard(let scope):
                for index in try sourceSchema.wildcardSlots(scope: scope) {
                    let slot = sourceSchema.layout.slots[index]
                    result.append((.column(ColumnRef(table: slot.scope, column: slot.name)), nil))
                }
            }
        }
        return result
    }

    private func groupedOrderExpression(
        _ expression: Expression, projection: [ProjectionItem],
        sourceSchema: CanonicalRelationSchema? = nil, prefersInput: Bool = false
    ) throws -> Expression {
        guard let sourceSchema else { return expression }
        if let index = try projectionReferenceIndex(expression, projection: projection,
            sourceSchema: sourceSchema, prefersInput: prefersInput) {
            return try expandedProjectionItems(projection, sourceSchema: sourceSchema)[index].expression
        }
        return expression
    }

    private func resolvedOrderBy(
        _ orderBy: [SortKey]?,
        projection: [ProjectionItem],
        sourceSchema: CanonicalRelationSchema? = nil
    ) throws -> [SortKey]? {
        try orderBy?.map { key in
            SortKey(try groupedOrderExpression(key.expression, projection: projection,
                sourceSchema: sourceSchema), direction: key.direction, nulls: key.nulls)
        }
    }

    private func canonicalQueryWithResolvedReferences(
        _ query: SelectQuery,
        namedSubqueries: [NamedSubquery],
        queryBinding: CanonicalQueryBindingEnvironment? = nil,
        outerRow: CanonicalSourceRow?
    ) throws -> SelectQuery {
        guard !query.source.usesSPARQLSemantics,
              !sourceRequiresRuntimeInferredSchema(query.source, namedSubqueries: namedSubqueries) else { return query }
        let schema = try canonicalRelationSchema(for: query.source, namedSubqueries: namedSubqueries, queryBinding: queryBinding,
            outerSchemas: try outerRow.map { [try relationSchema(for: $0.layout)] } ?? [])
        return try resolvedQueryReferences(query, sourceSchema: schema, queryBinding: queryBinding)
    }

    private func locatingBindingReference<Value>(
        _ binding: CanonicalQueryBindingEnvironment?,
        resolve: () throws -> Value
    ) throws -> Value {
        do { return try resolve() }
        catch let cause as CanonicalReadError {
            try throwLocatedBindingFailure(cause, binding: binding)
        } catch let cause as DatabaseExpressionEvaluationError {
            try throwLocatedBindingFailure(cause, binding: binding)
        }
    }

    private func throwLocatedBindingFailure(_ cause: CanonicalReadError,
        binding: CanonicalQueryBindingEnvironment?
    ) throws -> Never {
        try throwLocatedBindingFailure(.read(cause), binding: binding)
    }

    private func throwLocatedBindingFailure(_ cause: DatabaseExpressionEvaluationError,
        binding: CanonicalQueryBindingEnvironment?
    ) throws -> Never {
        try throwLocatedBindingFailure(.expression(cause), binding: binding)
    }

    private func throwLocatedBindingFailure(_ cause: CanonicalSQLBindingCause,
        binding: CanonicalQueryBindingEnvironment?
    ) throws -> Never {
        guard let binding, binding.occurrences != nil else {
            switch cause {
            case .read(let error): throw error
            case .expression(let error): throw error
            }
        }
        throw CanonicalSQLBindingFailure(cause: cause, occurrence: binding.occurrence,
            root: binding.root ?? binding.occurrence)
    }

    private func resolvedQueryReferences(
        _ query: SelectQuery,
        sourceSchema: CanonicalRelationSchema,
        queryBinding: CanonicalQueryBindingEnvironment? = nil
    ) throws -> SelectQuery {
        let ordering = try query.orderBy?.enumerated().map { index, key in
            try locatingBindingReference(queryBinding?.child(.orderBy, index: UInt32(index))) {
                SortKey(try groupedOrderExpression(key.expression, projection: query.projection,
                    sourceSchema: sourceSchema), direction: key.direction, nulls: key.nulls)
            }
        }
        return query.replacing(orderBy: ordering)
    }

    private func projectGroupedRows(
        _ groups: CanonicalRetainedGroups, projection: [ProjectionItem], columns: [QueryColumn],
        sourceSchema: CanonicalRelationSchema, workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?, boundPlan suppliedPlan: CanonicalBoundSelect? = nil
    ) async throws -> CanonicalRetainedQueryRows {
        guard let plan = suppliedPlan ?? evaluationContext?.boundPlan else {
            throw CanonicalReadError.unsupportedSelectQuery("Grouped projection has no executable SELECT binding")
        }
        var output = try DatabaseRetainedQueryRowsBuilder(columns: columns, workMeter: workMeter,
            stage: .projection, expectedCount: groups.count, payloadLeases: [groups.payloadLease], schemaLeases: [])
        var resultLeases: [DatabaseQueryPayloadLease] = []
        let leaseClaim = try workMeter.reserveIntermediate(bytes: 64, at: .projection)
        for group in groups {
            var values: [QueryValue] = []
            let scratch = try workMeter.reserveIntermediate(bytes: UInt64(columns.count) * 128, at: .projection)
            defer { scratch.release() }
            for bound in plan.projection {
                let cell = try await evaluateBoundValue(bound, on: group.representative, group: group,
                    context: evaluationContext, workMeter: workMeter)
                try leaseClaim.reserveAdditional(bytes: UInt64(cell.leases.count) * 64, at: .projection)
                resultLeases.append(contentsOf: cell.leases)
                values.append(cell.value)
            }
            try output.append(QueryRow(values: values, columns: columns, annotations: group.representative.annotations))
        }
        let rows = try output.finish().moveToSharedOwnership(at: .projection)
        return DatabaseSharedRetainedQueryRows(storage: try retainedQueryRowStorage(rows, workMeter: workMeter),
            columns: columns, payloadLeases: rows.originPayloadLeases + resultLeases + [DatabaseQueryPayloadLease(retaining: leaseClaim)],
            schemaLeases: rows.schemaLeases)
    }

    private func retainedQueryRowStorage(_ rows: DatabaseSharedRetainedQueryRows,
        workMeter: DatabaseWorkMeter) throws -> DatabaseSharedRetainedArray<QueryRow> {
        var builder = try DatabaseRetainedArrayBuilder<QueryRow>(workMeter: workMeter, stage: .projection,
            layout: DatabaseRetainedArrayLayout.forElement(QueryRow.self), expectedCount: rows.count)
        for index in 0..<rows.count {
            try rows.withElement(at: index) { row in
                try builder.append(footprint: CanonicalRelationalFootprintMeter.positionalValueArrayFootprint(
                    count: row.values.count, valueAt: { _ in .primitive(.null) }, annotations: row.annotations,
                    version: row.version, workMeter: workMeter, stage: .projection), make: { copy row })
            }
        }
        return try builder.finish().moveToSharedOwnership(at: .projection)
    }

    private func validateGroupedWildcardProjection(
        _ projection: [ProjectionItem], sourceSchema: CanonicalRelationSchema, groupBy: [Expression]
    ) throws {
        for item in projection {
            guard case .wildcard(let qualifier) = item else { continue }
            for index in try sourceSchema.wildcardSlots(scope: qualifier) {
                let slot = sourceSchema.layout.slots[index]
                let qualified = Expression.column(ColumnRef(table: slot.scope, column: slot.name))
                let unqualified = Expression.column(ColumnRef(slot.name))
                guard groupBy.contains(qualified) || (sourceSchema.occurrenceCount(of: slot.name) == 1 && groupBy.contains(unqualified)) else {
                    throw CanonicalReadError.aggregateEvaluation(.invalidGroupedExpression(
                        "Wildcard projection contains non-grouped column '\(slot.name)'"))
                }
            }
        }
    }

    private func evaluateNumericAggregate(
        _ aggregate: AggregateFunction,
        bound: CanonicalBoundExpression,
        rows: CanonicalRetainedRows,
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> ProjectedQueryValue {
        guard let input = bound.children.first else {
            throw CanonicalReadError.unsupportedSelectQuery("A numeric aggregate has no selected input type")
        }
        let isAverage: Bool
        let distinct: Bool
        switch aggregate {
        case .sum(_, let selectedDistinct): isAverage = false; distinct = selectedDistinct
        case .avg(_, let selectedDistinct): isAverage = true; distinct = selectedDistinct
        default:
            throw CanonicalReadError.unsupportedSelectQuery("A numeric aggregate requires SUM or AVG")
        }
        let distinctClaim: DatabaseIntermediateReservation?
        var seen = try CanonicalSQLIdentitySet(workMeter: workMeter,
            stage: .aggregateInput, expectedCount: distinct ? rows.count : 0)
        if distinct {
            let footprint = try DatabaseIntermediateFootprint(
                bytes: UInt64(MemoryLayout<CanonicalSQLValueIdentity>.stride + 32)).multiplied(by: UInt64(rows.count))
            distinctClaim = try workMeter.reserveIntermediate(rows: UInt64(rows.count), bytes: footprint.bytes,
                at: .aggregateInput)
        } else { distinctClaim = nil }
        defer { distinctClaim?.release() }
        var accumulator = DatabaseNumericAggregateAccumulator(inputType: input.type, workMeter: workMeter)
        do {
            for row in rows {
                try workMeter.consume(at: .aggregateInput)
                let cell = try await evaluateBoundValue(input, on: row,
                    context: evaluationContext, workMeter: workMeter)
                if cell.value.isNull { continue }
                if distinct {
                    let identity = try canonicalQueryValueIdentity(cell.value,
                        operation: isAverage ? "AVG(DISTINCT)" : "SUM(DISTINCT)", type: input.type, workMeter: workMeter)
                    guard try seen.insert(identity) else { continue }
                }
                try accumulator.add(cell.value)
            }
            let result = try isAverage ? accumulator.average() : accumulator.sum()
            return ProjectedQueryValue(value: try bound.normalize(result ?? .primitive(.null)),
                leases: [], payloadIsRetained: false)
        } catch let failure as DatabaseNumericAggregateAccumulator.Failure {
            throw CanonicalReadError.aggregateEvaluation(
                aggregateNumericError(function: isAverage ? "AVG" : "SUM", failure: failure))
        }
    }

    private func evaluateAggregate(
        _ aggregate: AggregateFunction,
        bound: CanonicalBoundExpression? = nil,
        rows: CanonicalRetainedRows,
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> DatabaseQueryScopedFieldValue {
        let functionName: String
        let expression: Expression?
        let distinct: Bool
        let orderedRows: CanonicalRetainedRows
        switch aggregate {
        case .sql:
            throw CanonicalReadError.unsupportedSelectQuery("SQL aggregate requires its selected QueryValue path")
        case .count(let value, let isDistinct):
            functionName = "COUNT"
            expression = value
            distinct = isDistinct
            orderedRows = rows
        case .sum(let value, let isDistinct):
            functionName = "SUM"
            expression = value
            distinct = isDistinct
            orderedRows = rows
        case .avg(let value, let isDistinct):
            functionName = "AVG"
            expression = value
            distinct = isDistinct
            orderedRows = rows
        case .min(let value):
            functionName = "MIN"
            expression = value
            distinct = false
            orderedRows = rows
        case .max(let value):
            functionName = "MAX"
            expression = value
            distinct = false
            orderedRows = rows
        case .groupConcat(let value, _, let isDistinct):
            functionName = "GROUP_CONCAT"
            expression = value
            distinct = isDistinct
            orderedRows = rows
        case .sample(let value):
            functionName = "SAMPLE"
            expression = value
            distinct = false
            orderedRows = rows
        case .arrayAgg(let value, let orderBy, let isDistinct):
            functionName = "ARRAY_AGG"
            expression = value
            distinct = isDistinct
            orderedRows = try await applyOrder(
                orderBy,
                boundOrder: bound.map { Array($0.children.dropFirst()) } ?? [],
                to: rows,
                workMeter: workMeter,
                evaluationContext: evaluationContext
            )
        }

        if expression == nil, distinct {
            throw CanonicalReadError.aggregateEvaluation(
                .invalidGroupedExpression("COUNT(DISTINCT *) is not valid")
            )
        }

        var aggregateScratch = try DatabaseIntermediateCollectionMeter
            .arrayFootprint(
                count: orderedRows.count,
                element: FieldValue.self
            ).adding(
                DatabaseIntermediateCollectionMeter.arrayFootprint(
                    count: orderedRows.count,
                    element: DatabaseQueryScopedFieldValue.self
                )
            )
        if case .groupConcat = aggregate {
            aggregateScratch = try aggregateScratch.adding(
                DatabaseIntermediateCollectionMeter.arrayFootprint(
                    count: orderedRows.count,
                    element: String.self
                )
            )
        }
        if distinct {
            aggregateScratch = try aggregateScratch
                .adding(
                    try DatabaseIntermediateCollectionMeter.arrayFootprint(
                        count: orderedRows.count,
                        element: FieldValue.self
                    )
                )
                .adding(
                    try DatabaseIntermediateCollectionMeter.arrayFootprint(
                        count: orderedRows.count,
                        element: DatabaseQueryScopedFieldValue.self
                    )
                )
                .adding(
                    try DatabaseIntermediateFootprint(
                        bytes: UInt64(
                            max(1, MemoryLayout<FieldValue>.stride + 32)
                        )
                    ).multiplied(by: UInt64(orderedRows.count))
                )
        }
        let aggregateScratchRows = try DatabaseIntermediateFootprint(
            rows: UInt64(orderedRows.count)
        ).multiplied(by: distinct ? 4 : 1).rows
        let aggregateScratchReservation = try workMeter.reserveIntermediate(
            rows: aggregateScratchRows,
            bytes: aggregateScratch.bytes,
            at: .aggregateInput
        )
        defer { aggregateScratchReservation.release() }
        var scopedValues: [DatabaseQueryScopedFieldValue] = []
        scopedValues.reserveCapacity(orderedRows.count)
        var values: [FieldValue] = []
        values.reserveCapacity(orderedRows.count)
        for row in orderedRows {
            try workMeter.consume(at: .aggregateInput)
            if expression != nil {
                guard let child = bound?.children.first else {
                    throw CanonicalReadError.unsupportedSelectQuery("An aggregate input has no executable binding")
                }
                let scopedValue = try await evaluateBoundScopedValue(
                    child,
                    on: row,
                    context: evaluationContext,
                    workMeter: workMeter
                )
                scopedValues.append(scopedValue)
                scopedValue.withValue {
                    values.append($0)
                }
            } else {
                scopedValues.append(.borrowing(.bool(true)))
                values.append(.bool(true))
            }
        }
        if distinct {
            var seen = Set<FieldValue>()
            var distinctValues: [FieldValue] = []
            var distinctScopedValues: [DatabaseQueryScopedFieldValue] = []
            distinctValues.reserveCapacity(values.count)
            distinctScopedValues.reserveCapacity(scopedValues.count)
            for (value, scopedValue) in zip(values, scopedValues) {
                let identity = try canonicalValueIdentity(
                    value,
                    operation: "\(functionName)(DISTINCT)", type: bound?.children.first?.type, workMeter: workMeter
                )
                if seen.insert(identity).inserted {
                    distinctValues.append(value)
                    distinctScopedValues.append(scopedValue)
                }
            }
            values = distinctValues
            scopedValues = distinctScopedValues
        }

        func retainingResult(
            _ value: FieldValue
        ) throws -> DatabaseQueryScopedFieldValue {
            try .retaining(
                value,
                workMeter: workMeter,
                stage: .aggregateInput
            )
        }

        switch aggregate {
        case .sql:
            throw CanonicalReadError.unsupportedSelectQuery("SQL aggregate requires its selected QueryValue path")
        case .count(let expression, _):
            let count = expression == nil
                ? values.count
                : values.lazy.filter { !$0.isNull }.count
            guard let result = Int64(exactly: count) else {
                throw CanonicalReadError.aggregateEvaluation(.countOverflow)
            }
            return try retainingResult(.int64(result))

        case .sum, .avg:
            // Selected SUM/AVG are dispatched before the primitive aggregate owner.
            throw CanonicalReadError.unsupportedSelectQuery("Numeric aggregate requires its selected QueryValue path")

        case .min, .max:
            var result: FieldValue?
            for value in values where !value.isNull {
                guard let current = result else {
                    result = value
                    continue
                }
                do {
                    let comparison = try FieldValueComparator.compare(
                        value,
                        current
                    )
                    if (functionName == "MIN" && comparison == .lessThan)
                        || (functionName == "MAX" && comparison == .greaterThan) {
                        result = value
                    }
                } catch let failure {
                    switch failure {
                    case .incomparable(let left, let right):
                        throw CanonicalReadError.aggregateEvaluation(
                            .incomparable(
                                function: functionName,
                                left: left,
                                right: right
                            )
                        )
                    case .unorderedFloatingPoint:
                        throw CanonicalReadError.aggregateEvaluation(
                            .nonFiniteValue(function: functionName)
                        )
                    }
                }
            }
            return try retainingResult(result ?? .null)

        case .groupConcat(_, let separator, _):
            var strings: [String] = []
            strings.reserveCapacity(values.count)
            for value in values where !value.isNull {
                guard case .string(let string) = value else {
                    throw CanonicalReadError.aggregateEvaluation(
                        .invalidStringValue(function: functionName)
                    )
                }
                strings.append(string)
            }
            guard !strings.isEmpty else {
                return try retainingResult(.null)
            }
            let separatorBytes = UInt64((separator ?? ",").utf8.count)
            var outputBytes = try CanonicalRelationalFootprintMeter.prospectiveValueFootprint().bytes
            for string in strings {
                outputBytes = try DatabaseIntermediateFootprint(
                    bytes: outputBytes
                ).adding(
                    DatabaseIntermediateFootprint(
                        bytes: UInt64(string.utf8.count)
                    )
                ).bytes
            }
            if strings.count > 1 {
                outputBytes = try DatabaseIntermediateFootprint(
                    bytes: outputBytes
                ).adding(
                    try DatabaseIntermediateFootprint(
                        bytes: separatorBytes
                    ).multiplied(by: UInt64(strings.count - 1))
                ).bytes
            }
            let outputFootprint = DatabaseIntermediateFootprint(
                bytes: outputBytes
            )
            try DatabaseByteProcessingMeter.consume(
                byteCount: outputFootprint.bytes,
                workMeter: workMeter,
                stage: .aggregateInput
            )
            return try .producing(
                maximumFootprint: outputFootprint,
                workMeter: workMeter,
                stage: .aggregateInput
            ) {
                .string(strings.joined(separator: separator ?? ","))
            }

        case .sample:
            return try retainingResult(
                values.first(where: { !$0.isNull }) ?? .null
            )

        case .arrayAgg:
            guard !values.isEmpty else {
                return try retainingResult(.null)
            }
            let outputFootprint = try prospectiveArrayValueFootprint(
                values,
                workMeter: workMeter,
                stage: .aggregateInput
            )
            return try .producing(
                maximumFootprint: outputFootprint,
                workMeter: workMeter,
                stage: .aggregateInput
            ) {
                .array(values)
            }
        }
    }

    private func aggregateNumericError(
        function: String,
        failure: DatabaseNumericAggregateAccumulator.Failure
    ) -> DatabaseAggregateEvaluationError {
        switch failure {
        case .incompatibleNumericKinds:
            return .incompatibleNumericKinds(function: function)
        case .nonNumericValue:
            return .nonNumericValue(function: function)
        case .nonFiniteValue:
            return .nonFiniteValue(function: function)
        case .numericOverflow:
            return .numericOverflow(function: function)
        case .resultNotRepresentable:
            return .resultNotRepresentable(function: function)
        }
    }

    private static func evaluateSelectedKernel(
        _ expression: DatabaseKit.Expression,
        on row: CanonicalSourceRow,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseQueryScopedFieldValue {
        var retainedLifetimes = try DatabaseRetainedArrayBuilder<DatabaseIntermediateReservation>(
            workMeter: workMeter, stage: .expressionEvaluation,
            layout: DatabaseRetainedArrayLayout.forElement(DatabaseIntermediateReservation.self))
        let admission = try preadmitExpressionPayload(expression, on: row, workMeter: workMeter,
            retainedLifetimes: &retainedLifetimes,
            forceRootOwnership: canonicalExpressionResultRequiresOwnedPayload(expression))
        let lifetimes = try retainedLifetimes.finish().moveToSharedOwnership(at: .expressionEvaluation)
        defer { withExtendedLifetime(lifetimes) {} }
        let evaluator = DatabaseExpressionEvaluator(fields: [:], workMeter: workMeter)
        if let admission {
            return try .producing(maximumFootprint: admission.maximumFootprint,
                reservation: admission.reservation, stage: .expressionEvaluation) {
                    try evaluator.evaluate(expression)
                }
        }
        return .borrowing(try evaluator.evaluate(expression))
    }

    private func executeExists(_ query: SelectQuery, plan: CanonicalBoundSelect,
        outerRow: CanonicalSourceRow, context: CanonicalQueryEvaluationContext?
    ) async throws -> Bool {
        guard let context else {
            throw CanonicalReadError.unsupportedSelectQuery("EXISTS requires an admitted query scope")
        }
        let selected = CanonicalBoundSelect(columns: [], projection: [], filter: plan.filter,
            groupBy: [], having: nil, orderBy: [], sourcePlan: plan.sourcePlan,
            pagination: plan.pagination, ctes: plan.ctes,
            metadataLease: plan.metadataLease, bindingMetadata: plan.bindingMetadata, grouping: plan.grouping, windows: plan.windows)
        let active = CanonicalQueryEvaluationContext(options: executionContextWithoutExternalPageWindow(context.options),
            transaction: context.transaction, partitionValues: context.partitionValues,
            partitionMode: context.partitionMode, namedSubqueries: context.namedSubqueries,
            queryBinding: context.queryBinding, boundPlan: selected, cteEnvironment: context.cteEnvironment,
            outerRow: outerRow, preparedFusionGraph: context.preparedFusionGraph, fusionSession: context.fusionSession)
        let producer = try await makeSelectProducer(query.replacing(projection: []).replacing(orderBy: nil),
            plan: selected, context: active)
        do {
            let exists = try await producer.next() != nil
            try await producer.close()
            return exists
        } catch {
            let primary = error
            do { try await producer.close() }
            catch { throw SQLRelationCleanupError(operationError: primary, cleanupError: error) }
            throw primary
        }
    }

    private func executeNestedQuery(
        _ query: SelectQuery,
        boundPlan: CanonicalBoundSelect? = nil,
        outerRow: CanonicalSourceRow,
        context: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedQueryResponse {
        guard let context else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Subquery evaluation requires a transaction-bound query context"
            )
        }
        return try await queryCanonical(
            query,
            options: executionContextWithoutExternalPageWindow(context.options),
            partitionValues: context.partitionValues,
            partitionMode: context.partitionMode,
            transaction: context.transaction,
            inheritedSubqueries: context.namedSubqueries, queryBinding: context.queryBinding,
            boundPlan: boundPlan,
            outerRow: outerRow,
            cteEnvironment: context.cteEnvironment,
            preparedFusionGraph: context.preparedFusionGraph,
            fusionSession: context.fusionSession
        )
    }

    private func canonicalProjectionName(
        for expression: DatabaseKit.Expression,
        index: Int
    ) -> String {
        switch expression {
        case .column(let column):
            return column.column
        case .variable(let variable):
            let name = variable.name
            return name.hasPrefix("?") || name.hasPrefix("$") ? String(name.dropFirst()) : name
        case .aggregate(let aggregate):
            switch aggregate {
            case .count: return "count"
            case .sum: return "sum"
            case .avg: return "avg"
            case .min: return "min"
            case .max: return "max"
            case .groupConcat: return "group_concat"
            case .sample: return "sample"
            case .arrayAgg: return "array_agg"
            case .sql(let call): return call.function.rawValue
            }
        default:
            return "column\(index)"
        }
    }

    private func canonicalUniqueRows(
        _ rows: CanonicalRetainedQueryRows,
        workMeter: DatabaseWorkMeter
    ) throws -> CanonicalRetainedQueryRows {
        let setBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(
                max(1, MemoryLayout<[CanonicalSQLValueIdentity]>.stride + 32)
            )
        ).multiplied(by: UInt64(rows.count)).bytes
        let setReservation = try workMeter.reserveIntermediate(
            bytes: setBytes,
            at: .deduplication
        )
        defer { setReservation.release() }
        var seen = try CanonicalSQLIdentitySet(workMeter: workMeter,
            stage: .deduplication, expectedCount: rows.count)
        var unique = try DatabaseRetainedArrayBuilder<QueryRow>(
            workMeter: workMeter,
            stage: .deduplication,
            layout: try DatabaseRetainedArrayLayout.forElement(QueryRow.self),
            expectedCount: rows.count
        )
        for index in 0..<rows.count {
            try rows.withElement(at: index) { row in
                try workMeter.consume(at: .deduplication)
                let identityFootprint = try CanonicalRelationalFootprintMeter.positionalRowFootprint(
                    values: row.values, annotations: [:], version: nil,
                    workMeter: workMeter, stage: .deduplication
                ).adding(identityArrayAdditionalFootprint(count: row.values.count))
                try setReservation.reserveAdditional(bytes: identityFootprint.bytes, at: .deduplication)
                var identity: [CanonicalSQLValueIdentity] = []
                identity.reserveCapacity(row.values.count)
                for (index, value) in row.values.enumerated() {
                    identity.append(try canonicalQueryValueIdentity(value, operation: "SELECT DISTINCT", type: rows.columns[index].type, workMeter: workMeter))
                }
                if try seen.insert(identity) {
                    try unique.append(
                        footprint: CanonicalRelationalFootprintMeter.positionalValueArrayFootprint(
                            count: row.values.count, valueAt: { _ in .primitive(.null) },
                            annotations: row.annotations, version: row.version, workMeter: workMeter, stage: .deduplication),
                        make: { copy row }
                    )
                } else {
                    setReservation.releaseGuaranteedPartial(bytes: identityFootprint.bytes)
                }
            }
        }
        return DatabaseSharedRetainedQueryRows(
            storage: try unique.finish().moveToSharedOwnership(at: .deduplication),
            columns: rows.columns, payloadLeases: rows.originPayloadLeases, schemaLeases: rows.schemaLeases
        )
    }

    private func canonicalGroupedRowFootprint(
        _ group: CanonicalGroupedRow,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        var footprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: group.key.values.count,
                element: QueryValue.self
            ).adding(
            try DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: group.key.identity.count,
                element: CanonicalSQLValueIdentity.self
            )
        )
        for value in group.key.values {
            footprint = try footprint.adding(
                CanonicalRelationalFootprintMeter.valueFootprint(
                    of: value,
                    workMeter: workMeter,
                    stage: .aggregateInput
                )
            )
        }
        for identity in group.key.identity {
            footprint = try footprint.adding(
                identity.payloadFootprint(workMeter: workMeter, stage: .aggregateInput)
            )
        }
        footprint = try footprint.adding(
            CanonicalRelationalFootprintMeter.sourceAliasFootprint(
                of: group.representative,
                workMeter: workMeter
            )
        )
        return footprint
    }

    private func prospectiveBindingPlanBytes(_ query: SelectQuery, workMeter: DatabaseWorkMeter) throws -> UInt64 {
        enum Visit {
            case expression(Expression)
            case source(DataSource)
            case statement(SelectQuery)
        }
        let layout = try DatabaseRetainedArrayLayout.forElement(Visit.self)
        let reservation = try workMeter.reserveIntermediate(bytes: layout.containerByteCount, at: .bindingCandidate)
        defer { reservation.release() }
        var stack: [Visit] = []
        var capacity = 0
        var bytes: UInt64 = 0
        func addBytes(_ amount: UInt64) throws {
            let next = bytes.addingReportingOverflow(amount)
            guard !next.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
            bytes = next.partialValue
        }
        func push(_ visit: Visit) throws {
            try workMeter.checkpoint(at: .bindingCandidate)
            let required = stack.count.addingReportingOverflow(1)
            guard !required.overflow else {
                throw DatabaseRetainedArrayLayoutError.capacityOverflow(currentCapacity: capacity)
            }
            let growth = try layout.growth(from: capacity, toFit: required.partialValue)
            if growth.capacity > capacity {
                try reservation.reserveAdditional(bytes: growth.additionalByteCount, at: .bindingCandidate)
                stack.reserveCapacity(growth.capacity)
                if stack.capacity > growth.capacity {
                    let excess = try DatabaseIntermediateFootprint(bytes: layout.elementCapacitySlotByteCount)
                        .multiplied(by: UInt64(stack.capacity - growth.capacity))
                    try reservation.reserveAdditional(bytes: excess.bytes, at: .bindingCandidate)
                }
                capacity = stack.capacity
            }
            stack.append(visit)
        }
        try push(.statement(query))
        while let visit = stack.popLast() {
            try workMeter.consume(at: .bindingCandidate)
            try addBytes(1024)
            // Reverse pushes preserve the recursive traversal's depth-first work order.
            switch visit {
            case .expression(let value):
                switch value {
                case .concatenate(let left, let right), .add(let left, let right), .subtract(let left, let right),
                     .multiply(let left, let right), .divide(let left, let right), .modulo(let left, let right),
                     .equal(let left, let right), .notEqual(let left, let right), .lessThan(let left, let right),
                     .lessThanOrEqual(let left, let right), .greaterThan(let left, let right),
                     .greaterThanOrEqual(let left, let right), .and(let left, let right), .or(let left, let right),
                     .nullIf(let left, let right), .isDistinctFrom(let left, let right, _),
                     .collectionOperation(_, let left, let right):
                    try push(.expression(right)); try push(.expression(left))
                case .arrayAccess(let base, let subscripts):
                    for item in subscripts.reversed() {
                        switch item {
                        case .index(let value): try push(.expression(value))
                        case .slice(let lower, let upper):
                            if let upper { try push(.expression(upper)) }
                            if let lower { try push(.expression(lower)) }
                        }
                    }
                    try push(.expression(base))
                case .patternMatch(let subject, let pattern, let escape, _):
                    if let escape { try push(.expression(escape)) }
                    try push(.expression(pattern)); try push(.expression(subject))
                case .collate(let child, _), .field(let child, _), .fieldExpansion(let child),
                     .negate(let child), .not(let child), .truthTest(let child, _, _), .isNull(let child),
                     .isNotNull(let child), .regex(let child, _, _), .cast(let child, _), .isTriple(let child),
                     .subject(let child), .predicate(let child), .object(let child):
                    try push(.expression(child))
                case .between(let child, let low, let high):
                    try push(.expression(high)); try push(.expression(low)); try push(.expression(child))
                case .inList(let child, let members), .notInList(let child, let members):
                    for member in members.reversed() { try push(.expression(member)) }
                    try push(.expression(child))
                case .function(let call):
                    for argument in call.arguments.reversed() { try push(.expression(argument)) }
                case .caseWhen(let operand, let pairs, let fallback):
                    if let fallback { try push(.expression(fallback)) }
                    for pair in pairs.reversed() {
                        try push(.expression(pair.result)); try push(.expression(pair.condition))
                    }
                    if let operand { try push(.expression(operand)) }
                case .array(let members), .row(let members), .coalesce(let members), .greatest(let members), .least(let members):
                    for member in members.reversed() { try push(.expression(member)) }
                case .window(let call): try canonicalVisitWindowExpressions(call) { try push(.expression($0)) }
                case .grouping(let arguments): for value in arguments.reversed() { try push(.expression(value)) }
                case .aggregate(let aggregate):
                    switch aggregate {
                    case .sql(let call): try canonicalVisitAggregateExpressions(call) { try push(.expression($0)) }
                    case .count(let child, _):
                        if let child { try push(.expression(child)) }
                    case .sum(let child, _), .avg(let child, _), .min(let child), .max(let child),
                         .sample(let child), .groupConcat(let child, _, _):
                        try push(.expression(child))
                    case .arrayAgg(let child, let keys, _):
                        for key in (keys ?? []).reversed() { try push(.expression(key.expression)) }
                        try push(.expression(child))
                    }
                case .arraySubquery(let nested), .subquery(let nested), .exists(let nested):
                    try push(.statement(nested))
                case .quantifiedComparison(let lhs, _, _, let source):
                    switch source {
                    case .array(let value): try push(.expression(value))
                    case .subquery(let query): try push(.statement(query))
                    }
                    try push(.expression(lhs))
                case .inSubquery(let child, let nested):
                    try push(.statement(nested)); try push(.expression(child))
                case .triple(let subject, let predicate, let object):
                    try push(.expression(object)); try push(.expression(predicate)); try push(.expression(subject))
                case .wholeRow, .currentValue, .literal, .column, .variable, .parameter, .bound: break
                }
            case .source(let value):
                switch value {
                case .subquery(let nested): try push(.statement(nested))
                case .aliased(let child, _, _): try push(.source(child))
                case .join(let join):
                    if case .on(let condition) = join.condition { try push(.expression(condition)) }
                    try push(.source(join.right)); try push(.source(join.left))
                case .setOperation(_, _, let left, let right):
                    try push(.statement(right)); try push(.statement(left))
                case .values(let rows):
                    for row in rows.reversed() {
                        try addBytes(128)
                        for expression in row.reversed() { try push(.expression(expression)) }
                    }
                case .graphTable(let table):
                    for item in (table.columns ?? []).reversed() { try push(.expression(item.expression)) }
                #if DATABASE_MULTI_BASE
                case .base(_, let child): try push(.source(child))
                #endif
                case .table, .logical, .graphPattern, .namedGraph, .service: break
                }
            case .statement(let value):
                for key in (value.orderBy ?? []).reversed() { try push(.expression(key.expression)) }
                if let having = value.having { try push(.expression(having)) }
                try canonicalVisitGroupingExpressions(value.groupBy ?? []) { try push(.expression($0)) }
                for window in value.windows { try canonicalVisitWindowExpressions(window.specification) { try push(.expression($0)) } }
                if let filter = value.filter { try push(.expression(filter)) }
                if case .expression(let expression) = value.pagination.limit.map({ limit -> QueryCount? in
                    if case .count(let count, _) = limit { return count }; return nil
                }) ?? nil { try push(.expression(expression)) }
                if case .expression(let expression) = value.pagination.offset { try push(.expression(expression)) }
                if case .distinctOn(let keys) = value.quantifier { for key in keys.reversed() { try push(.expression(key)) } }
                for item in value.projection.reversed() {
                    if case .expression(let expression, _) = item { try push(.expression(expression)) }
                }
                for definition in (value.subqueries ?? []).reversed() { try push(.statement(definition.query)) }
                try push(.source(value.source))
            }
        }
        return bytes
    }

    @_spi(DatabaseExecution)
    public func boundQueryParameters(
        _ query: SelectQuery,
        parameters: [QueryParameter],
        workMeter: DatabaseWorkMeter,
        allowingSPARQLFunctions: Bool = false,
        occurrences: QueryOccurrenceArena? = nil,
        occurrence: QueryOccurrenceID? = nil
    ) throws -> [QueryParameter] {
        let binding = CanonicalQueryBindingEnvironment(
            parameters: try CanonicalParameterBindingState(parameters: parameters, workMeter: workMeter), workMeter: workMeter,
            occurrences: occurrences, occurrence: occurrence, allowingSPARQLFunctions: allowingSPARQLFunctions)
        let plan = try bindCanonicalQuery(query, inherited: [], outerSchemas: [], binding: binding)
        try binding.parameters.finalize()
        defer { withExtendedLifetime(plan) {} }
        return binding.parameters.resolvedParameters
    }

    private func bindCanonicalQuery(
        _ query: SelectQuery,
        inherited: [NamedSubquery],
        outerSchemas: [CanonicalRelationSchema],
        binding: CanonicalQueryBindingEnvironment
    ) throws -> CanonicalBoundSelect {
        do {
        try binding.workMeter.consume(at: .bindingCandidate)
        let claim = try canonicalBindingReservation(query, binding: binding)
        var binding = binding
        binding.planReservation = claim
        let scope = try bindCanonicalQueryScope(query, inherited: inherited, outerSchemas: outerSchemas, binding: binding)
        let source = try bindCanonicalSource(query.source, namedSubqueries: scope.named,
            outerSchemas: outerSchemas, binding: scope.scoped.child(.source))
        return try finishCanonicalQueryBinding(query, source: source, namedSubqueries: scope.named,
            outerSchemas: outerSchemas, binding: binding, scopedBinding: scope.scoped, ctes: scope.ctes, reservation: claim)
        } catch let cause as DatabaseExpressionEvaluationError {
            try throwQueryBindingExpressionFailure(cause, binding: binding)
        } catch let failure as CanonicalReadError {
            try throwLocatedBindingFailure(failure, binding: binding)
        }
    }

    private func canonicalBindingReservation(_ query: SelectQuery, binding: CanonicalQueryBindingEnvironment
    ) throws -> DatabaseIntermediateReservation {
        if let reservation = binding.planReservation { return reservation }
        return try binding.workMeter.reserveIntermediate(
            bytes: prospectiveBindingPlanBytes(query, workMeter: binding.workMeter), at: .bindingCandidate)
    }

    private func throwQueryBindingExpressionFailure(_ cause: DatabaseExpressionEvaluationError,
        binding: CanonicalQueryBindingEnvironment
    ) throws -> Never {
        guard binding.occurrences != nil else { throw CanonicalReadError.expressionEvaluation(cause) }
        try throwLocatedBindingFailure(cause, binding: binding)
    }

    private func bindCanonicalQueryScope(_ query: SelectQuery, inherited: [NamedSubquery],
        outerSchemas: [CanonicalRelationSchema], binding: CanonicalQueryBindingEnvironment
    ) throws -> (named: [NamedSubquery], scoped: CanonicalQueryBindingEnvironment, ctes: [CanonicalBoundCTE]) {
        let local = query.subqueries ?? []
        let named = try mergeNamedSubqueries(local: local, inherited: inherited)
        var visible = inherited
        var scoped = binding
        var ctes: [CanonicalBoundCTE] = []
        for (index, definition) in local.enumerated() {
            let child = scoped.child(.cte, index: UInt32(index))
            let plan = try bindCanonicalQuery(definition.query, inherited: visible,
                outerSchemas: outerSchemas, binding: child)
            guard definition.columns == nil || (definition.columns?.count ?? 0) <= plan.columns.count else {
                throw CanonicalReadError.unsupportedSelectQuery("CTE column count differs from its query")
            }
            ctes.append(CanonicalBoundCTE(identity: index, definition: definition, plan: plan,
                referenceCount: 1, isVolatile: selectedPlanIsVolatile(plan)))
            scoped.namedPlans = CanonicalNamedBindingScope(parent: scoped.namedPlans, name: definition.name, plan: plan)
            visible.insert(definition, at: 0)
        }
        return (named, scoped, ctes)
    }

    private func finishCanonicalQueryBinding(_ query: SelectQuery, source: CanonicalSourceBindingPlan,
        namedSubqueries named: [NamedSubquery], outerSchemas: [CanonicalRelationSchema],
        binding: CanonicalQueryBindingEnvironment, scopedBinding scoped: CanonicalQueryBindingEnvironment,
        ctes: [CanonicalBoundCTE], reservation claim: DatabaseIntermediateReservation
    ) throws -> CanonicalBoundSelect {
        let resolved = try resolvedQueryReferences(query, sourceSchema: source.schema, queryBinding: scoped)
        let plan = try bindCanonicalSelectProjection(resolved, original: query, source: source,
            namedSubqueries: named, outerSchemas: outerSchemas, binding: scoped, ctes: ctes)
        return try finishCanonicalQueryPlan(resolved, plan: plan, source: source,
            namedSubqueries: named, outerSchemas: outerSchemas, binding: binding,
            scopedBinding: scoped, reservation: claim)
    }

    /// Binds composition's already-materialized input through the same grouping and clause contract.
    /// A synthetic VALUES source deliberately carries no base-entity functional dependency.
    private func bindCanonicalMaterializedSelect(_ query: SelectQuery, schema: CanonicalRelationSchema,
        binding: CanonicalQueryBindingEnvironment
    ) throws -> CanonicalBoundSelect {
        let claim = try binding.workMeter.reserveIntermediate(bytes:
            prospectiveBindingPlanBytes(query, workMeter: binding.workMeter), at: .bindingCandidate)
        var binding = binding
        binding.planReservation = claim
        let scope = try bindCanonicalQueryScope(query, inherited: [], outerSchemas: [], binding: binding)
        let source = CanonicalSourceBindingPlan(source: .values([]), schema: schema,
            binding: scope.scoped, namedSubqueries: scope.named, reservation: claim)
        let resolved = try resolvedQueryReferences(query, sourceSchema: schema, queryBinding: scope.scoped)
        let plan = try bindCanonicalSelectProjection(resolved, original: query, source: source,
            namedSubqueries: scope.named, outerSchemas: [], binding: scope.scoped, ctes: scope.ctes)
        try validateCanonicalGroupedBinding(resolved, plan: plan, source: source, binding: binding)
        return plan.withSourcePlan(source)
    }

    private func bindCanonicalSelectProjection(_ resolved: SelectQuery, original query: SelectQuery,
        source: CanonicalSourceBindingPlan, namedSubqueries: [NamedSubquery],
        outerSchemas: [CanonicalRelationSchema], binding: CanonicalQueryBindingEnvironment,
        ctes: [CanonicalBoundCTE]
    ) throws -> CanonicalBoundSelect {
        if query.source.usesSPARQLSemantics { return try bindCanonicalSPARQLProjection(query, source: source) }
        var binding = binding
        if query.groupBy?.contains(where: { element in
            if case .expression = element { return false }; return true
        }) == true {
            try CanonicalBindingStorage.append(source.schema.layout, to: &binding.groupingOuterLayouts,
                reservation: binding.planReservation)
        }
        let binder = try typeBinder(schema: source.schema, namedSubqueries: namedSubqueries,
            queryBinding: binding, outerSchemas: outerSchemas).preparingWindows(resolved.windows)
        let projected = try binder.bindProjection(resolved.projection, occurrence: binding.occurrence)
        return try bindCanonicalSelectClauses(resolved, original: query, projected: projected,
            binder: binder, source: source, binding: binding, ctes: ctes)
    }

    private func bindCanonicalSPARQLProjection(_ query: SelectQuery,
        source: CanonicalSourceBindingPlan
    ) throws -> CanonicalBoundSelect {
        let columns = try canonicalProjectionColumns(query.projection, sourceSchema: source.schema)
            .enumerated().map { index, name in
                try QueryColumn(number: UInt32(index + 1), name: name, type: .primitive(.rdfTerm), isNullable: true)
            }
        return CanonicalBoundSelect(columns: columns, projection: [], filter: nil, groupBy: [], having: nil, orderBy: [])
    }

    private func bindCanonicalSelectClauses(_ resolved: SelectQuery, original query: SelectQuery,
        projected: CanonicalBoundSelect, binder: CanonicalQueryTypeBinder,
        source: CanonicalSourceBindingPlan, binding: CanonicalQueryBindingEnvironment,
        ctes: [CanonicalBoundCTE]
    ) throws -> CanonicalBoundSelect {
        let filter = try bindCanonicalPredicate(resolved.filter, binder: binder, binding: binding.child(.filter))
        let grouping = try bindCanonicalGroupKeys(resolved, original: query, projected: projected,
            source: source, binder: binder, binding: binding)
        let having = try resolved.having.map { try binder.bindHaving($0, occurrence: binding.child(.having).occurrence) }
        let ordering = try bindCanonicalOrderKeys(query, projected: projected, binder: binder, binding: binding)
        let distinctOn = try bindCanonicalDistinctKeys(query, projected: projected, ordering: ordering,
            binder: binder, binding: binding)
        let pagination = try binder.bindPagination(query.pagination,
            localColumnCount: source.schema.layout.slots.count, sql: !query.source.usesSPARQLSemantics)
        guard !pagination.withTies || !ordering.isEmpty else {
            throw CanonicalReadError.unsupportedSelectQuery("WITH TIES requires ORDER BY")
        }
        var columns = projected.columns
        if let grouping, grouping.sets.contains(where: { $0.count < grouping.leaves.count }) {
            let claim = try binding.workMeter.reserveIntermediate(bytes: UInt64(columns.count) * 128 + 64, at: .bindingCandidate)
            func mayBeOmitted(_ value: CanonicalBoundExpression) throws -> Bool {
                if value.aggregate != nil || value.window != nil { return false }
                switch value.expression {
                case .grouping, .isNull, .isNotNull, .truthTest, .isDistinctFrom: return false
                case .coalesce:
                    for child in value.children {
                        if !child.isNullable {
                            if try !mayBeOmitted(child) { return false }
                        }
                    }
                    return true
                case .function(let function) where function.name == "concat": return false
                default: break
                }
                for index in grouping.leaves.indices where grouping.sets.contains(where: { !$0.contains(index) }) {
                    if try CanonicalBoundKey(expression: value).isSemanticallyEquivalent(to: CanonicalBoundKey(expression: grouping.leaves[index]),
                        workMeter: binding.workMeter, isVolatileSubquery: selectedPlanIsVolatile) { return true }
                }
                for child in value.children { if try mayBeOmitted(child) { return true } }
                return false
            }
            columns = try columns.enumerated().map { index, column in
                try QueryColumn(number: column.number, name: column.name, type: column.type,
                    isNullable: column.isNullable || mayBeOmitted(projected.projection[index]))
            }
            try projected.bindingMetadata?.retain(claim)
        }
        return CanonicalBoundSelect(columns: columns, projection: projected.projection,
            filter: filter, groupBy: grouping?.leaves ?? [], having: having, orderBy: ordering,
            distinctOn: distinctOn, pagination: pagination, ctes: ctes,
            metadataLease: projected.metadataLease, bindingMetadata: projected.bindingMetadata,
            grouping: grouping, windows: binder.windowDefinitions)
    }

    private func bindCanonicalPredicate(_ expression: Expression?, binder: CanonicalQueryTypeBinder,
        binding: CanonicalQueryBindingEnvironment
    ) throws -> CanonicalBoundExpression? {
        guard let expression else { return nil }
        return try binder.bindPredicate(expression, occurrence: binding.occurrence)
    }

    private func bindCanonicalGroupKeys(_ resolved: SelectQuery, original query: SelectQuery,
        projected: CanonicalBoundSelect, source: CanonicalSourceBindingPlan,
        binder: CanonicalQueryTypeBinder, binding: CanonicalQueryBindingEnvironment
    ) throws -> CanonicalBoundGrouping? {
        guard let elements = query.groupBy else { return nil }
        let claim = try binding.workMeter.reserveIntermediate(bytes: 128, at: .bindingCandidate)
        var leaves: [CanonicalBoundExpression] = []
        func append<T>(_ value: T, to array: inout [T]) throws {
            try CanonicalBindingStorage.append(value, to: &array, reservation: claim)
        }
        func leaf(_ expression: Expression, path: CanonicalQueryBindingEnvironment) throws -> Int {
            try locatingBindingReference(path) {
                let selected: CanonicalBoundExpression
                if let output = try projectionReferenceIndex(expression, projection: query.projection,
                    sourceSchema: source.schema, prefersInput: true) {
                    selected = projected.projection[output]
                } else {
                    selected = try binder.bindGroupingExpression(expression, occurrence: path.occurrence)
                }
                func legal(_ value: CanonicalBoundExpression) throws {
                    try binding.workMeter.consume(at: .bindingCandidate)
                    if value.aggregate != nil {
                        throw CanonicalReadError.aggregateEvaluation(.invalidGroupedExpression("GROUP BY cannot contain aggregates"))
                    }
                    if value.window != nil {
                        throw CanonicalReadError.aggregateEvaluation(.invalidGroupedExpression("GROUP BY cannot contain windows"))
                    }
                    if case .grouping = value.expression { throw CanonicalReadError.aggregateEvaluation(.invalidGroupedExpression("GROUP BY cannot contain GROUPING")) }
                    for child in value.children { try legal(child) }
                }
                try legal(selected)
                try binder.requireSQLComparison(selected.type, operation: "GROUP BY")
                for index in leaves.indices {
                    if try CanonicalBoundKey(expression: selected).isSemanticallyEquivalent(
                        to: CanonicalBoundKey(expression: leaves[index]), workMeter: binding.workMeter,
                        isVolatileSubquery: selectedPlanIsVolatile) { return index }
                }
                try append(selected, to: &leaves)
                return leaves.count - 1
            }
        }
        func product(_ lhs: [[Int]], _ rhs: [[Int]]) throws -> [[Int]] {
            var output: [[Int]] = []
            for left in lhs { for right in rhs {
                try binding.workMeter.consume(at: .bindingCandidate)
                var combined: [Int] = []
                for index in left { try append(index, to: &combined) }
                for index in right where !combined.contains(index) { try append(index, to: &combined) }
                try append(combined, to: &output)
            } }
            return output
        }
        func sequence(_ values: [GroupingElement], path: CanonicalQueryBindingEnvironment) throws -> [[Int]] {
            var result: [[Int]] = []
            try append([], to: &result)
            for (index, value) in values.enumerated() {
                result = try product(result, expand(value, path: path.child(.groupingElement, index: UInt32(index))))
            }
            return result
        }
        func expand(_ value: GroupingElement, path: CanonicalQueryBindingEnvironment) throws -> [[Int]] {
            try binding.workMeter.consume(at: .bindingCandidate)
            switch value {
            case .expression(let expression):
                let index = try leaf(expression, path: path.child(.operand))
                var set: [Int] = []; try append(index, to: &set)
                var output: [[Int]] = []; try append(set, to: &output); return output
            case .groupingSets(let sets):
                var output: [[Int]] = []
                for (index, set) in sets.enumerated() {
                    for expanded in try sequence(set, path: path.child(.groupingSet, index: UInt32(index))) {
                        try append(expanded, to: &output)
                    }
                }
                return output
            case .rollup(let units), .cube(let units):
                var selected: [[Int]] = []
                for (unitIndex, unit) in units.enumerated() {
                    var indices: [Int] = []
                    for (index, expression) in unit.enumerated() {
                        let leafIndex = try leaf(expression, path: path.child(.groupingComponent, index: UInt32(unitIndex)).child(.argument, index: UInt32(index)))
                        if !indices.contains(leafIndex) { try append(leafIndex, to: &indices) }
                    }
                    try append(indices, to: &selected)
                }
                var output: [[Int]] = []
                switch value {
                case .rollup:
                    for count in stride(from: selected.count, through: 0, by: -1) {
                        var set: [Int] = []
                        for unit in selected.prefix(count) { for index in unit where !set.contains(index) { try append(index, to: &set) } }
                        try append(set, to: &output)
                    }
                default:
                    try append([], to: &output)
                    for unit in selected.reversed() {
                        var next: [[Int]] = []
                        for set in output {
                            var included: [Int] = []
                            for index in unit { try append(index, to: &included) }
                            for index in set where !included.contains(index) { try append(index, to: &included) }
                            try append(included, to: &next)
                            try append(set, to: &next)
                        }
                        output = next
                    }
                }
                return output
            }
        }
        var sets: [[Int]] = []; try append([], to: &sets)
        for (index, value) in elements.enumerated() {
            sets = try product(sets, expand(value, path: binding.child(.groupBy, index: UInt32(index))))
        }
        return CanonicalBoundGrouping(leaves: leaves, sets: sets,
            metadataLease: DatabaseQueryPayloadLease(retaining: claim))
    }

    private func bindCanonicalOrderKeys(_ query: SelectQuery, projected: CanonicalBoundSelect,
        binder: CanonicalQueryTypeBinder, binding: CanonicalQueryBindingEnvironment
    ) throws -> [CanonicalBoundKey] {
        let isSet: Bool
        if case .setOperation = query.source { isSet = true } else { isSet = false }
        return try (query.orderBy ?? []).enumerated().map { index, key in
            try binder.bindOrderKey(key, projection: projected.projection, output: projected.columns,
                allowSourceExpressions: !isSet, occurrence: binding.child(.orderBy, index: UInt32(index)).occurrence,
                isVolatileSubquery: selectedPlanIsVolatile)
        }
    }

    private func bindCanonicalDistinctKeys(_ query: SelectQuery, projected: CanonicalBoundSelect,
        ordering: [CanonicalBoundKey], binder: CanonicalQueryTypeBinder,
        binding: CanonicalQueryBindingEnvironment
    ) throws -> [CanonicalBoundKey] {
        if case .distinctOn(let expressions) = query.quantifier {
            let keys = try expressions.enumerated().map { index, expression in
                let key = try binder.bindOrderKey(SortKey(expression), projection: projected.projection,
                    output: projected.columns, occurrence: binding.child(.distinctOn, index: UInt32(index)).occurrence,
                    isVolatileSubquery: selectedPlanIsVolatile)
                return CanonicalBoundKey(expression: key.expression, projectionIndex: key.projectionIndex,
                    occurrence: key.occurrence)
            }
            try validateDistinctOrderPrefix(distinct: keys, ordering: ordering, meter: binding.workMeter)
            return keys
        }
        if query.quantifier == .distinct {
            for column in projected.columns { try binder.requireSQLComparison(column.type, operation: "DISTINCT") }
        }
        return []
    }

    private func finishCanonicalQueryPlan(_ resolved: SelectQuery, plan: CanonicalBoundSelect,
        source: CanonicalSourceBindingPlan, namedSubqueries: [NamedSubquery],
        outerSchemas: [CanonicalRelationSchema], binding: CanonicalQueryBindingEnvironment,
        scopedBinding: CanonicalQueryBindingEnvironment, reservation: DatabaseIntermediateReservation
    ) throws -> CanonicalBoundSelect {
        try validateCanonicalGroupedBinding(resolved, plan: plan, source: source, binding: binding)
        let owned = CanonicalSourceBindingPlan(source: source.source, schema: source.schema,
            children: source.children, queryPlan: source.queryPlan, condition: source.condition,
            query: resolved, valueBindings: source.valueBindings, binding: scopedBinding,
            namedSubqueries: namedSubqueries, reservation: reservation,
            bindingFrames: [source.schema.layout] + outerSchemas.map { $0.layout })
        return plan.withSourcePlan(owned)
    }

    private func validateCanonicalGroupedBinding(_ resolved: SelectQuery, plan: CanonicalBoundSelect,
        source: CanonicalSourceBindingPlan, binding: CanonicalQueryBindingEnvironment
    ) throws {
        if !canonicalQueryRequiresAggregation(resolved) {
            func rejectGrouping(_ value: CanonicalBoundExpression) throws {
                if case .grouping = value.expression { throw CanonicalReadError.aggregateEvaluation(.invalidGroupedExpression("GROUPING requires grouping expressions")) }
                for child in value.children { try rejectGrouping(child) }
            }
            for value in plan.projection { try rejectGrouping(value) }
            for key in plan.orderBy { try rejectGrouping(key.expression) }
        }
        if canonicalQueryRequiresAggregation(resolved) {
            func grouped(_ value: CanonicalBoundExpression) throws -> Bool {
                for key in plan.groupBy {
                    if try CanonicalBoundKey(expression: value).isSemanticallyEquivalent(to: CanonicalBoundKey(expression: key),
                        workMeter: binding.workMeter, isVolatileSubquery: selectedPlanIsVolatile) { return true }
                }
                return false
            }
            var visitedQueries = Set<ObjectIdentifier>()
            var visitedSources = Set<ObjectIdentifier>()
            let groupedSlots = Set(plan.groupBy.compactMap { $0.columnSlot })
            let lineageClaim = try binding.workMeter.reserveIntermediate(bytes: 128, at: .bindingCandidate)
            defer { lineageClaim.release() }
            func identitySlots(_ value: CanonicalSourceBindingPlan) throws -> [Int]? {
                try binding.workMeter.consume(at: .bindingCandidate)
                switch value.source {
                case .table(let table):
                    guard value.queryPlan == nil,
                          let runtime = try readPolicy().entityRuntime(named: table.table),
                          let fields = runtime.groupingIdentityFields else { return nil }
                    var slots: [Int] = []
                    for field in fields {
                        let matches = value.schema.layout.matching(ColumnRef(table: table.alias ?? table.effectiveName, column: field))
                        guard matches.count == 1, let slot = matches.first else { return nil }
                        try CanonicalBindingStorage.append(slot, to: &slots, reservation: lineageClaim)
                    }
                    return slots
                case .aliased:
                    guard value.children.count == 1,
                          let childKeys = try identitySlots(value.children[0]) else { return nil }
                    var slots: [Int] = []
                    let visible = value.children[0].schema.layout.visibleSlots
                    for key in childKeys {
                        guard let ordinal = visible.firstIndex(of: key), value.schema.layout.visibleSlots.indices.contains(ordinal) else { return nil }
                        try CanonicalBindingStorage.append(value.schema.layout.visibleSlots[ordinal], to: &slots, reservation: lineageClaim)
                    }
                    return slots
                default: return nil
                }
            }
            let dependentColumns: Bool
            if let identity = try identitySlots(source), !identity.isEmpty {
                if let grouping = plan.grouping {
                    dependentColumns = !grouping.sets.isEmpty && grouping.sets.allSatisfy { set in
                        identity.allSatisfy { key in set.contains { grouping.leaves[$0].columnSlot == key } }
                    }
                } else { dependentColumns = identity.allSatisfy { groupedSlots.contains($0) } }
            } else { dependentColumns = false }
            func rejectUngrouped(_ value: CanonicalBoundExpression, localSlot: Int) throws -> Never {
                let address = source.schema.layout.slots[localSlot]
                let name = ColumnRef(table: address.scope, column: address.name).displayName
                let failure = CanonicalReadError.aggregateEvaluation(.invalidGroupedExpression(
                    "Column '\(name)' is neither grouped nor aggregated"))
                if binding.occurrences != nil {
                    throw CanonicalSQLBindingFailure(cause: .read(failure), occurrence: value.occurrence,
                        root: binding.root ?? binding.occurrence)
                }
                throw failure
            }
            func validateOuterExpression(_ expression: CanonicalBoundExpression,
                frames: [CanonicalSourceLayout]) throws {
                try binding.workMeter.consume(at: .bindingCandidate)
                if let frame = frames.firstIndex(where: { $0 === source.schema.layout }),
                   let slot = expression.columnSlot {
                    let offset = frames.prefix(frame).reduce(0) { $0 + $1.slots.count }
                    if slot >= offset && slot < offset + source.schema.layout.slots.count,
                       !groupedSlots.contains(slot - offset), !dependentColumns {
                        try rejectUngrouped(expression, localSlot: slot - offset)
                    }
                }
                if let frame = frames.firstIndex(where: { $0 === source.schema.layout }),
                   let whole = expression.wholeRow, !dependentColumns {
                    let offset = frames.prefix(frame).reduce(0) { $0 + $1.slots.count }
                    for slot in whole.fieldSlots where slot >= offset && slot < offset + source.schema.layout.slots.count && !groupedSlots.contains(slot - offset) {
                        try rejectUngrouped(expression, localSlot: slot - offset)
                    }
                }
                for child in expression.children { try validateOuterExpression(child, frames: frames) }
                if let nested = expression.subqueryBinding { try validateOuterQuery(nested) }
            }
            func validateOuterSource(_ sourcePlan: CanonicalSourceBindingPlan) throws {
                guard visitedSources.insert(ObjectIdentifier(sourcePlan)).inserted else { return }
                if let condition = sourcePlan.condition {
                    try validateOuterExpression(condition, frames: sourcePlan.bindingFrames)
                }
                if let query = sourcePlan.queryPlan { try validateOuterQuery(query) }
                for child in sourcePlan.children { try validateOuterSource(child) }
            }
            func validateOuterQuery(_ nested: CanonicalBoundSelect) throws {
                guard visitedQueries.insert(ObjectIdentifier(nested)).inserted,
                      let nestedSource = nested.sourcePlan else { return }
                for expression in nested.projection { try validateOuterExpression(expression, frames: nestedSource.bindingFrames) }
                if let filter = nested.filter { try validateOuterExpression(filter, frames: nestedSource.bindingFrames) }
                for expression in nested.groupBy { try validateOuterExpression(expression, frames: nestedSource.bindingFrames) }
                if let having = nested.having { try validateOuterExpression(having, frames: nestedSource.bindingFrames) }
                for key in nested.orderBy { try validateOuterExpression(key.expression, frames: nestedSource.bindingFrames) }
                try validateOuterSource(nestedSource)
            }
            func validate(_ value: CanonicalBoundExpression) throws {
                if try grouped(value) { return }
                if case .grouping = value.expression {
                    for child in value.children where try !grouped(child) {
                        throw CanonicalReadError.aggregateEvaluation(.invalidGroupedExpression("GROUPING arguments must match grouping expressions"))
                    }
                    return
                }
                if let window = value.window {
                    if let aggregate = window.aggregate { for child in aggregate.children { try validate(child) } }
                    for argument in window.arguments { try validate(argument) }
                    for key in window.partitionBy { try validate(key) }
                    for key in window.orderBy { try validate(key.expression) }
                    return
                }
                if case .aggregate = value.expression {
                    for argument in value.aggregate?.directArguments ?? [] { try validate(argument) }
                    return
                }
                if let whole = value.wholeRow, !dependentColumns {
                    for slot in whole.fieldSlots where slot < source.schema.layout.slots.count && !groupedSlots.contains(slot) {
                        try rejectUngrouped(value, localSlot: slot)
                    }
                }
                if let slot = value.columnSlot, slot < source.schema.layout.slots.count, !dependentColumns {
                    try rejectUngrouped(value, localSlot: slot)
                }
                for child in value.children { try validate(child) }
                if let nested = value.subqueryBinding { try validateOuterQuery(nested) }
            }
            for expression in plan.projection { try validate(expression) }
            if let having = plan.having { try validate(having) }
            for key in plan.orderBy { try validate(key.expression) }
            for window in plan.windows {
                for key in window.partitionBy { try validate(key) }
                for key in window.orderBy { try validate(key.expression) }
            }

        }
    }

    private func bindCanonicalValues(_ rows: [[DatabaseKit.Expression]], namedSubqueries: [NamedSubquery],
        outerSchemas: [CanonicalRelationSchema], binding: CanonicalQueryBindingEnvironment,
        reservation claim: DatabaseIntermediateReservation
    ) throws -> (schema: CanonicalRelationSchema, valueBindings: [[CanonicalBoundExpression]]) {
        var valueBindings: [[CanonicalBoundExpression]] = []
        let schema: CanonicalRelationSchema
        let empty = try CanonicalRelationSchema()
        let binder = try typeBinder(schema: empty, namedSubqueries: namedSubqueries,
            queryBinding: binding, outerSchemas: outerSchemas)
        typealias ValueCell = (bound: CanonicalBoundExpression, original: DatabaseKit.Expression?)
        let scratch = try binding.workMeter.reserveIntermediate(bytes: 256, at: .bindingCandidate)
        defer { scratch.release() }
        var known: [[ValueCell]] = []
        for (rowIndex, expressions) in rows.enumerated() {
            var cells: [ValueCell] = []
            for (cellIndex, expression) in expressions.enumerated() {
                try binding.workMeter.consume(at: .bindingCandidate)
                let occurrence = binding.child(.valuesRow, index: UInt32(rowIndex))
                    .child(.valuesCell, index: UInt32(cellIndex)).occurrence
                let bound = try binder.bindScalarExpression(expression, occurrence: occurrence)
                if case .fieldExpansion = expression {
                    guard let source = bound.children.first,
                          case .sql(.composite(let descriptor)) = source.type else {
                        throw DatabaseExpressionEvaluationError.typeMismatch(operation: "VALUES field expansion")
                    }
                    for (ordinal, field) in descriptor.columns.enumerated() {
                        try binding.workMeter.consume(at: .bindingCandidate)
                        let selected = CanonicalBoundExpression(expression: .field(source.expression, name: field.name),
                            type: field.type, isNullable: source.isNullable || field.isNullable,
                            children: [source], occurrence: occurrence, fieldOrdinal: ordinal)
                        try CanonicalBindingStorage.append((bound: selected, original: nil), to: &cells, reservation: scratch)
                    }
                } else {
                    try CanonicalBindingStorage.append((bound: bound, original: expression), to: &cells, reservation: scratch)
                }
            }
            try CanonicalBindingStorage.append(cells, to: &known, reservation: scratch)
        }
        let width = known.first?.count ?? 0
        guard known.allSatisfy({ $0.count == width }) else {
            throw CanonicalReadError.unsupportedSelectQuery("VALUES expanded row arity differs")
        }
        func contextualNull(_ value: CanonicalBoundExpression) -> Bool {
            if case .field = value.expression, let ordinal = value.fieldOrdinal,
               let source = value.children.first, case .row = source.expression,
               source.children.indices.contains(ordinal) {
                return contextualNull(source.children[ordinal])
            }
            return expressionIsContextualNull(value.expression)
        }
        var types: [QueryColumnType] = []
        var inputs: [QueryColumnType] = []
        for column in 0..<width {
            inputs.removeAll(keepingCapacity: true)
            for row in known where !contextualNull(row[column].bound) {
                try CanonicalBindingStorage.append(row[column].bound.type, to: &inputs, reservation: scratch)
            }
            try CanonicalBindingStorage.append(binder.commonSourceType(inputs), to: &types, reservation: scratch)
        }
        for row in known {
            var selected: [CanonicalBoundExpression] = []
            for (column, cell) in row.enumerated() {
                let value: CanonicalBoundExpression
                if let original = cell.original {
                    value = try binder.bindSourceValue(original, to: types[column], occurrence: cell.bound.occurrence)
                } else {
                    guard let source = cell.bound.children.first, let ordinal = cell.bound.fieldOrdinal,
                          case .sql(.composite(let descriptor)) = source.type else {
                        throw DatabaseExpressionEvaluationError.typeMismatch(operation: "VALUES expanded field binding")
                    }
                    var fields: [QueryColumn] = []
                    for (fieldIndex, field) in descriptor.columns.enumerated() {
                        let target = try QueryColumn(number: field.number, name: field.name,
                            type: types[column - ordinal + fieldIndex], isNullable: field.isNullable)
                        try CanonicalBindingStorage.append(target, to: &fields, reservation: scratch)
                    }
                    let contextual = try CanonicalBindingStorage.composite(columns: fields, workMeter: binding.workMeter)
                    let rebound = try binder.bind(source.expression, contextualType: .sql(.composite(contextual.type)),
                        occurrence: source.occurrence)
                    guard case .sql(.composite(let selectedDescriptor)) = rebound.type else {
                        throw DatabaseExpressionEvaluationError.typeMismatch(operation: "VALUES contextual composite binding")
                    }
                    let field = selectedDescriptor.columns[ordinal]
                    let selectedField = CanonicalBoundExpression(expression: .field(rebound.expression, name: field.name),
                        type: field.type, isNullable: rebound.isNullable || field.isNullable,
                        children: [rebound], occurrence: cell.bound.occurrence, fieldOrdinal: ordinal,
                        metadataLease: contextual.lease)
                    value = try binder.bindSourceValue(selectedField, to: types[column])
                }
                try CanonicalBindingStorage.append(value, to: &selected, reservation: claim)
            }
            try CanonicalBindingStorage.append(selected, to: &valueBindings, reservation: claim)
        }
        // Explicit optional elements avoid Swift's implicit array runtime cast, unavailable in Embedded.
        schema = try CanonicalRelationSchema(unscopedColumns: (0..<width).map { "column\($0 + 1)" },
            types: types.map { .some($0) }, nullability: (0..<width).map { column in known.contains { $0[column].bound.isNullable } })
        return (schema, valueBindings)
    }

    private func bindCanonicalSource(_ source: DataSource, namedSubqueries: [NamedSubquery],
        outerSchemas: [CanonicalRelationSchema], binding: CanonicalQueryBindingEnvironment
    ) throws -> CanonicalSourceBindingPlan {
        do {
            try binding.workMeter.consume(at: .bindingCandidate)
            guard let claim = binding.planReservation else {
                throw CanonicalReadError.unsupportedSelectQuery("Source binding has no admitted plan owner")
            }
            switch source {
            case .table:
                return try bindCanonicalTableSource(source, namedSubqueries: namedSubqueries,
                    outerSchemas: outerSchemas, binding: binding, reservation: claim)
            case .aliased:
                return try bindCanonicalAliasedSource(source, namedSubqueries: namedSubqueries,
                    outerSchemas: outerSchemas, binding: binding, reservation: claim)
            case .subquery:
                return try bindCanonicalSubquerySource(source, namedSubqueries: namedSubqueries,
                    outerSchemas: outerSchemas, binding: binding, reservation: claim)
            case .values:
                return try bindCanonicalValuesSource(source, namedSubqueries: namedSubqueries,
                    outerSchemas: outerSchemas, binding: binding, reservation: claim)
            case .join:
                return try bindCanonicalJoinSource(source, namedSubqueries: namedSubqueries,
                    outerSchemas: outerSchemas, binding: binding, reservation: claim)
            case .setOperation:
                return try bindCanonicalSetSource(source, namedSubqueries: namedSubqueries,
                    outerSchemas: outerSchemas, binding: binding, reservation: claim)
            case .graphTable:
                return try bindCanonicalGraphTableSource(source, namedSubqueries: namedSubqueries,
                    outerSchemas: outerSchemas, binding: binding, reservation: claim)
            default:
                return try bindCanonicalNonrelationalSource(source, namedSubqueries: namedSubqueries,
                    outerSchemas: outerSchemas, binding: binding, reservation: claim)
            }
        } catch let cause as CanonicalReadError {
            try throwLocatedBindingFailure(cause, binding: binding)
        } catch let cause as DatabaseExpressionEvaluationError {
            try throwLocatedBindingFailure(cause, binding: binding)
        }
    }

    private func admittedCanonicalSource(_ source: DataSource, schema: CanonicalRelationSchema,
        children: [CanonicalSourceBindingPlan] = [], queryPlan: CanonicalBoundSelect? = nil,
        condition: CanonicalBoundExpression? = nil, valueBindings: [[CanonicalBoundExpression]] = [],
        namedSubqueries: [NamedSubquery], outerSchemas: [CanonicalRelationSchema],
        binding: CanonicalQueryBindingEnvironment, reservation claim: DatabaseIntermediateReservation
    ) throws -> CanonicalSourceBindingPlan {
        try claim.reserveAdditional(bytes: UInt64(schema.layout.slots.count) * 512
            + UInt64(valueBindings.reduce(0) { $0 + $1.count }) * 1024, at: .bindingCandidate)
        return CanonicalSourceBindingPlan(source: source, schema: schema, children: children,
            queryPlan: queryPlan, condition: condition, valueBindings: valueBindings, binding: binding,
            namedSubqueries: namedSubqueries, reservation: claim,
            bindingFrames: [schema.layout] + outerSchemas.map { $0.layout })
    }

    private func bindCanonicalNonrelationalSource(_ source: DataSource, namedSubqueries: [NamedSubquery],
        outerSchemas: [CanonicalRelationSchema], binding: CanonicalQueryBindingEnvironment,
        reservation claim: DatabaseIntermediateReservation
    ) throws -> CanonicalSourceBindingPlan {
        let schema: CanonicalRelationSchema
        switch source {
        case .logical(let logical):
            guard logical.kindIdentifier == LogicalSourceKind.polymorphic else { throw CanonicalReadError.unsupportedSource("Unknown logical source") }
            schema = try polymorphicRelationSchema(container.polymorphicGroup(identifier: logical.identifier), sourceName: logical.effectiveName)
        case .graphPattern(let pattern), .namedGraph(_, let pattern):
            let names = sparqlVariables(in: pattern)
            schema = try CanonicalRelationSchema(unscopedColumns: names, types: names.map { _ in .primitive(.rdfTerm) })
        case .service(let endpoint, _, _):
            throw CanonicalReadError.unsupportedSource("SERVICE source '\(endpoint)' requires its graph executor")
        #if DATABASE_MULTI_BASE
        case .base(_, let nested):
            schema = try canonicalRelationSchema(for: nested, namedSubqueries: namedSubqueries, queryBinding: binding, outerSchemas: outerSchemas)
        #endif
        default:
            throw CanonicalReadError.unsupportedSelectQuery("A relational source reached the nonrelational binder")
        }
        return try admittedCanonicalSource(source, schema: schema, namedSubqueries: namedSubqueries,
            outerSchemas: outerSchemas, binding: binding, reservation: claim)
    }

    private func bindCanonicalTableSource(_ source: DataSource,
        namedSubqueries: [NamedSubquery], outerSchemas: [CanonicalRelationSchema],
        binding: CanonicalQueryBindingEnvironment, reservation claim: DatabaseIntermediateReservation
    ) throws -> CanonicalSourceBindingPlan {
        guard case .table(let table) = source else {
            throw CanonicalReadError.unsupportedSelectQuery("Source kind does not match its table binding path")
        }
        let schema: CanonicalRelationSchema
        var queryPlan: CanonicalBoundSelect? = nil
        // FIXME(INCOMPLETE_IMPLEMENTATION): S11 namespace resolution is not implemented.
        // The selected source binder rejects qualified names before reads; completion requires catalog-backed schema lookup.
        guard table.schema == nil else {
            throw CanonicalReadError.unsupportedSource("SQL schema-qualified relation lookup requires S11")
        }
        if let named = namedSubqueries.first(where: { $0.name == table.table }) {
            guard let plan = try binding.namedPlans.resolve(named.name, workMeter: binding.workMeter) else {
                throw CanonicalReadError.unsupportedSelectQuery("A CTE has no earlier bound definition")
            }
            queryPlan = plan
            let names = try aliasedColumnNames(plan.columns.map { $0.name }, overrides: named.columns)
            schema = try CanonicalRelationSchema(scopes: [CanonicalRelationScope(name: table.table,
                columns: names, types: plan.columns.map { $0.type }, nullability: plan.columns.map { $0.isNullable })])
        } else { schema = try tableRelationSchema(table) }
        return try admittedCanonicalSource(source, schema: schema, queryPlan: queryPlan, namedSubqueries: namedSubqueries,
            outerSchemas: outerSchemas, binding: binding, reservation: claim)
    }

    private func bindCanonicalAliasedSource(_ source: DataSource,
        namedSubqueries: [NamedSubquery], outerSchemas: [CanonicalRelationSchema],
        binding: CanonicalQueryBindingEnvironment, reservation claim: DatabaseIntermediateReservation
    ) throws -> CanonicalSourceBindingPlan {
        guard case .aliased(let nested, let name, let columns) = source else {
            throw CanonicalReadError.unsupportedSelectQuery("Source kind does not match its aliased binding path")
        }
        let schema: CanonicalRelationSchema
        var children: [CanonicalSourceBindingPlan] = []
        let child = try bindCanonicalSource(nested, namedSubqueries: namedSubqueries,
            outerSchemas: outerSchemas, binding: binding.child(.source))
        children = [child]
        let visible = child.schema.layout.visibleSlots
        schema = try CanonicalRelationSchema(scopes: [CanonicalRelationScope(name: name,
            columns: aliasedColumnNames(child.schema.visibleColumns, overrides: columns),
            types: visible.map { child.schema.slotTypes[$0] }, nullability: visible.map { child.schema.slotNullability[$0] })])
        return try admittedCanonicalSource(source, schema: schema, children: children, namedSubqueries: namedSubqueries,
            outerSchemas: outerSchemas, binding: binding, reservation: claim)
    }

    private func bindCanonicalSubquerySource(_ source: DataSource,
        namedSubqueries: [NamedSubquery], outerSchemas: [CanonicalRelationSchema],
        binding: CanonicalQueryBindingEnvironment, reservation claim: DatabaseIntermediateReservation
    ) throws -> CanonicalSourceBindingPlan {
        guard case .subquery(let query) = source else {
            throw CanonicalReadError.unsupportedSelectQuery("Source kind does not match its subquery binding path")
        }
        let schema: CanonicalRelationSchema
        var queryPlan: CanonicalBoundSelect? = nil
        let plan = try bindCanonicalQuery(query, inherited: namedSubqueries, outerSchemas: outerSchemas,
            binding: binding.child(.subquery))
        queryPlan = plan
        schema = try CanonicalRelationSchema(unscopedColumns: plan.columns.map { $0.name },
            types: plan.columns.map { $0.type }, nullability: plan.columns.map { $0.isNullable })
        return try admittedCanonicalSource(source, schema: schema, queryPlan: queryPlan, namedSubqueries: namedSubqueries,
            outerSchemas: outerSchemas, binding: binding, reservation: claim)
    }

    private func bindCanonicalValuesSource(_ source: DataSource,
        namedSubqueries: [NamedSubquery], outerSchemas: [CanonicalRelationSchema],
        binding: CanonicalQueryBindingEnvironment, reservation claim: DatabaseIntermediateReservation
    ) throws -> CanonicalSourceBindingPlan {
        guard case .values(let rows) = source else {
            throw CanonicalReadError.unsupportedSelectQuery("Source kind does not match its values binding path")
        }
        let schema: CanonicalRelationSchema
        var valueBindings: [[CanonicalBoundExpression]] = []
        let selected = try bindCanonicalValues(rows, namedSubqueries: namedSubqueries,
            outerSchemas: outerSchemas, binding: binding, reservation: claim)
        schema = selected.schema
        valueBindings = selected.valueBindings
        return try admittedCanonicalSource(source, schema: schema, valueBindings: valueBindings, namedSubqueries: namedSubqueries,
            outerSchemas: outerSchemas, binding: binding, reservation: claim)
    }

    private func bindCanonicalJoinSource(_ source: DataSource,
        namedSubqueries: [NamedSubquery], outerSchemas: [CanonicalRelationSchema],
        binding: CanonicalQueryBindingEnvironment, reservation claim: DatabaseIntermediateReservation
    ) throws -> CanonicalSourceBindingPlan {
        guard case .join(let join) = source else {
            throw CanonicalReadError.unsupportedSelectQuery("Source kind does not match its join binding path")
        }
        try validateJoinDeclaration(join)
        let left = try bindCanonicalSource(join.left, namedSubqueries: namedSubqueries,
            outerSchemas: outerSchemas, binding: binding.child(.joinLeft))
        let lateral = join.type == .lateral || join.type == .leftLateral
        let right = try bindCanonicalSource(join.right, namedSubqueries: namedSubqueries,
            outerSchemas: lateral ? [left.schema] + outerSchemas : outerSchemas, binding: binding.child(.joinRight))
        return try finishCanonicalJoinSource(source, join: join, left: left, right: right,
            namedSubqueries: namedSubqueries, outerSchemas: outerSchemas, binding: binding, reservation: claim)
    }

    private func finishCanonicalJoinSource(_ source: DataSource, join: JoinClause,
        left: CanonicalSourceBindingPlan, right: CanonicalSourceBindingPlan,
        namedSubqueries: [NamedSubquery], outerSchemas: [CanonicalRelationSchema],
        binding: CanonicalQueryBindingEnvironment, reservation claim: DatabaseIntermediateReservation
    ) throws -> CanonicalSourceBindingPlan {
        let schema: CanonicalRelationSchema
        var children: [CanonicalSourceBindingPlan] = []
        var condition: CanonicalBoundExpression? = nil
        children = [left, right]
        let selectedCondition: JoinCondition?
        switch join.type {
        case .natural, .naturalLeft, .naturalRight, .naturalFull:
            selectedCondition = .using(inferNaturalJoinColumns(leftSchema: left.schema, rightSchema: right.schema))
        default: selectedCondition = join.condition
        }
        try validateJoinCondition(selectedCondition, leftSchema: left.schema, rightSchema: right.schema, type: join.type)
        if case .on(let expression) = selectedCondition {
            condition = try typeBinder(schema: left.schema.merged(with: right.schema),
                namedSubqueries: namedSubqueries, queryBinding: binding, outerSchemas: outerSchemas)
                .bindPredicate(expression, occurrence: binding.child(.joinCondition).occurrence)
        }
        let leftSchema = (join.type == .right || join.type == .naturalRight || join.type == .full || join.type == .naturalFull)
            ? try left.schema.nullExtended() : left.schema
        let rightSchema = (join.type == .left || join.type == .naturalLeft || join.type == .full || join.type == .naturalFull || join.type == .leftLateral)
            ? try right.schema.nullExtended() : right.schema
        schema = try joinOutputSchema(leftSchema, rightSchema, condition: selectedCondition)
        return try admittedCanonicalSource(source, schema: schema, children: children, condition: condition, namedSubqueries: namedSubqueries,
            outerSchemas: outerSchemas, binding: binding, reservation: claim)
    }

    private func bindCanonicalSetSource(_ source: DataSource,
        namedSubqueries: [NamedSubquery], outerSchemas: [CanonicalRelationSchema],
        binding: CanonicalQueryBindingEnvironment, reservation claim: DatabaseIntermediateReservation
    ) throws -> CanonicalSourceBindingPlan {
        guard case .setOperation(_, _, let left, let right) = source else {
            throw CanonicalReadError.unsupportedSelectQuery("Source kind does not match its set binding path")
        }
        let lhs = try bindCanonicalQuery(left, inherited: namedSubqueries, outerSchemas: outerSchemas, binding: binding.child(.setLeft))
        let rhs = try bindCanonicalQuery(right, inherited: namedSubqueries, outerSchemas: outerSchemas, binding: binding.child(.setRight))
        return try finishCanonicalSetSource(source, lhs: lhs, rhs: rhs, namedSubqueries: namedSubqueries,
            outerSchemas: outerSchemas, binding: binding, reservation: claim)
    }

    private func finishCanonicalSetSource(_ source: DataSource,
        lhs: CanonicalBoundSelect, rhs: CanonicalBoundSelect,
        namedSubqueries: [NamedSubquery], outerSchemas: [CanonicalRelationSchema],
        binding: CanonicalQueryBindingEnvironment, reservation claim: DatabaseIntermediateReservation
    ) throws -> CanonicalSourceBindingPlan {
        guard case .setOperation(let operation, let quantifier, let left, let right) = source else {
            throw CanonicalReadError.unsupportedSelectQuery("Source kind does not match its set binding path")
        }
        let schema: CanonicalRelationSchema
        var children: [CanonicalSourceBindingPlan] = []
        guard lhs.columns.count == rhs.columns.count else { throw CanonicalReadError.unsupportedSelectQuery("Set operation input arity differs") }
        children = [CanonicalSourceBindingPlan(source: .subquery(left), schema: try outputSchema(lhs.columns), queryPlan: lhs, binding: binding.child(.setLeft), namedSubqueries: namedSubqueries, reservation: claim),
            CanonicalSourceBindingPlan(source: .subquery(right), schema: try outputSchema(rhs.columns), queryPlan: rhs, binding: binding.child(.setRight), namedSubqueries: namedSubqueries, reservation: claim)]
        let binder = try typeBinder(schema: CanonicalRelationSchema(), namedSubqueries: namedSubqueries, queryBinding: binding)
        let types = try lhs.columns.indices.map { index -> QueryColumnType in
            var inputs: [QueryColumnType] = []
            if !queryColumnIsContextualNull(left, ordinal: index, namedSubqueries: namedSubqueries) { inputs.append(lhs.columns[index].type) }
            if !queryColumnIsContextualNull(right, ordinal: index, namedSubqueries: namedSubqueries) { inputs.append(rhs.columns[index].type) }
            let type = try binder.commonSourceType(inputs)
            if quantifier == .distinct || operation != .union { try binder.requireSQLComparison(type, operation: "set operation") }
            return type
        }
        // Construct optional elements explicitly; Embedded cannot perform the implicit array runtime cast.
        schema = try CanonicalRelationSchema(unscopedColumns: lhs.columns.map { $0.name }, types: types.map { .some($0) },
            nullability: lhs.columns.indices.map { lhs.columns[$0].isNullable || rhs.columns[$0].isNullable })
        return try admittedCanonicalSource(source, schema: schema, children: children, namedSubqueries: namedSubqueries,
            outerSchemas: outerSchemas, binding: binding, reservation: claim)
    }

    private func bindCanonicalGraphTableSource(_ source: DataSource,
        namedSubqueries: [NamedSubquery], outerSchemas: [CanonicalRelationSchema],
        binding: CanonicalQueryBindingEnvironment, reservation claim: DatabaseIntermediateReservation
    ) throws -> CanonicalSourceBindingPlan {
        guard case .graphTable(let graph) = source else {
            throw CanonicalReadError.unsupportedSelectQuery("Source kind does not match its graphtable binding path")
        }
        let schema: CanonicalRelationSchema
        var queryPlan: CanonicalBoundSelect? = nil
        let rawSchema = try graphTableRawRelationSchema(graph)
        if let columns = graph.columns, !columns.isEmpty {
            let binder = try typeBinder(schema: rawSchema, namedSubqueries: namedSubqueries, queryBinding: binding, outerSchemas: outerSchemas)
            let selected = try binder.bindSelect(SelectQuery(projection: columns.map { ProjectionItem($0.expression, alias: $0.alias) }, source: source))
            queryPlan = selected
            schema = try outputSchema(selected.columns)
        } else { schema = rawSchema }
        return try admittedCanonicalSource(source, schema: schema, queryPlan: queryPlan, namedSubqueries: namedSubqueries,
            outerSchemas: outerSchemas, binding: binding, reservation: claim)
    }

    private func aliasedColumnNames(_ names: [String], overrides: [String]?) throws -> [String] {
        guard let overrides else { return names }
        guard overrides.count <= names.count else { throw CanonicalReadError.unsupportedSelectQuery("Source column alias count exceeds its output arity") }
        return overrides + names.dropFirst(overrides.count)
    }

    private func outputSchema(_ columns: [QueryColumn]) throws -> CanonicalRelationSchema {
        try CanonicalRelationSchema(unscopedColumns: columns.map { $0.name }, types: columns.map { $0.type }, nullability: columns.map { $0.isNullable })
    }

    /// Resolves complete output metadata from the admitted schema before any
    /// source row is fetched. This operation does not open a transaction.
    @_spi(DatabaseExecution)
    public func boundQueryColumns(
        _ query: SelectQuery,
        parameters: [QueryParameter] = [],
        workMeter: DatabaseWorkMeter? = nil
    ) throws -> DatabaseRetainedQueryColumns {
        let execution = ReadExecutionContext(options: .default, monotonicClock: container.monotonicClock,
            workMeter: workMeter, temporalEnvironment: inheritedSQLTemporalEnvironment)
        try QueryStructuralValidator.validate(query, limits: execution.queryStructuralLimits)
        let binding = CanonicalQueryBindingEnvironment(
            parameters: try CanonicalParameterBindingState(parameters: parameters, workMeter: execution.workMeter),
            workMeter: execution.workMeter, occurrences: nil, occurrence: nil, allowingSPARQLFunctions: false)
        let plan = try bindCanonicalQuery(query, inherited: [], outerSchemas: [], binding: binding)
        try binding.parameters.finalize()
        return DatabaseRetainedQueryColumns(columns: plan.columns,
            payloadLeases: try retainedBindingClaims(plan, workMeter: execution.workMeter))
    }

    private func retainedBindingClaims(_ plan: CanonicalBoundSelect,
        workMeter: DatabaseWorkMeter) throws -> [DatabaseQueryPayloadLease] {
        enum Visit {
            case selected(CanonicalBoundSelect)
            case source(CanonicalSourceBindingPlan)
            case expression(CanonicalBoundExpression)
            case window(CanonicalBoundWindowDefinition)
        }
        let storage = try workMeter.reserveIntermediate(bytes: 256, at: .bindingCandidate)
        let traversal = try workMeter.reserveIntermediate(bytes: UInt64(MemoryLayout<[Visit]>.stride), at: .bindingCandidate)
        defer { traversal.release() }
        var pending: [Visit] = []
        var leases: [DatabaseQueryPayloadLease] = [DatabaseQueryPayloadLease(retaining: storage)]
        var visitedPlans = Set<ObjectIdentifier>()
        var visitedSources = Set<ObjectIdentifier>()
        func push(_ visit: Visit) throws {
            try workMeter.checkpoint(at: .bindingCandidate)
            try CanonicalBindingStorage.append(visit, to: &pending, reservation: traversal)
        }
        func keep(_ lease: DatabaseQueryPayloadLease?) throws {
            if let lease {
                try storage.reserveAdditional(bytes: 64, at: .bindingCandidate)
                leases.append(lease)
            }
        }
        try push(.selected(plan))
        while let visit = pending.popLast() {
            try workMeter.consume(at: .bindingCandidate)
            // Reverse pushes preserve the former recursive lease visitation order.
            switch visit {
            case .expression(let value):
                try keep(value.metadataLease); try keep(value.constructionLease)
                try keep(value.bindingMetadata?.payloadLease); try keep(value.wholeRow?.metadataLease)
                for child in value.children.reversed() { try push(.expression(child)) }
                if let nested = value.subqueryBinding { try push(.selected(nested)) }
            case .source(let value):
                guard visitedSources.insert(ObjectIdentifier(value)).inserted else { continue }
                try storage.reserveAdditional(bytes: 64, at: .bindingCandidate)
                try keep(DatabaseQueryPayloadLease(retaining: value.reservation))
                for lease in value.schema.layout.payloadLeases { try keep(lease) }
                if let owner = value.schema.layout.reservation { try keep(DatabaseQueryPayloadLease(retaining: owner)) }
                for child in value.children.reversed() { try push(.source(child)) }
                for row in value.valueBindings.reversed() {
                    for cell in row.reversed() { try push(.expression(cell)) }
                }
                if let condition = value.condition { try push(.expression(condition)) }
                if let nested = value.queryPlan { try push(.selected(nested)) }
            case .window(let value):
                if let cell = value.frame?.endOffset { try push(.expression(cell)) }
                if let cell = value.frame?.startOffset { try push(.expression(cell)) }
                for key in value.orderBy.reversed() { try push(.expression(key.expression)) }
                for cell in value.partitionBy.reversed() { try push(.expression(cell)) }
            case .selected(let value):
                guard visitedPlans.insert(ObjectIdentifier(value)).inserted else { continue }
                try storage.reserveAdditional(bytes: 64, at: .bindingCandidate)
                try keep(value.metadataLease); try keep(value.bindingMetadata?.payloadLease)
                try keep(value.grouping?.metadataLease)
                for definition in value.ctes.reversed() { try push(.selected(definition.plan)) }
                if let root = value.sourcePlan { try push(.source(root)) }
                if case .expression(let cell) = value.pagination.limit { try push(.expression(cell)) }
                if case .expression(let cell) = value.pagination.offset { try push(.expression(cell)) }
                for key in value.distinctOn.reversed() { try push(.expression(key.expression)) }
                for key in value.orderBy.reversed() { try push(.expression(key.expression)) }
                if let having = value.having { try push(.expression(having)) }
                if let filter = value.filter { try push(.expression(filter)) }
                for cell in value.groupBy.reversed() { try push(.expression(cell)) }
                for cell in value.projection.reversed() { try push(.expression(cell)) }
                for window in value.windows.reversed() { try push(.window(window)) }
            }
        }
        return leases
    }

    private func boundQueryColumns(_ query: SelectQuery, inheritedNamedSubqueries: [NamedSubquery],
        queryBinding: CanonicalQueryBindingEnvironment? = nil, outerSchemas: [CanonicalRelationSchema] = []
    ) throws -> [QueryColumn] {
        let meter = queryBinding?.workMeter ?? ReadExecutionContext(options: .default, monotonicClock: container.monotonicClock).workMeter
        let binding = try queryBinding ?? CanonicalQueryBindingEnvironment(parameters: CanonicalParameterBindingState(parameters: [], workMeter: meter),
            workMeter: meter, occurrences: nil, occurrence: nil, allowingSPARQLFunctions: false)
        return try bindCanonicalQuery(query, inherited: inheritedNamedSubqueries, outerSchemas: outerSchemas, binding: binding).columns
    }

    private func typeBinder(
        schema: CanonicalRelationSchema,
        namedSubqueries: [NamedSubquery],
        queryBinding: CanonicalQueryBindingEnvironment? = nil,
        outerSchemas: [CanonicalRelationSchema] = []
    ) throws -> CanonicalQueryTypeBinder {
        let frames = [schema] + outerSchemas
        let definitions = try frames.enumerated().flatMap { index, frame in
            let columns = try frame.typedColumns()
            guard index > 0, queryBinding?.groupingOuterLayouts.contains(where: { $0 === frame.layout }) == true else { return columns }
            return try columns.map { column in
                try QueryColumn(number: column.number, name: column.name, type: column.type, isNullable: true)
            }
        }
        return CanonicalQueryTypeBinder(
            columns: definitions,
            parameterState: queryBinding?.parameters,
            allowingSPARQLFunctions: queryBinding?.allowingSPARQLFunctions ?? false,
            occurrences: queryBinding?.occurrences,
            root: queryBinding?.root ?? queryBinding?.occurrence,
            resolveColumn: { reference in
                var offset = 0
                for frame in frames {
                    let matches = frame.layout.matching(reference)
                    if matches.count > 1 {
                        throw DatabaseExpressionEvaluationError.ambiguousColumn(reference.displayName)
                    }
                    if let index = matches.first { return offset + index }
                    if let scope = reference.table, frame.scopes.contains(where: { $0.name == scope }) {
                        throw DatabaseExpressionEvaluationError.missingColumn(reference.displayName)
                    }
                    offset += frame.layout.slots.count
                }
                throw DatabaseExpressionEvaluationError.missingColumn(reference.displayName)
            },
            wildcardSlots: { try schema.wildcardSlots(scope: $0) },
            resolveWholeRow: { name in
                var slotOffset = 0
                var presenceOffset = 0
                for (frameIndex, frame) in frames.enumerated() {
                    if let presence = frame.layout.sourcePresenceIndex(named: name) {
                        let slots = try frame.wildcardSlots(scope: name)
                        let columns = try slots.enumerated().map { index, slot -> QueryColumn in
                            guard let type = frame.slotTypes[slot] else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: "whole-row descriptor") }
                            return try QueryColumn(number: UInt32(index + 1), name: frame.layout.slots[slot].name,
                                type: type, isNullable: frame.slotNullability[slot] || (frameIndex > 0
                                    && queryBinding?.groupingOuterLayouts.contains(where: { $0 === frame.layout }) == true))
                        }
                        let descriptor = try CanonicalBindingStorage.composite(columns: columns, workMeter: queryBinding?.workMeter)
                        return CanonicalBoundWholeRow(type: descriptor.type, fieldSlots: slots.map { slotOffset + $0 },
                            presenceIndex: presenceOffset + presence, metadataLease: descriptor.lease)
                    }
                    slotOffset += frame.layout.slots.count
                    presenceOffset += frame.layout.frameScopes.reduce(0) { $0 + $1.count }
                }
                throw DatabaseExpressionEvaluationError.missingColumn(name)
            },
            bindSubquery: { try self.boundQueryColumns($0, inheritedNamedSubqueries: namedSubqueries,
                queryBinding: queryBinding, outerSchemas: [schema] + outerSchemas) },
            bindLocatedSubquery: queryBinding.map { binding in
                { query, occurrence in
                    try self.bindCanonicalQuery(query, inherited: namedSubqueries,
                        outerSchemas: [schema] + outerSchemas,
                        binding: self.canonicalSubqueryBinding(binding, occurrence: occurrence))
                }
            },
            workMeter: queryBinding?.workMeter
        )
    }

    private func canonicalSubqueryBinding(_ binding: CanonicalQueryBindingEnvironment,
        occurrence: QueryOccurrenceID?
    ) -> CanonicalQueryBindingEnvironment {
        CanonicalQueryBindingEnvironment(parameters: binding.parameters,
            workMeter: binding.workMeter, occurrences: binding.occurrences, occurrence: occurrence,
            allowingSPARQLFunctions: binding.allowingSPARQLFunctions, namedPlans: binding.namedPlans,
            root: binding.root ?? binding.occurrence, planReservation: binding.planReservation,
            groupingOuterLayouts: binding.groupingOuterLayouts)
    }

    private func relationSchema(for layout: CanonicalSourceLayout) throws -> CanonicalRelationSchema {
        let unscoped = layout.slots.indices.filter { layout.slots[$0].scope == nil }
        return try CanonicalRelationSchema(
            unscopedColumns: unscoped.map { layout.slots[$0].name },
            scopes: layout.scopes.map { scope in
                let indices = layout.slots.indices.filter { layout.slots[$0].scope == scope }
                return CanonicalRelationScope(name: scope, columns: indices.map { layout.slots[$0].name },
                    types: indices.map { layout.types[$0] }, nullability: indices.map { layout.nullability[$0] })
            }, coalescedColumns: layout.coalescedColumns,
            types: unscoped.map { layout.types[$0] }, nullability: unscoped.map { layout.nullability[$0] },
            visibleSlots: layout.visibleSlots, physicalLayout: layout)
    }

    private func tableRelationSchema(
        _ tableRef: TableRef
    ) throws -> CanonicalRelationSchema {
        // FIXME(INCOMPLETE_IMPLEMENTATION): S11 namespace resolution is not implemented.
        // Source metadata binding rejects qualified names; completion requires the same catalog namespace proof as execution.
        guard tableRef.schema == nil else {
            throw CanonicalReadError.unsupportedSource("SQL schema-qualified relation lookup is not available")
        }
        let entity = try resolveEntity(named: tableRef.table)
        return try CanonicalRelationSchema(
            scopes: [
                CanonicalRelationScope(
                    name: tableRef.alias ?? tableRef.effectiveName,
                    columns: entity.allFields,
                    types: entity.allFields.map { name in
                        entity.fieldMapByName[name].map {
                            $0.queryType ?? .primitive($0.type, isArray: $0.isArray)
                        }
                    },
                    nullability: entity.allFields.map { entity.fieldMapByName[$0]?.isOptional ?? true }
                )
            ]
        )
    }

    private func polymorphicRelationSchema(
        _ group: PolymorphicGroup,
        sourceName: String
    ) throws -> CanonicalRelationSchema {
        let entities = try group.memberTypeNames.map { try resolveEntity(named: $0) }
        var seen = Set<String>()
        let names = entities.flatMap { $0.allFields }.filter { seen.insert($0).inserted }
        let binder = CanonicalQueryTypeBinder(columns: [], resolveColumn: { reference in
            throw DatabaseExpressionEvaluationError.missingColumn(reference.displayName)
        }, wildcardSlots: { _ in [] })
        let types = try names.map { name -> QueryColumnType? in
            try binder.commonSourceType(entities.compactMap { entity in
                entity.fieldMapByName[name].map { $0.queryType ?? .primitive($0.type, isArray: $0.isArray) }
            })
        }
        let nullability = names.map { name in entities.contains { $0.fieldMapByName[name]?.isOptional != false } }
        return try CanonicalRelationSchema(scopes: [CanonicalRelationScope(name: sourceName,
            columns: names, types: types, nullability: nullability)])
    }

    private func graphTableRawRelationSchema(
        _ source: GraphTableSource
    ) throws -> CanonicalRelationSchema {
        let policy = try readPolicy()
        guard let executor = policy.graphTableSourceExecutor else {
            throw CanonicalReadError.unsupportedSource("GRAPH_TABLE executor is not registered")
        }
        let raw = try executor.columns(graphTableSource: source, schema: policy.schema)
        return try CanonicalRelationSchema(unscopedColumns: raw.map { $0.name },
            types: raw.map { $0.type }, nullability: raw.map { $0.isNullable })
    }

    private func materializeQueryRelation(
        _ rows: CanonicalRetainedQueryRowView,
        query: SelectQuery,
        explicitColumns: [String]?,
        alias: String?,
        namedSubqueries: [NamedSubquery],
        queryBinding: CanonicalQueryBindingEnvironment? = nil,
        workMeter: DatabaseWorkMeter
    ) throws -> CanonicalRelation {
        let output = rows.owner.columns
        let names = explicitColumns ?? output.map { $0.name }
        guard names.count == output.count else {
            throw CanonicalReadError.unsupportedSelectQuery("CTE output arity differs from its declaration")
        }
        let schema: CanonicalRelationSchema
        if let alias {
            schema = try CanonicalRelationSchema(scopes: [CanonicalRelationScope(name: alias,
                columns: names, types: output.map { $0.type }, nullability: output.map { $0.isNullable })])
        } else {
            schema = try CanonicalRelationSchema(unscopedColumns: names,
                types: output.map { $0.type }, nullability: output.map { $0.isNullable })
        }
        let layoutFootprint = try CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
            fieldNames: names, sourceName: alias, workMeter: workMeter, stage: .bindingCandidate)
        let reservation = try workMeter.reserveIntermediate(bytes: layoutFootprint.bytes, at: .bindingCandidate)
        let layout = CanonicalSourceLayout(slots: names.map { .init(name: $0, scope: alias) },
            scopes: alias.map { [$0] } ?? [], reservation: reservation,
            types: output.map { $0.type }, nullability: output.map { $0.isNullable })
        var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter, stage: .bindingCandidate,
            layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self), expectedCount: rows.count)
        for index in 0..<rows.count {
            try rows.withElement(at: index) { row in
                let footprint = try CanonicalRelationalFootprintMeter.positionalValueArrayFootprint(
                    count: row.values.count, valueAt: { _ in .primitive(.null) },
                    annotations: row.annotations, version: row.version,
                    workMeter: workMeter, stage: .bindingCandidate)
                try retained.append(footprint: footprint) {
                    CanonicalSourceRow(values: row.values, layout: layout, annotations: row.annotations,
                        version: row.version, payloadLeases: rows.owner.originPayloadLeases)
                }
            }
        }
        return CanonicalRelation(schema: schema.retaining(layout: layout), rows: try retained.finish().moveToSharedOwnership(at: .bindingCandidate))
    }

    private func canonicalRelationSchema(for source: DataSource, namedSubqueries: [NamedSubquery],
        queryBinding: CanonicalQueryBindingEnvironment? = nil, outerSchemas: [CanonicalRelationSchema] = []
    ) throws -> CanonicalRelationSchema {
        let meter = queryBinding?.workMeter ?? ReadExecutionContext(options: .default,
            monotonicClock: container.monotonicClock).workMeter
        var binding = try queryBinding ?? CanonicalQueryBindingEnvironment(
            parameters: CanonicalParameterBindingState(parameters: [], workMeter: meter), workMeter: meter,
            occurrences: nil, occurrence: nil, allowingSPARQLFunctions: false)
        if binding.planReservation == nil {
            binding.planReservation = try meter.reserveIntermediate(bytes: 1024, at: .bindingCandidate)
        }
        return try bindCanonicalSource(source, namedSubqueries: namedSubqueries, outerSchemas: outerSchemas, binding: binding).schema
    }

    private func sourceRequiresRuntimeInferredSchema(_ source: DataSource,
        namedSubqueries: [NamedSubquery], queryBinding: CanonicalQueryBindingEnvironment? = nil
    ) -> Bool { false }


    private func canonicalProjectionColumns(
        _ projection: [ProjectionItem], sourceSchema: CanonicalRelationSchema
    ) throws -> [String] {
        var names: [String] = []
        for item in projection {
            switch item {
            case .wildcard(let scope):
                names.append(contentsOf: try sourceSchema.wildcardSlots(scope: scope).map { sourceSchema.layout.slots[$0].name })
            case .expression(let expression, let alias):
                names.append(alias ?? canonicalProjectionName(for: expression, index: names.count))
            }
        }
        return names
    }

    private func sparqlVariables(in pattern: GraphPattern) -> [String] {
        var result: [String] = []
        var seen = Set<String>()
        func append(_ name: String) {
            if seen.insert(name).inserted { result.append(name) }
        }
        func visitTerm(_ term: SPARQLTerm) {
            switch term {
            case .variable(let name):
                append(name)
            case .tripleTerm(let subject, let predicate, let object):
                visitTerm(subject)
                visitTerm(predicate)
                visitTerm(object)
            case .reifiedTriple(let subject, let predicate, let object, let reifier):
                visitTerm(subject)
                visitTerm(predicate)
                visitTerm(object)
                visitTerm(reifier)
            default:
                break
            }
        }
        func visit(_ current: GraphPattern) {
            switch current {
            case .basic(let basic):
                for element in basic.elements {
                    switch element {
                    case .triple(let triple):
                        visitTerm(triple.subject)
                        visitTerm(triple.predicate)
                        visitTerm(triple.object)
                    case .propertyPath(let path):
                        visitTerm(path.subject)
                        visitTerm(path.object)
                    }
                }
            case .join(let lhs, let rhs), .optional(let lhs, let rhs),
                    .union(let lhs, let rhs), .minus(let lhs, let rhs),
                    .lateral(let lhs, let rhs):
                visit(lhs)
                visit(rhs)
            case .filter(let nested, _):
                visit(nested)
            case .graph(let name, let nested):
                visitTerm(name)
                visit(nested)
            case .service(_, let nested, _):
                visit(nested)
            case .bind(let nested, let variable, _):
                visit(nested)
                append(variable)
            case .values(let variables, _):
                variables.forEach(append)
            case .subquery(let query):
                for (index, item) in query.projection.enumerated() {
                    switch item {
                    case .expression(let expression, let alias): append(alias ?? canonicalProjectionName(for: expression, index: index))
                    case .wildcard(nil):
                        switch query.source {
                        case .graphPattern(let nested), .namedGraph(_, let nested), .service(_, let nested, _): visit(nested)
                        default: break
                        }
                    case .wildcard: break
                    }
                }
            case .groupBy(let nested, _, let aggregates):
                visit(nested)
                aggregates.forEach { append($0.variable) }
            }
        }
        visit(pattern)
        return result
    }

    private func setOperationSchema(
        sources: [DataSource],
        namedSubqueries: [NamedSubquery],
        queryBinding: CanonicalQueryBindingEnvironment? = nil,
        requiresEquality: Bool = true,
        outerSchemas: [CanonicalRelationSchema] = []
    ) throws -> CanonicalRelationSchema {
        guard let firstSource = sources.first else { return try CanonicalRelationSchema() }
        let first = try canonicalRelationSchema(for: firstSource, namedSubqueries: namedSubqueries, queryBinding: queryBinding,
                                               outerSchemas: outerSchemas)
        let firstSlots = first.layout.visibleSlots
        let binder = try typeBinder(schema: first, namedSubqueries: namedSubqueries, queryBinding: queryBinding, outerSchemas: outerSchemas)
        var types: [[QueryColumnType]] = Array(repeating: [], count: firstSlots.count)
        var nullability = Array(repeating: false, count: firstSlots.count)
        for source in sources {
            let schema = try canonicalRelationSchema(for: source, namedSubqueries: namedSubqueries, queryBinding: queryBinding,
                                                    outerSchemas: outerSchemas)
            let slots = schema.layout.visibleSlots
            guard slots.count == firstSlots.count else {
                throw CanonicalReadError.unsupportedSelectQuery("Set operation input arity differs")
            }
            for (ordinal, slot) in slots.enumerated() {
                guard let type = schema.layout.types[slot] else {
                    throw CanonicalReadError.unsupportedSelectQuery("Set operation column has no declared type")
                }
                if !sourceColumnIsContextualNull(source, ordinal: ordinal, namedSubqueries: namedSubqueries) {
                    types[ordinal].append(type)
                }
                nullability[ordinal] = nullability[ordinal] || schema.layout.nullability[slot]
            }
        }
        let selectedTypes = try types.map { try binder.commonSourceType($0) }
        if requiresEquality {
            for type in selectedTypes { try binder.requireSQLComparison(type, operation: "set equality") }
        }
        // The schema owns optional descriptors. This boundary copy constructs
        // that representation explicitly instead of a reflective Array conversion.
        return try CanonicalRelationSchema(
            unscopedColumns: firstSlots.map { first.layout.slots[$0].name },
            types: selectedTypes.map(Optional.some), nullability: nullability
        )
    }

    private func sourceColumnIsContextualNull(_ source: DataSource, ordinal: Int,
        namedSubqueries: [NamedSubquery], queryBinding: CanonicalQueryBindingEnvironment? = nil
    ) -> Bool {
        switch source {
        case .values(let rows): return !rows.isEmpty && rows.allSatisfy { $0.indices.contains(ordinal) && expressionIsContextualNull($0[ordinal]) }
        case .subquery(let query): return queryColumnIsContextualNull(query, ordinal: ordinal, namedSubqueries: namedSubqueries)
        case .aliased(let nested, _, _): return sourceColumnIsContextualNull(nested, ordinal: ordinal, namedSubqueries: namedSubqueries)
        case .setOperation(_, _, let left, let right): return queryColumnIsContextualNull(left, ordinal: ordinal, namedSubqueries: namedSubqueries) && queryColumnIsContextualNull(right, ordinal: ordinal, namedSubqueries: namedSubqueries)
        case .table(let table):
            guard let cte = namedSubqueries.first(where: { $0.name == table.table }) else { return false }
            return queryColumnIsContextualNull(cte.query, ordinal: ordinal, namedSubqueries: namedSubqueries)
        default: return false
        }
    }

    private func queryColumnIsContextualNull(_ query: SelectQuery, ordinal: Int,
        namedSubqueries: [NamedSubquery], queryBinding: CanonicalQueryBindingEnvironment? = nil
    ) -> Bool {
        if query.projection == [.wildcard()] { return sourceColumnIsContextualNull(query.source, ordinal: ordinal, namedSubqueries: namedSubqueries) }
        guard query.projection.indices.contains(ordinal), case .expression(let expression, _) = query.projection[ordinal] else { return false }
        return expressionIsContextualNull(expression)
    }

    private func expressionIsContextualNull(_ expression: DatabaseKit.Expression) -> Bool {
        switch expression {
        case .literal(.null), .literal(.queryValue(.primitive(.null), type: nil)): return true
        case .coalesce(let children): return children.allSatisfy(expressionIsContextualNull)
        case .caseWhen(_, let pairs, let fallback):
            return pairs.allSatisfy { expressionIsContextualNull($0.result) }
                && (fallback.map(expressionIsContextualNull) ?? true)
        default: return false
        }
    }

    private func admittedSetOperationLayout(
        _ schema: CanonicalRelationSchema,
        retaining firstLease: DatabaseQueryPayloadLease,
        and secondLease: DatabaseQueryPayloadLease? = nil,
        workMeter: DatabaseWorkMeter
    ) throws -> CanonicalSourceLayout {
        guard firstLease.workMeter === workMeter,
              secondLease.map({ $0.workMeter === workMeter }) ?? true else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        guard schema.scopes.isEmpty, schema.layout.visibleSlots.count == schema.unscopedColumns.count else {
            throw CanonicalReadError.unsupportedSelectQuery("Set operation output requires one positional scope")
        }
        let footprint = try CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
            fieldNames: schema.unscopedColumns, sourceName: nil,
            workMeter: workMeter, stage: .bindingCandidate
        ).adding(DatabaseIntermediateCollectionMeter.arrayFootprint(
            count: secondLease == nil ? 1 : 2, element: DatabaseQueryPayloadLease.self))
        let reservation = try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .bindingCandidate)
        var payloadLeases = [firstLease]
        if let secondLease { payloadLeases.append(secondLease) }
        return CanonicalSourceLayout(
            slots: schema.unscopedColumns.map { .init(name: $0, scope: nil) },
            reservation: reservation, types: schema.unscopedTypes, nullability: schema.unscopedNullability,
            payloadLeases: payloadLeases
        )
    }

    private func appendAlignedSetOperationRows(
        _ relation: CanonicalRelation,
        to outputSchema: CanonicalRelationSchema,
        layout outputLayout: CanonicalSourceLayout,
        into aligned: inout DatabaseRetainedArrayBuilder<CanonicalSourceRow>,
        workMeter: DatabaseWorkMeter,
        temporalEnvironment: SQLTemporalEnvironment? = nil
    ) throws {
        let sourceSlots = relation.schema.layout.visibleSlots
        let outputSlots = outputLayout.visibleSlots
        guard sourceSlots.count == outputSlots.count else {
            throw CanonicalReadError.unsupportedSelectQuery("Set operation input arity differs")
        }
        guard outputLayout.reservation?.workMeter === workMeter,
              relation.rows.workMeter === workMeter,
              outputLayout.payloadLeases.allSatisfy({ $0.workMeter === workMeter }),
              outputLayout.slots == outputSchema.layout.slots else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        for slot in outputSlots where outputLayout.types[slot] == nil {
            throw CanonicalReadError.unsupportedSelectQuery("Set operation column has no bound result type")
        }
        for row in relation.rows {
            try workMeter.consume(at: .bindingCandidate)
            guard row.payloadLeases.allSatisfy({ $0.workMeter === workMeter }),
                  row.layout.payloadLeases.allSatisfy({ $0.workMeter === workMeter }) else {
                throw DatabaseIntermediateReservationError.workMeterMismatch
            }
            guard row.layout.visibleSlots.count == sourceSlots.count else {
                throw CanonicalReadError.unsupportedSelectQuery("Set operation row differs from its admitted schema")
            }
            let claim = try workMeter.reserveIntermediate(bytes: 256, at: .bindingCandidate)
            var values: [QueryValue] = []
            var leases: [DatabaseQueryPayloadLease] = []
            for lease in row.payloadLeases { try CanonicalBindingStorage.append(lease, to: &leases, reservation: claim) }
            try CanonicalBindingStorage.append(relation.rows.payloadLease, to: &leases, reservation: claim)
            for ordinal in sourceSlots.indices {
                let sourceSlot = sourceSlots[ordinal]
                guard row.values.indices.contains(sourceSlot),
                      let sourceType = relation.schema.layout.types[sourceSlot],
                      let targetType = outputLayout.types[outputSlots[ordinal]] else {
                    throw CanonicalReadError.unsupportedSelectQuery("Set operation source slot is missing")
                }
                let converted = try CanonicalSQLScalarCast.alignSource(row.values[sourceSlot], from: sourceType, to: targetType,
                    temporalEnvironment: temporalEnvironment ?? inheritedSQLTemporalEnvironment,
                    workMeter: workMeter, limits: Self.selectedValueLimits(context: nil, meter: workMeter))
                try CanonicalBindingStorage.append(converted.value, to: &values, reservation: claim)
                if let lease = converted.lease { try CanonicalBindingStorage.append(lease, to: &leases, reservation: claim) }
            }
            try CanonicalBindingStorage.append(DatabaseQueryPayloadLease(retaining: claim), to: &leases, reservation: claim)
            let selected = CanonicalSourceRow(values: values, layout: outputLayout,
                annotations: row.annotations, version: row.version, payloadLeases: leases)
            try aligned.append(footprint: CanonicalRelationalFootprintMeter.sourceAliasFootprint(of: selected,
                workMeter: workMeter, stage: .bindingCandidate), make: { selected })
        }
    }

    private func alignSetOperationRows(
        _ relation: CanonicalRelation,
        to outputSchema: CanonicalRelationSchema,
        layout outputLayout: CanonicalSourceLayout,
        workMeter: DatabaseWorkMeter,
        temporalEnvironment: SQLTemporalEnvironment? = nil
    ) throws -> CanonicalRetainedRows {
        var aligned = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .bindingCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(
                CanonicalSourceRow.self
            ),
            expectedCount: relation.rows.count
        )
        try appendAlignedSetOperationRows(
            relation,
            to: outputSchema,
            layout: outputLayout,
            into: &aligned,
            workMeter: workMeter,
            temporalEnvironment: temporalEnvironment
        )
        return try aligned.finish().moveToSharedOwnership(at: .bindingCandidate)
    }

    /// Converts a logical-source result while its request-accounted owner is
    /// still alive. The source rows are borrowed one at a time and the
    /// canonical rows receive their own admission before the source owner is
    /// consumed.
    private func materializeLogicalSourceRows(
        _ rows: consuming DatabaseRetainedQueryRows,
        sourceName: String?,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> CanonicalRetainedRows {
        guard rows.workMeter === workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        let source = try rows.moveToSharedOwnership(at: stage)
        let columns = source.columns
        let layoutFootprint = try CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
            fieldNames: columns.lazy.map { $0.name }, sourceName: sourceName, workMeter: workMeter, stage: stage)
        let layoutReservation = try workMeter.reserveIntermediate(bytes: layoutFootprint.bytes, at: stage)
        let layout = CanonicalSourceLayout(
            slots: columns.map { .init(name: $0.name, scope: sourceName) },
            scopes: sourceName.map { [$0] } ?? [], reservation: layoutReservation,
            types: columns.map { $0.type }, nullability: columns.map { $0.isNullable })
        let payloadLeases = source.originPayloadLeases
        var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter, stage: stage,
            layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self), expectedCount: source.count)
        for index in 0..<source.count {
            try source.withElement(at: index) { row in
                try workMeter.consume(at: stage)
                let structural = try CanonicalRelationalFootprintMeter.positionalValueArrayFootprint(
                    count: row.values.count, valueAt: { _ in .primitive(.null) },
                    annotations: row.annotations, version: row.version, workMeter: workMeter, stage: stage)
                try retained.append(footprint: structural) {
                    CanonicalSourceRow(values: row.values, layout: layout,
                        annotations: row.annotations, version: row.version, payloadLeases: payloadLeases)
                }
            }
        }
        return try retained.finish().moveToSharedOwnership(at: stage)
    }

    private func retainLogicalQueryRows(
        _ rows: consuming DatabaseRetainedQueryRows,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> CanonicalRetainedQueryRows {
        guard rows.workMeter === workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        return try rows.moveToSharedOwnership(at: stage)
    }

    private func retainedCanonicalRow(
        _ row: CanonicalSourceRow,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> CanonicalRetainedRows {
        try retainedCanonicalRows(
            CollectionOfOne(row),
            expectedCount: 1,
            workMeter: workMeter,
            stage: stage
        )
    }

    /// Keeps source rows admitted while a composed row is being constructed.
    /// Overlay and JOIN ON composition allocate dictionaries before their
    /// destination row can be measured; the source composition claim closes
    /// that otherwise unadmitted interval, while the retained destination
    /// receives its exact post-construction footprint.
    private func retainedConstructedSourceRow(
        prospectiveFootprint: DatabaseIntermediateFootprint,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage,
        make: () throws -> CanonicalSourceRow
    ) throws -> CanonicalRetainedRows {
        let constructionReservation = try workMeter.reserveIntermediate(
            rows: prospectiveFootprint.rows,
            bytes: prospectiveFootprint.bytes,
            at: stage
        )
        defer { constructionReservation.release() }
        let row = try make()
        return try retainedCanonicalRow(
            row,
            workMeter: workMeter,
            stage: stage
        )
    }

    private static func prospectiveSourceCompositionFootprint(
        _ row: CanonicalSourceRow,
        outer: CanonicalSourceRow?,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        // The input rows retain their payload roots during synchronous adapters.
        // Overlay arrays, name lookup dictionaries and qualified labels are new.
        var footprint = DatabaseIntermediateFootprint(bytes: 256)
        func append(_ source: CanonicalSourceRow) throws {
            footprint = try footprint.adding(DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: source.layout.bindingSlots.count, element: CanonicalSourceLayout.BindingSlot.self))
            footprint = try footprint.adding(CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
                fieldNames: source.layout.slots.lazy.map { $0.name }, sourceName: nil,
                workMeter: workMeter, stage: .expressionEvaluation))
            footprint = try footprint.adding(DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: source.values.count, element: QueryValue.self))
            footprint = try footprint.adding(DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: source.payloadLeases.count, element: DatabaseQueryPayloadLease.self))
            for slot in source.layout.slots {
                let label = try DatabaseIntermediateFootprint(bytes: UInt64(slot.name.utf8.count))
                    .adding(DatabaseIntermediateFootprint(bytes: UInt64(slot.scope?.utf8.count ?? 0)))
                footprint = try footprint.adding(label.multiplied(by: 4))
                    .adding(DatabaseIntermediateFootprint(bytes: 512))
            }
            footprint = try footprint.adding(DatabaseIntermediateFootprint(bytes: 128)
                .multiplied(by: UInt64(source.annotations.count)))
        }
        try append(row)
        if let outer { try append(outer) }
        return footprint
    }

    private func retainedCanonicalRows<Rows: Sequence>(
        _ rows: Rows,
        expectedCount: Int,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> CanonicalRetainedRows where Rows.Element == CanonicalSourceRow {
        var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: stage,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: expectedCount
        )
        for row in rows {
            try retained.append(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: row,
                    workMeter: workMeter
                ),
                make: { row }
            )
        }
        return try retained.finish().moveToSharedOwnership(at: stage)
    }

    private func retainedCanonicalRows(
        _ rows: [CanonicalSourceRow],
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> CanonicalRetainedRows {
        try retainedCanonicalRows(
            rows,
            expectedCount: rows.count,
            workMeter: workMeter,
            stage: stage
        )
    }

    private func emptyCanonicalRows(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> CanonicalRetainedRows {
        try retainedCanonicalRows(
            EmptyCollection<CanonicalSourceRow>(),
            expectedCount: 0,
            workMeter: workMeter,
            stage: stage
        )
    }
}
