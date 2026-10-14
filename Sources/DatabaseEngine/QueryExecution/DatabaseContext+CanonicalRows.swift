import DatabaseKit
import DatabaseTypes
import StorageKit

/// Immutable name/scope addressing shared by positional source rows.
final class CanonicalSourceLayout: Sendable {
    struct Slot: Sendable, Hashable {
        let name: String
        let scope: String?
    }

    let slots: [Slot]
    let visibleSlots: [Int]
    let scopes: [String]
    let coalescedColumns: Set<String>
    let reservation: DatabaseIntermediateReservation?
    let payloadLeases: [DatabaseQueryPayloadLease]
    let types: [QueryColumnType?]
    let nullability: [Bool]

    init(slots: [Slot], scopes: [String] = [], coalescedColumns: Set<String> = [], reservation: DatabaseIntermediateReservation? = nil, types: [QueryColumnType?]? = nil, nullability: [Bool]? = nil, visibleSlots: [Int]? = nil, payloadLeases: [DatabaseQueryPayloadLease] = []) {
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

    func matching(_ column: ColumnRef) -> [Int] {
        if let scope = column.table {
            return slots.indices.filter {
                (slots[$0].scope == scope && slots[$0].name == column.column)
                    || (slots[$0].scope == nil && slots[$0].name == column.displayName)
            }
        }
        return visibleSlots.filter { slots[$0].name == column.column }
    }
}

struct CanonicalSourceRow: Sendable {
    let values: [QueryValue]
    let layout: CanonicalSourceLayout
    let annotations: [String: FieldValue]
    let version: PersistableVersionToken?
    let payloadLeases: [DatabaseQueryPayloadLease]
    private let ambiguityOverride: Set<String>?

    init(
        values: [QueryValue],
        layout: CanonicalSourceLayout,
        annotations: [String: FieldValue] = [:],
        version: PersistableVersionToken? = nil,
        payloadLeases: [DatabaseQueryPayloadLease] = [],
        ambiguityOverride: Set<String>? = nil
    ) {
        precondition(values.count == layout.slots.count)
        self.values = values
        self.layout = layout
        self.annotations = annotations
        self.version = version
        self.payloadLeases = payloadLeases
        self.ambiguityOverride = ambiguityOverride
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
        return CanonicalSourceRow(
            values: values + other.values,
            layout: CanonicalSourceLayout(
                slots: layout.slots + other.layout.slots,
                scopes: layout.scopes + other.layout.scopes,
                coalescedColumns: layout.coalescedColumns.union(other.layout.coalescedColumns),
                types: layout.types + other.layout.types,
                nullability: layout.nullability + other.layout.nullability,
                visibleSlots: layout.visibleSlots + other.layout.visibleSlots.map { layout.slots.count + $0 }
            ),
            annotations: annotations.merging(other.annotations) { current, _ in current },
            payloadLeases: payloadLeases + other.payloadLeases
        )
    }

    func retainingPayload(_ lease: DatabaseQueryPayloadLease) -> Self {
        Self(values: values, layout: layout, annotations: annotations, version: version,
             payloadLeases: payloadLeases + [lease], ambiguityOverride: ambiguityOverride)
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
        let inherited = outer.layout.slots.indices.filter {
            let slot = outer.layout.slots[$0]
            if let scope = slot.scope { return !localScopes.contains(scope) }
            return !localNames.contains(slot.name)
        }
        return CanonicalSourceRow(
            values: inherited.map { outer.values[$0] } + values,
            layout: CanonicalSourceLayout(
                slots: inherited.map { outer.layout.slots[$0] } + layout.slots,
                scopes: outer.layout.scopes.filter { !localScopes.contains($0) } + layout.scopes,
                coalescedColumns: outer.coalescedColumns.subtracting(localNames).union(coalescedColumns),
                types: inherited.map { outer.layout.types[$0] } + layout.types,
                nullability: inherited.map { outer.layout.nullability[$0] } + layout.nullability,
                visibleSlots: inherited.enumerated().compactMap { position, original in
                    outer.layout.visibleSlots.contains(original) && !localNames.contains(outer.layout.slots[original].name)
                        ? position : nil
                } + layout.visibleSlots.map { inherited.count + $0 }
            ),
            annotations: outer.annotations.merging(annotations) { _, local in local },
            version: version,
            payloadLeases: outer.payloadLeases + payloadLeases,
            ambiguityOverride: ambiguousUnqualifiedColumns.union(
                outer.ambiguousUnqualifiedColumns.subtracting(localNames)
            )
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

private struct CanonicalRelationScope: Sendable, Equatable {
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

private struct CanonicalRelationSchema: Sendable, Equatable {
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
        visibleSlots: [Int]? = nil
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
        self.layout = CanonicalSourceLayout(
            slots: unscopedColumns.map { .init(name: $0, scope: nil) }
                + scopes.flatMap { scope in scope.columns.map { .init(name: $0, scope: scope.name) } },
            scopes: scopes.map { $0.name }, coalescedColumns: coalescedColumns,
            types: self.unscopedTypes + scopes.flatMap(\.types),
            nullability: self.unscopedNullability + scopes.flatMap(\.nullability), visibleSlots: visibleSlots)
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

    var slotTypes: [QueryColumnType?] { unscopedTypes + scopes.flatMap(\.types) }
    var slotNullability: [Bool] { unscopedNullability + scopes.flatMap(\.nullability) }

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
            return try binder.commonType([leftType, rightType])
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
        return CanonicalSourceRow(values: Array(repeating: .primitive(.null), count: layout.slots.count), layout: layout)
    }
}

private typealias CanonicalRetainedRows =
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
    return try CanonicalRelationalFootprintMeter.positionalValueArrayFootprint(
        count: count, valueAt: { _ in .primitive(.null) },
        annotations: left.annotations.merging(right.annotations) { current, _ in current },
        version: nil, workMeter: workMeter, stage: .joinCandidate
    ).adding(DatabaseIntermediateCollectionMeter.arrayFootprint(
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
    case .string(let value):
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
    case .literal(.string(let value)):
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
    case .caseWhen(let cases, let elseResult):
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
    case .coalesce(let values):
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
    case .caseWhen(let cases, let elseResult):
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
    case .caseWhen(let cases, let elseResult):
        let caseRequires = cases.contains {
            canonicalExpressionResultRequiresOwnedPayload($0.result)
        }
        let elseRequires = elseResult.map {
            canonicalExpressionResultRequiresOwnedPayload($0)
        } ?? false
        return caseRequires || elseRequires
    case .coalesce(let values):
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
    case .literal, .column, .variable, .parameter, .bound,
            .aggregate, .subquery, .exists:
        break
    case .add(let lhs, let rhs),
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
            .nullIf(let lhs, let rhs):
        try retainChild(lhs)
        try retainChild(rhs)
    case .negate(let nested),
            .not(let nested),
            .isNull(let nested),
            .isNotNull(let nested),
            .like(let nested, _),
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
    case .inSubquery(let value, _):
        try retainChild(value)
    case .function(let function):
        for argument in function.arguments {
            try retainChild(argument)
        }
    case .caseWhen(let cases, let elseResult):
        for pair in cases {
            try retainChild(pair.condition)
            try retainChild(pair.result)
        }
        if let elseResult {
            try retainChild(elseResult)
        }
    case .coalesce(let values):
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

private struct CanonicalRelation: Sendable {
    let schema: CanonicalRelationSchema
    let rows: CanonicalRetainedRows

    init(schema: CanonicalRelationSchema, rows: CanonicalRetainedRows, schemaLayout: CanonicalSourceLayout? = nil) {
        self.schema = schemaLayout.map { schema.retaining(layout: $0) } ?? schema
        self.rows = rows
    }
}

private struct CanonicalGroupKey: Sendable {
    let values: [FieldValue]
    let identity: [FieldValue]
}

private struct CanonicalGroupedRow: Sendable {
    let key: CanonicalGroupKey
    let representative: CanonicalSourceRow
    let rows: CanonicalRetainedRows
}

private struct CanonicalRowValueIdentity: Sendable, Hashable {
    let values: [QueryValue]
    let reservation: DatabaseIntermediateReservation

    static func == (lhs: Self, rhs: Self) -> Bool { lhs.values == rhs.values }

    func hash(into hasher: inout Hasher) { hasher.combine(values) }
}

@inline(__always)
func canonicalHashLookupSlot(hashValue: Int, mask: Int) -> Int {
    precondition(mask >= 0)
    return Int(UInt(bitPattern: hashValue) & UInt(mask))
}

func canonicalQueryRequiresAggregation(_ query: SelectQuery) -> Bool {
    if query.groupBy != nil { return true }
    if query.having != nil { return true }
    if query.orderBy?.contains(
        where: { canonicalExpressionContainsAggregate($0.expression) }
    ) == true {
        return true
    }
    switch query.projection {
    case .all, .allFrom:
        return false
    case .items(let items), .distinctItems(let items):
        return items.contains {
            canonicalExpressionContainsAggregate($0.expression)
        }
    }
}

private func canonicalExpressionContainsAggregate(
    _ expression: Expression
) -> Bool {
    switch expression {
    case .aggregate:
        return true
    case .add(let lhs, let rhs), .subtract(let lhs, let rhs),
            .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
            .modulo(let lhs, let rhs), .equal(let lhs, let rhs),
            .notEqual(let lhs, let rhs), .lessThan(let lhs, let rhs),
            .lessThanOrEqual(let lhs, let rhs),
            .greaterThan(let lhs, let rhs),
            .greaterThanOrEqual(let lhs, let rhs), .and(let lhs, let rhs),
            .or(let lhs, let rhs), .nullIf(let lhs, let rhs):
        return canonicalExpressionContainsAggregate(lhs)
            || canonicalExpressionContainsAggregate(rhs)
    case .negate(let nested), .not(let nested), .isNull(let nested),
            .isNotNull(let nested), .like(let nested, _),
            .regex(let nested, _, _), .cast(let nested, _),
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
    case .inSubquery(let value, _):
        return canonicalExpressionContainsAggregate(value)
    case .function(let function):
        return function.arguments.contains(
            where: canonicalExpressionContainsAggregate
        )
    case .caseWhen(let pairs, let fallback):
        return pairs.contains {
            canonicalExpressionContainsAggregate($0.condition)
                || canonicalExpressionContainsAggregate($0.result)
        } || fallback.map(canonicalExpressionContainsAggregate) == true
    case .coalesce(let values):
        return values.contains(where: canonicalExpressionContainsAggregate)
    case .triple(let subject, let predicate, let object):
        return canonicalExpressionContainsAggregate(subject)
            || canonicalExpressionContainsAggregate(predicate)
            || canonicalExpressionContainsAggregate(object)
    case .literal, .column, .variable, .parameter, .bound, .subquery,
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

private struct CanonicalQueryEvaluationContext: Sendable {
    let options: ReadExecutionContext
    let transaction: DatabaseReadTransaction
    let partitionValues: FieldObject?
    let partitionMode: CanonicalPartitionRoutingMode
    let namedSubqueries: [NamedSubquery]
    let outerRow: CanonicalSourceRow?
    let preparedFusionGraph: FusionPreparedQueryGraph
    let fusionSession: DatabaseReadSession
}

extension DatabaseContext {
    public func query(
        _ selectQuery: SelectQuery,
        options: ReadExecutionOptions = .default,
        graphPartitions: FieldObject = FieldObject()
    ) async throws -> QueryResponse {
        let execution = ReadExecutionContext(
            options: options,
            monotonicClock: container.monotonicClock
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
        graphPartitions: FieldObject = FieldObject()
    ) async throws -> QueryResponse {
        let response = try await queryRetained(
            selectQuery,
            execution: execution,
            graphPartitions: graphPartitions
        )
        return try response.promoteToPublicResponse()
    }

    func queryRetained(
        _ selectQuery: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject = FieldObject()
    ) async throws -> CanonicalReadyQueryResponse {
        do {
            return try await queryRetainedUnmapped(
                selectQuery,
                execution: execution,
                graphPartitions: graphPartitions
            )
        } catch {
            throw sanitizedFusionExecutionError(error)
        }
    }

    private func queryRetainedUnmapped(
        _ selectQuery: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject
    ) async throws -> CanonicalReadyQueryResponse {
        try QueryStructuralValidator.validate(
            selectQuery,
            limits: execution.queryStructuralLimits
        )
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

    /// Runs a preflighted relational Fusion input on the caller-owned
    /// transaction without promoting its retained rows to a public response.
    func executeFusionRelationalRows(
        _ selectQuery: SelectQuery,
        options: ReadExecutionContext,
        preparedFusionGraph: FusionPreparedQueryGraph,
        session: DatabaseReadSession,
        authorization: DatabaseReadAuthorization,
        listAuthorizationRequirement:
            DatabaseListReadAuthorizationRequirement
    ) async throws -> CanonicalRetainedQueryResponse {
        let internalOptions = executionContextWithoutExternalPageWindow(options)
        let authorizedSession = try session.authorizedSession(authorization)
        return try await queryCanonical(
            selectQuery,
            options: internalOptions,
            partitionValues: FieldObject(),
            partitionMode: .strict,
            transaction: authorizedSession.transaction,
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
        authorization: DatabaseReadAuthorization
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
            columns: declared.map(\.name), types: declared.map { $0.type }, nullability: declared.map(\.isNullable))])
        let layoutFootprint = try CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
            fieldNames: declared.lazy.map(\.name), sourceName: sourceName,
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
                outerRow: nil,
                preparedFusionGraph: preparedFusionGraph,
                fusionSession: authorizedSession
            )
        )
    }

    /// Resolves relational bindings before Fusion opens a physical index.
    func validateFusionRelationalInput(
        _ selectQuery: SelectQuery,
        entity: Schema.Entity
    ) throws {
        guard case .table(let tableRef) = selectQuery.source else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "A Fusion relational input must use a table source"
            )
        }
        try validateRelationalQueryBindings(
            selectQuery,
            sourceSchema: try CanonicalRelationSchema(
                scopes: [
                    CanonicalRelationScope(
                        name: tableRef.alias ?? tableRef.effectiveName,
                        columns: entity.allFields
                    )
                ]
            ),
            outerRow: nil
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
        session: DatabaseReadSession
    ) async throws -> QueryResponse {
        do {
            let collecting = try await querySessionBoundRetainedUnmapped(
                selectQuery,
                execution: execution,
                graphPartitions: graphPartitions,
                session: session
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

    package func retainedSessionBoundPage(
        _ selectQuery: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject = FieldObject(),
        session: DatabaseReadSession
    ) async throws -> DatabaseRetainedQueryPage {
        do {
            let collecting = try await querySessionBoundRetainedUnmapped(
                selectQuery,
                execution: execution,
                graphPartitions: graphPartitions,
                session: session
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

    private func querySessionBoundRetainedUnmapped(
        _ selectQuery: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject,
        session: DatabaseReadSession
    ) async throws -> CanonicalRetainedQueryResponse {
        try QueryStructuralValidator.validate(
            selectQuery,
            limits: execution.queryStructuralLimits
        )
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
        try validateAcyclicNamedSubqueries(local)
        let localNames = Set(local.map { $0.name })
        return local + inherited.filter { !localNames.contains($0.name) }
    }

    private func validateAcyclicNamedSubqueries(
        _ subqueries: [NamedSubquery]
    ) throws {
        let names = Set(subqueries.map { $0.name })
        let dependencies = Dictionary(
            uniqueKeysWithValues: subqueries.map { subquery in
                (
                    subquery.name,
                    referencedTableNames(
                        in: subquery.query,
                        among: names
                    )
                )
            }
        )
        var visiting = Set<String>()
        var visited = Set<String>()

        func visit(_ name: String) throws {
            if visiting.contains(name) {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "Recursive common table expression '\(name)' is not supported"
                )
            }
            guard !visited.contains(name) else { return }
            visiting.insert(name)
            for dependency in dependencies[name, default: []] {
                try visit(dependency)
            }
            visiting.remove(name)
            visited.insert(name)
        }

        for name in names {
            try visit(name)
        }
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
            case .add(let lhs, let rhs), .subtract(let lhs, let rhs),
                    .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
                    .modulo(let lhs, let rhs), .equal(let lhs, let rhs),
                    .notEqual(let lhs, let rhs), .lessThan(let lhs, let rhs),
                    .lessThanOrEqual(let lhs, let rhs),
                    .greaterThan(let lhs, let rhs),
                    .greaterThanOrEqual(let lhs, let rhs),
                    .and(let lhs, let rhs), .or(let lhs, let rhs),
                    .nullIf(let lhs, let rhs):
                collect(lhs)
                collect(rhs)
            case .negate(let operand), .not(let operand),
                    .isNull(let operand), .isNotNull(let operand),
                    .like(let operand, _), .regex(let operand, _, _),
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
            case .caseWhen(let cases, let elseResult):
                for pair in cases {
                    collect(pair.condition)
                    collect(pair.result)
                }
                if let elseResult { collect(elseResult) }
            case .coalesce(let expressions):
                expressions.forEach(collect)
            case .triple(let subject, let predicate, let object):
                collect(subject)
                collect(predicate)
                collect(object)
            case .subquery(let subquery), .exists(let subquery):
                names.formUnion(
                    referencedTableNames(
                        in: subquery,
                        among: visibleCandidates
                    )
                )
            case .literal, .column, .variable, .parameter, .bound:
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
            case .subquery(let subquery, _):
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
            case .union(let sources), .unionAll(let sources),
                    .intersect(let sources):
                sources.forEach(collect)
            case .except(let lhs, let rhs):
                collect(lhs)
                collect(rhs)
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
            case .logical, .values:
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
        switch query.projection {
        case .items(let items), .distinctItems(let items):
            items.forEach { collect($0.expression) }
        case .all, .allFrom:
            break
        }
        if let filter = query.filter { collect(filter) }
        for expression in query.groupBy ?? [] { collect(expression) }
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

    private func validateRelationalQueryBindings(
        _ query: SelectQuery,
        sourceSchema: CanonicalRelationSchema,
        outerRow: CanonicalSourceRow?,
        namedSubqueries: [NamedSubquery] = []
    ) throws {
        _ = try canonicalProjectionColumns(
            query.projection,
            sourceSchema: sourceSchema
        )
        switch query.projection {
        case .items(let items), .distinctItems(let items):
            for item in items {
                try validateExpressionBindings(
                    item.expression,
                    sourceSchema: sourceSchema,
                    outerRow: outerRow,
                    namedSubqueries: namedSubqueries
                )
            }
        case .all, .allFrom:
            break
        }
        if let filter = query.filter {
            try validateExpressionBindings(
                filter,
                sourceSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
        }
        for expression in query.groupBy ?? [] {
            try validateExpressionBindings(
                expression,
                sourceSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
        }
        if let having = query.having {
            try validateExpressionBindings(
                having,
                sourceSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
        }
        for sortKey in query.orderBy ?? [] {
            let expression = groupedOrderExpression(
                sortKey.expression,
                projection: query.projection
            )
            try validateExpressionBindings(
                expression,
                sourceSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
        }

        guard canonicalQueryRequiresAggregation(query) else { return }
        let groupBy = query.groupBy ?? []
        let fullSourceRow = sourceSchema.nullRow()
        let currentColumnNames = Set(sourceSchema.unscopedColumns).union(
            sourceSchema.scopes.flatMap { $0.columns }
        )
        let maskedParentRow = outerRow.map { parent in
            CanonicalSourceRow(values: parent.values, layout: parent.layout,
                annotations: parent.annotations, version: parent.version, payloadLeases: parent.payloadLeases,
                ambiguityOverride: parent.ambiguousUnqualifiedColumns.union(currentColumnNames))
        }
        let groupedOuterRow = groupedOuterScope(
            sourceRow: fullSourceRow,
            groupBy: groupBy
        ).overlaying(outer: maskedParentRow)

        func validateGrouped(_ expression: Expression) throws {
            guard !groupBy.contains(expression) else { return }
            try validateGroupedSubqueryBindings(
                expression,
                groupedOuterRow: groupedOuterRow,
                fullSourceRow: fullSourceRow,
                namedSubqueries: namedSubqueries
            )
        }

        switch query.projection {
        case .items(let items), .distinctItems(let items):
            for item in items { try validateGrouped(item.expression) }
        case .all, .allFrom:
            break
        }
        if let having = query.having {
            try validateGrouped(having)
        }
        for sortKey in query.orderBy ?? [] {
            try validateGrouped(
                groupedOrderExpression(
                    sortKey.expression,
                    projection: query.projection
                )
            )
        }
    }

    private func validateGroupedSubqueryBindings(
        _ expression: Expression,
        groupedOuterRow: CanonicalSourceRow,
        fullSourceRow: CanonicalSourceRow,
        namedSubqueries: [NamedSubquery]
    ) throws {
        func validate(_ nested: Expression) throws {
            try validateGroupedSubqueryBindings(
                nested,
                groupedOuterRow: groupedOuterRow,
                fullSourceRow: fullSourceRow,
                namedSubqueries: namedSubqueries
            )
        }

        func validateNested(_ query: SelectQuery) throws {
            do {
                _ = try validateNestedQueryBindings(
                    query,
                    outerSchema: try CanonicalRelationSchema(),
                    outerRow: groupedOuterRow,
                    namedSubqueries: namedSubqueries
                )
            } catch CanonicalReadError.expressionEvaluation(
                .missingColumn(let column)
            ) where sourceRow(fullSourceRow, containsColumnNamed: column) {
                throw CanonicalReadError.aggregateEvaluation(
                    .invalidGroupedExpression(
                        "Correlated column '\(column)' is neither grouped nor aggregated"
                    )
                )
            } catch CanonicalReadError.expressionEvaluation(
                .ambiguousColumn(let column)
            ) where sourceRow(fullSourceRow, containsColumnNamed: column) {
                throw CanonicalReadError.aggregateEvaluation(
                    .invalidGroupedExpression(
                        "Correlated column '\(column)' is neither grouped nor aggregated"
                    )
                )
            }
        }

        switch expression {
        case .add(let lhs, let rhs), .subtract(let lhs, let rhs),
                .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
                .modulo(let lhs, let rhs), .equal(let lhs, let rhs),
                .notEqual(let lhs, let rhs), .lessThan(let lhs, let rhs),
                .lessThanOrEqual(let lhs, let rhs),
                .greaterThan(let lhs, let rhs),
                .greaterThanOrEqual(let lhs, let rhs),
                .and(let lhs, let rhs), .or(let lhs, let rhs),
                .nullIf(let lhs, let rhs):
            try validate(lhs)
            try validate(rhs)
        case .negate(let nested), .not(let nested), .isNull(let nested),
                .isNotNull(let nested), .like(let nested, _),
                .regex(let nested, _, _), .cast(let nested, _),
                .isTriple(let nested), .subject(let nested),
                .predicate(let nested), .object(let nested):
            try validate(nested)
        case .between(let nested, let lower, let upper):
            try validate(nested)
            try validate(lower)
            try validate(upper)
        case .inList(let nested, let values),
                .notInList(let nested, let values):
            try validate(nested)
            for value in values { try validate(value) }
        case .inSubquery(let nested, let query):
            try validate(nested)
            try validateNested(query)
        case .function(let function):
            for argument in function.arguments { try validate(argument) }
        case .caseWhen(let pairs, let fallback):
            for pair in pairs {
                try validate(pair.condition)
                try validate(pair.result)
            }
            if let fallback { try validate(fallback) }
        case .coalesce(let values):
            for value in values { try validate(value) }
        case .triple(let subject, let predicate, let object):
            try validate(subject)
            try validate(predicate)
            try validate(object)
        case .subquery(let query), .exists(let query):
            try validateNested(query)
        case .aggregate:
            // Aggregate arguments are evaluated against each source row, not
            // against the grouped representative.
            return
        case .literal, .column, .variable, .parameter, .bound:
            return
        }
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

    private func validateExpressionBindings(
        _ expression: Expression,
        sourceSchema: CanonicalRelationSchema,
        outerRow: CanonicalSourceRow?,
        namedSubqueries: [NamedSubquery] = []
    ) throws {
        func validate(_ nested: Expression) throws {
            try validateExpressionBindings(
                nested,
                sourceSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
        }

        switch expression {
        case .column(let column):
            try validateColumnBinding(
                column,
                sourceSchema: sourceSchema,
                outerRow: outerRow
            )
        case .add(let lhs, let rhs), .subtract(let lhs, let rhs),
                .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
                .modulo(let lhs, let rhs), .equal(let lhs, let rhs),
                .notEqual(let lhs, let rhs), .lessThan(let lhs, let rhs),
                .lessThanOrEqual(let lhs, let rhs),
                .greaterThan(let lhs, let rhs),
                .greaterThanOrEqual(let lhs, let rhs),
                .and(let lhs, let rhs), .or(let lhs, let rhs),
                .nullIf(let lhs, let rhs):
            try validate(lhs)
            try validate(rhs)
        case .negate(let operand), .not(let operand),
                .isNull(let operand), .isNotNull(let operand),
                .like(let operand, _), .regex(let operand, _, _),
                .cast(let operand, _), .isTriple(let operand),
                .subject(let operand), .predicate(let operand),
                .object(let operand):
            try validate(operand)
        case .between(let operand, let lower, let upper):
            try validate(operand)
            try validate(lower)
            try validate(upper)
        case .inList(let operand, let values),
                .notInList(let operand, let values):
            try validate(operand)
            for value in values { try validate(value) }
        case .inSubquery(let operand, let query):
            try validate(operand)
            let columnCount = try validateNestedQueryBindings(
                query,
                outerSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
            guard columnCount == 1 else {
                throw CanonicalReadError.invalidMembershipSubquery(
                    columnCount: columnCount
                )
            }
        case .aggregate(let aggregate):
            try validateAggregateBindings(
                aggregate,
                sourceSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
        case .function(let function):
            for argument in function.arguments { try validate(argument) }
        case .caseWhen(let cases, let elseResult):
            for pair in cases {
                try validate(pair.condition)
                try validate(pair.result)
            }
            if let elseResult { try validate(elseResult) }
        case .coalesce(let expressions):
            for expression in expressions { try validate(expression) }
        case .triple(let subject, let predicate, let object):
            try validate(subject)
            try validate(predicate)
            try validate(object)
        case .subquery(let query):
            let columnCount = try validateNestedQueryBindings(
                query,
                outerSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
            guard columnCount == 1 else {
                throw CanonicalReadError.invalidScalarSubquery(
                    rowCount: nil,
                    columnCount: columnCount
                )
            }
        case .exists(let query):
            _ = try validateNestedQueryBindings(
                query,
                outerSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
        case .literal, .variable, .parameter, .bound:
            break
        }
    }

    private func validateAggregateBindings(
        _ aggregate: AggregateFunction,
        sourceSchema: CanonicalRelationSchema,
        outerRow: CanonicalSourceRow?,
        namedSubqueries: [NamedSubquery]
    ) throws {
        func validate(_ expression: Expression) throws {
            try validateExpressionBindings(
                expression,
                sourceSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
        }
        switch aggregate {
        case .count(let expression, _):
            if let expression { try validate(expression) }
        case .sum(let expression, _), .avg(let expression, _),
                .min(let expression), .max(let expression),
                .groupConcat(let expression, _, _),
                .sample(let expression):
            try validate(expression)
        case .arrayAgg(let expression, let orderBy, _):
            try validate(expression)
            for sortKey in orderBy ?? [] {
                try validate(sortKey.expression)
            }
        }
    }

    private func validateNestedQueryBindings(
        _ query: SelectQuery,
        outerSchema: CanonicalRelationSchema,
        outerRow: CanonicalSourceRow?,
        namedSubqueries: [NamedSubquery]
    ) throws -> Int {
        let visibleNamedSubqueries = try mergeNamedSubqueries(
            local: query.subqueries ?? [],
            inherited: namedSubqueries
        )
        if sourceRequiresRuntimeInferredSchema(
            query.source,
            namedSubqueries: visibleNamedSubqueries
        ) {
            switch query.projection {
            case .items, .distinctItems:
                return try canonicalProjectionColumns(
                    query.projection,
                    sourceSchema: CanonicalRelationSchema()
                ).count
            case .all, .allFrom:
                throw CanonicalReadError.unsupportedSelectQuery(
                    "A nested query over a runtime-inferred source must declare exactly one output column"
                )
            }
        }
        let sourceSchema = try canonicalRelationSchema(
            for: query.source,
            namedSubqueries: visibleNamedSubqueries
        )
        let syntheticOuterRow = outerSchema.nullRow().overlaying(
            outer: outerRow
        )
        try validateRelationalQueryBindings(
            query,
            sourceSchema: sourceSchema,
            outerRow: syntheticOuterRow,
            namedSubqueries: visibleNamedSubqueries
        )
        try validateStaticJoinBindings(
            query.source,
            namedSubqueries: visibleNamedSubqueries,
            outerRow: syntheticOuterRow
        )
        return try canonicalProjectionColumns(
            query.projection,
            sourceSchema: sourceSchema
        ).count
    }

    private func validateColumnBinding(
        _ column: ColumnRef,
        sourceSchema: CanonicalRelationSchema,
        outerRow: CanonicalSourceRow?
    ) throws {
        let local = sourceSchema.layout.matching(column)
        if local.count > 1 {
            throw CanonicalReadError.expressionEvaluation(.ambiguousColumn(column.displayName))
        }
        if local.count == 1 { return }
        if let qualifier = column.table,
           sourceSchema.scopes.contains(where: { $0.name == qualifier }) {
            throw CanonicalReadError.expressionEvaluation(.missingColumn(column.displayName))
        }
        if let outerRow {
            let outer = outerRow.layout.matching(column)
            if outer.count > 1 || (column.table == nil && outerRow.ambiguousUnqualifiedColumns.contains(column.column)) {
                throw CanonicalReadError.expressionEvaluation(.ambiguousColumn(column.displayName))
            }
            if outer.count == 1 { return }
        }
        throw CanonicalReadError.expressionEvaluation(.missingColumn(column.displayName))
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
        evaluationContext: CanonicalQueryEvaluationContext? = nil
    ) async throws -> CanonicalRetainedQueryResponse {
        try validateRelationalQueryBindings(
            selectQuery,
            sourceSchema: sourceSchema,
            outerRow: evaluationContext?.outerRow,
            namedSubqueries: evaluationContext?.namedSubqueries ?? []
        )
        let binder = try typeBinder(schema: sourceSchema,
            namedSubqueries: evaluationContext?.namedSubqueries ?? [],
            outerSchemas: try evaluationContext?.outerRow.map { [try relationSchema(for: $0.layout)] } ?? [])
        let outputColumns = try binder.bind(selectQuery.replacing(
            orderBy: resolvedOrderBy(selectQuery.orderBy, projection: selectQuery.projection)))
        let filteredRows = try await applyFilter(
            residualFilter,
            to: sourceRows,
            workMeter: options.workMeter,
            evaluationContext: evaluationContext
        )

        let projectedRows: CanonicalRetainedQueryRows
        if canonicalQueryRequiresAggregation(selectQuery) {
            try validateGroupedWildcardProjection(
                selectQuery.projection,
                sourceSchema: sourceSchema,
                groupBy: selectQuery.groupBy ?? []
            )
            let groups = try await makeCanonicalGroups(
                filteredRows,
                groupBy: selectQuery.groupBy ?? [],
                workMeter: options.workMeter,
                evaluationContext: evaluationContext
            )
            let havingGroups = try await applyHaving(
                selectQuery.having,
                to: groups,
                groupBy: selectQuery.groupBy ?? [],
                workMeter: options.workMeter,
                evaluationContext: evaluationContext
            )
            let orderedGroups = try await applyGroupedOrder(
                residualOrderBy,
                to: havingGroups,
                projection: selectQuery.projection,
                groupBy: selectQuery.groupBy ?? [],
                workMeter: options.workMeter,
                evaluationContext: evaluationContext
            )
            projectedRows = try await projectGroupedRows(
                orderedGroups,
                projection: selectQuery.projection,
                columns: outputColumns,
                sourceSchema: sourceSchema,
                groupBy: selectQuery.groupBy ?? [],
                workMeter: options.workMeter,
                evaluationContext: evaluationContext
            )
        } else {
            let orderedRows: CanonicalRetainedRows
            if sourceRowsAlreadyOrdered,
               residualOrderBy?.isEmpty ?? true {
                orderedRows = filteredRows
            } else {
                orderedRows = try await applyOrder(
                    resolvedOrderBy(
                        residualOrderBy,
                        projection: selectQuery.projection
                    ),
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
                evaluationContext: evaluationContext
            )
        }

        let distinctRows: CanonicalRetainedQueryRows
        if selectQuery.distinct {
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
    package func executeCompositionCrossBaseJoin(
        _ selectQuery: SelectQuery,
        join: JoinClause,
        leftRows: consuming DatabaseRetainedQueryRows,
        leftTable: TableRef,
        rightRows: consuming DatabaseRetainedQueryRows,
        rightTable: TableRef,
        options: ReadExecutionContext
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
            fieldNames: leftColumns.lazy.map(\.name), sourceName: leftName,
            workMeter: options.workMeter, stage: .joinCandidate
        )
        let rightFootprint = try CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
            fieldNames: rightColumns.lazy.map(\.name), sourceName: rightName,
            workMeter: options.workMeter, stage: .joinCandidate
        )
        let leftClaim = try options.workMeter.reserveIntermediate(bytes: leftFootprint.bytes, at: .joinCandidate)
        let rightClaim = try options.workMeter.reserveIntermediate(bytes: rightFootprint.bytes, at: .joinCandidate)
        let leftLayout = CanonicalSourceLayout(
            slots: leftColumns.map { .init(name: $0.name, scope: leftName) }, scopes: [leftName],
            reservation: leftClaim, types: leftColumns.map(\.type), nullability: leftColumns.map(\.isNullable)
        )
        let rightLayout = CanonicalSourceLayout(
            slots: rightColumns.map { .init(name: $0.name, scope: rightName) }, scopes: [rightName],
            reservation: rightClaim, types: rightColumns.map(\.type), nullability: rightColumns.map(\.isNullable)
        )
        // Metadata must remain admitted even when either source or the join is empty.
        defer {
            withExtendedLifetime(leftLayout) {}
            withExtendedLifetime(rightLayout) {}
        }
        let leftSchema = try CanonicalRelationSchema(scopes: [CanonicalRelationScope(
            name: leftName, columns: leftColumns.map(\.name),
            types: leftColumns.map(\.type), nullability: leftColumns.map(\.isNullable)
        )]).retaining(layout: leftLayout)
        let rightSchema = try CanonicalRelationSchema(scopes: [CanonicalRelationScope(
            name: rightName, columns: rightColumns.map(\.name),
            types: rightColumns.map(\.type), nullability: rightColumns.map(\.isNullable)
        )]).retaining(layout: rightLayout)
        let left = try materializeLogicalSourceRows(consume leftRows, sourceName: leftName,
                                                   workMeter: options.workMeter, stage: .joinCandidate)
        let right = try materializeLogicalSourceRows(consume rightRows, sourceName: rightName,
                                                    workMeter: options.workMeter, stage: .joinCandidate)
        let joined = try await performJoin(
            left: CanonicalRelation(schema: leftSchema, rows: left, schemaLayout: leftLayout),
            right: CanonicalRelation(schema: rightSchema, rows: right, schemaLayout: rightLayout),
            type: join.type, condition: join.condition, workMeter: options.workMeter
        )
        let collecting = try await finalizeRelationalRows(
            selectQuery, sourceRows: joined.rows, sourceSchema: joined.schema,
            residualFilter: selectQuery.filter, residualOrderBy: selectQuery.orderBy, options: options
        )
        guard collecting.continuation == nil,
              collecting.visibleRange == 0..<collecting.rows.count else {
            throw CompositionQueryError.workspaceCorrupted
        }
        // The Composition snapshot owns the producing resource close. Carry
        // collecting rows and their claims into its event owner without promotion.
        return collecting.rows
    }
    #endif

    private func queryCanonical(
        _ selectQuery: SelectQuery,
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
        partitionMode: CanonicalPartitionRoutingMode,
        transaction: DatabaseReadTransaction,
        inheritedSubqueries: [NamedSubquery] = [],
        outerRow: CanonicalSourceRow? = nil,
        preparedFusionGraph: FusionPreparedQueryGraph,
        fusionSession: DatabaseReadSession,
        admittedListAuthorizationRequirement:
            DatabaseListReadAuthorizationRequirement? = nil
    ) async throws -> CanonicalRetainedQueryResponse {
        let namedSubqueries = try mergeNamedSubqueries(
            local: selectQuery.subqueries ?? [],
            inherited: inheritedSubqueries
        )
        let evaluationContext = CanonicalQueryEvaluationContext(
            options: options,
            transaction: transaction,
            partitionValues: partitionValues,
            partitionMode: partitionMode,
            namedSubqueries: namedSubqueries,
            outerRow: outerRow,
            preparedFusionGraph: preparedFusionGraph,
            fusionSession: fusionSession
        )
        if !selectQuery.source.usesSPARQLSemantics,
           !sourceRequiresRuntimeInferredSchema(
            selectQuery.source,
            namedSubqueries: namedSubqueries
        ) {
            let sourceSchema = try canonicalRelationSchema(
                for: selectQuery.source,
                namedSubqueries: namedSubqueries,
                outerSchemas: try outerRow.map { [try relationSchema(for: $0.layout)] } ?? []
            )
            try validateRelationalQueryBindings(
                selectQuery,
                sourceSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
            try validateStaticJoinBindings(
                selectQuery.source,
                namedSubqueries: namedSubqueries,
                outerRow: outerRow
            )
        }
        _ = try boundQueryColumns(selectQuery, inheritedNamedSubqueries: inheritedSubqueries,
            outerSchemas: try outerRow.map { [try relationSchema(for: $0.layout)] } ?? [])
        if let accessPath = selectQuery.accessPath {
            return try await executeAccessPathRows(
                selectQuery,
                accessPath: accessPath,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                evaluationContext: evaluationContext,
                preparedFusionGraph: preparedFusionGraph
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
                    admittedListAuthorizationRequirement
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
            namedSubqueries: namedSubqueries,
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
        preparedFusionGraph: FusionPreparedQueryGraph
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
                let listAuthorizationRequirement = try DatabaseReadPolicy
                    .listRequirement(
                        entityName: tableRef.table,
                        selectQuery: selectQuery
                    )
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
                    options: options
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
                evaluationContext: evaluationContext
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
    private func admittedRelationSchema(for layout: CanonicalSourceLayout) throws -> CanonicalRelationSchema {
        guard let claim = layout.reservation else {
            throw CanonicalReadError.unsupportedSource("Source layout has no admitted metadata owner")
        }
        let footprint = try CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
            fieldNames: layout.slots.lazy.map(\.name), sourceName: nil,
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
                selectQuery: selectQuery
            )
        let pushdown = try await fetchTableSourceRows(
            runtime: runtime,
            sourceName: sourceName,
            selectQuery: selectQuery,
            authorizationRequirement: authorizationRequirement,
            options: options,
            transaction: transaction
        )

        // When LIMIT/OFFSET are pushed down to the typed fetch, strip them from the
        // pagination input so pagination doesn't re-apply them.
        let paginationQuery: SelectQuery
        if pushdown.limitPushed || pushdown.offsetPushed {
            var modified = selectQuery
            if pushdown.limitPushed { modified = modified.replacing(limit: nil) }
            if pushdown.offsetPushed { modified = modified.replacing(offset: nil) }
            paginationQuery = modified
        } else {
            paginationQuery = selectQuery
        }

        return try await finalizeRelationalRows(
            selectQuery,
            sourceRows: pushdown.rows,
            sourceSchema: try admittedRelationSchema(for: pushdown.sourceLayout),
            residualFilter: pushdown.residualFilter,
            residualOrderBy: pushdown.residualOrderBy,
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
            selectQuery: authorizationQuery
        )
        let sourceName = tableRef.alias ?? tableRef.effectiveName
        let select = SelectQuery(
            projection: .all,
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
            queryStructuralLimits: options.queryStructuralLimits
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
                fieldNames: entity.fields.lazy.filter { admitted.contains($0.name) }.map(\.name),
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
        }, wildcardSlots: { _ in [] })
        let types = try names.map { name -> QueryColumnType? in
            var candidates: [QueryColumnType] = []
            for member in group.memberTypeNames {
                guard let entity = declarations[member] else { throw DatabaseReadSessionError.authorizationMismatch }
                guard let field = entity.fieldMapByName[name] else { continue }
                guard allowed[member]?.contains(name) == true else { throw DatabaseReadSessionError.authorizationMismatch }
                candidates.append(field.queryType ?? .primitive(field.type, isArray: field.isArray))
            }
            return try binder.commonType(candidates)
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
        let sourceName = logicalSource.alias ?? logicalSource.effectiveName
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
        namedSubqueries: [NamedSubquery],
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
                    inheritedSubqueries: namedSubqueries,
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
                    namedSubqueries: namedSubqueries,
                    workMeter: options.workMeter
                )
            }

            return try await materializeUnwindowedTableSourceRows(
                tableRef,
                options: options,
                transaction: transaction,
                authorizationQuery: authorizationQuery
            )

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
            let sourceName = logicalSource.alias ?? logicalSource.effectiveName
            return try materializePolymorphicSourceRows(
                entities,
                group: group,
                sourceName: sourceName,
                authorization: authorization,
                stage: .bindingCandidate
            )

        case .subquery(let query, let alias):
            let response = try await queryCanonical(
                query,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                inheritedSubqueries: namedSubqueries,
                outerRow: allowsOuterReferences ? outerRow : nil,
                preparedFusionGraph: preparedFusionGraph,
                fusionSession: fusionSession
            )
            return try materializeQueryRelation(
                response.visibleRows,
                query: query,
                explicitColumns: nil,
                alias: alias,
                namedSubqueries: namedSubqueries,
                workMeter: options.workMeter
            )

        case .join(let clause):
            return try await materializeJoinRows(
                clause,
                namedSubqueries: namedSubqueries,
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

        case .union(let sources):
            return try await materializeUnionRows(
                sources,
                deduplicate: true,
                namedSubqueries: namedSubqueries,
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

        case .unionAll(let sources):
            return try await materializeUnionRows(
                sources,
                deduplicate: false,
                namedSubqueries: namedSubqueries,
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

        case .intersect(let sources):
            return try await materializeIntersectRows(
                sources,
                namedSubqueries: namedSubqueries,
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

        case .except(let lhs, let rhs):
            return try await materializeExceptRows(
                lhs,
                rhs,
                namedSubqueries: namedSubqueries,
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

        case .values(let rows, _):
            let schema = try canonicalRelationSchema(for: source, namedSubqueries: namedSubqueries)
            let names = schema.unscopedColumns
            let layoutFootprint = try CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
                fieldNames: names, sourceName: nil, workMeter: options.workMeter, stage: .bindingCandidate)
            let layoutClaim = try options.workMeter.reserveIntermediate(bytes: layoutFootprint.bytes, at: .bindingCandidate)
            let layout = CanonicalSourceLayout(slots: names.map { .init(name: $0, scope: nil) },
                reservation: layoutClaim, types: schema.slotTypes, nullability: schema.slotNullability)
            var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
                workMeter: options.workMeter, stage: .bindingCandidate,
                layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self), expectedCount: rows.count)
            for literals in rows {
                try options.workMeter.consume(at: .bindingCandidate)
                let footprint = try prospectiveLiteralSourceRowFootprint(
                    fieldNames: names, literals: literals, workMeter: options.workMeter, stage: .bindingCandidate)
                try retained.append(footprint: footprint) {
                    let values = try literals.enumerated().map { index, literal -> QueryValue in
                        let value: QueryValue
                        if case .queryValue(let rich, _) = literal { value = rich }
                        else { value = .primitive(try literal.toFieldValue()) }
                        guard let type = schema.slotTypes[index] else {
                            throw CanonicalReadError.unsupportedSelectQuery("VALUES column type is unresolved")
                        }
                        return try CanonicalQueryTypeBinder.normalizeSourceCarrier(value, to: type)
                    }
                    return CanonicalSourceRow(values: values, layout: layout)
                }
            }
            return CanonicalRelation(schema: schema,
                rows: try retained.finish().moveToSharedOwnership(at: .bindingCandidate), schemaLayout: layout)

        case .graphTable(let graphTableSource):
            guard let executor = try readPolicy().graphTableSourceExecutor else {
                throw CanonicalReadError.unsupportedSource("GRAPH_TABLE executor is not registered")
            }
            let sourceRows = try await executor.executeInTransaction(
                session: fusionSession, graphTableSource: graphTableSource,
                options: options, partitions: partitionValues ?? FieldObject())
            let rawColumns = sourceRows.columns
            let rawSchema = try CanonicalRelationSchema(unscopedColumns: rawColumns.map(\.name),
                types: rawColumns.map { $0.type }, nullability: rawColumns.map(\.isNullable))
            let graphRows = try materializeLogicalSourceRows(consume sourceRows, sourceName: nil,
                workMeter: options.workMeter, stage: .bindingCandidate)
            let projection: Projection
            let outputColumns: [QueryColumn]
            if let columns = graphTableSource.columns, !columns.isEmpty {
                projection = .items(columns.map { ProjectionItem($0.expression, alias: $0.alias) })
                let binder = try typeBinder(schema: rawSchema, namedSubqueries: namedSubqueries)
                outputColumns = try columns.enumerated().map { index, column in
                    let expression = try binder.bind(column.expression)
                    return try QueryColumn(number: UInt32(index + 1), name: column.alias,
                        type: expression.type, isNullable: expression.isNullable)
                }
            } else { projection = .all; outputColumns = rawColumns }
            let projected = try await projectRows(graphRows, projection: projection, columns: outputColumns,
                sourceSchema: rawSchema, workMeter: options.workMeter,
                evaluationContext: CanonicalQueryEvaluationContext(options: options, transaction: transaction,
                    partitionValues: partitionValues, partitionMode: partitionMode, namedSubqueries: namedSubqueries,
                    outerRow: allowsOuterReferences ? outerRow : nil,
                    preparedFusionGraph: preparedFusionGraph, fusionSession: fusionSession))
            let view = CanonicalRetainedQueryRowView(owner: projected, range: 0..<projected.count)
            return try materializeQueryRelation(view, query: authorizationQuery,
                explicitColumns: nil, alias: graphTableSource.alias,
                namedSubqueries: namedSubqueries, workMeter: options.workMeter)

        case .graphPattern:
            guard let executor = try readPolicy().sparqlSourceExecutor else {
                throw CanonicalReadError.unsupportedSource("SPARQL source executor is not registered")
            }
            let sourceSession = try fusionSession.admittingRDFDatasetRead()
            let sourceRows = try await executor.executeInTransaction(
                session: sourceSession,
                selectQuery: SelectQuery(
                    projection: .all,
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
                    unscopedColumns: declaredColumns.map(\.name),
                    types: declaredColumns.map { $0.type }, nullability: declaredColumns.map(\.isNullable)
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
                    projection: .all,
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
                    unscopedColumns: declaredColumns.map(\.name),
                    types: declaredColumns.map { $0.type }, nullability: declaredColumns.map(\.isNullable)
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
        namedSubqueries: [NamedSubquery],
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
            namedSubqueries: namedSubqueries,
            outerRow: outerRow,
            preparedFusionGraph: preparedFusionGraph,
            fusionSession: fusionSession
        )
        switch clause.type {
        case .lateral, .leftLateral:
            guard !sourceRequiresRuntimeInferredSchema(
                clause.right,
                namedSubqueries: namedSubqueries
            ) else {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "A LATERAL source must declare a stable output schema"
                )
            }
            let left = try await materializeRows(
                for: clause.left,
                namedSubqueries: namedSubqueries,
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
            let rightSchema = try canonicalRelationSchema(
                for: clause.right,
                namedSubqueries: namedSubqueries,
                outerSchemas: [left.schema] + (try outerRow.map { [try relationSchema(for: $0.layout)] } ?? [])
            )
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
                    prospectiveFootprint: try prospectiveSourceCompositionFootprint(
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
                        for: clause.right,
                        namedSubqueries: namedSubqueries,
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
                for: clause.left,
                namedSubqueries: namedSubqueries,
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
                for: clause.right,
                namedSubqueries: namedSubqueries,
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
                workMeter: options.workMeter,
                evaluationContext: evaluationContext
            )
        default:
            let left = try await materializeRows(
                for: clause.left,
                namedSubqueries: namedSubqueries,
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
                for: clause.right,
                namedSubqueries: namedSubqueries,
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
            fieldNames: outputSchema.layout.slots.lazy.map(\.name), sourceName: nil,
            workMeter: workMeter, stage: .joinCandidate)
        let layoutClaim = try workMeter.reserveIntermediate(bytes: layoutFootprint.bytes, at: .joinCandidate)
        let declaredLayout = outputSchema.layout
        let outputLayout = CanonicalSourceLayout(slots: declaredLayout.slots, scopes: declaredLayout.scopes,
            coalescedColumns: declaredLayout.coalescedColumns, reservation: layoutClaim,
            types: outputSchema.slotTypes, nullability: outputSchema.slotNullability,
            visibleSlots: declaredLayout.visibleSlots,
            payloadLeases: [left.rows.payloadLease, right.rows.payloadLease])
        if case .on(let expression) = condition {
            try validateExpressionBindings(
                expression,
                sourceSchema: outputSchema,
                outerRow: evaluationContext?.outerRow,
                namedSubqueries: evaluationContext?.namedSubqueries ?? []
            )
        }
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
                                condition: condition, layout: outputLayout
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
                    joinType: type,
                    evaluationContext: evaluationContext,
                    workMeter: workMeter
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
                                condition: condition, layout: outputLayout
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
                            condition: condition, layout: outputLayout
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
                            condition: condition, layout: outputLayout
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
        layout: CanonicalSourceLayout
    ) throws -> CanonicalSourceRow {
        let coalescing: [String]
        if case .using(let columns) = condition { coalescing = columns } else { coalescing = [] }
        let coalesced = Set(coalescing)
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
            values.append(try CanonicalQueryTypeBinder.normalize(lhs.isNull ? rhs : lhs, to: type))
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
        return CanonicalSourceRow(values: values, layout: layout,
            annotations: left.annotations.merging(right.annotations) { current, _ in current },
            payloadLeases: left.payloadLeases + right.payloadLeases)
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
        let values: [FieldValue]
    }

    private func performHashJoin(
        leftRows: CanonicalRetainedRows,
        rightRows: CanonicalRetainedRows,
        type: JoinType,
        condition: JoinCondition?,
        emptyLeft: CanonicalSourceRow,
        emptyRight: CanonicalSourceRow,
        outputLayout: CanonicalSourceLayout,
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedRows? {
        guard let plan = canonicalHashJoinPlan(
            condition: condition,
            leftRows: leftRows,
            rightRows: rightRows
        ) else {
            return nil
        }

        let keySlotBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(max(1, MemoryLayout<FieldValue?>.stride + 16))
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
                row: row
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
            if let key = try canonicalJoinKey(sources: plan.left, row: left) {
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
                       joinType: type,
                       evaluationContext: evaluationContext,
                       workMeter: workMeter
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
                            condition: condition, layout: outputLayout
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
                            condition: condition, layout: outputLayout
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
                            condition: condition, layout: outputLayout
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
        sources: [CanonicalJoinKeySource],
        row: CanonicalSourceRow
    ) throws -> CanonicalJoinKey? {
        var values: [FieldValue] = []
        values.reserveCapacity(sources.count)
        for source in sources {
            guard let value = joinValue(source: source, row: row),
                  value != .null else {
                return nil
            }
            values.append(
                try canonicalValueIdentity(value, operation: "hash JOIN")
            )
        }
        return CanonicalJoinKey(values: values)
    }

    private func joinValue(
        source: CanonicalJoinKeySource,
        row: CanonicalSourceRow
    ) -> FieldValue? {
        switch source {
        case .column(let column):
            return row.value(for: column)
        case .unqualified(let column):
            return firstScopedFieldValue(named: column, in: row)
        }
    }

    private func joinMatches(
        left: CanonicalSourceRow,
        right: CanonicalSourceRow,
        condition: JoinCondition?,
        joinType: JoinType,
        evaluationContext: CanonicalQueryEvaluationContext?,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        if joinType == .cross {
            return true
        }

        guard let condition else { return true }
        switch condition {
        case .using(let columns):
            for column in columns {
                guard let leftValue = firstScopedFieldValue(
                    named: column,
                    in: left
                ), let rightValue = firstScopedFieldValue(
                    named: column,
                    in: right
                ) else {
                    return false
                }
                do {
                    guard try FieldValueComparator.equal(
                        leftValue,
                        rightValue
                    ) else {
                        return false
                    }
                } catch let failure {
                    throw canonicalComparisonReadError(
                        failure,
                        operation: "JOIN USING equality"
                    )
                }
            }
            return true
        case .on(let expression):
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
                try await evaluateQueryBoolean(
                    expression,
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
        let leftColumns = Set(
            leftSchema.unscopedColumns
                + leftSchema.scopes.flatMap { $0.columns }
        )
        let rightColumns = Set(
            rightSchema.unscopedColumns
                + rightSchema.scopes.flatMap { $0.columns }
        )
        return Array(leftColumns.intersection(rightColumns)).sorted()
    }

    private func naturalJoinBaseType(_ type: JoinType) -> JoinType {
        switch type {
        case .naturalLeft:
            return .left
        case .naturalRight:
            return .right
        case .naturalFull:
            return .full
        default:
            return .inner
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

    private func materializeUnionRows(
        _ sources: [DataSource],
        deduplicate: Bool,
        namedSubqueries: [NamedSubquery],
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
        let outputSchema = try setOperationSchema(
            sources: sources, namedSubqueries: namedSubqueries,
            outerSchemas: try allowsOuterReferences ? outerRow.map { [try relationSchema(for: $0.layout)] } ?? [] : []
        )
        let first = try await materializeRows(
            for: firstSource,
            namedSubqueries: namedSubqueries,
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
        for source in sources.dropFirst() {
            let relation = try await materializeRows(
                for: source,
                namedSubqueries: namedSubqueries,
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
        namedSubqueries: [NamedSubquery],
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
        let outputSchema = try setOperationSchema(
            sources: sources, namedSubqueries: namedSubqueries,
            outerSchemas: try allowsOuterReferences ? outerRow.map { [try relationSchema(for: $0.layout)] } ?? [] : []
        )
        let firstRelation = try await materializeRows(
            for: first,
            namedSubqueries: namedSubqueries,
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
        for source in sources.dropFirst() {
            let nextRelation = try await materializeRows(
                for: source,
                namedSubqueries: namedSubqueries,
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
            var nextKeys = Set<CanonicalRowValueIdentity>()
            nextKeys.reserveCapacity(next.count)
            for row in next {
                try options.workMeter.consume(at: .deduplication)
                nextKeys.insert(
                    try identityRow(row, operation: "INTERSECT", workMeter: options.workMeter)
                )
            }
            var intersected = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
                workMeter: options.workMeter,
                stage: .joinCandidate,
                layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
                expectedCount: accumulator.count
            )
            for row in accumulator {
                try options.workMeter.consume(at: .joinCandidate)
                guard nextKeys.contains(
                    try identityRow(row, operation: "INTERSECT", workMeter: options.workMeter)
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
        namedSubqueries: [NamedSubquery],
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
        let outputSchema = try setOperationSchema(
            sources: [lhs, rhs], namedSubqueries: namedSubqueries,
            outerSchemas: try allowsOuterReferences ? outerRow.map { [try relationSchema(for: $0.layout)] } ?? [] : []
        )
        let left = try await materializeRows(
            for: lhs,
            namedSubqueries: namedSubqueries,
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
            for: rhs,
            namedSubqueries: namedSubqueries,
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
        var rightKeys = Set<CanonicalRowValueIdentity>()
        rightKeys.reserveCapacity(rightRows.count)
        for row in rightRows {
            try options.workMeter.consume(at: .deduplication)
            rightKeys.insert(
                try identityRow(row, operation: "EXCEPT", workMeter: options.workMeter)
            )
        }
        var difference = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: options.workMeter,
            stage: .joinCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: leftRows.count
        )
        for row in leftRows {
            try options.workMeter.consume(at: .joinCandidate)
            guard !rightKeys.contains(
                try identityRow(row, operation: "EXCEPT", workMeter: options.workMeter)
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
        var seen = Set<CanonicalRowValueIdentity>()
        seen.reserveCapacity(rows.count)
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
            if seen.insert(key).inserted {
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
        )
        let reservation = try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .deduplication)
        return try CanonicalRowValueIdentity(
            values: canonicalIdentityValues(row, operation: operation),
            reservation: reservation
        )
    }

    private func canonicalIdentityValues(
        _ row: CanonicalSourceRow,
        operation: String
    ) throws -> [QueryValue] {
        var values: [QueryValue] = []
        values.reserveCapacity(row.layout.visibleSlots.count)
        for slot in row.layout.visibleSlots {
            let value = row.values[slot]
            values.append(try canonicalQueryValueIdentity(value, operation: operation))
        }
        return values
    }

    private func canonicalValueIdentity(
        _ value: FieldValue,
        operation: String
    ) throws -> FieldValue {
        do {
            return try RelationalValueIdentity.canonicalize(value).value
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

    private func canonicalQueryValueIdentity(_ value: QueryValue, operation: String) throws -> QueryValue {
        switch value {
        case .primitive(let primitive):
            return .primitive(try canonicalValueIdentity(primitive, operation: operation))
        case .numeric(let numeric):
            return .primitive(try canonicalValueIdentity(.decimal(numeric.value), operation: operation))
        default:
            // Other rich SQL equality is owned by the later semantic comparator.
            return value
        }
    }

    private func applyFilter(
        _ filter: DatabaseKit.Expression?,
        to rows: CanonicalRetainedRows,
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedRows {
        guard let filter else { return rows }
        var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .filterEvaluation,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: rows.count
        )
        for row in rows {
            try workMeter.consume(at: .filterEvaluation)
            guard try await evaluateQueryBoolean(
                filter,
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
        to rows: CanonicalRetainedRows,
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedRows {
        guard let orderBy, !orderBy.isEmpty else { return rows }
        let outerArrayFootprint = try DatabaseIntermediateCollectionMeter
            .arrayFootprint(
                count: rows.count,
                element: (
                    CanonicalSourceRow,
                    [DatabaseQueryScopedFieldValue],
                    ByteString
                ).self
            )
        let nestedValuesFootprint = try DatabaseIntermediateCollectionMeter
            .arrayFootprint(
                count: orderBy.count,
                element: DatabaseQueryScopedFieldValue.self
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
            [DatabaseQueryScopedFieldValue],
            ByteString
        )] = []
        decorated.reserveCapacity(rows.count)
        for row in rows {
            var values: [DatabaseQueryScopedFieldValue] = []
            values.reserveCapacity(orderBy.count)
            for key in orderBy {
                values.append(
                    try await evaluateQueryExpression(
                        key.expression,
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
            [DatabaseQueryScopedFieldValue],
            ByteString
        )]
        do {
            sorted = try decorated.sorted { lhs, rhs in
                for (index, sortKey) in orderBy.enumerated() {
                    try workMeter.consume(2, at: .sortComparison)
                    var comparison = QueryComparison.equal
                    try lhs.1[index].withValue { lhsValue in
                        try rhs.1[index].withValue { rhsValue in
                            comparison = try FieldValueComparator.compare(
                                lhsValue,
                                rhsValue,
                                using: sortKey
                            )
                        }
                    }
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

    private struct ProjectedQueryValue {
        let value: QueryValue
        let leases: [DatabaseQueryPayloadLease]
        let payloadIsRetained: Bool
    }

    private func projectedQueryValue(
        _ expression: Expression,
        on row: CanonicalSourceRow,
        context: CanonicalQueryEvaluationContext?,
        workMeter: DatabaseWorkMeter
    ) async throws -> ProjectedQueryValue {
        switch expression {
        case .literal(.queryValue(let value, _)):
            let footprint = try CanonicalRelationalFootprintMeter.valueFootprint(of: value,
                workMeter: workMeter, stage: .projection)
            let admitted = try footprint.adding(DatabaseIntermediateFootprint(bytes: 64))
            let claim = try workMeter.reserveIntermediate(bytes: admitted.bytes, at: .projection)
            return ProjectedQueryValue(value: value, leases: [DatabaseQueryPayloadLease(retaining: claim)], payloadIsRetained: true)
        case .column(let reference):
            if let value = row.queryValue(for: reference) {
                return ProjectedQueryValue(value: value, leases: [], payloadIsRetained: true)
            }
        case .subquery(let query):
            let effectiveRows = try retainedConstructedSourceRow(
                prospectiveFootprint: prospectiveSourceCompositionFootprint(
                    row, outer: context?.outerRow, workMeter: workMeter),
                workMeter: workMeter, stage: .expressionEvaluation,
                make: { row.overlaying(outer: context?.outerRow) })
            // The COW row copy borrows shared payload storage. Keep the
            // admitted overlay owner live through the nested asynchronous read.
            defer { withExtendedLifetime(effectiveRows) {} }
            let effectiveRow = effectiveRows.withElement(at: 0) { copy $0 }
            let result = try await executeNestedQuery(query, outerRow: effectiveRow, context: context)
            guard result.visibleRows.count <= 1 else {
                throw CanonicalReadError.invalidScalarSubquery(rowCount: result.visibleRows.count, columnCount: nil)
            }
            guard result.rows.columns.count == 1 else {
                throw CanonicalReadError.invalidScalarSubquery(rowCount: result.visibleRows.count, columnCount: result.rows.columns.count)
            }
            if result.visibleRows.isEmpty {
                return ProjectedQueryValue(value: .primitive(.null), leases: [], payloadIsRetained: false)
            }
            var cell: QueryValue?
            result.visibleRows.withElement(at: 0) { cell = $0.values[0] }
            guard let cell else { throw CanonicalReadError.invalidScalarSubquery(rowCount: 0, columnCount: 1) }
            return ProjectedQueryValue(value: cell, leases: result.rows.originPayloadLeases, payloadIsRetained: true)
        case .caseWhen(let cases, let fallback):
            for branch in cases where try await evaluateQueryBoolean(
                branch.condition, on: row, context: context, workMeter: workMeter) {
                return try await projectedQueryValue(branch.result, on: row, context: context, workMeter: workMeter)
            }
            guard let fallback else { return ProjectedQueryValue(value: .primitive(.null), leases: [], payloadIsRetained: false) }
            return try await projectedQueryValue(fallback, on: row, context: context, workMeter: workMeter)
        case .coalesce(let expressions):
            for expression in expressions {
                let cell = try await projectedQueryValue(expression, on: row, context: context, workMeter: workMeter)
                if !cell.value.isNull { return cell }
            }
            return ProjectedQueryValue(value: .primitive(.null), leases: [], payloadIsRetained: false)
        default: break
        }
        let scoped = try await evaluateQueryExpression(expression, on: row, context: context, workMeter: workMeter)
        let leases = scoped.retainedReservation.map { [DatabaseQueryPayloadLease(retaining: $0)] } ?? []
        return scoped.withValue { ProjectedQueryValue(value: .primitive(copy $0), leases: leases, payloadIsRetained: !leases.isEmpty) }
    }

    private func projectRows(
        _ rows: CanonicalRetainedRows,
        projection: Projection,
        columns: [QueryColumn],
        sourceSchema: CanonicalRelationSchema,
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedQueryRows {
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
        let selectedSlots: [Int]?
        let expressions: [Expression]?
        switch projection {
        case .all: selectedSlots = try sourceSchema.wildcardSlots(scope: nil); expressions = nil
        case .allFrom(let scope): selectedSlots = try sourceSchema.wildcardSlots(scope: scope); expressions = nil
        case .items(let items), .distinctItems(let items): selectedSlots = nil; expressions = items.map(\.expression)
        }
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
                for (index, expression) in (expressions ?? []).enumerated() {
                    let cell = try await projectedQueryValue(expression, on: row, context: evaluationContext, workMeter: workMeter)
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
        if case .distinctItems = projection { return try canonicalUniqueRows(result, workMeter: workMeter) }
        return result
    }

    private func makeCanonicalGroups(
        _ rows: CanonicalRetainedRows,
        groupBy: [Expression],
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
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

        func lookupSlot(for identity: [FieldValue]) -> (Int, Int?) {
            let mask = lookupCapacity - 1
            var slot = canonicalHashLookupSlot(
                hashValue: identity.hashValue,
                mask: mask
            )
            while let groupIndex = groupLookup[slot] {
                if keys[groupIndex].identity == identity {
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
            var scopedValues: [DatabaseQueryScopedFieldValue] = []
            scopedValues.reserveCapacity(groupBy.count)
            for expression in groupBy {
                scopedValues.append(
                    try await evaluateQueryExpression(
                        expression,
                        on: row,
                        context: evaluationContext,
                        workMeter: workMeter
                    )
                )
            }
            let keyArrays = try DatabaseIntermediateCollectionMeter
                .arrayFootprint(
                    count: groupBy.count,
                    element: FieldValue.self
                ).adding(
                    DatabaseIntermediateCollectionMeter.arrayFootprint(
                        count: groupBy.count,
                        element: FieldValue.self
                    )
                )
            let keyReservation = try workMeter.reserveIntermediate(
                bytes: keyArrays.bytes,
                at: .aggregateInput
            )
            defer { keyReservation.release() }
            var values: [FieldValue] = []
            values.reserveCapacity(groupBy.count)
            var identities: [FieldValue] = []
            identities.reserveCapacity(groupBy.count)
            for scopedValue in scopedValues {
                try scopedValue.withValue { value in
                    let valueFootprint = try CanonicalRelationalFootprintMeter
                        .valueFootprint(
                            of: value,
                            workMeter: workMeter,
                            stage: .aggregateInput
                        )
                    try keyReservation.reserveAdditional(
                        bytes: valueFootprint.bytes,
                        at: .aggregateInput
                    )
                    values.append(value)

                    let identityScratch = try workMeter.reserveIntermediate(
                        bytes: valueFootprint.bytes,
                        at: .aggregateInput
                    )
                    defer { identityScratch.release() }
                    let identity = try canonicalValueIdentity(
                        value,
                        operation: "GROUP BY"
                    )
                    let identityFootprint = try CanonicalRelationalFootprintMeter
                        .valueFootprint(
                            of: identity,
                            workMeter: workMeter,
                            stage: .aggregateInput
                        )
                    precondition(
                        identityFootprint.bytes <= valueFootprint.bytes,
                        "Canonical value identity cannot grow its source payload"
                    )
                    try keyReservation.reserveAdditional(
                        bytes: identityFootprint.bytes,
                        at: .aggregateInput
                    )
                    identities.append(identity)
                }
            }
            let (slot, existingGroupIndex) = lookupSlot(for: identities)
            if let groupIndex = existingGroupIndex {
                nextRow[groupTails[groupIndex]] = position
                groupTails[groupIndex] = position
            } else {
                try stateReservation.absorbAll(from: keyReservation)
                let groupIndex = keys.count
                keys.append(
                    CanonicalGroupKey(
                        values: values,
                        identity: identities
                    )
                )
                representatives.append(row)
                groupHeads.append(position)
                groupTails.append(position)
                groupLookup[slot] = groupIndex
            }
        }

        if rows.isEmpty, groupBy.isEmpty {
            keys.append(CanonicalGroupKey(values: [], identity: []))
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
            let group = CanonicalGroupedRow(
                key: keys[index],
                representative: representatives[index],
                rows: retainedGroupRows
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
        to groups: CanonicalRetainedGroups,
        groupBy: [Expression],
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedGroups {
        guard let having else { return groups }
        var result = try DatabaseRetainedArrayBuilder<CanonicalGroupedRow>(
            workMeter: workMeter,
            stage: .aggregateInput,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalGroupedRow.self),
            expectedCount: groups.count
        )
        for group in groups {
            try workMeter.consume(at: .aggregateInput)
            let value = try await evaluateGroupedExpression(
                having,
                group: group,
                groupBy: groupBy,
                workMeter: workMeter,
                evaluationContext: evaluationContext
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
        to groups: CanonicalRetainedGroups,
        projection: Projection,
        groupBy: [Expression],
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedGroups {
        guard let orderBy, !orderBy.isEmpty else { return groups }
        let outerArrayFootprint = try DatabaseIntermediateCollectionMeter
            .arrayFootprint(
                count: groups.count,
                element: (
                    CanonicalGroupedRow,
                    [DatabaseQueryScopedFieldValue]
                ).self
            )
        let nestedValuesFootprint = try DatabaseIntermediateCollectionMeter
            .arrayFootprint(
                count: orderBy.count,
                element: DatabaseQueryScopedFieldValue.self
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
            [DatabaseQueryScopedFieldValue]
        )] = []
        decorated.reserveCapacity(groups.count)
        for group in groups {
            var values: [DatabaseQueryScopedFieldValue] = []
            values.reserveCapacity(orderBy.count)
            for sortKey in orderBy {
                let expression = groupedOrderExpression(
                    sortKey.expression,
                    projection: projection
                )
                values.append(
                    try await evaluateGroupedExpression(
                        expression,
                        group: group,
                        groupBy: groupBy,
                        workMeter: workMeter,
                        evaluationContext: evaluationContext
                    )
                )
            }
            decorated.append((group, values))
        }
        let sorted: [(
            CanonicalGroupedRow,
            [DatabaseQueryScopedFieldValue]
        )]
        do {
            sorted = try decorated.sorted { lhs, rhs in
                for (index, sortKey) in orderBy.enumerated() {
                    try workMeter.consume(2, at: .sortComparison)
                    var comparison = QueryComparison.equal
                    try lhs.1[index].withValue { lhsValue in
                        try rhs.1[index].withValue { rhsValue in
                            comparison = try FieldValueComparator.compare(
                                lhsValue,
                                rhsValue,
                                using: sortKey
                            )
                        }
                    }
                    guard comparison != .equal else { continue }
                    return comparison == .lessThan
                }
                try workMeter.consume(2, at: .sortComparison)
                return lhs.0.key.identity.lexicographicallyPrecedes(
                    rhs.0.key.identity
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

    private func groupedOrderExpression(
        _ expression: Expression,
        projection: Projection
    ) -> Expression {
        guard case .column(let column) = expression,
              column.table == nil else {
            return expression
        }
        let items: [ProjectionItem]
        switch projection {
        case .items(let value), .distinctItems(let value):
            items = value
        case .all, .allFrom:
            return expression
        }
        return items.first(where: { $0.alias == column.column })?.expression
            ?? expression
    }

    private func resolvedOrderBy(
        _ orderBy: [SortKey]?,
        projection: Projection
    ) -> [SortKey]? {
        orderBy?.map { key in
            SortKey(
                groupedOrderExpression(
                    key.expression,
                    projection: projection
                ),
                direction: key.direction,
                nulls: key.nulls
            )
        }
    }

    private func projectGroupedRows(
        _ groups: CanonicalRetainedGroups,
        projection: Projection,
        columns: [QueryColumn],
        sourceSchema: CanonicalRelationSchema,
        groupBy: [Expression],
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedQueryRows {
        let selectedSlots: [Int]?
        switch projection {
        case .all:
            selectedSlots = try sourceSchema.wildcardSlots(scope: nil)
        case .allFrom(let scope):
            selectedSlots = try sourceSchema.wildcardSlots(scope: scope)
        case .items, .distinctItems:
            selectedSlots = nil
        }
        var rows = try DatabaseRetainedQueryRowsBuilder(
            columns: columns, workMeter: workMeter, stage: .projection,
            expectedCount: groups.count, payloadLeases: [groups.payloadLease], schemaLeases: [])
        for group in groups {
            try workMeter.consume(at: .projection)
            if let selectedSlots {
                guard selectedSlots.count == columns.count else {
                    throw QueryRowCodecError.arityMismatch(expected: columns.count, actual: selectedSlots.count)
                }
                func value(at ordinal: Int) throws -> QueryValue {
                    let slot = selectedSlots[ordinal]
                    guard group.representative.values.indices.contains(slot) else {
                        throw CanonicalReadError.unsupportedSelectQuery("Grouped source row slot is missing")
                    }
                    return try CanonicalQueryTypeBinder.normalize(
                        group.representative.values[slot], to: columns[ordinal].type
                    )
                }
                try rows.append(
                    footprint: CanonicalRelationalFootprintMeter.positionalValueArrayFootprint(
                        count: columns.count, valueAt: { _ in .primitive(.null) },
                        annotations: group.representative.annotations, version: nil,
                        workMeter: workMeter, stage: .projection
                    ),
                    make: {
                        var values: [QueryValue] = []
                        values.reserveCapacity(columns.count)
                        for ordinal in columns.indices { values.append(try value(at: ordinal)) }
                        return try QueryRow(values: values, columns: columns,
                                            annotations: group.representative.annotations)
                    }
                )
                continue
            }
            switch projection {
            case .items(let items), .distinctItems(let items):
                guard items.count == columns.count else {
                    throw QueryRowCodecError.arityMismatch(expected: columns.count, actual: items.count)
                }
                let scopedValueReservation = try reserveQueryScopedValues(
                    count: items.count,
                    workMeter: workMeter,
                    stage: .projection
                )
                defer { scopedValueReservation.release() }
                var scopedValues: [DatabaseQueryScopedFieldValue] = []
                scopedValues.reserveCapacity(items.count)
                for item in items {
                    scopedValues.append(
                        try await evaluateGroupedExpression(
                            item.expression,
                            group: group,
                            groupBy: groupBy,
                            workMeter: workMeter,
                            evaluationContext: evaluationContext
                        )
                    )
                }
                func value(at ordinal: Int) throws -> QueryValue {
                    try scopedValues[ordinal].withValue {
                        try CanonicalQueryTypeBinder.normalize(.primitive(copy $0), to: columns[ordinal].type)
                    }
                }
                try rows.append(
                    footprint: CanonicalRelationalFootprintMeter.positionalValueArrayFootprint(
                        count: columns.count, valueAt: value,
                        annotations: [:], version: nil,
                        workMeter: workMeter, stage: .projection
                    ),
                    make: {
                        var values: [QueryValue] = []
                        values.reserveCapacity(columns.count)
                        for ordinal in columns.indices { values.append(try value(at: ordinal)) }
                        return try QueryRow(values: values, columns: columns)
                    }
                )
                continue
            case .all, .allFrom:
                throw CanonicalReadError.unsupportedSelectQuery("Grouped wildcard slots are unavailable")
            }
        }
        let projectedRows = try rows.finish().moveToSharedOwnership(at: .projection)
        if case .distinctItems = projection {
            return try canonicalUniqueRows(
                projectedRows,
                workMeter: workMeter
            )
        }
        return projectedRows
    }

    private func validateGroupedWildcardProjection(
        _ projection: Projection,
        sourceSchema: CanonicalRelationSchema,
        groupBy: [Expression]
    ) throws {
        func requireGroupedColumn(
            _ column: String,
            sourceName: String?
        ) throws {
            let unqualified = Expression.column(ColumnRef(column))
            let isGrouped: Bool
            if let sourceName {
                let qualified = Expression.column(
                    ColumnRef(table: sourceName, column: column)
                )
                isGrouped = groupBy.contains(qualified)
                    || (sourceSchema.occurrenceCount(of: column) == 1
                        && groupBy.contains(unqualified))
            } else {
                isGrouped = groupBy.contains(unqualified)
            }
            guard isGrouped else {
                let displayName = sourceName.map { "\($0).\(column)" }
                    ?? column
                throw CanonicalReadError.aggregateEvaluation(
                    .invalidGroupedExpression(
                        "Wildcard projection contains non-grouped column '\(displayName)'"
                    )
                )
            }
        }

        switch projection {
        case .items, .distinctItems:
            return
        case .all:
            for index in sourceSchema.layout.visibleSlots {
                let slot = sourceSchema.layout.slots[index]
                try requireGroupedColumn(slot.name, sourceName: slot.scope)
            }
        case .allFrom(let sourceName):
            guard let scope = sourceSchema.scopes.first(
                where: { $0.name == sourceName }
            ) else {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "Projection source '\(sourceName)' not found"
                )
            }
            for column in scope.columns {
                try requireGroupedColumn(column, sourceName: sourceName)
            }
        }
    }

    private func evaluateGroupedExpression(
        _ expression: Expression,
        group: CanonicalGroupedRow,
        groupBy: [Expression],
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> DatabaseQueryScopedFieldValue {
        var retainedLifetimes = try DatabaseRetainedArrayBuilder<
            DatabaseIntermediateReservation
        >(
            workMeter: workMeter,
            stage: .expressionEvaluation,
            layout: try DatabaseRetainedArrayLayout.forElement(
                DatabaseIntermediateReservation.self
            )
        )
        let expressionScratch = try workMeter.reserveIntermediate(
            at: .expressionEvaluation
        )
        defer { expressionScratch.release() }
        let rewritten = try await rewriteGroupedExpression(
            expression,
            group: group,
            groupBy: groupBy,
            workMeter: workMeter,
            evaluationContext: evaluationContext,
            retainedLifetimes: &retainedLifetimes,
            expressionScratch: expressionScratch
        )
        let effectiveRowConstruction = try workMeter.reserveIntermediate(
            bytes: prospectiveSourceCompositionFootprint(
                group.representative,
                outer: evaluationContext?.outerRow,
                workMeter: workMeter
            ).bytes,
            at: .expressionEvaluation
        )
        defer { effectiveRowConstruction.release() }
        let effectiveRow = group.representative.overlaying(
            outer: evaluationContext?.outerRow
        )
        let resolved = try await resolveQueryScopedExpression(
            rewritten,
            on: effectiveRow,
            context: evaluationContext,
            workMeter: workMeter,
            retainedLifetimes: &retainedLifetimes,
            expressionScratch: expressionScratch
        )
        let outputAdmission = try preadmitExpressionPayload(
            resolved,
            on: effectiveRow,
            workMeter: workMeter,
            retainedLifetimes: &retainedLifetimes,
            forceRootOwnership: canonicalExpressionResultRequiresOwnedPayload(
                expression
            )
        )
        let expressionLifetimes = try retainedLifetimes.finish()
            .moveToSharedOwnership(at: .expressionEvaluation)
        defer { withExtendedLifetime(expressionLifetimes) {} }
        do {
            let evaluator = DatabaseExpressionEvaluator(
                fields: effectiveRow.fields,
                ambiguousColumns: effectiveRow.ambiguousUnqualifiedColumns,
                workMeter: workMeter
            )
            if let outputAdmission {
                return try .producing(
                    maximumFootprint: outputAdmission.maximumFootprint,
                    reservation: outputAdmission.reservation,
                    stage: .expressionEvaluation
                ) {
                    try evaluator.evaluate(resolved)
                }
            }
            return .borrowing(try evaluator.evaluate(resolved))
        } catch let error as DatabaseExpressionEvaluationError {
            throw CanonicalReadError.expressionEvaluation(error)
        }
    }

    private func rewriteGroupedExpression(
        _ expression: Expression,
        group: CanonicalGroupedRow,
        groupBy: [Expression],
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?,
        retainedLifetimes: inout DatabaseRetainedArrayBuilder<
            DatabaseIntermediateReservation
        >,
        expressionScratch: DatabaseIntermediateReservation
    ) async throws -> Expression {
        if !canonicalExpressionContainsAggregate(expression),
           groupBy.contains(
            where: {
                groupedExpression(
                    expression,
                    matches: $0,
                    on: group.representative
                )
            }
           ) {
            return expression
        }

        func rewrite(
            _ nested: Expression
        ) async throws -> Expression {
            try await rewriteGroupedExpression(
                nested,
                group: group,
                groupBy: groupBy,
                workMeter: workMeter,
                evaluationContext: evaluationContext,
                retainedLifetimes: &retainedLifetimes,
                expressionScratch: expressionScratch
            )
        }

        func unary(
            _ nested: Expression,
            make: (Expression) -> Expression
        ) async throws -> Expression {
            make(try await rewrite(nested))
        }

        func binary(
            _ lhs: Expression,
            _ rhs: Expression,
            make: (Expression, Expression) -> Expression
        ) async throws -> Expression {
            let rewrittenLHS = try await rewrite(lhs)
            let rewrittenRHS = try await rewrite(rhs)
            return make(rewrittenLHS, rewrittenRHS)
        }

        func rewriteList(
            _ expressions: [Expression]
        ) async throws -> [Expression] {
            let footprint = try DatabaseIntermediateCollectionMeter
                .arrayFootprint(
                    count: expressions.count,
                    element: Expression.self
                )
            try expressionScratch.reserveAdditional(
                bytes: footprint.bytes,
                at: .expressionEvaluation
            )
            var result: [Expression] = []
            result.reserveCapacity(expressions.count)
            for expression in expressions {
                result.append(try await rewrite(expression))
            }
            return result
        }

        switch expression {
        case .aggregate(let aggregate):
            let value = try await evaluateAggregate(
                aggregate,
                rows: group.rows,
                workMeter: workMeter,
                evaluationContext: evaluationContext
            )
            return try retainQueryScopedLiteral(
                value,
                retainedLifetimes: &retainedLifetimes
            )
        case .literal:
            return expression
        case .column(let column):
            throw CanonicalReadError.aggregateEvaluation(
                .invalidGroupedExpression(
                    "Column '\(column.displayName)' is neither grouped nor aggregated"
                )
            )
        case .variable(let variable):
            throw CanonicalReadError.aggregateEvaluation(
                .invalidGroupedExpression(
                    "Variable '\(variable.name)' is neither grouped nor aggregated"
                )
            )
        case .parameter, .bound:
            return expression
        case .add(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.add)
        case .subtract(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.subtract)
        case .multiply(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.multiply)
        case .divide(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.divide)
        case .modulo(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.modulo)
        case .negate(let nested):
            return try await unary(nested, make: Expression.negate)
        case .equal(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.equal)
        case .notEqual(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.notEqual)
        case .lessThan(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.lessThan)
        case .lessThanOrEqual(let lhs, let rhs):
            return try await binary(
                lhs,
                rhs,
                make: Expression.lessThanOrEqual
            )
        case .greaterThan(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.greaterThan)
        case .greaterThanOrEqual(let lhs, let rhs):
            return try await binary(
                lhs,
                rhs,
                make: Expression.greaterThanOrEqual
            )
        case .and(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.and)
        case .or(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.or)
        case .not(let nested):
            return try await unary(nested, make: Expression.not)
        case .isNull(let nested):
            return try await unary(nested, make: Expression.isNull)
        case .isNotNull(let nested):
            return try await unary(nested, make: Expression.isNotNull)
        case .like(let nested, let pattern):
            return try await unary(nested) { .like($0, pattern: pattern) }
        case .regex(let nested, let pattern, let flags):
            return try await unary(nested) {
                .regex($0, pattern: pattern, flags: flags)
            }
        case .between(let nested, let low, let high):
            let rewrittenValue = try await rewrite(nested)
            let rewrittenLow = try await rewrite(low)
            let rewrittenHigh = try await rewrite(high)
            return .between(
                rewrittenValue,
                low: rewrittenLow,
                high: rewrittenHigh
            )
        case .inList(let nested, let values),
             .notInList(let nested, let values):
            let rewrittenFootprint = try DatabaseIntermediateCollectionMeter
                .arrayFootprint(
                    count: values.count + 1,
                    element: Expression.self
                )
            try expressionScratch.reserveAdditional(
                bytes: rewrittenFootprint.bytes,
                at: .expressionEvaluation
            )
            var rewritten: [Expression] = []
            rewritten.reserveCapacity(values.count + 1)
            rewritten.append(try await rewrite(nested))
            for value in values {
                rewritten.append(try await rewrite(value))
            }
            let isNegated: Bool
            if case .notInList = expression {
                isNegated = true
            } else {
                isNegated = false
            }
            let membersFootprint = try DatabaseIntermediateCollectionMeter
                .arrayFootprint(
                    count: values.count,
                    element: Expression.self
                )
            try expressionScratch.reserveAdditional(
                bytes: membersFootprint.bytes,
                at: .expressionEvaluation
            )
            let memberExpressions = Array(rewritten.dropFirst())
            return isNegated
                ? .notInList(rewritten[0], values: memberExpressions)
                : .inList(rewritten[0], values: memberExpressions)
        case .function(let function):
            return .function(
                FunctionCall(
                    name: function.name,
                    arguments: try await rewriteList(function.arguments),
                    distinct: function.distinct
                )
            )
        case .caseWhen(let pairs, let fallback):
            let rewrittenCount = (pairs.count * 2) + (fallback == nil ? 0 : 1)
            let rewrittenFootprint = try DatabaseIntermediateCollectionMeter
                .arrayFootprint(
                    count: rewrittenCount,
                    element: Expression.self
                )
            try expressionScratch.reserveAdditional(
                bytes: rewrittenFootprint.bytes,
                at: .expressionEvaluation
            )
            var rewritten: [Expression] = []
            rewritten.reserveCapacity(rewrittenCount)
            for pair in pairs {
                rewritten.append(try await rewrite(pair.condition))
                rewritten.append(try await rewrite(pair.result))
            }
            if let fallback {
                rewritten.append(try await rewrite(fallback))
            }
            let pairFootprint = try DatabaseIntermediateCollectionMeter
                .arrayFootprint(
                    count: pairs.count,
                    element: CaseWhenPair.self
                )
            try expressionScratch.reserveAdditional(
                bytes: pairFootprint.bytes,
                at: .expressionEvaluation
            )
            var rewrittenPairs: [CaseWhenPair] = []
            rewrittenPairs.reserveCapacity(pairs.count)
            for index in pairs.indices {
                rewrittenPairs.append(
                    CaseWhenPair(
                        condition: rewritten[index * 2],
                        result: rewritten[(index * 2) + 1]
                    )
                )
            }
            return .caseWhen(
                cases: rewrittenPairs,
                elseResult: fallback == nil ? nil : rewritten.last
            )
        case .coalesce(let values):
            return .coalesce(try await rewriteList(values))
        case .nullIf(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.nullIf)
        case .cast(let nested, let type):
            return try await unary(nested) {
                .cast($0, targetType: type)
            }
        case .triple(let subject, let predicate, let object):
            let rewrittenSubject = try await rewrite(subject)
            let rewrittenPredicate = try await rewrite(predicate)
            let rewrittenObject = try await rewrite(object)
            return .triple(
                subject: rewrittenSubject,
                predicate: rewrittenPredicate,
                object: rewrittenObject
            )
        case .isTriple(let nested):
            return try await unary(nested, make: Expression.isTriple)
        case .subject(let nested):
            return try await unary(nested, make: Expression.subject)
        case .predicate(let nested):
            return try await unary(nested, make: Expression.predicate)
        case .object(let nested):
            return try await unary(nested, make: Expression.object)
        case .inSubquery(let value, let query):
            return try await unary(value) {
                .inSubquery($0, subquery: query)
            }
        case .subquery, .exists:
            return expression
        }
    }

    private func groupedExpression(
        _ expression: Expression,
        matches groupedExpression: Expression,
        on row: CanonicalSourceRow
    ) -> Bool {
        if expression == groupedExpression {
            return true
        }
        guard case .column(let projectedColumn) = expression,
              case .column(let groupedColumn) = groupedExpression,
              projectedColumn.column == groupedColumn.column else {
            return false
        }
        if let projectedTable = projectedColumn.table,
           let groupedTable = groupedColumn.table {
            return projectedTable == groupedTable
        }
        guard !row.ambiguousUnqualifiedColumns.contains(
            projectedColumn.column
        ) else {
            return false
        }
        return row.value(for: projectedColumn) != nil
            && row.value(for: groupedColumn) != nil
    }

    private func evaluateAggregate(
        _ aggregate: AggregateFunction,
        rows: CanonicalRetainedRows,
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> DatabaseQueryScopedFieldValue {
        let functionName: String
        let expression: Expression?
        let distinct: Bool
        let orderedRows: CanonicalRetainedRows
        switch aggregate {
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
            if let expression {
                let scopedValue = try await evaluateQueryExpression(
                    expression,
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
                    operation: "\(functionName)(DISTINCT)"
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
        case .count(let expression, _):
            let count = expression == nil
                ? values.count
                : values.lazy.filter { !$0.isNull }.count
            guard let result = Int64(exactly: count) else {
                throw CanonicalReadError.aggregateEvaluation(.countOverflow)
            }
            return try retainingResult(.int64(result))

        case .sum, .avg:
            var accumulator = DatabaseNumericAggregateAccumulator()
            do {
                for value in values where !value.isNull {
                    try accumulator.add(value)
                }
                let result: FieldValue?
                if case .sum = aggregate {
                    result = try accumulator.sum()
                } else {
                    result = try accumulator.average()
                }
                return try retainingResult(result ?? .null)
            } catch let failure as DatabaseNumericAggregateAccumulator.Failure {
                throw CanonicalReadError.aggregateEvaluation(
                    aggregateNumericError(
                        function: functionName,
                        failure: failure
                    )
                )
            }

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

    private func evaluateQueryBoolean(
        _ expression: DatabaseKit.Expression,
        on row: CanonicalSourceRow,
        context: CanonicalQueryEvaluationContext?,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        let value = try await evaluateQueryExpression(
            expression,
            on: row,
            context: context,
            workMeter: workMeter
        )
        do {
            var result = false
            try value.withValue { borrowed in
                result = try DatabaseExpressionEvaluator(
                    fields: ["value": borrowed]
                ).predicate(.column(ColumnRef("value")))
            }
            return result
        } catch let error as DatabaseExpressionEvaluationError {
            throw CanonicalReadError.expressionEvaluation(error)
        }
    }

    private func evaluateQueryExpression(
        _ expression: DatabaseKit.Expression,
        on row: CanonicalSourceRow,
        context: CanonicalQueryEvaluationContext?,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseQueryScopedFieldValue {
        switch expression {
        case .isNull(let operand), .isNotNull(let operand):
            let value = try await projectedQueryValue(operand, on: row,
                context: context, workMeter: workMeter).value
            let isNull = value.isNull
            let result: Bool
            if case .isNull = expression { result = isNull } else { result = !isNull }
            return try .retaining(.bool(result), workMeter: workMeter, stage: .expressionEvaluation)
        default: break
        }
        let effectiveRowConstruction = try workMeter.reserveIntermediate(
            bytes: prospectiveSourceCompositionFootprint(
                row,
                outer: context?.outerRow,
                workMeter: workMeter
            ).bytes,
            at: .expressionEvaluation
        )
        defer { effectiveRowConstruction.release() }
        let effectiveRow = row.overlaying(outer: context?.outerRow)
        var retainedLifetimes = try DatabaseRetainedArrayBuilder<
            DatabaseIntermediateReservation
        >(
            workMeter: workMeter,
            stage: .expressionEvaluation,
            layout: try DatabaseRetainedArrayLayout.forElement(
                DatabaseIntermediateReservation.self
            )
        )
        let expressionScratch = try workMeter.reserveIntermediate(
            at: .expressionEvaluation
        )
        defer { expressionScratch.release() }
        let resolved = try await resolveQueryScopedExpression(
            expression,
            on: effectiveRow,
            context: context,
            workMeter: workMeter,
            retainedLifetimes: &retainedLifetimes,
            expressionScratch: expressionScratch
        )
        let outputAdmission = try preadmitExpressionPayload(
            resolved,
            on: effectiveRow,
            workMeter: workMeter,
            retainedLifetimes: &retainedLifetimes,
            forceRootOwnership: canonicalExpressionResultRequiresOwnedPayload(
                expression
            )
        )
        let expressionLifetimes = try retainedLifetimes.finish()
            .moveToSharedOwnership(at: .expressionEvaluation)
        defer { withExtendedLifetime(expressionLifetimes) {} }
        do {
            let evaluator = DatabaseExpressionEvaluator(
                fields: effectiveRow.fields,
                ambiguousColumns: effectiveRow.ambiguousUnqualifiedColumns,
                workMeter: workMeter
            )
            if let outputAdmission {
                return try .producing(
                    maximumFootprint: outputAdmission.maximumFootprint,
                    reservation: outputAdmission.reservation,
                    stage: .expressionEvaluation
                ) {
                    try evaluator.evaluate(resolved)
                }
            }
            return .borrowing(try evaluator.evaluate(resolved))
        } catch let error as DatabaseExpressionEvaluationError {
            throw CanonicalReadError.expressionEvaluation(error)
        }
    }

    private func evaluateExpressions(
        _ expressions: [Expression],
        on row: CanonicalSourceRow,
        context: CanonicalQueryEvaluationContext?,
        workMeter: DatabaseWorkMeter
    ) async throws -> [DatabaseQueryScopedFieldValue] {
        var values: [DatabaseQueryScopedFieldValue] = []
        values.reserveCapacity(expressions.count)
        for expression in expressions {
            values.append(
                try await evaluateQueryExpression(
                    expression,
                    on: row,
                    context: context,
                    workMeter: workMeter
                )
            )
        }
        return values
    }

    private func retainQueryScopedLiteral(
        _ value: DatabaseQueryScopedFieldValue,
        retainedLifetimes: inout DatabaseRetainedArrayBuilder<
            DatabaseIntermediateReservation
        >
    ) throws -> Expression {
        var conversionReservation: DatabaseIntermediateReservation?
        var requiresArrayConversion = false
        value.withValue { borrowed in
            if case .array = borrowed {
                requiresArrayConversion = true
            }
        }
        if requiresArrayConversion {
            let footprint = try value.withValue { borrowed in
                try CanonicalRelationalFootprintMeter.valueFootprint(
                    of: borrowed,
                    workMeter: retainedLifetimes.workMeter,
                    stage: .expressionEvaluation
                )
            }
            conversionReservation = try retainedLifetimes.workMeter
                .reserveIntermediate(
                    bytes: footprint.bytes,
                    at: .expressionEvaluation
                )
        }
        defer { conversionReservation?.release() }
        var literal: Literal?
        try value.withValue { borrowed in
            literal = try borrowed.toLiteral()
        }
        guard let literal else {
            preconditionFailure("A query-scoped value did not produce a literal")
        }
        if let reservation = value.retainedReservation {
            try retainedLifetimes.append(
                footprint: DatabaseIntermediateFootprint(),
                at: .expressionEvaluation,
                make: { reservation }
            )
        }
        if let reservation = conversionReservation {
            try retainedLifetimes.append(
                footprint: DatabaseIntermediateFootprint(),
                at: .expressionEvaluation,
                make: { reservation }
            )
            conversionReservation = nil
        }
        return .literal(literal)
    }

    private func resolveQueryScopedExpression(
        _ expression: Expression,
        on row: CanonicalSourceRow,
        context: CanonicalQueryEvaluationContext?,
        workMeter: DatabaseWorkMeter,
        retainedLifetimes: inout DatabaseRetainedArrayBuilder<
            DatabaseIntermediateReservation
        >,
        expressionScratch: DatabaseIntermediateReservation
    ) async throws -> Expression {
        func resolve(
            _ nested: Expression
        ) async throws -> Expression {
            try await resolveQueryScopedExpression(
                nested,
                on: row,
                context: context,
                workMeter: workMeter,
                retainedLifetimes: &retainedLifetimes,
                expressionScratch: expressionScratch
            )
        }

        func unary(
            _ nested: Expression,
            make: (Expression) -> Expression
        ) async throws -> Expression {
            make(try await resolve(nested))
        }

        func binary(
            _ lhs: Expression,
            _ rhs: Expression,
            make: (Expression, Expression) -> Expression
        ) async throws -> Expression {
            let resolvedLHS = try await resolve(lhs)
            let resolvedRHS = try await resolve(rhs)
            return make(resolvedLHS, resolvedRHS)
        }

        func resolveList(
            _ expressions: [Expression]
        ) async throws -> [Expression] {
            let footprint = try DatabaseIntermediateCollectionMeter
                .arrayFootprint(
                    count: expressions.count,
                    element: Expression.self
                )
            try expressionScratch.reserveAdditional(
                bytes: footprint.bytes,
                at: .expressionEvaluation
            )
            var result: [Expression] = []
            result.reserveCapacity(expressions.count)
            for expression in expressions {
                result.append(try await resolve(expression))
            }
            return result
        }

        switch expression {
        case .subquery(let query):
            let columnCount = try nestedQueryOutputColumnCount(
                query,
                context: context
            )
            guard columnCount == 1 else {
                throw CanonicalReadError.invalidScalarSubquery(
                    rowCount: nil,
                    columnCount: columnCount
                )
            }
            let response = try await executeNestedQuery(
                query,
                outerRow: row,
                context: context
            )
            let visibleRows = response.visibleRows
            guard visibleRows.count <= 1 else {
                throw CanonicalReadError.invalidScalarSubquery(
                    rowCount: visibleRows.count,
                    columnCount: nil
                )
            }
            guard !visibleRows.isEmpty else {
                return .literal(.null)
            }
            var retainedValue: DatabaseQueryScopedFieldValue?
            try visibleRows.withElement(at: 0) { resultRow in
                guard resultRow.values.count == 1 else {
                    throw CanonicalReadError.invalidScalarSubquery(
                        rowCount: 1,
                        columnCount: resultRow.values.count
                    )
                }
                // FIXME(INCOMPLETE_IMPLEMENTATION): Rich scalar operations in the primitive
                // expression evaluator remain unsupported until S04/S07 coercion is implemented.
                guard case .primitive(let value) = resultRow.values[0] else {
                    throw CanonicalReadError.unsupportedSelectQuery("Rich scalar expression coercion is not implemented")
                }
                retainedValue = try .retaining(
                    value,
                    workMeter: workMeter,
                    stage: .projection
                )
            }
            guard let retainedValue else {
                preconditionFailure("A scalar subquery did not retain its value")
            }
            return try retainQueryScopedLiteral(
                retainedValue,
                retainedLifetimes: &retainedLifetimes
            )

        case .exists(let query):
            let response = try await executeNestedQuery(
                query,
                outerRow: row,
                context: context
            )
            return .literal(.bool(!response.visibleRows.isEmpty))

        case .inSubquery(let value, let query):
            let resolvedValue = try await resolve(value)
            let candidate: DatabaseQueryScopedFieldValue
            let candidateMaximumFootprint = try
                prospectiveExpressionResultFootprint(
                    resolvedValue,
                    on: row,
                    workMeter: workMeter,
                    stage: .expressionEvaluation
                )
            let candidateReservation = try workMeter.reserveIntermediate(
                rows: candidateMaximumFootprint.rows,
                bytes: candidateMaximumFootprint.bytes,
                at: .expressionEvaluation
            )
            do {
                candidate = try .producing(
                    maximumFootprint: candidateMaximumFootprint,
                    reservation: candidateReservation,
                    stage: .expressionEvaluation
                ) {
                    try DatabaseExpressionEvaluator(
                        fields: row.fields,
                        ambiguousColumns: row.ambiguousUnqualifiedColumns,
                        workMeter: workMeter
                    ).evaluate(resolvedValue)
                }
            } catch let error as DatabaseExpressionEvaluationError {
                throw CanonicalReadError.expressionEvaluation(error)
            }
            var candidateIsNull = false
            candidate.withValue { candidateIsNull = $0.isNull }
            if candidateIsNull {
                return .literal(.null)
            }
            let columnCount = try nestedQueryOutputColumnCount(
                query,
                context: context
            )
            guard columnCount == 1 else {
                throw CanonicalReadError.invalidMembershipSubquery(
                    columnCount: columnCount
                )
            }
            let response = try await executeNestedQuery(
                query,
                outerRow: row,
                context: context
            )
            let visibleRows = response.visibleRows
            var sawNull = false
            for position in 0..<visibleRows.count {
                var matches = false
                try visibleRows.withElement(at: position) { resultRow in
                    guard resultRow.values.count == 1 else {
                        throw CanonicalReadError.invalidMembershipSubquery(
                            columnCount: resultRow.values.count
                        )
                    }
                    // FIXME(INCOMPLETE_IMPLEMENTATION): Rich IN-subquery comparison remains
                    // unsupported on this production primitive evaluator path until S04 semantics.
                    guard case .primitive(let value) = resultRow.values[0] else {
                        throw CanonicalReadError.unsupportedSelectQuery("Rich membership comparison is not implemented")
                    }
                    if value.isNull {
                        sawNull = true
                        return
                    }
                    do {
                        try candidate.withValue { candidateValue in
                            matches = try FieldValueComparator.equal(
                                candidateValue,
                                value
                            )
                        }
                    } catch let failure as FieldValueComparisonError {
                        throw canonicalComparisonReadError(
                            failure,
                            operation: "IN subquery equality"
                        )
                    }
                }
                if matches {
                    return .literal(.bool(true))
                }
            }
            return .literal(sawNull ? .null : .bool(false))

        case .aggregate:
            throw CanonicalReadError.aggregateEvaluation(
                .invalidGroupedExpression(
                    "Aggregate expression reached scalar evaluation without a group"
                )
            )
        case .literal, .column, .variable, .parameter, .bound:
            return expression
        case .add(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.add)
        case .subtract(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.subtract)
        case .multiply(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.multiply)
        case .divide(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.divide)
        case .modulo(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.modulo)
        case .negate(let nested):
            return try await unary(nested, make: Expression.negate)
        case .equal(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.equal)
        case .notEqual(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.notEqual)
        case .lessThan(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.lessThan)
        case .lessThanOrEqual(let lhs, let rhs):
            return try await binary(
                lhs,
                rhs,
                make: Expression.lessThanOrEqual
            )
        case .greaterThan(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.greaterThan)
        case .greaterThanOrEqual(let lhs, let rhs):
            return try await binary(
                lhs,
                rhs,
                make: Expression.greaterThanOrEqual
            )
        case .and(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.and)
        case .or(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.or)
        case .not(let nested):
            return try await unary(nested, make: Expression.not)
        case .isNull(let nested):
            return try await unary(nested, make: Expression.isNull)
        case .isNotNull(let nested):
            return try await unary(nested, make: Expression.isNotNull)
        case .like(let nested, let pattern):
            return try await unary(nested) {
                .like($0, pattern: pattern)
            }
        case .regex(let nested, let pattern, let flags):
            return try await unary(nested) {
                .regex($0, pattern: pattern, flags: flags)
            }
        case .between(let nested, let low, let high):
            let resolvedValue = try await resolve(nested)
            let resolvedLow = try await resolve(low)
            let resolvedHigh = try await resolve(high)
            return .between(
                resolvedValue,
                low: resolvedLow,
                high: resolvedHigh
            )
        case .inList(let nested, let values),
             .notInList(let nested, let values):
            let resolvedFootprint = try DatabaseIntermediateCollectionMeter
                .arrayFootprint(
                    count: values.count + 1,
                    element: Expression.self
                )
            try expressionScratch.reserveAdditional(
                bytes: resolvedFootprint.bytes,
                at: .expressionEvaluation
            )
            var resolved: [Expression] = []
            resolved.reserveCapacity(values.count + 1)
            resolved.append(try await resolve(nested))
            for value in values {
                resolved.append(try await resolve(value))
            }
            let isNegated: Bool
            if case .notInList = expression {
                isNegated = true
            } else {
                isNegated = false
            }
            let membersFootprint = try DatabaseIntermediateCollectionMeter
                .arrayFootprint(
                    count: values.count,
                    element: Expression.self
                )
            try expressionScratch.reserveAdditional(
                bytes: membersFootprint.bytes,
                at: .expressionEvaluation
            )
            let memberExpressions = Array(resolved.dropFirst())
            return isNegated
                ? .notInList(resolved[0], values: memberExpressions)
                : .inList(resolved[0], values: memberExpressions)
        case .function(let function):
            return .function(
                FunctionCall(
                    name: function.name,
                    arguments: try await resolveList(function.arguments),
                    distinct: function.distinct
                )
            )
        case .caseWhen(let pairs, let fallback):
            let resolvedCount = (pairs.count * 2) + (fallback == nil ? 0 : 1)
            let resolvedFootprint = try DatabaseIntermediateCollectionMeter
                .arrayFootprint(
                    count: resolvedCount,
                    element: Expression.self
                )
            try expressionScratch.reserveAdditional(
                bytes: resolvedFootprint.bytes,
                at: .expressionEvaluation
            )
            var resolved: [Expression] = []
            resolved.reserveCapacity(resolvedCount)
            for pair in pairs {
                resolved.append(try await resolve(pair.condition))
                resolved.append(try await resolve(pair.result))
            }
            if let fallback {
                resolved.append(try await resolve(fallback))
            }
            let pairFootprint = try DatabaseIntermediateCollectionMeter
                .arrayFootprint(
                    count: pairs.count,
                    element: CaseWhenPair.self
                )
            try expressionScratch.reserveAdditional(
                bytes: pairFootprint.bytes,
                at: .expressionEvaluation
            )
            var resolvedPairs: [CaseWhenPair] = []
            resolvedPairs.reserveCapacity(pairs.count)
            for index in pairs.indices {
                resolvedPairs.append(
                    CaseWhenPair(
                        condition: resolved[index * 2],
                        result: resolved[(index * 2) + 1]
                    )
                )
            }
            return .caseWhen(
                cases: resolvedPairs,
                elseResult: fallback == nil ? nil : resolved.last
            )
        case .coalesce(let values):
            return .coalesce(try await resolveList(values))
        case .nullIf(let lhs, let rhs):
            return try await binary(lhs, rhs, make: Expression.nullIf)
        case .cast(let nested, let type):
            return try await unary(nested) {
                .cast($0, targetType: type)
            }
        case .triple(let subject, let predicate, let object):
            let resolvedSubject = try await resolve(subject)
            let resolvedPredicate = try await resolve(predicate)
            let resolvedObject = try await resolve(object)
            return .triple(
                subject: resolvedSubject,
                predicate: resolvedPredicate,
                object: resolvedObject
            )
        case .isTriple(let nested):
            return try await unary(nested, make: Expression.isTriple)
        case .subject(let nested):
            return try await unary(nested, make: Expression.subject)
        case .predicate(let nested):
            return try await unary(nested, make: Expression.predicate)
        case .object(let nested):
            return try await unary(nested, make: Expression.object)
        }
    }

    private func executeNestedQuery(
        _ query: SelectQuery,
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
            inheritedSubqueries: context.namedSubqueries,
            outerRow: outerRow,
            preparedFusionGraph: context.preparedFusionGraph,
            fusionSession: context.fusionSession
        )
    }

    private func nestedQueryOutputColumnCount(
        _ query: SelectQuery,
        context: CanonicalQueryEvaluationContext?
    ) throws -> Int {
        let inherited = context?.namedSubqueries ?? []
        let namedSubqueries = try mergeNamedSubqueries(
            local: query.subqueries ?? [],
            inherited: inherited
        )
        if sourceRequiresRuntimeInferredSchema(
            query.source,
            namedSubqueries: namedSubqueries
        ) {
            switch query.projection {
            case .items, .distinctItems:
                return try canonicalProjectionColumns(
                    query.projection,
                    sourceSchema: CanonicalRelationSchema()
                ).count
            case .all, .allFrom:
                throw CanonicalReadError.unsupportedSelectQuery(
                    "A nested query over a runtime-inferred source must declare exactly one output column"
                )
            }
        }
        let sourceSchema = try canonicalRelationSchema(
            for: query.source,
            namedSubqueries: namedSubqueries
        )
        return try canonicalProjectionColumns(
            query.projection,
            sourceSchema: sourceSchema
        ).count
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
                max(1, MemoryLayout<[QueryValue]>.stride + 32)
            )
        ).multiplied(by: UInt64(rows.count)).bytes
        let setReservation = try workMeter.reserveIntermediate(
            bytes: setBytes,
            at: .deduplication
        )
        defer { setReservation.release() }
        var seen: Set<[QueryValue]> = []
        seen.reserveCapacity(rows.count)
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
                )
                try setReservation.reserveAdditional(bytes: identityFootprint.bytes, at: .deduplication)
                var identity: [QueryValue] = []
                identity.reserveCapacity(row.values.count)
                for value in row.values {
                    identity.append(try canonicalQueryValueIdentity(value, operation: "SELECT DISTINCT"))
                }
                if seen.insert(identity).inserted {
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
                element: FieldValue.self
            ).adding(
            try DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: group.key.identity.count,
                element: FieldValue.self
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
                CanonicalRelationalFootprintMeter.valueFootprint(
                    of: identity,
                    workMeter: workMeter,
                    stage: .aggregateInput
                )
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

    /// Resolves complete output metadata from the admitted schema before any
    /// source row is fetched. This operation does not open a transaction.
    @_spi(DatabaseExecution)
    public func boundQueryColumns(_ query: SelectQuery) throws -> [QueryColumn] {
        try boundQueryColumns(query, inheritedNamedSubqueries: [])
    }

    private func boundQueryColumns(
        _ query: SelectQuery,
        inheritedNamedSubqueries: [NamedSubquery],
        outerSchemas: [CanonicalRelationSchema] = []
    ) throws -> [QueryColumn] {
        do {
            let named = try mergeNamedSubqueries(local: query.subqueries ?? [], inherited: inheritedNamedSubqueries)
            let schema = try canonicalRelationSchema(for: query.source, namedSubqueries: named, outerSchemas: outerSchemas)
            switch query.source {
            case .graphPattern, .namedGraph:
                let names = try canonicalProjectionColumns(query.projection, sourceSchema: schema)
                return try names.enumerated().map { index, name in
                    try QueryColumn(number: UInt32(index + 1), name: name,
                                    type: .primitive(.rdfTerm), isNullable: true)
                }
            default: break
            }
            let binder = try typeBinder(schema: schema, namedSubqueries: named, outerSchemas: outerSchemas)
            let bindingQuery = query.replacing(orderBy: query.orderBy.map { keys in
                keys.map { key in
                    SortKey(groupedOrderExpression(key.expression, projection: query.projection),
                            direction: key.direction, nulls: key.nulls)
                }
            })
            return try binder.bind(bindingQuery)
        } catch let error as DatabaseExpressionEvaluationError {
            throw CanonicalReadError.expressionEvaluation(error)
        }
    }

    private func typeBinder(
        schema: CanonicalRelationSchema,
        namedSubqueries: [NamedSubquery],
        outerSchemas: [CanonicalRelationSchema] = []
    ) throws -> CanonicalQueryTypeBinder {
        let frames = [schema] + outerSchemas
        let definitions = try frames.flatMap { try $0.typedColumns() }
        return CanonicalQueryTypeBinder(
            columns: definitions,
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
            bindSubquery: { try self.boundQueryColumns($0, inheritedNamedSubqueries: namedSubqueries,
                                                      outerSchemas: [schema] + outerSchemas) }
        )
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
            visibleSlots: layout.visibleSlots)
    }

    private func tableRelationSchema(
        _ tableRef: TableRef
    ) throws -> CanonicalRelationSchema {
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
        let names = entities.flatMap(\.allFields).filter { seen.insert($0).inserted }
        let binder = CanonicalQueryTypeBinder(columns: [], resolveColumn: { reference in
            throw DatabaseExpressionEvaluationError.missingColumn(reference.displayName)
        }, wildcardSlots: { _ in [] })
        let types = try names.map { name -> QueryColumnType? in
            try binder.commonType(entities.compactMap { entity in
                entity.fieldMapByName[name].map { $0.queryType ?? .primitive($0.type, isArray: $0.isArray) }
            })
        }
        let nullability = names.map { name in entities.contains { $0.fieldMapByName[name]?.isOptional != false } }
        return try CanonicalRelationSchema(scopes: [CanonicalRelationScope(name: sourceName,
            columns: names, types: types, nullability: nullability)])
    }

    private func graphTableRelationSchema(
        _ source: GraphTableSource,
        rows: CanonicalRetainedRows?
    ) throws -> CanonicalRelationSchema {
        let policy = try readPolicy()
        guard let executor = policy.graphTableSourceExecutor else {
            throw CanonicalReadError.unsupportedSource("GRAPH_TABLE executor is not registered")
        }
        let raw = try executor.columns(graphTableSource: source, schema: policy.schema)
        let rawSchema = try CanonicalRelationSchema(unscopedColumns: raw.map(\.name),
            types: raw.map { $0.type }, nullability: raw.map(\.isNullable))
        let columns: [QueryColumn]
        if let projection = source.columns, !projection.isEmpty {
            let binder = try typeBinder(schema: rawSchema, namedSubqueries: [])
            columns = try projection.enumerated().map { index, item in
                let bound = try binder.bind(item.expression)
                return try QueryColumn(number: UInt32(index + 1), name: item.alias,
                                       type: bound.type, isNullable: bound.isNullable)
            }
        } else { columns = raw }
        let schema = try CanonicalRelationSchema(unscopedColumns: columns.map(\.name),
            types: columns.map { $0.type }, nullability: columns.map(\.isNullable))
        if let alias = source.alias { return try schema.applyingAlias(alias) }
        return schema
    }

    private func materializeQueryRelation(
        _ rows: CanonicalRetainedQueryRowView,
        query: SelectQuery,
        explicitColumns: [String]?,
        alias: String?,
        namedSubqueries: [NamedSubquery],
        workMeter: DatabaseWorkMeter
    ) throws -> CanonicalRelation {
        let output = rows.owner.columns
        let names = explicitColumns ?? output.map(\.name)
        guard names.count == output.count else {
            throw CanonicalReadError.unsupportedSelectQuery("CTE output arity differs from its declaration")
        }
        let schema: CanonicalRelationSchema
        if let alias {
            schema = try CanonicalRelationSchema(scopes: [CanonicalRelationScope(name: alias,
                columns: names, types: output.map { $0.type }, nullability: output.map(\.isNullable))])
        } else {
            schema = try CanonicalRelationSchema(unscopedColumns: names,
                types: output.map { $0.type }, nullability: output.map(\.isNullable))
        }
        let layoutFootprint = try CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
            fieldNames: names, sourceName: alias, workMeter: workMeter, stage: .bindingCandidate)
        let reservation = try workMeter.reserveIntermediate(bytes: layoutFootprint.bytes, at: .bindingCandidate)
        let layout = CanonicalSourceLayout(slots: names.map { .init(name: $0, scope: alias) },
            scopes: alias.map { [$0] } ?? [], reservation: reservation,
            types: output.map { $0.type }, nullability: output.map(\.isNullable))
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

    private func canonicalRelationSchema(
        for source: DataSource,
        namedSubqueries: [NamedSubquery],
        outerSchemas: [CanonicalRelationSchema] = []
    ) throws -> CanonicalRelationSchema {
        switch source {
        case .table(let table):
            guard let named = namedSubqueries.first(where: { $0.name == table.table }) else {
                return try tableRelationSchema(table)
            }
            let projected = try boundQueryColumns(named.query, inheritedNamedSubqueries: namedSubqueries, outerSchemas: outerSchemas)
            let names = named.columns ?? projected.map(\.name)
            guard names.count == projected.count else {
                throw CanonicalReadError.unsupportedSelectQuery("CTE column count differs from its query")
            }
            return try CanonicalRelationSchema(scopes: [CanonicalRelationScope(
                name: table.alias ?? named.name, columns: names,
                types: projected.map { $0.type }, nullability: projected.map(\.isNullable)
            )])
        case .logical(let source):
            guard source.kindIdentifier == LogicalSourceKind.polymorphic else {
                throw CanonicalReadError.unsupportedSource("Logical source has no declared relation schema")
            }
            return try polymorphicRelationSchema(
                container.polymorphicGroup(identifier: source.identifier), sourceName: source.effectiveName)
        case .subquery(let query, let alias):
            let columns = try boundQueryColumns(query, inheritedNamedSubqueries: namedSubqueries, outerSchemas: outerSchemas)
            return try CanonicalRelationSchema(scopes: [CanonicalRelationScope(
                name: alias, columns: columns.map(\.name), types: columns.map { $0.type },
                nullability: columns.map(\.isNullable)
            )])
        case .join(let join):
            try validateJoinDeclaration(join)
            var left = try canonicalRelationSchema(for: join.left, namedSubqueries: namedSubqueries, outerSchemas: outerSchemas)
            let rightOuter: [CanonicalRelationSchema]
            switch join.type {
            case .lateral, .leftLateral: rightOuter = [left] + outerSchemas
            default: rightOuter = outerSchemas
            }
            var right = try canonicalRelationSchema(for: join.right, namedSubqueries: namedSubqueries, outerSchemas: rightOuter)
            let condition: JoinCondition?
            switch join.type {
            case .natural, .naturalLeft, .naturalRight, .naturalFull:
                condition = .using(inferNaturalJoinColumns(leftSchema: left, rightSchema: right))
            default: condition = join.condition
            }
            try validateJoinCondition(condition, leftSchema: left, rightSchema: right, type: join.type)
            if case .on(let expression) = condition {
                let schema = try left.merged(with: right)
                do {
                    _ = try typeBinder(schema: schema, namedSubqueries: namedSubqueries, outerSchemas: outerSchemas)
                        .bind(expression, contextualType: .sql(.boolean))
                } catch let error as DatabaseExpressionEvaluationError {
                    throw CanonicalReadError.expressionEvaluation(error)
                }
            }
            switch join.type {
            case .left, .naturalLeft, .leftLateral: right = try right.nullExtended()
            case .right, .naturalRight: left = try left.nullExtended()
            case .full, .naturalFull:
                left = try left.nullExtended(); right = try right.nullExtended()
            default: break
            }
            return try joinOutputSchema(left, right, condition: condition)
        case .values(let rows, let explicitNames):
            let names = explicitNames ?? rows.first?.indices.map { "column\($0)" } ?? []
            let binder = CanonicalQueryTypeBinder(columns: [], resolveColumn: { reference in
                throw DatabaseExpressionEvaluationError.missingColumn(reference.displayName)
            }, wildcardSlots: { _ in [] })
            guard rows.allSatisfy({ $0.count == names.count }) else {
                throw CanonicalReadError.unsupportedSelectQuery("VALUES row arity differs")
            }
            let types = try names.indices.map { index -> QueryColumnType? in
                guard !rows.isEmpty else {
                    throw CanonicalReadError.unsupportedSelectQuery("An empty VALUES input needs declared column types")
                }
                let declarations = try rows.compactMap { row -> QueryColumnType? in
                    let expression = DatabaseKit.Expression.literal(row[index])
                    guard !expressionIsContextualNull(expression) else { return nil }
                    return try binder.bind(expression).type
                }
                return try binder.commonType(declarations)
            }
            return try CanonicalRelationSchema(unscopedColumns: names, types: types)
        case .union(let sources), .unionAll(let sources), .intersect(let sources):
            return try setOperationSchema(sources: sources, namedSubqueries: namedSubqueries,
                                          outerSchemas: outerSchemas)
        case .except(let first, let second):
            return try setOperationSchema(sources: [first, second], namedSubqueries: namedSubqueries,
                                          outerSchemas: outerSchemas)
        case .graphTable(let graph): return try graphTableRelationSchema(graph, rows: nil)
        case .graphPattern(let pattern), .namedGraph(_, let pattern):
            let names = sparqlVariables(in: pattern)
            return try CanonicalRelationSchema(unscopedColumns: names,
                types: names.map { _ in .primitive(.rdfTerm) })
        case .service(let endpoint, _, _):
            throw CanonicalReadError.unsupportedSource("SERVICE source '\(endpoint)' requires its graph executor")
        #if DATABASE_MULTI_BASE
        case .base(_, let nested):
            // Metadata binding performs no storage access and confers no Base authority.
            return try canonicalRelationSchema(for: nested, namedSubqueries: namedSubqueries)
        #endif
        }
    }

    private func sourceRequiresRuntimeInferredSchema(
        _ source: DataSource,
        namedSubqueries: [NamedSubquery]
    ) -> Bool {
        switch source {
        case .table(let table):
            guard let named = namedSubqueries.first(
                where: { $0.name == table.table }
            ) else {
                return false
            }
            let nestedNames = (named.query.subqueries ?? []).map { $0.name }
            let inherited = namedSubqueries.filter {
                !nestedNames.contains($0.name)
            }
            switch named.query.projection {
            case .items, .distinctItems:
                return false
            case .all, .allFrom:
                return sourceRequiresRuntimeInferredSchema(
                    named.query.source,
                    namedSubqueries: (named.query.subqueries ?? []) + inherited
                )
            }
        case .subquery(let query, _):
            let nestedNames = (query.subqueries ?? []).map { $0.name }
            let inherited = namedSubqueries.filter {
                !nestedNames.contains($0.name)
            }
            switch query.projection {
            case .items, .distinctItems:
                return false
            case .all, .allFrom:
                return sourceRequiresRuntimeInferredSchema(
                    query.source,
                    namedSubqueries: (query.subqueries ?? []) + inherited
                )
            }
        case .join(let join):
            return sourceRequiresRuntimeInferredSchema(
                join.left,
                namedSubqueries: namedSubqueries
            ) || sourceRequiresRuntimeInferredSchema(
                join.right,
                namedSubqueries: namedSubqueries
            )
        case .union(let sources), .unionAll(let sources),
                .intersect(let sources):
            guard let first = sources.first else { return false }
            return sourceRequiresRuntimeInferredSchema(
                first,
                namedSubqueries: namedSubqueries
            )
        case .except(let lhs, _):
            return sourceRequiresRuntimeInferredSchema(
                lhs,
                namedSubqueries: namedSubqueries
            )
        case .graphTable(let graphTable):
            return graphTable.columns?.isEmpty ?? true
        #if DATABASE_MULTI_BASE
        case .base(_, let nested):
            return sourceRequiresRuntimeInferredSchema(
                nested,
                namedSubqueries: namedSubqueries
            )
        #endif
        case .logical, .values, .graphPattern, .namedGraph, .service:
            return false
        }
    }

    private func validateStaticJoinBindings(
        _ source: DataSource,
        namedSubqueries: [NamedSubquery],
        outerRow: CanonicalSourceRow?
    ) throws {
        switch source {
        case .join(let join):
            let leftSchema = try canonicalRelationSchema(
                for: join.left,
                namedSubqueries: namedSubqueries,
                outerSchemas: try outerRow.map { [try relationSchema(for: $0.layout)] } ?? []
            )
            let rightOuterRow: CanonicalSourceRow?
            switch join.type {
            case .lateral, .leftLateral:
                rightOuterRow = leftSchema.nullRow().overlaying(
                    outer: outerRow
                )
            default:
                rightOuterRow = outerRow
            }
            let rightSchema = try canonicalRelationSchema(
                for: join.right, namedSubqueries: namedSubqueries,
                outerSchemas: try rightOuterRow.map { [try relationSchema(for: $0.layout)] } ?? []
            )
            try validateStaticJoinBindings(
                join.left,
                namedSubqueries: namedSubqueries,
                outerRow: outerRow
            )
            try validateStaticJoinBindings(
                join.right,
                namedSubqueries: namedSubqueries,
                outerRow: rightOuterRow
            )
            if case .on(let expression) = join.condition {
                let outputSchema = try joinOutputSchema(
                    leftSchema,
                    rightSchema,
                    condition: join.condition
                )
                try validateExpressionBindings(
                    expression,
                    sourceSchema: outputSchema,
                    outerRow: outerRow,
                    namedSubqueries: namedSubqueries
                )
            }
        case .subquery(let query, _):
            let visibleNamedSubqueries = try mergeNamedSubqueries(
                local: query.subqueries ?? [],
                inherited: namedSubqueries
            )
            guard !sourceRequiresRuntimeInferredSchema(
                query.source,
                namedSubqueries: visibleNamedSubqueries
            ) else {
                return
            }
            let sourceSchema = try canonicalRelationSchema(
                for: query.source,
                namedSubqueries: visibleNamedSubqueries,
                outerSchemas: try outerRow.map { [try relationSchema(for: $0.layout)] } ?? []
            )
            try validateRelationalQueryBindings(
                query,
                sourceSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: visibleNamedSubqueries
            )
            try validateStaticJoinBindings(
                query.source,
                namedSubqueries: visibleNamedSubqueries,
                outerRow: outerRow
            )
        case .table(let table):
            guard let named = namedSubqueries.first(
                where: { $0.name == table.table }
            ) else {
                return
            }
            let visibleNamedSubqueries = try mergeNamedSubqueries(
                local: named.query.subqueries ?? [],
                inherited: namedSubqueries
            )
            guard !sourceRequiresRuntimeInferredSchema(
                named.query.source,
                namedSubqueries: visibleNamedSubqueries
            ) else {
                return
            }
            let sourceSchema = try canonicalRelationSchema(
                for: named.query.source,
                namedSubqueries: visibleNamedSubqueries
            )
            try validateRelationalQueryBindings(
                named.query,
                sourceSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: visibleNamedSubqueries
            )
            try validateStaticJoinBindings(
                named.query.source,
                namedSubqueries: visibleNamedSubqueries,
                outerRow: outerRow
            )
        case .union(let sources), .unionAll(let sources),
                .intersect(let sources):
            for source in sources {
                try validateStaticJoinBindings(
                    source,
                    namedSubqueries: namedSubqueries,
                    outerRow: outerRow
                )
            }
        case .except(let lhs, let rhs):
            try validateStaticJoinBindings(
                lhs,
                namedSubqueries: namedSubqueries,
                outerRow: outerRow
            )
            try validateStaticJoinBindings(
                rhs,
                namedSubqueries: namedSubqueries,
                outerRow: outerRow
            )
        #if DATABASE_MULTI_BASE
        case .base(_, let nested):
            try validateStaticJoinBindings(
                nested,
                namedSubqueries: namedSubqueries,
                outerRow: outerRow
            )
        #endif
        case .logical, .values, .graphTable, .graphPattern, .namedGraph,
                .service:
            break
        }
    }

    private func canonicalProjectionColumns(
        _ projection: Projection,
        sourceSchema: CanonicalRelationSchema
    ) throws -> [String] {
        let columns: [String]
        switch projection {
        case .all:
            columns = sourceSchema.visibleColumns
        case .allFrom(let sourceName):
            guard let scope = sourceSchema.scopes.first(
                where: { $0.name == sourceName }
            ) else {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "Projection source '\(sourceName)' not found"
                )
            }
            columns = scope.columns
        case .items(let items), .distinctItems(let items):
            columns = items.enumerated().map { index, item in
                item.alias ?? canonicalProjectionName(
                    for: item.expression,
                    index: index
                )
            }
        }
        return columns
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
                switch query.projection {
                case .items(let items), .distinctItems(let items):
                    for (index, item) in items.enumerated() {
                        append(
                            item.alias ?? canonicalProjectionName(
                                for: item.expression,
                                index: index
                            )
                        )
                    }
                case .all:
                    switch query.source {
                    case .graphPattern(let nested),
                            .namedGraph(_, let nested),
                            .service(_, let nested, _):
                        visit(nested)
                    default:
                        break
                    }
                case .allFrom:
                    break
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
        outerSchemas: [CanonicalRelationSchema] = []
    ) throws -> CanonicalRelationSchema {
        guard let firstSource = sources.first else { return try CanonicalRelationSchema() }
        let first = try canonicalRelationSchema(for: firstSource, namedSubqueries: namedSubqueries,
                                               outerSchemas: outerSchemas)
        let firstSlots = first.layout.visibleSlots
        let binder = try typeBinder(schema: first, namedSubqueries: namedSubqueries, outerSchemas: outerSchemas)
        var types: [[QueryColumnType]] = Array(repeating: [], count: firstSlots.count)
        var nullability = Array(repeating: false, count: firstSlots.count)
        for source in sources {
            let schema = try canonicalRelationSchema(for: source, namedSubqueries: namedSubqueries,
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
        return try CanonicalRelationSchema(
            unscopedColumns: firstSlots.map { first.layout.slots[$0].name },
            types: types.map { try binder.commonType($0) }, nullability: nullability
        )
    }

    private func sourceColumnIsContextualNull(
        _ source: DataSource,
        ordinal: Int,
        namedSubqueries: [NamedSubquery]
    ) -> Bool {
        switch source {
        case .values(let rows, _):
            return !rows.isEmpty && rows.allSatisfy { row in
                row.indices.contains(ordinal) && expressionIsContextualNull(.literal(row[ordinal]))
            }
        case .subquery(let query, _):
            return queryColumnIsContextualNull(query, ordinal: ordinal, namedSubqueries: namedSubqueries)
        case .table(let table):
            guard let named = namedSubqueries.first(where: { $0.name == table.table }) else { return false }
            return queryColumnIsContextualNull(named.query, ordinal: ordinal, namedSubqueries: namedSubqueries)
        case .union(let sources), .unionAll(let sources), .intersect(let sources):
            return !sources.isEmpty && sources.allSatisfy {
                sourceColumnIsContextualNull($0, ordinal: ordinal, namedSubqueries: namedSubqueries)
            }
        case .except(let first, let second):
            return sourceColumnIsContextualNull(first, ordinal: ordinal, namedSubqueries: namedSubqueries)
                && sourceColumnIsContextualNull(second, ordinal: ordinal, namedSubqueries: namedSubqueries)
        #if DATABASE_MULTI_BASE
        case .base(_, let nested):
            return sourceColumnIsContextualNull(nested, ordinal: ordinal, namedSubqueries: namedSubqueries)
        #endif
        default: return false
        }
    }

    private func queryColumnIsContextualNull(
        _ query: SelectQuery,
        ordinal: Int,
        namedSubqueries: [NamedSubquery]
    ) -> Bool {
        switch query.projection {
        case .items(let items), .distinctItems(let items):
            return items.indices.contains(ordinal) && expressionIsContextualNull(items[ordinal].expression)
        case .all:
            let local = query.subqueries ?? []
            let inherited = namedSubqueries.filter { inherited in !local.contains { $0.name == inherited.name } }
            return sourceColumnIsContextualNull(query.source, ordinal: ordinal,
                                                namedSubqueries: local + inherited)
        case .allFrom: return false
        }
    }

    private func expressionIsContextualNull(_ expression: DatabaseKit.Expression) -> Bool {
        switch expression {
        case .literal(.null), .literal(.queryValue(.primitive(.null), type: nil)): return true
        case .coalesce(let children): return children.allSatisfy(expressionIsContextualNull)
        case .caseWhen(let pairs, let fallback):
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
        workMeter: DatabaseWorkMeter
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
            let (leaseCount, leaseOverflow) = row.payloadLeases.count.addingReportingOverflow(1)
            guard !leaseOverflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
            func value(at ordinal: Int) throws -> QueryValue {
                let sourceSlot = sourceSlots[ordinal]
                guard row.values.indices.contains(sourceSlot),
                      let type = outputLayout.types[outputSlots[ordinal]] else {
                    throw CanonicalReadError.unsupportedSelectQuery("Set operation source slot is missing")
                }
                return try CanonicalQueryTypeBinder.normalize(row.values[sourceSlot], to: type)
            }
            let footprint = try CanonicalRelationalFootprintMeter.positionalValueArrayFootprint(
                count: sourceSlots.count, valueAt: value,
                annotations: row.annotations, version: row.version,
                workMeter: workMeter, stage: .bindingCandidate)
                .adding(DatabaseIntermediateCollectionMeter.arrayFootprint(
                    count: leaseCount, element: DatabaseQueryPayloadLease.self))
            try aligned.append(footprint: footprint) {
                var values: [QueryValue] = []
                values.reserveCapacity(sourceSlots.count)
                for ordinal in sourceSlots.indices { values.append(try value(at: ordinal)) }
                return CanonicalSourceRow(values: values, layout: outputLayout,
                    annotations: row.annotations, version: row.version,
                    payloadLeases: row.payloadLeases + [relation.rows.payloadLease])
            }
        }
    }

    private func alignSetOperationRows(
        _ relation: CanonicalRelation,
        to outputSchema: CanonicalRelationSchema,
        layout outputLayout: CanonicalSourceLayout,
        workMeter: DatabaseWorkMeter
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
            workMeter: workMeter
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
            fieldNames: columns.lazy.map(\.name), sourceName: sourceName, workMeter: workMeter, stage: stage)
        let layoutReservation = try workMeter.reserveIntermediate(bytes: layoutFootprint.bytes, at: stage)
        let layout = CanonicalSourceLayout(
            slots: columns.map { .init(name: $0.name, scope: sourceName) },
            scopes: sourceName.map { [$0] } ?? [], reservation: layoutReservation,
            types: columns.map { $0.type }, nullability: columns.map(\.isNullable))
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

    private func prospectiveSourceCompositionFootprint(
        _ row: CanonicalSourceRow,
        outer: CanonicalSourceRow?,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        // The input rows retain their payload roots during synchronous adapters.
        // Overlay arrays, name lookup dictionaries and qualified labels are new.
        var footprint = DatabaseIntermediateFootprint(bytes: 256)
        func append(_ source: CanonicalSourceRow) throws {
            footprint = try footprint.adding(CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
                fieldNames: source.layout.slots.lazy.map(\.name), sourceName: nil,
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
