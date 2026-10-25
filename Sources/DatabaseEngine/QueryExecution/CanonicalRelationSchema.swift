import DatabaseKit
import DatabaseTypes

struct CanonicalRelationSchema: Sendable, Equatable {
    let unscopedColumns: [String]
    let unscopedTypes: [QueryColumnType?]
    let unscopedNullability: [Bool]
    let scopes: [CanonicalRelationScope]
    let coalescedColumns: CanonicalSourceNames
    private(set) var layout: CanonicalSourceLayout

    init(
        unscopedColumns: [String] = [],
        scopes: [CanonicalRelationScope] = [],
        coalescedColumns: CanonicalSourceNames = [],
        types: [QueryColumnType?]? = nil,
        nullability: [Bool]? = nil,
        visibleSlots: [Int]? = nil,
        physicalLayout: CanonicalSourceLayout? = nil
    ) throws {
        guard CanonicalSourceNames(scopes.map { $0.name }).count == scopes.count else {
            throw CanonicalReadError.invalidQueryStructure(.duplicateAlias, "A relation contains duplicate source aliases")
        }
        guard coalescedColumns.isSubset(of: CanonicalSourceNames(unscopedColumns)) else {
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
        CanonicalSourceName.equal(left.unscopedColumns, right.unscopedColumns)
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

    func typedColumns() throws -> [QueryColumn] { try layout.typedColumns() }

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
        guard layout.scopes.contains(where: { CanonicalSourceName.equal($0, scope) }) else {
            throw DatabaseExpressionEvaluationError.missingColumn(scope)
        }
        return layout.slots.indices.filter { CanonicalSourceName.equal(layout.slots[$0].scope, scope) }
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
        let coalesced = CanonicalSourceNames(columns)
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
        return CanonicalSourceRow(values: Array(repeating: .primitive(.null), count: layout.slots.count), layout: layout,
            sourcePresence: Array(repeating: false, count: layout.frameScopes.reduce(0) { $0 + $1.count }))
    }
}
