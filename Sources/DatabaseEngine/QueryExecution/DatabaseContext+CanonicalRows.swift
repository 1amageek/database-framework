import DatabaseKit
import DatabaseTypes
import StorageKit

struct CanonicalSourceRow: Sendable {
    let fields: [String: FieldValue]
    let unscopedFields: [String: FieldValue]
    let scopedFields: [String: [String: FieldValue]]
    let coalescedColumns: Set<String>
    let annotations: [String: FieldValue]
    let version: PersistableVersionToken?
    private let ambiguityOverride: Set<String>?

    init(
        fields: [String: FieldValue],
        annotations: [String: FieldValue] = [:],
        version: PersistableVersionToken? = nil
    ) {
        self.fields = fields
        self.unscopedFields = fields
        self.scopedFields = [:]
        self.coalescedColumns = []
        self.annotations = annotations
        self.version = version
        self.ambiguityOverride = nil
    }

    fileprivate init(
        unscopedFields: [String: FieldValue],
        scopedFields: [String: [String: FieldValue]],
        coalescedColumns: Set<String> = [],
        annotations: [String: FieldValue],
        version: PersistableVersionToken?
    ) {
        self.fields = CanonicalSourceRow.flatten(
            unscopedFields: unscopedFields,
            scopedFields: scopedFields,
            coalescedColumns: coalescedColumns
        )
        self.unscopedFields = unscopedFields
        self.scopedFields = scopedFields
        self.coalescedColumns = coalescedColumns
        self.annotations = annotations
        self.version = version
        self.ambiguityOverride = nil
    }

    fileprivate init(
        materializedFields: [String: FieldValue],
        unscopedFields: [String: FieldValue],
        scopedFields: [String: [String: FieldValue]],
        coalescedColumns: Set<String> = [],
        annotations: [String: FieldValue] = [:],
        version: PersistableVersionToken? = nil,
        ambiguityOverride: Set<String>? = nil
    ) {
        self.fields = materializedFields
        self.unscopedFields = unscopedFields
        self.scopedFields = scopedFields
        self.coalescedColumns = coalescedColumns
        self.annotations = annotations
        self.version = version
        self.ambiguityOverride = ambiguityOverride
    }

    static func fromBaseFields(
        _ fields: [String: FieldValue],
        sourceName: String?,
        annotations: [String: FieldValue] = [:],
        version: PersistableVersionToken? = nil
    ) -> CanonicalSourceRow {
        guard let sourceName else {
            return CanonicalSourceRow(fields: fields, annotations: annotations, version: version)
        }
        return CanonicalSourceRow(
            unscopedFields: [:],
            scopedFields: [sourceName: fields],
            coalescedColumns: [],
            annotations: annotations,
            version: version
        )
    }

    func applyingAlias(_ alias: String?) -> CanonicalSourceRow {
        guard let alias else { return self }
        return CanonicalSourceRow(
            unscopedFields: [:],
            scopedFields: [alias: wildcardFields],
            coalescedColumns: [],
            annotations: annotations,
            version: version
        )
    }

    func merged(with other: CanonicalSourceRow) throws -> CanonicalSourceRow {
        let duplicateUnscopedColumns = Set(unscopedFields.keys)
            .intersection(other.unscopedFields.keys)
        guard duplicateUnscopedColumns.isEmpty else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "JOIN inputs contain duplicate unqualified columns: \(duplicateUnscopedColumns.sorted().joined(separator: ", "))"
            )
        }
        let duplicateScopes = Set(scopedFields.keys)
            .intersection(other.scopedFields.keys)
        guard duplicateScopes.isEmpty else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "JOIN inputs require distinct source aliases: \(duplicateScopes.sorted().joined(separator: ", "))"
            )
        }
        return CanonicalSourceRow(
            unscopedFields: unscopedFields.merging(other.unscopedFields) {
                current, _ in current
            },
            scopedFields: scopedFields.merging(other.scopedFields) {
                current, _ in current
            },
            coalescedColumns: coalescedColumns.union(
                other.coalescedColumns
            ),
            annotations: annotations.merging(other.annotations) { current, _ in current },
            version: nil
        )
    }

    func value(for column: ColumnRef) -> FieldValue? {
        if let table = column.table {
            return scopedFields[table]?[column.column]
        }
        return fields[column.column]
    }

    func fields(for sourceName: String) -> [String: FieldValue]? {
        scopedFields[sourceName]
    }

    var wildcardFields: [String: FieldValue] {
        CanonicalSourceRow.flattenWildcard(
            unscopedFields: unscopedFields,
            scopedFields: scopedFields,
            coalescedColumns: coalescedColumns
        )
    }

    var ambiguousUnqualifiedColumns: Set<String> {
        if let ambiguityOverride { return ambiguityOverride }
        var counts: [String: Int] = [:]
        for fields in scopedFields.values {
            for key in fields.keys {
                counts[key, default: 0] += 1
            }
        }
        for key in unscopedFields.keys where !coalescedColumns.contains(key) {
            counts[key, default: 0] += 1
        }
        return Set(counts.compactMap {
            $0.value > 1 && !coalescedColumns.contains($0.key)
                ? $0.key
                : nil
        })
    }

    func overlaying(outer: CanonicalSourceRow?) -> CanonicalSourceRow {
        guard let outer else { return self }
        let localColumnNames = Set(unscopedFields.keys).union(
            scopedFields.values.reduce(into: Set<String>()) {
                $0.formUnion($1.keys)
            }
        )
        let mergedUnscopedFields = outer.unscopedFields.merging(
            unscopedFields
        ) { _, local in local }
        let mergedScopedFields = outer.scopedFields.merging(
            scopedFields
        ) { _, local in local }
        let mergedCoalescedColumns = outer.coalescedColumns
            .subtracting(localColumnNames)
            .union(coalescedColumns)
        var materializedFields = outer.fields
        materializedFields.merge(fields) { _, local in local }
        materializedFields.merge(
            CanonicalSourceRow.flatten(
                unscopedFields: mergedUnscopedFields,
                scopedFields: mergedScopedFields,
                coalescedColumns: mergedCoalescedColumns
            )
        ) { _, resolved in resolved }
        for column in localColumnNames {
            if ambiguousUnqualifiedColumns.contains(column) {
                materializedFields.removeValue(forKey: column)
            } else if let localValue = fields[column] {
                materializedFields[column] = localValue
            }
        }
        return CanonicalSourceRow(
            materializedFields: materializedFields,
            unscopedFields: mergedUnscopedFields,
            scopedFields: mergedScopedFields,
            coalescedColumns: mergedCoalescedColumns,
            annotations: outer.annotations.merging(annotations) {
                _, local in local
            },
            version: version,
            ambiguityOverride: ambiguousUnqualifiedColumns.union(
                outer.ambiguousUnqualifiedColumns.subtracting(
                    localColumnNames
                )
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

    private static func flattenWildcard(
        unscopedFields: [String: FieldValue],
        scopedFields: [String: [String: FieldValue]],
        coalescedColumns: Set<String>
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
            for (key, value) in sourceFields
                where !coalescedColumns.contains(key) {
                if counts[key] == 1 {
                    flattened[key] = value
                } else {
                    flattened["\(sourceName).\(key)"] = value
                }
            }
        }
        return flattened
    }
}

private struct CanonicalRelationScope: Sendable, Equatable {
    let name: String
    let columns: [String]
}

private struct CanonicalRelationSchema: Sendable, Equatable {
    let unscopedColumns: [String]
    let scopes: [CanonicalRelationScope]
    let coalescedColumns: Set<String>

    init(
        unscopedColumns: [String] = [],
        scopes: [CanonicalRelationScope] = [],
        coalescedColumns: Set<String> = []
    ) throws {
        guard Set(unscopedColumns).count == unscopedColumns.count else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "A relation contains duplicate unqualified column names"
            )
        }
        guard Set(scopes.map { $0.name }).count == scopes.count else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "A relation contains duplicate source aliases"
            )
        }
        for scope in scopes where Set(scope.columns).count != scope.columns.count {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Source '\(scope.name)' contains duplicate column names"
            )
        }
        let scopedColumnNames = Set(scopes.flatMap { $0.columns })
        let unqualifiedScopeCollisions = Set(unscopedColumns)
            .intersection(scopedColumnNames)
            .subtracting(coalescedColumns)
        guard unqualifiedScopeCollisions.isEmpty else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "A JOIN input without an alias collides with scoped columns: \(unqualifiedScopeCollisions.sorted().joined(separator: ", "))"
            )
        }
        guard coalescedColumns.isSubset(of: Set(unscopedColumns)) else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Coalesced JOIN columns must be present in the unqualified schema"
            )
        }
        self.unscopedColumns = unscopedColumns
        self.scopes = scopes
        self.coalescedColumns = coalescedColumns
    }

    var visibleColumns: [String] {
        var counts: [String: Int] = [:]
        for column in unscopedColumns {
            counts[column, default: 0] += 1
        }
        for scope in scopes {
            for column in scope.columns {
                counts[column, default: 0] += 1
            }
        }

        var result = unscopedColumns.filter {
            counts[$0] == 1 || coalescedColumns.contains($0)
        }
        for scope in scopes {
            for column in scope.columns {
                if coalescedColumns.contains(column) { continue }
                result.append(counts[column] == 1 ? column : "\(scope.name).\(column)")
            }
        }
        return result
    }

    func occurrenceCount(of column: String) -> Int {
        if coalescedColumns.contains(column) { return 1 }
        return unscopedColumns.filter { $0 == column }.count
            + scopes.reduce(into: 0) { count, scope in
                count += scope.columns.filter { $0 == column }.count
            }
    }

    func applyingAlias(_ alias: String) throws -> CanonicalRelationSchema {
        return try CanonicalRelationSchema(
            scopes: [CanonicalRelationScope(name: alias, columns: visibleColumns)],
            coalescedColumns: []
        )
    }

    func merged(with other: CanonicalRelationSchema) throws -> CanonicalRelationSchema {
        let leftColumnNames = Set(
            unscopedColumns + scopes.flatMap { $0.columns }
        )
        let rightColumnNames = Set(
            other.unscopedColumns + other.scopes.flatMap { $0.columns }
        )
        return try CanonicalRelationSchema(
            unscopedColumns: unscopedColumns + other.unscopedColumns,
            scopes: scopes + other.scopes,
            coalescedColumns: coalescedColumns
                .subtracting(rightColumnNames)
                .union(
                    other.coalescedColumns.subtracting(leftColumnNames)
                )
        )
    }

    func merged(
        with other: CanonicalRelationSchema,
        coalescing columns: [String]
    ) throws -> CanonicalRelationSchema {
        let coalesced = Set(columns)
        let leftColumnNames = Set(
            unscopedColumns + scopes.flatMap { $0.columns }
        )
        let rightColumnNames = Set(
            other.unscopedColumns + other.scopes.flatMap { $0.columns }
        )
        return try CanonicalRelationSchema(
            unscopedColumns: unscopedColumns.filter {
                !coalesced.contains($0)
            } + other.unscopedColumns.filter {
                !coalesced.contains($0)
            } + columns,
            scopes: scopes + other.scopes,
            coalescedColumns: coalescedColumns
                .subtracting(rightColumnNames.subtracting(coalesced))
                .union(
                    other.coalescedColumns.subtracting(
                        leftColumnNames.subtracting(coalesced)
                    )
                )
                .union(coalesced)
        )
    }

    func nullRow() -> CanonicalSourceRow {
        let unscoped = Dictionary(
            uniqueKeysWithValues: unscopedColumns.map { ($0, FieldValue.null) }
        )
        let scoped = Dictionary(
            uniqueKeysWithValues: scopes.map { scope in
                (
                    scope.name,
                    Dictionary(
                        uniqueKeysWithValues: scope.columns.map {
                            ($0, FieldValue.null)
                        }
                    )
                )
            }
        )
        return CanonicalSourceRow(
            unscopedFields: unscoped,
            scopedFields: scoped,
            coalescedColumns: coalescedColumns,
            annotations: [:],
            version: nil
        )
    }
}

private typealias CanonicalRetainedRows =
    DatabaseSharedRetainedArray<CanonicalSourceRow>
private typealias CanonicalRetainedGroups =
    DatabaseSharedRetainedArray<CanonicalGroupedRow>
typealias CanonicalRetainedQueryRows =
    DatabaseSharedRetainedArray<QueryRow>

/// Zero-copy page view that keeps the canonical row owner and its request
/// reservation alive while one row is borrowed. It deliberately does not
/// conform to Collection and cannot expose a row by subscript.
struct CanonicalRetainedQueryRowView: Sendable {
    private let owner: CanonicalRetainedQueryRows
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

private func prospectiveQueryRowFootprint(
    fieldNames: [String],
    values: [DatabaseQueryScopedFieldValue],
    annotations: [String: FieldValue] = [:],
    version: PersistableVersionToken? = nil,
    workMeter: DatabaseWorkMeter,
    stage: DatabaseWorkStage = .projection
) throws -> DatabaseIntermediateFootprint {
    precondition(fieldNames.count == values.count)
    var footprint = try CanonicalRelationalFootprintMeter.footprint(
        of: QueryRow(
            fields: [:],
            annotations: annotations,
            version: version
        ),
        workMeter: workMeter,
        stage: stage
    )
    for (fieldName, value) in zip(fieldNames, values) {
        try value.withValue { borrowed in
            footprint = try footprint.adding(
                CanonicalRelationalFootprintMeter.fieldEntryFootprint(
                    nameUTF8Count: fieldName.utf8.count,
                    value: borrowed,
                    workMeter: workMeter,
                    stage: stage
                )
            )
        }
    }
    return footprint
}

private func prospectiveSourceRowFootprint(
    fieldNames: [String],
    values: [DatabaseQueryScopedFieldValue],
    sourceName: String?,
    annotations: [String: FieldValue] = [:],
    version: PersistableVersionToken? = nil,
    workMeter: DatabaseWorkMeter,
    stage: DatabaseWorkStage
) throws -> DatabaseIntermediateFootprint {
    precondition(fieldNames.count == values.count)
    var footprint = try CanonicalRelationalFootprintMeter.sourceRowFootprint(
        fields: [:],
        sourceName: sourceName,
        annotations: annotations,
        version: version,
        workMeter: workMeter,
        stage: stage
    )
    for (fieldName, value) in zip(fieldNames, values) {
        try value.withValue { borrowed in
            let base = try CanonicalRelationalFootprintMeter
                .fieldEntryFootprint(
                    nameUTF8Count: fieldName.utf8.count,
                    value: borrowed,
                    workMeter: workMeter,
                    stage: stage
                )
            footprint = try footprint.adding(base)
            if let sourceName {
                footprint = try footprint.adding(base)
                footprint = try footprint.adding(
                    CanonicalRelationalFootprintMeter.fieldEntryFootprint(
                        nameUTF8Count: sourceName.utf8.count
                            + 1
                            + fieldName.utf8.count,
                        value: borrowed,
                        workMeter: workMeter,
                        stage: stage
                    )
                )
            }
        }
    }
    return footprint
}

private func prospectiveWildcardQueryRowFootprint(
    _ row: CanonicalSourceRow,
    workMeter: DatabaseWorkMeter,
    stage: DatabaseWorkStage = .projection
) throws -> DatabaseIntermediateFootprint {
    var footprint = try CanonicalRelationalFootprintMeter.footprint(
        of: QueryRow(
            fields: [:],
            annotations: row.annotations,
            version: row.version
        ),
        workMeter: workMeter,
        stage: stage
    )
    func occurrenceCount(_ name: String) -> Int {
        var count = row.unscopedFields[name] == nil ? 0 : 1
        for scoped in row.scopedFields.values where scoped[name] != nil {
            count += 1
        }
        return count
    }
    func append(nameUTF8Count: Int, value: borrowing FieldValue) throws {
        footprint = try footprint.adding(
            CanonicalRelationalFootprintMeter.fieldEntryFootprint(
                nameUTF8Count: nameUTF8Count,
                value: value,
                workMeter: workMeter,
                stage: stage
            )
        )
    }
    for (name, value) in row.unscopedFields
        where occurrenceCount(name) == 1 || row.coalescedColumns.contains(name) {
        try append(nameUTF8Count: name.utf8.count, value: value)
    }
    for (sourceName, fields) in row.scopedFields {
        for (name, value) in fields where !row.coalescedColumns.contains(name) {
            if occurrenceCount(name) == 1 {
                try append(nameUTF8Count: name.utf8.count, value: value)
            } else {
                try append(
                    nameUTF8Count: sourceName.utf8.count + 1 + name.utf8.count,
                    value: value
                )
            }
        }
    }
    return footprint
}

private func prospectiveAliasedSourceRowFootprint(
    _ row: CanonicalSourceRow,
    alias: String?,
    workMeter: DatabaseWorkMeter,
    stage: DatabaseWorkStage
) throws -> DatabaseIntermediateFootprint {
    guard let alias else {
        return try CanonicalRelationalFootprintMeter.footprint(
            of: row,
            workMeter: workMeter
        )
    }
    var footprint = try CanonicalRelationalFootprintMeter.sourceRowFootprint(
        fields: [:],
        sourceName: alias,
        annotations: row.annotations,
        version: row.version,
        workMeter: workMeter,
        stage: stage
    )
    func occurrenceCount(_ name: String) -> Int {
        var count = row.unscopedFields[name] == nil ? 0 : 1
        for scoped in row.scopedFields.values where scoped[name] != nil {
            count += 1
        }
        return count
    }
    func append(name: String, value: borrowing FieldValue) throws {
        let base = try CanonicalRelationalFootprintMeter.fieldEntryFootprint(
            nameUTF8Count: name.utf8.count,
            value: value,
            workMeter: workMeter,
            stage: stage
        )
        footprint = try footprint.adding(base).adding(base)
        footprint = try footprint.adding(
            CanonicalRelationalFootprintMeter.fieldEntryFootprint(
                nameUTF8Count: alias.utf8.count + 1 + name.utf8.count,
                value: value,
                workMeter: workMeter,
                stage: stage
            )
        )
    }
    for (name, value) in row.unscopedFields
        where occurrenceCount(name) == 1 || row.coalescedColumns.contains(name) {
        try append(name: name, value: value)
    }
    for (sourceName, fields) in row.scopedFields {
        for (name, value) in fields where !row.coalescedColumns.contains(name) {
            let outputName = occurrenceCount(name) == 1
                ? name
                : "\(sourceName).\(name)"
            try append(name: outputName, value: value)
        }
    }
    return footprint
}

private func prospectiveJoinRowFootprint(
    left: CanonicalSourceRow,
    right: CanonicalSourceRow,
    condition: JoinCondition?,
    workMeter: DatabaseWorkMeter
) throws -> DatabaseIntermediateFootprint {
    let usingColumns: [String]
    if case .using(let columns) = condition {
        usingColumns = columns
    } else {
        usingColumns = []
    }
    func isUsing(_ name: String) -> Bool {
        usingColumns.contains(name)
    }
    func hasOutputUnscopedField(_ name: String) -> Bool {
        if isUsing(name) { return true }
        return left.unscopedFields[name] != nil
            || right.unscopedFields[name] != nil
    }
    func occurrenceCount(_ name: String) -> Int {
        var count = hasOutputUnscopedField(name) ? 1 : 0
        for fields in left.scopedFields.values where fields[name] != nil {
            count += 1
        }
        for fields in right.scopedFields.values where fields[name] != nil {
            count += 1
        }
        return count
    }
    func isCoalesced(_ name: String) -> Bool {
        isUsing(name)
            || left.coalescedColumns.contains(name)
            || right.coalescedColumns.contains(name)
    }

    var footprint = try CanonicalRelationalFootprintMeter
        .emptySourceRowFootprint(
            workMeter: workMeter,
            stage: .joinCandidate
        )
    func appendField(
        nameUTF8Count: Int,
        value: borrowing FieldValue
    ) throws {
        footprint = try footprint.adding(
            CanonicalRelationalFootprintMeter.fieldEntryFootprint(
                nameUTF8Count: nameUTF8Count,
                value: value,
                workMeter: workMeter,
                stage: .joinCandidate
            )
        )
    }
    func appendUnscoped(name: String, value: borrowing FieldValue) throws {
        if occurrenceCount(name) == 1 || isCoalesced(name) {
            try appendField(nameUTF8Count: name.utf8.count, value: value)
        }
    }

    for (name, value) in left.unscopedFields where !isUsing(name) {
        try appendUnscoped(name: name, value: value)
    }
    for (name, value) in right.unscopedFields where !isUsing(name) {
        try appendUnscoped(name: name, value: value)
    }
    for name in usingColumns {
        let leftValue = firstJoinFieldValue(named: name, in: left)
        let rightValue = firstJoinFieldValue(named: name, in: right)
        let value = leftValue.flatMap { $0.isNull ? nil : $0 }
            ?? rightValue
            ?? .null
        try appendUnscoped(name: name, value: value)
    }

    func appendScopes(_ scopes: [String: [String: FieldValue]]) throws {
        for (sourceName, fields) in scopes {
            footprint = try footprint.adding(
                CanonicalRelationalFootprintMeter.sourceScopeBaseFootprint(
                    nameUTF8Count: sourceName.utf8.count,
                    workMeter: workMeter,
                    stage: .joinCandidate
                )
            )
            for (name, value) in fields {
                try appendField(nameUTF8Count: name.utf8.count, value: value)
                try appendField(
                    nameUTF8Count: sourceName.utf8.count + 1 + name.utf8.count,
                    value: value
                )
                if occurrenceCount(name) == 1 {
                    try appendField(nameUTF8Count: name.utf8.count, value: value)
                }
            }
        }
    }
    try appendScopes(left.scopedFields)
    try appendScopes(right.scopedFields)

    for (name, value) in left.annotations {
        try appendField(nameUTF8Count: name.utf8.count, value: value)
    }
    for (name, value) in right.annotations where left.annotations[name] == nil {
        try appendField(nameUTF8Count: name.utf8.count, value: value)
    }
    return footprint
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
/// storage before the first FieldValue or destination dictionary exists.
private func prospectiveLiteralValueFootprint(
    _ literal: Literal
) throws -> UInt64 {
    let base = UInt64(MemoryLayout<FieldValue>.stride + 32)
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
        return try DatabaseIntermediateFootprint(
            bytes: base
        ).adding(
            DatabaseIntermediateFootprint(bytes: UInt64(value.count))
        ).bytes
    case .array(let values):
        var footprint = try DatabaseIntermediateFootprint(
            bytes: base + UInt64(MemoryLayout<[FieldValue]>.stride)
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: UInt64(MemoryLayout<FieldValue>.stride + 16)
            ).multiplied(by: UInt64(values.count))
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
        bytes: UInt64(MemoryLayout<FieldValue>.stride + 32 + 128)
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
    for (name, literal) in zip(fieldNames, literals) {
        let valueBytes = try prospectiveLiteralValueFootprint(literal)
        let entry = try DatabaseIntermediateFootprint(
            bytes: 32
        ).adding(
            DatabaseIntermediateFootprint(bytes: UInt64(name.utf8.count))
        ).adding(
            DatabaseIntermediateFootprint(bytes: valueBytes)
        )
        try DatabaseByteProcessingMeter.consume(
            byteCount: entry.bytes,
            workMeter: workMeter,
            stage: stage
        )
        footprint = try footprint.adding(entry)
    }
    return footprint
}

private func prospectiveArrayValueFootprint(
    _ values: [FieldValue],
    workMeter: DatabaseWorkMeter,
    stage: DatabaseWorkStage
) throws -> DatabaseIntermediateFootprint {
    var footprint = try DatabaseIntermediateFootprint(
        bytes: UInt64(MemoryLayout<FieldValue>.stride + 32)
    ).adding(
        StorageValueDecoder.retainedArrayStorageFootprint(
            elementCount: values.count
        )
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
            return DatabaseIntermediateFootprint(
                bytes: UInt64(MemoryLayout<FieldValue>.stride + 32)
            )
        }
        return try CanonicalRelationalFootprintMeter.valueFootprint(
            of: value,
            workMeter: workMeter,
            stage: stage
        )
    case .variable(let variable):
        guard let value = row.fields[variable.name] else {
            return DatabaseIntermediateFootprint(
                bytes: UInt64(MemoryLayout<FieldValue>.stride + 32)
            )
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
            return try DatabaseIntermediateFootprint(
                bytes: UInt64(MemoryLayout<FieldValue>.stride + 64)
            ).adding(transformedBytes)
        }
        return DatabaseIntermediateFootprint(
            bytes: UInt64(MemoryLayout<FieldValue>.stride + 32)
        )
    case .caseWhen(let cases, let elseResult):
        var result = DatabaseIntermediateFootprint(
            bytes: UInt64(MemoryLayout<FieldValue>.stride + 32)
        )
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
        var result = DatabaseIntermediateFootprint(
            bytes: UInt64(MemoryLayout<FieldValue>.stride + 32)
        )
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
        return DatabaseIntermediateFootprint(
            bytes: UInt64(MemoryLayout<FieldValue>.stride + 256)
        )
    default:
        return DatabaseIntermediateFootprint(
            bytes: UInt64(MemoryLayout<FieldValue>.stride + 32)
        )
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

    consuming func promoteToPublicResponse() -> QueryResponse {
        let visibleRange = visibleRange
        let continuation = continuation
        let metadata = metadata
        let affectedRows = affectedRows
        let metadataReservation = metadataReservation
        defer { withExtendedLifetime(metadataReservation) {} }
        guard !visibleRange.isEmpty else {
            return QueryResponse(
                rows: [],
                continuation: continuation,
                metadata: metadata,
                affectedRows: affectedRows
            )
        }

        var outputRows = rows.promoteToOutput()
        if visibleRange.upperBound < outputRows.count {
            outputRows.removeLast(outputRows.count - visibleRange.upperBound)
        }
        if visibleRange.lowerBound > 0 {
            outputRows.removeFirst(visibleRange.lowerBound)
        }
        return QueryResponse(
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
    let fields: [String: FieldValue]
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
        return response.promoteToPublicResponse()
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
        var builder = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: internalOptions.workMeter,
            stage: .bindingCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: candidates.count
        )
        try candidates.forEachEntry { entry in
            try internalOptions.workMeter.consume(at: .bindingCandidate)
            try builder.append(
                footprint: try CanonicalRelationalFootprintMeter
                    .sourceRowFootprint(
                        fields: entry.row.fields,
                        sourceName: sourceName,
                        annotations: entry.row.annotations,
                        version: entry.row.version,
                        workMeter: internalOptions.workMeter,
                        stage: .bindingCandidate
                    )
            ) {
                CanonicalSourceRow.fromBaseFields(
                    entry.row.fields,
                    sourceName: sourceName,
                    annotations: entry.row.annotations,
                    version: entry.row.version
                )
            }
        }
        let sourceRows = try builder.finish().moveToSharedOwnership(
            at: .bindingCandidate
        )
        let authorizedSession = try session.authorizedSession(authorization)
        return try await finalizeRelationalRows(
            selectQuery,
            sourceRows: sourceRows,
            sourceSchema: try tableRelationSchema(tableRef),
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
            return response.promoteToPublicResponse()
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
            CanonicalSourceRow(
                materializedFields: parent.fields,
                unscopedFields: parent.unscopedFields,
                scopedFields: parent.scopedFields,
                coalescedColumns: parent.coalescedColumns,
                annotations: parent.annotations,
                version: parent.version,
                ambiguityOverride: parent.ambiguousUnqualifiedColumns.union(
                    currentColumnNames
                )
            )
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
        if row.fields[name] != nil
            || row.ambiguousUnqualifiedColumns.contains(name) {
            return true
        }
        return row.scopedFields.contains { sourceName, fields in
            fields.keys.contains { "\(sourceName).\($0)" == name }
        }
    }

    private func groupedOuterScope(
        sourceRow: CanonicalSourceRow,
        groupBy: [Expression]
    ) -> CanonicalSourceRow {
        var unscopedFields: [String: FieldValue] = [:]
        // Preserve empty current scopes so a same-named ancestor scope cannot
        // become visible when this aggregate has no grouped column for it.
        var scopedFields = Dictionary(
            uniqueKeysWithValues: sourceRow.scopedFields.keys.map {
                ($0, [String: FieldValue]())
            }
        )

        for expression in groupBy {
            guard case .column(let column) = expression,
                  let value = sourceRow.value(for: column) else {
                continue
            }
            if let table = column.table {
                scopedFields[table, default: [:]][column.column] = value
                continue
            }
            if sourceRow.unscopedFields[column.column] != nil {
                unscopedFields[column.column] = value
                continue
            }
            let matchingScopes = sourceRow.scopedFields.compactMap {
                sourceName, fields in
                fields[column.column] == nil ? nil : sourceName
            }
            if matchingScopes.count == 1, let sourceName = matchingScopes.first {
                scopedFields[sourceName, default: [:]][column.column] = value
            }
        }

        return CanonicalSourceRow(
            unscopedFields: unscopedFields,
            scopedFields: scopedFields,
            coalescedColumns: sourceRow.coalescedColumns.intersection(
                unscopedFields.keys
            ),
            annotations: [:],
            version: nil
        )
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
        if let table = column.table {
            if let scope = sourceSchema.scopes.first(
                where: { $0.name == table }
            ) {
                guard scope.columns.contains(column.column) else {
                    throw CanonicalReadError.expressionEvaluation(
                        .missingColumn(column.displayName)
                    )
                }
                return
            }
            guard outerRow?.scopedFields[table]?[column.column] != nil else {
                throw CanonicalReadError.expressionEvaluation(
                    .missingColumn(column.displayName)
                )
            }
            return
        }

        let occurrenceCount = sourceSchema.occurrenceCount(
            of: column.column
        )
        if occurrenceCount == 1 { return }
        if occurrenceCount > 1 {
            throw CanonicalReadError.expressionEvaluation(
                .ambiguousColumn(column.column)
            )
        }
        if outerRow?.ambiguousUnqualifiedColumns.contains(column.column)
            == true {
            throw CanonicalReadError.expressionEvaluation(
                .ambiguousColumn(column.column)
            )
        }
        guard outerRow?.fields[column.column] != nil else {
            throw CanonicalReadError.expressionEvaluation(
                .missingColumn(column.column)
            )
        }
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
        leftRows: consuming DatabaseRetainedBuffer<QueryRow>,
        leftTable: TableRef,
        rightRows: consuming DatabaseRetainedBuffer<QueryRow>,
        rightTable: TableRef,
        options: ReadExecutionContext
    ) async throws -> QueryResponse {
        guard join.type == .inner else {
            throw CompositionQueryError.unsupportedPlan(
                "bounded cross-Base execution currently requires INNER JOIN"
            )
        }
        var leftBuilder = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: options.workMeter,
            stage: .joinCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: leftRows.count
        )
        try leftRows.withSpan { rows in
            for index in rows.indices {
                let row = rows[index]
                try leftBuilder.append(
                    footprint: try CanonicalRelationalFootprintMeter
                        .sourceRowFootprint(
                            fields: row.fields,
                            sourceName: leftTable.effectiveName,
                            annotations: row.annotations,
                            version: row.version,
                            workMeter: options.workMeter,
                            stage: .joinCandidate
                        ),
                    make: {
                        CanonicalSourceRow.fromBaseFields(
                            row.fields,
                            sourceName: leftTable.effectiveName,
                            annotations: row.annotations,
                            version: row.version
                        )
                    }
                )
            }
        }
        leftRows.discard()

        var rightBuilder = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: options.workMeter,
            stage: .joinCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: rightRows.count
        )
        try rightRows.withSpan { rows in
            for index in rows.indices {
                let row = rows[index]
                try rightBuilder.append(
                    footprint: try CanonicalRelationalFootprintMeter
                        .sourceRowFootprint(
                            fields: row.fields,
                            sourceName: rightTable.effectiveName,
                            annotations: row.annotations,
                            version: row.version,
                            workMeter: options.workMeter,
                            stage: .joinCandidate
                        ),
                    make: {
                        CanonicalSourceRow.fromBaseFields(
                            row.fields,
                            sourceName: rightTable.effectiveName,
                            annotations: row.annotations,
                            version: row.version
                        )
                    }
                )
            }
        }
        rightRows.discard()

        let left = try leftBuilder.finish().moveToSharedOwnership(
            at: .joinCandidate
        )
        let right = try rightBuilder.finish().moveToSharedOwnership(
            at: .joinCandidate
        )
        let leftSchema = try tableRelationSchema(leftTable)
        let rightSchema = try tableRelationSchema(rightTable)
        let joined = try await performJoin(
            left: CanonicalRelation(schema: leftSchema, rows: left),
            right: CanonicalRelation(schema: rightSchema, rows: right),
            type: join.type,
            condition: join.condition,
            workMeter: options.workMeter
        )
        let collecting = try await finalizeRelationalRows(
            selectQuery,
            sourceRows: joined.rows,
            sourceSchema: joined.schema,
            residualFilter: selectQuery.filter,
            residualOrderBy: selectQuery.orderBy,
            options: options
        )
        // The Composition read snapshot that planned this join still holds its
        // member transactions and Base leases, so it owns the check.
        let response = try finalizePostClosureResult(
            consume collecting,
            ownsProducingTransaction: false
        )
        return response.promoteToPublicResponse()
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
        if !isSPARQLSource(selectQuery.source),
           !sourceRequiresRuntimeInferredSchema(
            selectQuery.source,
            namedSubqueries: namedSubqueries
        ) {
            let sourceSchema = try canonicalRelationSchema(
                for: selectQuery.source,
                namedSubqueries: namedSubqueries
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

        if isSPARQLSource(selectQuery.source) {
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
        if let sourceMeter = rowSet.workMeter,
           sourceMeter !== options.workMeter {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        var retainedRows = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: options.workMeter,
            stage: .projection,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: rowSet.count
        )
        for index in 0..<rowSet.count {
            try options.workMeter.consume(at: .projection)
            try rowSet.withRow(at: index) { indexRow in
                try retainedRows.append(
                    footprint: try CanonicalRelationalFootprintMeter
                        .sourceRowFootprint(
                            fields: indexRow.fields,
                            sourceName: sourceName,
                            annotations: indexRow.annotations,
                            version: indexRow.version,
                            workMeter: options.workMeter
                        ),
                    make: {
                        CanonicalSourceRow.fromBaseFields(
                            indexRow.fields,
                            sourceName: sourceName,
                            annotations: indexRow.annotations,
                            version: indexRow.version
                        )
                    }
                )
            }
        }
        let canonicalRows = try retainedRows.finish().moveToSharedOwnership(
            at: .projection
        )

        let hasExplicitOrder = (selectQuery.orderBy?.isEmpty == false)
        return try await finalizeRelationalRows(
            selectQuery,
            sourceRows: canonicalRows,
            sourceSchema: sourceSchema,
            residualFilter: selectQuery.filter,
            residualOrderBy: selectQuery.orderBy,
            sourceRowsAlreadyOrdered:
                rowSet.ordering == .orderedByIndex && !hasExplicitOrder,
            metadata: rowSet.metadata,
            metadataReservation: rowSet.retainedMetadataReservation,
            options: options,
            evaluationContext: evaluationContext
        )
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
            sourceSchema: try tableRelationSchema(tableRef),
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
    ) async throws -> CanonicalRetainedRows {
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
        return rows.rows
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

    private func isSPARQLSource(_ source: DataSource) -> Bool {
        switch source {
        case .graphPattern, .namedGraph, .service:
            return true
        default:
            return false
        }
    }

    private func materializePolymorphicSourceRows(
        _ entities: borrowing DatabaseRetainedPolymorphicEntities,
        sourceName: String,
        stage: DatabaseWorkStage
    ) throws -> CanonicalRetainedRows {
        var sourceRows = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: entities.workMeter,
            stage: stage,
            layout: try DatabaseRetainedArrayLayout.forElement(
                CanonicalSourceRow.self
            ),
            expectedCount: entities.count
        )
        for index in 0..<entities.count {
            _ = try entities.appendCanonicalSourceRow(
                at: index,
                sourceName: sourceName,
                to: &sourceRows,
                stage: stage
            )
        }
        return try sourceRows.finish().moveToSharedOwnership(at: stage)
    }

    private func executePolymorphicRows(
        _ selectQuery: SelectQuery,
        logicalSource: LogicalSourceRef,
        options: ReadExecutionContext,
        transaction: DatabaseReadTransaction,
        evaluationContext: CanonicalQueryEvaluationContext
    ) async throws -> CanonicalRetainedQueryResponse {
        let group = try container.polymorphicGroup(identifier: logicalSource.identifier)
        let entities = try await evaluationContext.fusionSession
            .scanRetainedPolymorphicItems(
            group: group,
            selectQuery: selectQuery
        )
        let sourceName = logicalSource.alias ?? logicalSource.effectiveName
        let sourceRows = try materializePolymorphicSourceRows(
            entities,
            sourceName: sourceName,
            stage: .resultMaterialization
        )

        return try await finalizeRelationalRows(
            selectQuery,
            sourceRows: sourceRows,
            sourceSchema: try polymorphicRelationSchema(
                group,
                sourceName: sourceName
            ),
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

            let rows = try await materializeUnwindowedTableSourceRows(
                tableRef,
                options: options,
                transaction: transaction,
                authorizationQuery: authorizationQuery
            )
            return CanonicalRelation(
                schema: try tableRelationSchema(tableRef),
                rows: rows
            )

        case .logical(let logicalSource):
            guard logicalSource.kindIdentifier == LogicalSourceKind.polymorphic else {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "Logical source '\(logicalSource.kindIdentifier)' is not supported"
                )
            }
            let group = try container.polymorphicGroup(identifier: logicalSource.identifier)
            let entities = try await fusionSession
                .scanRetainedPolymorphicItems(
                group: group,
                selectQuery: authorizationQuery
            )
            let sourceName = logicalSource.alias ?? logicalSource.effectiveName
            let rows = try materializePolymorphicSourceRows(
                entities,
                sourceName: sourceName,
                stage: .bindingCandidate
            )
            return CanonicalRelation(
                schema: try polymorphicRelationSchema(
                    group,
                    sourceName: sourceName
                ),
                rows: rows
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

        case .values(let rows, let columnNames):
            let resolvedColumnNames = columnNames
                ?? rows.first?.indices.map { "column\($0)" }
                ?? []
            guard Set(resolvedColumnNames).count == resolvedColumnNames.count else {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "VALUES contains a duplicate column name"
                )
            }
            var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
                workMeter: options.workMeter,
                stage: .bindingCandidate,
                layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
                expectedCount: rows.count
            )
            for values in rows {
                try options.workMeter.consume(at: .bindingCandidate)
                guard resolvedColumnNames.count == values.count else {
                    throw CanonicalReadError.unsupportedSelectQuery("VALUES column count mismatch")
                }
                let footprint = try prospectiveLiteralSourceRowFootprint(
                    fieldNames: resolvedColumnNames,
                    literals: values,
                    workMeter: options.workMeter,
                    stage: .bindingCandidate
                )
                let admission = try retained.prepareAppend(
                    footprint: footprint,
                    at: .bindingCandidate
                )
                var fields: [String: FieldValue] = [:]
                fields.reserveCapacity(values.count)
                for (name, literal) in zip(resolvedColumnNames, values) {
                    fields[name] = try literal.toFieldValue()
                }
                retained.append(
                    CanonicalSourceRow(fields: fields),
                    using: admission
                )
            }
            return CanonicalRelation(
                schema: try CanonicalRelationSchema(
                    unscopedColumns: resolvedColumnNames
                ),
                rows: try retained.finish().moveToSharedOwnership(
                    at: .bindingCandidate
                )
            )

        case .graphTable(let graphTableSource):
            guard let executor = try readPolicy().graphTableSourceExecutor else {
                throw CanonicalReadError.unsupportedSource("graphTable executor is not registered")
            }
            let sourceRows = try await executor.executeInTransaction(
                session: fusionSession,
                graphTableSource: graphTableSource,
                options: options,
                partitions: partitionValues ?? FieldObject()
            )
            let graphRows = try materializeLogicalSourceRows(
                consume sourceRows,
                sourceName: nil,
                workMeter: options.workMeter,
                stage: .bindingCandidate
            )
            var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
                workMeter: options.workMeter,
                stage: .bindingCandidate,
                layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
                expectedCount: graphRows.count
            )
            for graphRow in graphRows {
                try options.workMeter.consume(at: .bindingCandidate)
                let sourceRow = canonicalGraphTableSourceRow(
                    from: graphRow.fields,
                    graphName: graphTableSource.graphName
                )
                if let columns = graphTableSource.columns,
                   !columns.isEmpty {
                    let scopedValueReservation = try reserveQueryScopedValues(
                        count: columns.count,
                        workMeter: options.workMeter,
                        stage: .bindingCandidate
                    )
                    defer { scopedValueReservation.release() }
                    var scopedValues: [DatabaseQueryScopedFieldValue] = []
                    scopedValues.reserveCapacity(columns.count)
                    for column in columns {
                        scopedValues.append(
                            try await evaluateQueryExpression(
                                column.expression,
                                on: sourceRow,
                                context: CanonicalQueryEvaluationContext(
                                    options: options,
                                    transaction: transaction,
                                    partitionValues: partitionValues,
                                    partitionMode: partitionMode,
                                    namedSubqueries: namedSubqueries,
                                    outerRow: allowsOuterReferences ? outerRow : nil,
                                    preparedFusionGraph: preparedFusionGraph,
                                    fusionSession: fusionSession
                                ),
                                workMeter: options.workMeter
                            )
                        )
                    }
                    let nameFootprint = try DatabaseIntermediateCollectionMeter
                        .arrayFootprint(
                            count: columns.count,
                            element: String.self
                        )
                    let nameReservation = try options.workMeter
                        .reserveIntermediate(
                            bytes: nameFootprint.bytes,
                            at: .bindingCandidate
                        )
                    defer { nameReservation.release() }
                    let names = columns.map(\.alias)
                    let footprint = try prospectiveSourceRowFootprint(
                        fieldNames: names,
                        values: scopedValues,
                        sourceName: graphTableSource.alias,
                        workMeter: options.workMeter,
                        stage: .bindingCandidate
                    )
                    let admission = try retained.prepareAppend(
                        footprint: footprint,
                        at: .bindingCandidate
                    )
                    var fields: [String: FieldValue] = [:]
                    fields.reserveCapacity(columns.count)
                    for (column, scopedValue) in zip(columns, scopedValues) {
                        scopedValue.withValue {
                            fields[column.alias] = $0
                        }
                    }
                    retained.append(
                        CanonicalSourceRow(fields: fields)
                            .applyingAlias(graphTableSource.alias),
                        using: admission
                    )
                } else {
                    try retained.append(
                        footprint: try prospectiveAliasedSourceRowFootprint(
                            sourceRow,
                            alias: graphTableSource.alias,
                            workMeter: options.workMeter,
                            stage: .bindingCandidate
                        ),
                        make: {
                            sourceRow.applyingAlias(graphTableSource.alias)
                        }
                    )
                }
            }
            let materializedRows = try retained.finish().moveToSharedOwnership(
                at: .bindingCandidate
            )
            let schema = try graphTableRelationSchema(
                graphTableSource,
                rows: materializedRows
            )
            return CanonicalRelation(schema: schema, rows: materializedRows)

        case .graphPattern(let pattern):
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
            let rows = try materializeLogicalSourceRows(
                consume sourceRows,
                sourceName: nil,
                workMeter: options.workMeter,
                stage: .bindingCandidate
            )
            return CanonicalRelation(
                schema: try CanonicalRelationSchema(
                    unscopedColumns: sparqlVariables(in: pattern)
                ),
                rows: rows
            )

        case .namedGraph(_, let pattern):
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
            let rows = try materializeLogicalSourceRows(
                consume sourceRows,
                sourceName: nil,
                workMeter: options.workMeter,
                stage: .bindingCandidate
            )
            return CanonicalRelation(
                schema: try CanonicalRelationSchema(
                    unscopedColumns: sparqlVariables(in: pattern)
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
                namedSubqueries: namedSubqueries
            )
            try validateJoinCondition(
                clause.condition,
                leftSchema: left.schema,
                rightSchema: rightSchema,
                type: clause.type
            )
            let outputSchema = try joinOutputSchema(
                left.schema,
                rightSchema,
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
                            .footprint(
                                of: row,
                                workMeter: options.workMeter
                            ),
                        make: { row }
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
            left.schema,
            right.schema,
            condition: condition
        )
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
                                condition: condition
                            )
                        }
                    )
                }
            }
            return CanonicalRelation(
                schema: outputSchema,
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
            workMeter: workMeter,
            evaluationContext: evaluationContext
        ) {
            return CanonicalRelation(schema: outputSchema, rows: hashJoined)
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
                                condition: condition
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
                            condition: condition
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
                            condition: condition
                        )
                    }
                )
            }
        }

        return CanonicalRelation(
            schema: outputSchema,
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
        condition: JoinCondition?
    ) throws -> CanonicalSourceRow {
        guard case .using(let columns) = condition else {
            return try left.merged(with: right)
        }
        let coalesced = Set(columns)
        let leftValues = Dictionary(
            uniqueKeysWithValues: columns.compactMap { column in
                firstScopedFieldValue(named: column, in: left).map {
                    (column, $0)
                }
            }
        )
        let rightValues = Dictionary(
            uniqueKeysWithValues: columns.compactMap { column in
                firstScopedFieldValue(named: column, in: right).map {
                    (column, $0)
                }
            }
        )
        var outputValues: [String: FieldValue] = [:]
        for column in columns {
            let leftValue = leftValues[column]
            let rightValue = rightValues[column]
            outputValues[column] = leftValue.flatMap {
                $0.isNull ? nil : $0
            } ?? rightValue ?? .null
        }
        let leftUnscoped = left.unscopedFields.filter {
            !coalesced.contains($0.key)
        }
        let rightUnscoped = right.unscopedFields.filter {
            !coalesced.contains($0.key)
        }
        let scopes = left.scopedFields.merging(right.scopedFields) {
            current, _ in current
        }
        return CanonicalSourceRow(
            unscopedFields: leftUnscoped
                .merging(rightUnscoped) { current, _ in current }
                .merging(outputValues) { current, _ in current },
            scopedFields: scopes,
            coalescedColumns: left.coalescedColumns
                .union(right.coalescedColumns)
                .union(coalesced),
            annotations: left.annotations.merging(right.annotations) {
                current, _ in current
            },
            version: nil
        )
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
                            condition: condition
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
                            condition: condition
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
                            condition: condition
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
        let outputSchema = try CanonicalRelationSchema(
            unscopedColumns: first.schema.visibleColumns
        )
        var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: options.workMeter,
            stage: .bindingCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self)
        )
        try appendAlignedSetOperationRows(
            first,
            to: outputSchema,
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
                rows: try uniqueSourceRows(rows, workMeter: options.workMeter)
            )
        }
        return CanonicalRelation(schema: outputSchema, rows: rows)
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
        let outputSchema = try CanonicalRelationSchema(
            unscopedColumns: firstRelation.schema.visibleColumns
        )
        var accumulator = try alignSetOperationRows(
            firstRelation,
            to: outputSchema,
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
                    try identityRow(row, operation: "INTERSECT")
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
                    try identityRow(row, operation: "INTERSECT")
                ) else { continue }
                try intersected.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: row,
                        workMeter: options.workMeter
                    ),
                    make: { row }
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
            )
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
        let outputSchema = try CanonicalRelationSchema(
            unscopedColumns: left.schema.visibleColumns
        )
        let leftRows = try alignSetOperationRows(
            left,
            to: outputSchema,
            workMeter: options.workMeter
        )
        let rightRows = try alignSetOperationRows(
            right,
            to: outputSchema,
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
                try identityRow(row, operation: "EXCEPT")
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
                try identityRow(row, operation: "EXCEPT")
            ) else { continue }
            try difference.append(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: row,
                    workMeter: options.workMeter
                ),
                make: { row }
            )
        }
        return CanonicalRelation(
            schema: outputSchema,
            rows: try uniqueSourceRows(
                try difference.finish().moveToSharedOwnership(
                    at: .joinCandidate
                ),
                workMeter: options.workMeter
            )
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
                operation: "relational DISTINCT"
            )
            if seen.insert(key).inserted {
                try unique.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: row,
                        workMeter: workMeter
                    ),
                    make: { row }
                )
            }
        }
        return try unique.finish().moveToSharedOwnership(at: .deduplication)
    }

    private func identityRow(
        _ row: CanonicalSourceRow,
        operation: String
    ) throws -> CanonicalRowValueIdentity {
        try CanonicalRowValueIdentity(
            fields: canonicalIdentityFields(
                row.fields,
                operation: operation
            )
        )
    }

    private func canonicalIdentityFields(
        _ fields: [String: FieldValue],
        operation: String
    ) throws -> [String: FieldValue] {
        var result: [String: FieldValue] = [:]
        result.reserveCapacity(fields.count)
        for (name, value) in fields {
            result[name] = try canonicalValueIdentity(
                value,
                operation: operation
            )
        }
        return result
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

    private func canonicalGraphTableSourceRow(
        from fields: [String: FieldValue],
        graphName: String
    ) -> CanonicalSourceRow {
        var baseFields: [String: FieldValue] = [:]
        var scopedFields: [String: [String: FieldValue]] = [graphName: [:]]

        for (key, value) in fields {
            if let dotIndex = key.firstIndex(of: ".") {
                let scope = String(key[..<dotIndex])
                let fieldName = String(key[key.index(after: dotIndex)...])
                scopedFields[scope, default: [:]][fieldName] = value
                baseFields[key] = value
                continue
            }

            baseFields[key] = value
            scopedFields[graphName, default: [:]][key] = value
        }

        var nonemptyScopes: [String: [String: FieldValue]] = [:]
        nonemptyScopes.reserveCapacity(scopedFields.count)
        for (scope, scopeFields) in scopedFields where !scopeFields.isEmpty {
            nonemptyScopes[scope] = scopeFields
        }

        let resolutionFields = CanonicalSourceRow.flatten(
            scopedFields: nonemptyScopes
        )
        return CanonicalSourceRow(
            materializedFields: baseFields.merging(resolutionFields) {
                current, _ in current
            },
            unscopedFields: [:],
            scopedFields: nonemptyScopes
        )
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
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: row,
                    workMeter: workMeter
                ),
                make: { row }
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
            let fingerprint = try CanonicalRowFingerprint.compute(
                QueryRow(
                    fields: row.fields,
                    annotations: row.annotations,
                    version: row.version
                ),
                workMeter: workMeter
            )
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
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: row,
                    workMeter: workMeter
                ),
                make: { row }
            )
        }
        return try retained.finish().moveToSharedOwnership(at: .sortInput)
    }

    private func projectRows(
        _ rows: CanonicalRetainedRows,
        projection: Projection,
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedQueryRows {
        var retained = try DatabaseRetainedArrayBuilder<QueryRow>(
            workMeter: workMeter,
            stage: .projection,
            layout: try DatabaseRetainedArrayLayout.forElement(QueryRow.self),
            expectedCount: rows.count
        )
        switch projection {
        case .all:
            for row in rows {
                try workMeter.consume(at: .projection)
                try retained.append(
                    footprint: try prospectiveWildcardQueryRowFootprint(
                        row,
                        workMeter: workMeter
                    ),
                    make: {
                        QueryRow(
                            fields: row.wildcardFields,
                            annotations: row.annotations,
                            version: row.version
                        )
                    }
                )
            }

        case .allFrom(let sourceName):
            for row in rows {
                try workMeter.consume(at: .projection)
                guard let fields = row.fields(for: sourceName) else {
                    throw CanonicalReadError.unsupportedSelectQuery("Projection source '\(sourceName)' not found")
                }
                try retained.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: QueryRow(
                            fields: fields,
                            annotations: row.annotations,
                            version: row.version
                        ),
                        workMeter: workMeter
                    ),
                    make: {
                        QueryRow(
                            fields: fields,
                            annotations: row.annotations,
                            version: row.version
                        )
                    }
                )
            }

        case .items(let items):
            let names = items.enumerated().map { index, item in
                item.alias ?? canonicalProjectionName(
                    for: item.expression,
                    index: index
                )
            }
            guard Set(names).count == names.count else {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "A projection exposes duplicate column names"
                )
            }
            for row in rows {
                try workMeter.consume(at: .projection)
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
                        try await evaluateQueryExpression(
                            item.expression,
                            on: row,
                            context: evaluationContext,
                            workMeter: workMeter
                        )
                    )
                }
                try retained.append(
                    footprint: try prospectiveQueryRowFootprint(
                        fieldNames: names,
                        values: scopedValues,
                        annotations: row.annotations,
                        workMeter: workMeter
                    ),
                    make: {
                        var fields: [String: FieldValue] = [:]
                        fields.reserveCapacity(items.count)
                        for (fieldName, scopedValue) in zip(
                            names,
                            scopedValues
                        ) {
                            scopedValue.withValue {
                                fields[fieldName] = $0
                            }
                        }
                        return QueryRow(
                            fields: fields,
                            annotations: row.annotations
                        )
                    }
                )
            }

        case .distinctItems(let items):
            return try canonicalUniqueRows(
                try await projectRows(
                    rows,
                    projection: .items(items),
                    workMeter: workMeter,
                    evaluationContext: evaluationContext
                ),
                workMeter: workMeter
            )
        }
        return try retained.finish().moveToSharedOwnership(at: .projection)
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
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: row,
                        workMeter: workMeter
                    ),
                    make: { row }
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
        groupBy: [Expression],
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedQueryRows {
        let projectedItemNames: [String]?
        switch projection {
        case .items(let items), .distinctItems(let items):
            let names = items.enumerated().map { index, item in
                item.alias ?? canonicalProjectionName(
                    for: item.expression,
                    index: index
                )
            }
            guard Set(names).count == names.count else {
                throw CanonicalReadError.aggregateEvaluation(
                    .invalidGroupedExpression(
                        "Aggregate projection names must be unique"
                    )
                )
            }
            projectedItemNames = names
        case .all, .allFrom:
            projectedItemNames = nil
        }
        var rows = try DatabaseRetainedArrayBuilder<QueryRow>(
            workMeter: workMeter,
            stage: .projection,
            layout: try DatabaseRetainedArrayLayout.forElement(QueryRow.self),
            expectedCount: groups.count
        )
        for group in groups {
            try workMeter.consume(at: .projection)
            switch projection {
            case .all:
                try rows.append(
                    footprint: try prospectiveWildcardQueryRowFootprint(
                        group.representative,
                        workMeter: workMeter
                    ),
                    make: {
                        QueryRow(
                            fields: group.representative.wildcardFields,
                            annotations: group.representative.annotations
                        )
                    }
                )
                continue
            case .allFrom(let sourceName):
                guard let fields = group.representative.fields(for: sourceName) else {
                    throw CanonicalReadError.unsupportedSelectQuery(
                        "Projection source '\(sourceName)' not found"
                    )
                }
                try rows.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: QueryRow(
                            fields: fields,
                            annotations: group.representative.annotations
                        ),
                        workMeter: workMeter
                    ),
                    make: {
                        QueryRow(
                            fields: fields,
                            annotations: group.representative.annotations
                        )
                    }
                )
                continue
            case .items(let items), .distinctItems(let items):
                guard let names = projectedItemNames else {
                    throw CanonicalReadError.aggregateEvaluation(
                        .invalidGroupedExpression(
                            "Grouped projection metadata is unavailable"
                        )
                    )
                }
                let scopedValueReservation = try reserveQueryScopedValues(
                    count: items.count,
                    workMeter: workMeter,
                    stage: .projection
                )
                defer { scopedValueReservation.release() }
                var scopedValues: [DatabaseQueryScopedFieldValue] = []
                scopedValues.reserveCapacity(items.count)
                for (_, item) in zip(names, items) {
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
                let footprint = try prospectiveQueryRowFootprint(
                    fieldNames: names,
                    values: scopedValues,
                    workMeter: workMeter
                )
                let admission = try rows.prepareAppend(
                    footprint: footprint,
                    at: .projection
                )
                var fields: [String: FieldValue] = [:]
                fields.reserveCapacity(items.count)
                for (name, scopedValue) in zip(names, scopedValues) {
                    scopedValue.withValue { fields[name] = $0 }
                }
                rows.append(QueryRow(fields: fields), using: admission)
                continue
            }
        }
        let projectedRows = try rows.finish().moveToSharedOwnership(
            at: .projection
        )
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
            for column in sourceSchema.unscopedColumns {
                try requireGroupedColumn(column, sourceName: nil)
            }
            for scope in sourceSchema.scopes {
                for column in scope.columns
                    where !sourceSchema.coalescedColumns.contains(column) {
                    try requireGroupedColumn(
                        column,
                        sourceName: scope.name
                    )
                }
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
            var outputBytes = UInt64(MemoryLayout<FieldValue>.stride + 64)
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
                guard resultRow.fields.count == 1,
                      let value = resultRow.fields.values.first else {
                    throw CanonicalReadError.invalidScalarSubquery(
                        rowCount: 1,
                        columnCount: resultRow.fields.count
                    )
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
                    guard resultRow.fields.count == 1,
                          let value = resultRow.fields.values.first else {
                        throw CanonicalReadError.invalidMembershipSubquery(
                            columnCount: resultRow.fields.count
                        )
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
                max(1, MemoryLayout<CanonicalRowValueIdentity>.stride + 32)
            )
        ).multiplied(by: UInt64(rows.count)).bytes
        let setReservation = try workMeter.reserveIntermediate(
            bytes: setBytes,
            at: .deduplication
        )
        defer { setReservation.release() }
        var seen: Set<CanonicalRowValueIdentity> = []
        seen.reserveCapacity(rows.count)
        var unique = try DatabaseRetainedArrayBuilder<QueryRow>(
            workMeter: workMeter,
            stage: .deduplication,
            layout: try DatabaseRetainedArrayLayout.forElement(QueryRow.self),
            expectedCount: rows.count
        )
        for row in rows {
            try workMeter.consume(at: .deduplication)
            if seen.insert(
                CanonicalRowValueIdentity(
                    fields: try canonicalIdentityFields(
                        row.fields,
                        operation: "SELECT DISTINCT"
                    )
                )
            ).inserted {
                try unique.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: row,
                        workMeter: workMeter
                    ),
                    make: { row }
                )
            }
        }
        return try unique.finish().moveToSharedOwnership(at: .deduplication)
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
            CanonicalRelationalFootprintMeter.footprint(
                of: group.representative,
                workMeter: workMeter
            )
        )
        return footprint
    }

    private func tableRelationSchema(
        _ tableRef: TableRef
    ) throws -> CanonicalRelationSchema {
        let entity = try resolveEntity(named: tableRef.table)
        return try CanonicalRelationSchema(
            scopes: [
                CanonicalRelationScope(
                    name: tableRef.alias ?? tableRef.effectiveName,
                    columns: entity.allFields
                )
            ]
        )
    }

    private func polymorphicRelationSchema(
        _ group: PolymorphicGroup,
        sourceName: String
    ) throws -> CanonicalRelationSchema {
        var seen = Set<String>()
        var columns: [String] = []
        for entityName in group.memberTypeNames {
            let entity = try resolveEntity(named: entityName)
            for field in entity.allFields where seen.insert(field).inserted {
                columns.append(field)
            }
        }
        return try CanonicalRelationSchema(
            scopes: [CanonicalRelationScope(name: sourceName, columns: columns)]
        )
    }

    private func graphTableRelationSchema(
        _ source: GraphTableSource,
        rows: CanonicalRetainedRows?
    ) throws -> CanonicalRelationSchema {
        if let columns = source.columns, !columns.isEmpty {
            let names = columns.map { $0.alias }
            if let alias = source.alias {
                return try CanonicalRelationSchema(
                    scopes: [CanonicalRelationScope(name: alias, columns: names)]
                )
            }
            return try CanonicalRelationSchema(unscopedColumns: names)
        }
        guard let rows, !rows.isEmpty else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "An empty GRAPH_TABLE source requires an explicit COLUMNS schema"
            )
        }
        let inferred = try rows.withElement(at: 0) { row in
            try CanonicalRelationSchema(
                unscopedColumns: row.fields.keys.sorted()
            )
        }
        if let alias = source.alias {
            return try inferred.applyingAlias(alias)
        }
        return inferred
    }

    private func materializeQueryRelation(
        _ rows: CanonicalRetainedQueryRowView,
        query: SelectQuery,
        explicitColumns: [String]?,
        alias: String,
        namedSubqueries: [NamedSubquery],
        workMeter: DatabaseWorkMeter
    ) throws -> CanonicalRelation {
        let visibleNamedSubqueries = try mergeNamedSubqueries(
            local: query.subqueries ?? [],
            inherited: namedSubqueries
        )
        if sourceRequiresRuntimeInferredSchema(
            query.source,
            namedSubqueries: visibleNamedSubqueries
        ) {
            switch query.projection {
            case .all, .allFrom:
                throw CanonicalReadError.unsupportedSelectQuery(
                    "A nested query over a runtime-inferred source must declare its output columns"
                )
            case .items, .distinctItems:
                break
            }
        }
        let sourceSchema: CanonicalRelationSchema
        switch query.projection {
        case .items, .distinctItems:
            sourceSchema = try CanonicalRelationSchema()
        case .all, .allFrom:
            sourceSchema = try canonicalRelationSchema(
                for: query.source,
                namedSubqueries: visibleNamedSubqueries
            )
        }
        let outputColumns = try canonicalProjectionColumns(
            query.projection,
            sourceSchema: sourceSchema
        )
        let columns = explicitColumns ?? outputColumns
        guard columns.count == outputColumns.count else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "A common table expression column list must match its query output"
            )
        }
        guard Set(columns).count == columns.count else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "A subquery exposes duplicate column names"
            )
        }

        let schema = try CanonicalRelationSchema(
            scopes: [CanonicalRelationScope(name: alias, columns: columns)]
        )
        var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .bindingCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: rows.count
        )
        for position in 0..<rows.count {
            try rows.withElement(at: position) { row in
                try workMeter.consume(at: .bindingCandidate)
                let footprint = try CanonicalRelationalFootprintMeter
                    .sourceRowFootprint(
                        sourceFields: row.fields,
                        sourceColumns: outputColumns,
                        targetColumns: columns,
                        sourceName: alias,
                        annotations: row.annotations,
                        version: row.version,
                        workMeter: workMeter,
                        stage: .bindingCandidate
                    )
                let admission = try retained.prepareAppend(
                    footprint: footprint,
                    at: .bindingCandidate
                )
                var fields: [String: FieldValue] = [:]
                fields.reserveCapacity(columns.count)
                for (sourceColumn, targetColumn) in zip(
                    outputColumns,
                    columns
                ) {
                    guard let value = row.fields[sourceColumn] else {
                        throw CanonicalReadError.unsupportedSelectQuery(
                            "Subquery output column '\(sourceColumn)' is missing"
                        )
                    }
                    fields[targetColumn] = value
                }
                retained.append(
                    CanonicalSourceRow.fromBaseFields(
                        fields,
                        sourceName: alias,
                        annotations: row.annotations,
                        version: row.version
                    ),
                    using: admission
                )
            }
        }
        return CanonicalRelation(
            schema: schema,
            rows: try retained.finish().moveToSharedOwnership(
                at: .bindingCandidate
            )
        )
    }

    private func canonicalRelationSchema(
        for source: DataSource,
        namedSubqueries: [NamedSubquery]
    ) throws -> CanonicalRelationSchema {
        switch source {
        case .table(let tableRef):
            if let named = namedSubqueries.first(where: { $0.name == tableRef.table }) {
                let visibleNamedSubqueries = try mergeNamedSubqueries(
                    local: named.query.subqueries ?? [],
                    inherited: namedSubqueries
                )
                let sourceSchema: CanonicalRelationSchema
                switch named.query.projection {
                case .items, .distinctItems:
                    sourceSchema = try CanonicalRelationSchema()
                case .all, .allFrom:
                    sourceSchema = try canonicalRelationSchema(
                        for: named.query.source,
                        namedSubqueries: visibleNamedSubqueries
                    )
                }
                let projected = try canonicalProjectionColumns(
                    named.query.projection,
                    sourceSchema: sourceSchema
                )
                let columns = named.columns ?? projected
                guard columns.count == projected.count else {
                    throw CanonicalReadError.unsupportedSelectQuery(
                        "A common table expression column list must match its query output"
                    )
                }
                return try CanonicalRelationSchema(
                    scopes: [
                        CanonicalRelationScope(
                            name: tableRef.alias ?? named.name,
                            columns: columns
                        )
                    ]
                )
            }
            return try tableRelationSchema(tableRef)

        case .logical(let source):
            guard source.kindIdentifier == LogicalSourceKind.polymorphic else {
                throw CanonicalReadError.unsupportedSource(
                    "Logical source '\(source.kindIdentifier)' has no relational schema provider"
                )
            }
            return try polymorphicRelationSchema(
                container.polymorphicGroup(identifier: source.identifier),
                sourceName: source.effectiveName
            )

        case .subquery(let query, let alias):
            let visibleNamedSubqueries = try mergeNamedSubqueries(
                local: query.subqueries ?? [],
                inherited: namedSubqueries
            )
            let sourceSchema: CanonicalRelationSchema
            switch query.projection {
            case .items, .distinctItems:
                sourceSchema = try CanonicalRelationSchema()
            case .all, .allFrom:
                sourceSchema = try canonicalRelationSchema(
                    for: query.source,
                    namedSubqueries: visibleNamedSubqueries
                )
            }
            return try CanonicalRelationSchema(
                scopes: [
                    CanonicalRelationScope(
                        name: alias,
                        columns: try canonicalProjectionColumns(
                            query.projection,
                            sourceSchema: sourceSchema
                        )
                    )
                ]
            )

        case .join(let join):
            try validateJoinDeclaration(join)
            let left = try canonicalRelationSchema(
                for: join.left,
                namedSubqueries: namedSubqueries
            )
            let right = try canonicalRelationSchema(
                for: join.right,
                namedSubqueries: namedSubqueries
            )
            let condition: JoinCondition?
            switch join.type {
            case .natural, .naturalLeft, .naturalRight, .naturalFull:
                condition = .using(
                    inferNaturalJoinColumns(
                        leftSchema: left,
                        rightSchema: right
                    )
                )
            default:
                condition = join.condition
            }
            try validateJoinCondition(
                condition,
                leftSchema: left,
                rightSchema: right,
                type: join.type
            )
            return try joinOutputSchema(
                left,
                right,
                condition: condition
            )

        case .values(let rows, let columnNames):
            return try CanonicalRelationSchema(
                unscopedColumns: columnNames
                    ?? rows.first?.indices.map { "column\($0)" }
                    ?? []
            )

        case .union(let sources), .unionAll(let sources), .intersect(let sources):
            guard let first = sources.first else {
                return try CanonicalRelationSchema()
            }
            return try CanonicalRelationSchema(
                unscopedColumns: canonicalRelationSchema(
                    for: first,
                    namedSubqueries: namedSubqueries
                ).visibleColumns
            )

        case .except(let lhs, _):
            return try CanonicalRelationSchema(
                unscopedColumns: canonicalRelationSchema(
                    for: lhs,
                    namedSubqueries: namedSubqueries
                ).visibleColumns
            )

        case .graphTable(let graphTable):
            return try graphTableRelationSchema(graphTable, rows: nil)

        case .graphPattern(let pattern):
            return try CanonicalRelationSchema(
                unscopedColumns: sparqlVariables(in: pattern)
            )

        case .namedGraph(_, let pattern):
            return try CanonicalRelationSchema(
                unscopedColumns: sparqlVariables(in: pattern)
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
                namedSubqueries: namedSubqueries
            )
            let rightSchema = try canonicalRelationSchema(
                for: join.right,
                namedSubqueries: namedSubqueries
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
                namedSubqueries: visibleNamedSubqueries
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
        guard Set(columns).count == columns.count else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "A projection exposes duplicate column names"
            )
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

    private func appendAlignedSetOperationRows(
        _ relation: CanonicalRelation,
        to outputSchema: CanonicalRelationSchema,
        into aligned: inout DatabaseRetainedArrayBuilder<CanonicalSourceRow>,
        workMeter: DatabaseWorkMeter
    ) throws {
        let sourceColumns = relation.schema.visibleColumns
        let outputColumns = outputSchema.visibleColumns
        guard sourceColumns.count == outputColumns.count else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Set operation inputs must expose the same number of columns"
            )
        }
        for row in relation.rows {
            try workMeter.consume(at: .bindingCandidate)
            var footprint = try CanonicalRelationalFootprintMeter
                .sourceRowFootprint(
                    fields: [:],
                    sourceName: nil,
                    annotations: row.annotations,
                    version: row.version,
                    workMeter: workMeter,
                    stage: .bindingCandidate
                )
            for (sourceColumn, outputColumn) in zip(
                sourceColumns,
                outputColumns
            ) {
                guard let value = row.fields[sourceColumn] else {
                    throw CanonicalReadError.unsupportedSelectQuery(
                        "Set operation input column '\(sourceColumn)' is missing"
                    )
                }
                footprint = try footprint.adding(
                    CanonicalRelationalFootprintMeter.fieldEntryFootprint(
                        nameUTF8Count: outputColumn.utf8.count,
                        value: value,
                        workMeter: workMeter,
                        stage: .bindingCandidate
                    )
                )
            }
            let admission = try aligned.prepareAppend(
                footprint: footprint,
                at: .bindingCandidate
            )
            var fields: [String: FieldValue] = [:]
            fields.reserveCapacity(outputColumns.count)
            for (sourceColumn, outputColumn) in zip(sourceColumns, outputColumns) {
                guard let value = row.fields[sourceColumn] else {
                    throw CanonicalReadError.unsupportedSelectQuery(
                        "Set operation input column '\(sourceColumn)' is missing"
                    )
                }
                fields[outputColumn] = value
            }
            aligned.append(
                CanonicalSourceRow(
                    fields: fields,
                    annotations: row.annotations,
                    version: row.version
                ),
                using: admission
            )
        }
    }

    private func alignSetOperationRows(
        _ relation: CanonicalRelation,
        to outputSchema: CanonicalRelationSchema,
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
        var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: stage,
            layout: try DatabaseRetainedArrayLayout.forElement(
                CanonicalSourceRow.self
            ),
            expectedCount: rows.count
        )
        for index in 0..<rows.count {
            try rows.withElement(at: index) { row in
                try workMeter.consume(at: stage)
                let footprint = try CanonicalRelationalFootprintMeter
                    .sourceRowFootprint(
                        fields: row.fields,
                        sourceName: sourceName,
                        annotations: row.annotations,
                        version: row.version,
                        workMeter: workMeter,
                        stage: stage
                    )
                let admission = try retained.prepareAppend(
                    footprint: footprint,
                    at: stage
                )
                retained.append(
                    CanonicalSourceRow.fromBaseFields(
                        row.fields,
                        sourceName: sourceName,
                        annotations: row.annotations,
                        version: row.version
                    ),
                    using: admission
                )
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
        var footprint = try CanonicalRelationalFootprintMeter.footprint(
            of: row,
            workMeter: workMeter
        )
        if let outer {
            footprint = try footprint.adding(
                CanonicalRelationalFootprintMeter.footprint(
                    of: outer,
                    workMeter: workMeter
                )
            )
        }
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
