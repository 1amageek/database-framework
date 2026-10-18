import DatabaseKit
import DatabaseTypes

/// Binds persisted SQL index expressions through their original column identities.
enum SQLIndexCompiler {
    static func validate(catalog: SQLCatalog, physicalSchema: Schema) throws {
        for index in catalog.indexes {
            guard let relation = catalog.relations.first(where: { SQLConstraintCompiler.same($0.id, index.relationID) }),
                !physicalSchema.indexDescriptors.contains(where: { $0.name == SQLIndexRuntime.storageName(index) }) else {
                throw SQLConstraintError.invalidDeclaration(relation: index.relationID, reason: "index relation or physical name is invalid")
            }
            _ = try compile(index, relation: relation, physicalSchema: physicalSchema)
        }
    }

    static func compile(_ index: SQLIndex, relation: SQLRelation, physicalSchema: Schema,
        workMeter: DatabaseWorkMeter? = nil) throws -> SQLCompiledIndex {
        let columns = try relation.columns.enumerated().map { slot, column in
            try QueryColumn(number: UInt32(slot + 1), name: column.name,
                type: SQLConstraintCompiler.columnType(column, relation: relation, physicalSchema: physicalSchema),
                isNullable: true)
        }
        for binding in index.bindings {
            guard let slot = relation.columns.firstIndex(where: { SQLConstraintCompiler.same($0.id, binding.id) }),
                  columns[slot].type == binding.type else {
                throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "index bound column type changed")
            }
        }
        let binder = CanonicalQueryTypeBinder(columns: columns, resolveColumn: { reference in
            guard let id = index.bindings.first(where: { SQLConstraintCompiler.same($0.name, reference.column) })?.id else {
                throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "index expression has no stable column binding")
            }
            guard let slot = relation.columns.firstIndex(where: { column in
                SQLConstraintCompiler.same(column.id, id)
            }) else {
                throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "index column binding is missing")
            }
            return slot
        }, wildcardSlots: { _ in Array(relation.columns.indices) }, workMeter: workMeter)
        var keys: [CanonicalBoundExpression] = []
        for key in index.keys {
            let bound = try binder.bindScalarExpression(key.expression)
            try validate(bound, relation: relation)
            try CanonicalSQLValueSemantics.requireComparable(bound.type, operation: "SQL index", workMeter: workMeter)
            keys.append(bound)
        }
        let predicate = try index.predicate.map { try binder.bindPredicate($0) }
        if let predicate { try validate(predicate, relation: relation) }
        return SQLCompiledIndex(declaration: index, relation: relation, keys: keys, predicate: predicate)
    }

    static func conflictColumns(_ index: SQLIndex, relation: SQLRelation) -> [String]? {
        guard index.unique else { return nil }
        return index.directColumnIDs
    }

    /// Exact conjunction containment is deliberately the whole implication rule.
    static func predicateImplies(query: Expression?, index: Expression?) -> Bool {
        guard let index else { return true }
        guard let query else { return false }
        func contains(_ target: Expression, in expression: Expression) -> Bool {
            if expression == target { return true }
            if case .and(let left, let right) = expression {
                return contains(target, in: left) || contains(target, in: right)
            }
            return false
        }
        func implied(_ predicate: Expression) -> Bool {
            if case .and(let left, let right) = predicate { return implied(left) && implied(right) }
            return contains(predicate, in: query)
        }
        return implied(index)
    }

    static func predicateImplies(query: Expression?, index: SQLIndex, relation: SQLRelation) throws -> Bool {
        guard let predicate = index.predicate else { return true }
        guard let query else { return false }
        func references(_ expression: Expression) -> Set<ColumnRef> {
            SelectQuery(projection: [.expression(expression)], source: .values([[]])).referencedColumns
        }
        let queryReferences = references(query), indexReferences = references(predicate)
        var used = Set((queryReferences.union(indexReferences)).map { $0.column })
        var tokens: [String: String] = [:]
        for (slot, column) in relation.columns.enumerated() {
            var name = "sql_index_slot_" + String(slot)
            while used.contains(name) { name += "_" }
            used.insert(name)
            tokens[column.id] = name
        }
        var selectedQuery = query, selectedIndex = predicate
        for reference in queryReferences {
            guard let column = relation.columns.first(where: { SQLConstraintCompiler.same($0.name, reference.column) }),
                  let token = tokens[column.id] else { continue }
            selectedQuery = try QueryColumnRenaming.rename(selectedQuery, from: reference, to: ColumnRef(column: token))
        }
        for reference in indexReferences {
            guard let column = index.bindings.first(where: { SQLConstraintCompiler.same($0.name, reference.column) }),
                  let token = tokens[column.id] else { return false }
            selectedIndex = try QueryColumnRenaming.rename(selectedIndex, from: reference, to: ColumnRef(column: token))
        }
        return predicateImplies(query: selectedQuery, index: selectedIndex)
    }

    private static func validate(_ bound: CanonicalBoundExpression, relation: SQLRelation) throws {
        func unstableTemporal() throws -> Never {
            throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "index expression depends on temporal session settings")
        }
        switch bound.expression {
        case .subquery, .arraySubquery, .exists, .inSubquery, .aggregate, .grouping, .window,
             .parameter, .variable, .wholeRow, .currentValue:
            throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "index requires immutable current-row expressions")
        case .function(let call):
            let name = call.name.lowercased()
            if (name == "age" && call.arguments.count == 1)
                || ["now", "transaction_timestamp", "statement_timestamp", "clock_timestamp", "timeofday",
                    "random", "setseed", "nextval", "currval", "lastval", "setval", "current_date", "current_time",
                    "current_timestamp", "localtime", "localtimestamp", "current_schema", "current_schemas",
                    "current_database", "current_user", "session_user"].contains(name) {
                throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "index expression must be immutable")
            }
            if ["timezone", "make_timestamptz", "to_date"].contains(name)
                || name == "to_timestamp" && call.arguments.count != 1 { try unstableTemporal() }
            // Selected DATE_BIN and numeric epoch conversion operate on stored
            // scalars. Other zoned functions may consult the session zone or its
            // injected abbreviation/rules provider, including text conversion.
            if !["date_bin", "to_timestamp", "greatest", "least", "coalesce", "nullif"].contains(name),
                hasZonedType(bound.type) || bound.children.contains(where: { hasZonedType($0.type) }) {
                try unstableTemporal()
            }
        case .cast:
            if let source = bound.children.first?.type {
                let fromTemporal = CanonicalSQLValueSemantics.isTemporal(source)
                let toTemporal = CanonicalSQLValueSemantics.isTemporal(bound.type)
                if fromTemporal && CanonicalSQLValueSemantics.isCharacter(bound.type)
                    || toTemporal && CanonicalSQLValueSemantics.isCharacter(source)
                    || fromTemporal && toTemporal && hasZonedType(source) != hasZonedType(bound.type) {
                    try unstableTemporal()
                }
            }
        case .add, .subtract:
            if hasZonedType(bound.type), bound.children.contains(where: {
                if case .sql(.interval) = $0.type { return true }; return false
            }) { try unstableTemporal() }
        default: break
        }
        for child in bound.children { try validate(child, relation: relation) }
    }

    private static func hasZonedType(_ type: QueryColumnType) -> Bool {
        switch type {
        case .sql(.timestamp(true, _)), .sql(.time(true, _)), .primitive(.timestamp, _): return true
        case .sql(.array(let element)): return hasZonedType(.sql(element))
        case .sql(.composite(let descriptor)): return descriptor.columns.contains { hasZonedType($0.type) }
        default: return false
        }
    }
}
