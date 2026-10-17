import DatabaseKit
import DatabaseTypes

package enum SQLConstraintCompiler {
    package static func validate(catalog: SQLCatalog, physicalSchema: Schema) throws {
        for relation in catalog.relations {
            let binder = try binder(relation: relation, physicalSchema: physicalSchema)
            for column in relation.columns {
                let type = try columnType(column, relation: relation, physicalSchema: physicalSchema)
                switch column.policy {
                case .input(let expression):
                    if let expression {
                        let bound = try binder.bindScalarAssignment(expression, to: type)
                        try validateExpression(bound, relation: relation, permitsColumns: false, immutable: false)
                    }
                case .generated(let expression):
                    _ = try bindGenerated(expression, column: column, relation: relation, physicalSchema: physicalSchema)
                case .identity:
                    switch type {
                    case .primitive(.int16, false), .primitive(.int32, false), .primitive(.int64, false),
                         .sql(.smallint), .sql(.integer), .sql(.bigint): break
                    default: throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "identity requires an integer column")
                    }
                }
            }
            for constraint in relation.constraints {
                switch constraint.kind {
                case .check(let expression):
                    let bound = try binder.bindPredicate(expression)
                    try validateExpression(bound, relation: relation, permitsColumns: true, immutable: false)
                case .unique(let ids, _):
                    for id in ids { try requireComparable(id, relation: relation, physicalSchema: physicalSchema) }
                case .foreignKey(let foreign):
                    guard let target = catalog.relations.first(where: { same($0.id, foreign.referencedRelationID) }),
                          let targetIDs = foreign.referencedColumnIDs ?? target.primaryKeyColumnIDs else {
                        throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "foreign key target is missing")
                    }
                    for (sourceID, targetID) in zip(foreign.columnIDs, targetIDs) {
                        guard let source = relation.columns.first(where: { same($0.id, sourceID) }),
                              let destination = target.columns.first(where: { same($0.id, targetID) }) else {
                            throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "foreign key column is missing")
                        }
                        let sourceType = try columnType(source, relation: relation, physicalSchema: physicalSchema)
                        let targetType = try columnType(destination, relation: target, physicalSchema: physicalSchema)
                        let comparison = CanonicalQueryTypeBinder(columns: [
                            try QueryColumn(number: 1, name: "source", type: sourceType, isNullable: true),
                            try QueryColumn(number: 2, name: "target", type: targetType, isNullable: true)
                        ], resolveColumn: { $0.column == "source" ? 0 : 1 }, wildcardSlots: { _ in [0, 1] })
                        _ = try comparison.bindPredicate(.equal(.col("source"), .col("target")))
                        try requireComparable(sourceID, relation: relation, physicalSchema: physicalSchema)
                    }
                case .notNull: break
                }
            }
            for id in relation.primaryKeyColumnIDs ?? [] { try requireComparable(id, relation: relation, physicalSchema: physicalSchema) }
        }
    }

    static func same(_ lhs: String, _ rhs: String) -> Bool { lhs.utf8.elementsEqual(rhs.utf8) }

    static func columnType(_ column: SQLColumn, relation: SQLRelation, physicalSchema: Schema) throws -> QueryColumnType {
        switch column.backing {
        case .virtual(let type): return type
        case .field(let number):
            guard let field = physicalSchema.entities.first(where: { same($0.name, relation.entityName) })?.fields.first(where: { $0.fieldNumber == number }) else {
                throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "physical column is missing")
            }
            return field.queryType ?? .primitive(field.type, isArray: field.isArray)
        }
    }

    static func binder(relation: SQLRelation, physicalSchema: Schema, workMeter: DatabaseWorkMeter? = nil, tableAlias: String? = nil) throws -> CanonicalQueryTypeBinder {
        let columns = try relation.columns.enumerated().map { index, column in
            try QueryColumn(number: UInt32(index + 1), name: column.name,
                type: columnType(column, relation: relation, physicalSchema: physicalSchema), isNullable: true)
        }
        return CanonicalQueryTypeBinder(columns: columns, resolveColumn: { reference in
            guard reference.table == nil || same(reference.table!, relation.name) || tableAlias.map({ same(reference.table!, $0) }) == true,
                  let index = relation.columns.firstIndex(where: { same($0.name, reference.column) }) else {
                throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "unknown policy column")
            }
            return index
        }, wildcardSlots: { _ in Array(relation.columns.indices) }, workMeter: workMeter)
    }

    static func bindGenerated(_ expression: Expression, column: SQLColumn, relation: SQLRelation,
        physicalSchema: Schema, workMeter: DatabaseWorkMeter? = nil) throws -> CanonicalBoundExpression {
        let bound = try binder(relation: relation, physicalSchema: physicalSchema, workMeter: workMeter)
            .bindScalarAssignment(expression, to: columnType(column, relation: relation, physicalSchema: physicalSchema))
        try validateExpression(bound, relation: relation, permitsColumns: true, immutable: true)
        return bound
    }

    private static func requireComparable(_ id: String, relation: SQLRelation, physicalSchema: Schema) throws {
        guard let column = relation.columns.first(where: { same($0.id, id) }) else {
            throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "key column is missing")
        }
        try CanonicalSQLValueSemantics.requireComparable(columnType(column, relation: relation, physicalSchema: physicalSchema), operation: "SQL constraint", workMeter: nil)
    }

    private static func validateExpression(_ bound: CanonicalBoundExpression, relation: SQLRelation,
        permitsColumns: Bool, immutable: Bool) throws {
        if let slot = bound.columnSlot {
            guard permitsColumns else { throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "default cannot reference a column") }
            if immutable, case .generated = relation.columns[slot].policy {
                throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "generated column cannot reference another generated column")
            }
        }
        switch bound.expression {
        case .subquery, .arraySubquery, .exists, .inSubquery, .aggregate, .grouping, .window, .parameter, .variable, .wholeRow:
            throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "policy requires a current-row scalar expression")
        case .currentValue where immutable:
            throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "generated expression must be immutable")
        case .function(let call) where immutable:
            let name = call.name.lowercased()
            if (name == "age" && call.arguments.count == 1)
                || ["now", "transaction_timestamp", "statement_timestamp", "clock_timestamp", "timeofday", "random", "setseed", "nextval", "currval", "lastval", "setval", "current_date", "current_time", "current_timestamp", "localtime", "localtimestamp", "current_schema", "current_schemas", "current_database", "current_user", "session_user"].contains(name) {
                throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "generated expression must be immutable")
            }
        default: break
        }
        for child in bound.children { try validateExpression(child, relation: relation, permitsColumns: permitsColumns, immutable: immutable) }
    }
}
