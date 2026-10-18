import DatabaseKit
import QueryAST

extension SQLInformationSchemaRelation {
    /// Emits one admitted fixed-width row at a time; aliases retain its lease.
    func forEachRow(resolution: SQLReadResolution, emit: (SQLInformationSchemaRow) throws -> Void) throws {
        let catalog = resolution.snapshot.catalog
        let visibility = SQLInformationSchemaVisibility(resolution: resolution)
        var rowText = SQLInformationSchemaRow.TextBuffer()
        func text(_ value: String?) -> QueryValue { value.map { .primitive(.string($0)) } ?? .primitive(.null) }
        func number(_ value: Int?) throws -> QueryValue {
            guard let value else { return .primitive(.null) }
            guard let value = Int32(exactly: value) else { throw DatabaseExpressionEvaluationError.numericOverflow }
            return .primitive(.int32(value))
        }
        func schema(_ id: String) throws -> String {
            guard let namespace = catalog.namespaces.first(where: { SQLCatalogName($0.id) == SQLCatalogName(id) }) else {
                throw SQLCatalogBindingError.missingNamespace(id)
            }
            return namespace.name
        }
        func row(_ values: [String: QueryValue]) throws {
            try resolution.workMeter.consume(at: .bindingCandidate)
            let admitted = try rowText.finish(columnNames: columnNames, values: values, workMeter: resolution.workMeter)
            try emit(admitted)
        }
        func base(_ namespaceID: String, _ name: String) throws -> [String: QueryValue] {
            ["table_schema": text(try schema(namespaceID)), "table_name": text(name)]
        }
        func emitColumn(_ name: String, ordinal: Int, type: QueryColumnType, nullable: Bool,
            identity: String, relationID: String? = nil, base: [String: QueryValue], policy: SQLColumnPolicy?, updatable: Bool) throws {
            var cells = base
            let attributes = SQLInformationSchemaType(type)
            cells["column_name"] = text(name); cells["ordinal_position"] = try number(ordinal)
            cells["is_nullable"] = text(nullable ? "YES" : "NO"); cells["data_type"] = text(attributes.name)
            cells["character_maximum_length"] = try number(attributes.length)
            cells["numeric_precision"] = try number(attributes.precision); cells["numeric_precision_radix"] = try number(attributes.radix)
            cells["numeric_scale"] = try number(attributes.scale); cells["datetime_precision"] = try number(attributes.datetimePrecision)
            cells["udt_name"] = text(attributes.userType); cells["udt_schema"] = text(attributes.userSchema)
            cells["dtd_identifier"] = text(identity); cells["is_self_referencing"] = text("NO")
            cells["is_identity"] = text("NO"); cells["is_generated"] = text("NEVER")
            cells["is_updatable"] = text(updatable ? "YES" : "NO")
            switch policy {
            case .input(let expression):
                var visibleDefault = true
                if let relationID, let bindings = resolution.snapshot.sequenceDefaultBindings(relationID: relationID, columnID: identity) {
                    for id in bindings.sequenceIDsByLiteral.values {
                        guard let sequence = catalog.sequences.first(where: { SQLCatalogName($0.id) == id }) else {
                            throw SQLSequenceExecutionError.unknownSequence(id.value)
                        }
                        if try !visibility.sequence(sequence) { visibleDefault = false; break }
                    }
                }
                if let expression, visibleDefault {
                    cells["column_default"] = text(try rowText.serialize(workMeter: resolution.workMeter) {
                        (serializer: SQLSerializer) throws(SQLSerializationError) -> String in try serializer.serialize(expression)
                    })
                }
            case .generated(let expression):
                cells["is_generated"] = text("ALWAYS")
                cells["generation_expression"] = text(try rowText.serialize(workMeter: resolution.workMeter) {
                    (serializer: SQLSerializer) throws(SQLSerializationError) -> String in try serializer.serialize(expression)
                })
            case .identity(let identity):
                guard let sequence = catalog.sequences.first(where: { SQLCatalogName($0.id) == SQLCatalogName(identity.sequenceID) }) else {
                    throw SQLSequenceExecutionError.unknownSequence(identity.sequenceID)
                }
                if try !visibility.sequence(sequence) { return }
                cells["is_identity"] = text("YES")
                cells["identity_generation"] = text(identity.generation == .always ? "ALWAYS" : "BY DEFAULT")
                cells["identity_start"] = text(String(sequence.start)); cells["identity_increment"] = text(String(sequence.increment))
                cells["identity_maximum"] = text(String(sequence.maximum)); cells["identity_minimum"] = text(String(sequence.minimum))
                cells["identity_cycle"] = text(sequence.cycle ? "YES" : "NO")
            case nil: break
            }
            try row(cells)
        }
        if self == .sequences {
            for sequence in catalog.sequences {
                try resolution.workMeter.consume(at: .bindingCandidate)
                guard try visibility.sequence(sequence) else { continue }
                let type = SQLInformationSchemaType(sequence.declaredType)
                try row(["sequence_schema": text(try schema(sequence.namespaceID)), "sequence_name": text(sequence.name),
                    "data_type": text(type.name), "numeric_precision": try number(type.precision), "numeric_precision_radix": try number(type.radix),
                    "numeric_scale": try number(type.scale), "start_value": text(String(sequence.start)),
                    "minimum_value": text(String(sequence.minimum)), "maximum_value": text(String(sequence.maximum)),
                    "increment": text(String(sequence.increment)), "cycle_option": text(sequence.cycle ? "YES" : "NO")])
            }
            return
        }
        for declaration in catalog.relations where self != .views {
            try resolution.workMeter.consume(at: .bindingCandidate)
            guard !catalog.materializedViews.contains(where: { SQLCatalogName($0.backingRelationID) == SQLCatalogName(declaration.id) }),
                  let relation = resolution.snapshot.resolvedRelation(id: declaration.id), try visibility.relation(relation) else { continue }
            let names = try base(declaration.namespaceID, declaration.name)
            if self == .tables {
                var cells = names; cells["table_type"] = text("BASE TABLE")
                cells["is_insertable_into"] = text("YES"); cells["is_typed"] = text("NO"); try row(cells)
            } else if self == .columns {
                for (index, column) in relation.columns.enumerated() {
                    guard try visibility.column(column, in: relation) else { continue }
                    let notNull = declaration.primaryKeyColumnIDs?.contains(where: { SQLCatalogName($0) == SQLCatalogName(column.declaration.id) }) == true
                        || declaration.constraints.contains { if case .notNull(let id) = $0.kind { return SQLCatalogName(id) == SQLCatalogName(column.declaration.id) }; return false }
                    try emitColumn(column.declaration.name, ordinal: index + 1, type: column.type,
                        nullable: column.isNullable && !notNull, identity: column.declaration.id, relationID: declaration.id,
                        base: names, policy: column.declaration.policy, updatable: true)
                }
            } else if self == .tableConstraints || self == .keyColumnUsage || self == .referentialConstraints {
                func constraint(_ attributes: SQLConstraintAttributes, kind: String, ids: [String],
                    nulls: SQLNullUniqueness? = nil, foreign: SQLForeignKey? = nil) throws {
                    guard try visibility.columns(ids, in: relation) else { return }
                    var cells = names
                    cells["constraint_schema"] = text(try schema(declaration.namespaceID)); cells["constraint_name"] = text(attributes.name)
                    var target: SQLCatalogResolvedRelation?
                    var targetIDs: [String] = []
                    var uniqueIDs: [String] = []
                    if let foreign {
                        guard let found = resolution.snapshot.resolvedRelation(id: foreign.referencedRelationID) else {
                            throw SQLViewExecutionError.missingDependency(foreign.referencedRelationID)
                        }
                        target = found; targetIDs = foreign.referencedColumnIDs ?? found.declaration.primaryKeyColumnIDs ?? []
                        guard try visibility.relation(found), try visibility.columns(targetIDs, in: found) else { return }
                        let references = Set(targetIDs.map(SQLCatalogName.init))
                        if let primary = found.declaration.primaryKeyColumnIDs, Set(primary.map(SQLCatalogName.init)) == references {
                            uniqueIDs = primary
                        } else if let unique = found.declaration.constraints.first(where: {
                            if case .unique(let ids, _) = $0.kind { return Set(ids.map(SQLCatalogName.init)) == references }; return false
                        }), case .unique(let ids, _) = unique.kind { uniqueIDs = ids }
                        else if let indexID = foreign.referencedUniqueIndexID,
                            let index = catalog.indexes.first(where: { SQLCatalogName($0.id) == SQLCatalogName(indexID) }),
                            let ids = index.directColumnIDs { uniqueIDs = ids }
                        else { uniqueIDs = targetIDs }
                    }
                    switch self {
                    case .tableConstraints:
                        cells["constraint_type"] = text(kind); cells["enforced"] = text("YES")
                        switch attributes.timing {
                        case .immediate: cells["is_deferrable"] = text("NO"); cells["initially_deferred"] = text("NO")
                        case .deferrable(let deferred): cells["is_deferrable"] = text("YES"); cells["initially_deferred"] = text(deferred ? "YES" : "NO")
                        }
                        cells["nulls_distinct"] = text(nulls.map { $0 == .distinct ? "YES" : "NO" })
                        try row(cells)
                    case .keyColumnUsage:
                        guard ["PRIMARY KEY", "UNIQUE", "FOREIGN KEY"].contains(kind) else { return }
                        for (index, id) in ids.enumerated() {
                            guard let column = relation.columns.first(where: { SQLCatalogName($0.declaration.id) == SQLCatalogName(id) }) else {
                                throw SQLViewExecutionError.missingColumn(relation: declaration.id, column: id)
                            }
                            cells["column_name"] = text(column.declaration.name); cells["ordinal_position"] = try number(index + 1)
                            if foreign != nil, targetIDs.indices.contains(index),
                               let position = uniqueIDs.firstIndex(where: { SQLCatalogName($0) == SQLCatalogName(targetIDs[index]) }) {
                                cells["position_in_unique_constraint"] = try number(position + 1)
                            } else { cells["position_in_unique_constraint"] = .primitive(.null) }
                            try row(cells)
                        }
                    case .referentialConstraints:
                        guard let foreign, let target else { return }
                        let targetDeclaration = target.declaration
                        let keyNames = Set(targetIDs.map(SQLCatalogName.init))
                        let targetName: String?
                        if let ids = targetDeclaration.primaryKeyColumnIDs, Set(ids.map(SQLCatalogName.init)) == keyNames {
                            targetName = targetDeclaration.primaryKeyConstraint?.name
                        } else {
                            targetName = targetDeclaration.constraints.first { constraint in
                                if case .unique(let ids, _) = constraint.kind { return Set(ids.map(SQLCatalogName.init)) == keyNames }; return false
                            }?.name
                        }
                        cells["unique_constraint_schema"] = text(targetName == nil ? nil : try schema(targetDeclaration.namespaceID))
                        cells["unique_constraint_name"] = text(targetName)
                        cells["match_option"] = text(foreign.match == .full ? "FULL" : "NONE")
                        cells["update_rule"] = text(Self.action(foreign.onUpdate)); cells["delete_rule"] = text(Self.action(foreign.onDelete))
                        try row(cells)
                    default: break
                    }
                }
                if let attributes = declaration.primaryKeyConstraint, let ids = declaration.primaryKeyColumnIDs {
                    try constraint(attributes, kind: "PRIMARY KEY", ids: ids)
                }
                for item in declaration.constraints {
                    switch item.kind {
                    case .unique(let ids, let nulls): try constraint(item.attributes, kind: "UNIQUE", ids: ids, nulls: nulls)
                    case .foreignKey(let foreign): try constraint(item.attributes, kind: "FOREIGN KEY", ids: foreign.columnIDs, foreign: foreign)
                    case .notNull(let id): try constraint(item.attributes, kind: "CHECK", ids: [id])
                    case .check(let expression):
                        if try visibility.expression(expression, in: relation) { try constraint(item.attributes, kind: "CHECK", ids: []) }
                    }
                }
            }
        }
        guard self == .tables || self == .columns || self == .views else { return }
        for view in catalog.views {
            try resolution.workMeter.consume(at: .bindingCandidate)
            guard try visibility.view(view.definition) else { continue }
            var cells = try base(view.namespaceID, view.name)
            switch self {
            case .tables:
                cells["table_type"] = text("VIEW"); cells["is_insertable_into"] = text("NO"); cells["is_typed"] = text("NO"); try row(cells)
            case .columns:
                for (index, column) in view.definition.columns.enumerated() {
                    try emitColumn(column.name, ordinal: index + 1, type: column.type, nullable: column.isNullable,
                        identity: column.id, base: cells, policy: nil, updatable: false)
                }
            case .views:
                cells["view_definition"] = text(try rowText.serialize(workMeter: resolution.workMeter) {
                    (serializer: SQLSerializer) throws(SQLSerializationError) -> String in try serializer.serialize(view.definition.query)
                })
                cells["check_option"] = text("NONE")
                for name in ["is_updatable", "is_insertable_into", "is_trigger_updatable", "is_trigger_deletable", "is_trigger_insertable_into"] { cells[name] = text("NO") }
                try row(cells)
            default: break
            }
        }
    }

    private static func action(_ value: SQLReferentialAction) -> String {
        switch value { case .noAction: return "NO ACTION"; case .restrict: return "RESTRICT"; case .cascade: return "CASCADE"; case .setNull: return "SET NULL"; case .setDefault: return "SET DEFAULT" }
    }
}
