import DatabaseKit

extension SQLTextWriter {
    mutating func ddl(_ value: SQLDDLStatement) throws(SQLSerializationError) {
        switch value {
        case .createSchema(let name, let conditional):
            try append("CREATE SCHEMA "); try ddlConditional(conditional, negated: true); try identifier(name)
        case .renameSchema(let name, let newName):
            try append("ALTER SCHEMA "); try identifier(name); try append(" RENAME TO "); try identifier(newName)
        case .dropSchema(let names, let conditional, let behavior):
            try append("DROP SCHEMA "); try ddlConditional(conditional); try identifiers(names); try ddlBehavior(behavior)
        case .createTable(let value):
            try append(value.temporary ? "CREATE TEMPORARY TABLE " : "CREATE TABLE ")
            try ddlConditional(value.ifNotExists, negated: true); try table(value.name)
            switch value.source {
            case .columns(let columns, let constraints):
                try append(" (")
                try list(columns) { (writer: inout SQLTextWriter, value: SQLColumnDefinition) throws(SQLSerializationError) -> Void in try writer.ddlColumn(value) }
                if !columns.isEmpty && !constraints.isEmpty { try append(", ") }
                try list(constraints) { (writer: inout SQLTextWriter, value: SQLTableConstraint) throws(SQLSerializationError) -> Void in try writer.ddlConstraint(value, column: nil) }
                try append(")")
            case .query(let query, let columns, let withData):
                try ddlColumnNames(columns); try append(" AS "); try select(query); try ddlData(withData)
            }
        case .alterTable(let value):
            try append("ALTER TABLE "); try ddlConditional(value.ifExists); try table(value.name); try append(" ")
            try list(value.actions) { (writer: inout SQLTextWriter, action: SQLAlterTableAction) throws(SQLSerializationError) -> Void in try writer.ddlAlterAction(action) }
        case .dropTable(let names, let conditional, let behavior): try ddlDrop("TABLE", names, conditional, behavior)
        case .createIndex(let value):
            try append(value.unique ? "CREATE UNIQUE INDEX " : "CREATE INDEX ")
            try ddlConditional(value.ifNotExists, negated: true); try table(value.name); try append(" ON "); try table(value.table)
            try append(" (")
            try list(value.keys) { (writer: inout SQLTextWriter, key: SortKey) throws(SQLSerializationError) -> Void in
                try writer.append("("); try writer.expression(key.expression); try writer.append(")")
                try writer.append(key.direction == .ascending ? " ASC" : " DESC")
                if let nulls = key.nulls { try writer.append(nulls == .first ? " NULLS FIRST" : " NULLS LAST") }
            }
            try append(")")
            if value.nulls == .notDistinct { try append(" NULLS NOT DISTINCT") }
            if let predicate = value.predicate { try append(" WHERE "); try expression(predicate) }
        case .dropIndex(let names, let conditional, let behavior): try ddlDrop("INDEX", names, conditional, behavior)
        case .createView(let value):
            try append(value.orReplace ? "CREATE OR REPLACE VIEW " : "CREATE VIEW ")
            try table(value.name); try ddlColumnNames(value.columns); try append(" AS "); try select(value.query)
        case .dropView(let names, let conditional, let behavior): try ddlDrop("VIEW", names, conditional, behavior)
        case .createMaterializedView(let value):
            try append("CREATE MATERIALIZED VIEW "); try ddlConditional(value.ifNotExists, negated: true)
            try table(value.name); try ddlColumnNames(value.columns); try append(" AS "); try select(value.query); try ddlData(value.withData)
        case .refreshMaterializedView(let name, let withData):
            try append("REFRESH MATERIALIZED VIEW "); try table(name); try ddlData(withData)
        case .dropMaterializedView(let names, let conditional, let behavior): try ddlDrop("MATERIALIZED VIEW", names, conditional, behavior)
        case .createSequence(let value):
            try append("CREATE SEQUENCE "); try ddlConditional(value.ifNotExists, negated: true); try table(value.name)
            try ddlSequenceOptions(value.options); try ddlOwnership(value.ownedBy)
        case .alterSequence(let value):
            try append("ALTER SEQUENCE "); try ddlConditional(value.ifExists); try table(value.name)
            if let name = value.renameTo { try append(" RENAME TO "); try identifier(name) }
            else {
                try ddlSequenceOptions(value.options)
                if let restart = value.restart {
                    try append(" RESTART")
                    if case .value(let value) = restart { try append(" WITH \(value)") }
                }
                try ddlOwnership(value.ownedBy)
            }
        case .dropSequence(let names, let conditional, let behavior): try ddlDrop("SEQUENCE", names, conditional, behavior)
        }
    }

    private mutating func ddlColumnNames(_ names: [String]?) throws(SQLSerializationError) {
        if let names { try append(" ("); try identifiers(names); try append(")") }
    }

    private mutating func ddlConditional(_ value: Bool, negated: Bool = false) throws(SQLSerializationError) {
        if value { try append(negated ? "IF NOT EXISTS " : "IF EXISTS ") }
    }

    private mutating func ddlBehavior(_ value: SQLDropBehavior) throws(SQLSerializationError) {
        try append(value == .cascade ? " CASCADE" : " RESTRICT")
    }

    private mutating func ddlData(_ value: Bool) throws(SQLSerializationError) {
        try append(value ? " WITH DATA" : " WITH NO DATA")
    }

    private mutating func ddlDrop(_ kind: String, _ names: [TableRef], _ conditional: Bool, _ behavior: SQLDropBehavior) throws(SQLSerializationError) {
        try append("DROP \(kind) "); try ddlConditional(conditional)
        try list(names) { (writer: inout SQLTextWriter, name: TableRef) throws(SQLSerializationError) -> Void in try writer.table(name) }
        try ddlBehavior(behavior)
    }

    private mutating func ddlColumn(_ value: SQLColumnDefinition) throws(SQLSerializationError) {
        try enter(); defer { depth -= 1 }
        try identifier(value.name); try append(" ")
        if case .serial = value.policy {
            switch value.type {
            case .smallint: try append("SMALLSERIAL")
            case .integer: try append("SERIAL")
            case .bigint: try append("BIGSERIAL")
            default: throw .invalidStructure("SERIAL requires an integral column")
            }
        } else { try type(value.type) }
        switch value.policy {
        case .input(let expression):
            if let expression { try append(" DEFAULT "); try self.expression(expression) }
        case .generated(let expression, let stored):
            try append(" GENERATED ALWAYS AS ("); try self.expression(expression); try append(stored ? ") STORED" : ") VIRTUAL")
        case .identity(let generation, let options):
            try append(generation == .always ? " GENERATED ALWAYS AS IDENTITY" : " GENERATED BY DEFAULT AS IDENTITY")
            if options != SQLSequenceOptions() { try append(" ("); try ddlSequenceOptions(options, leadingSpace: false); try append(")") }
        case .serial: break
        }
        try list(value.constraints, separator: "") { (writer: inout SQLTextWriter, constraint: SQLTableConstraint) throws(SQLSerializationError) -> Void in
            try writer.append(" "); try writer.ddlConstraint(constraint, column: value.name)
        }
    }

    private mutating func ddlConstraint(_ value: SQLTableConstraint, column: String?) throws(SQLSerializationError) {
        try enter(); defer { depth -= 1 }
        if let name = value.name { try append("CONSTRAINT "); try identifier(name); try append(" ") }
        switch value.kind {
        case .primaryKey(let columns):
            try append("PRIMARY KEY")
            if column == nil { try append(" ("); try identifiers(columns); try append(")") }
        case .unique(let columns, let nulls):
            try append("UNIQUE")
            if nulls == .notDistinct { try append(" NULLS NOT DISTINCT") }
            if column == nil { try append(" ("); try identifiers(columns); try append(")") }
        case .notNull: try append("NOT NULL")
        case .check(let value): try append("CHECK ("); try expression(value); try append(")")
        case .foreignKey(let value):
            if column == nil { try append("FOREIGN KEY ("); try identifiers(value.columns); try append(") ") }
            try append("REFERENCES "); try table(value.referencedTable)
            try ddlColumnNames(value.referencedColumns)
            try append(value.match == .full ? " MATCH FULL" : " MATCH SIMPLE")
            try append(" ON UPDATE "); try ddlReferenceAction(value.onUpdate)
            try append(" ON DELETE "); try ddlReferenceAction(value.onDelete)
            try ddlColumnNames(value.onDeleteColumns)
        }
        switch value.timing {
        case .immediate: break
        case .deferrable(let deferred): try append(deferred ? " DEFERRABLE INITIALLY DEFERRED" : " DEFERRABLE INITIALLY IMMEDIATE")
        }
    }

    private mutating func ddlReferenceAction(_ value: SQLReferentialAction) throws(SQLSerializationError) {
        switch value {
        case .noAction: try append("NO ACTION")
        case .restrict: try append("RESTRICT")
        case .cascade: try append("CASCADE")
        case .setNull: try append("SET NULL")
        case .setDefault: try append("SET DEFAULT")
        }
    }

    private mutating func ddlAlterAction(_ value: SQLAlterTableAction) throws(SQLSerializationError) {
        try enter(); defer { depth -= 1 }
        switch value {
        case .addColumn(let column, let conditional):
            try append("ADD COLUMN "); try ddlConditional(conditional, negated: true); try ddlColumn(column)
        case .dropColumn(let name, let conditional, let behavior):
            try append("DROP COLUMN "); try ddlConditional(conditional); try identifier(name); try ddlBehavior(behavior)
        case .renameColumn(let name, let newName):
            try append("RENAME COLUMN "); try identifier(name); try append(" TO "); try identifier(newName)
        case .alterColumnType(let name, let type, let using):
            try append("ALTER COLUMN "); try identifier(name); try append(" TYPE "); try self.type(type)
            if let using { try append(" USING "); try expression(using) }
        case .setDefault(let name, let value):
            try append("ALTER COLUMN "); try identifier(name); try append(" SET DEFAULT "); try expression(value)
        case .dropDefault(let name): try append("ALTER COLUMN "); try identifier(name); try append(" DROP DEFAULT")
        case .setNotNull(let name): try append("ALTER COLUMN "); try identifier(name); try append(" SET NOT NULL")
        case .dropNotNull(let name): try append("ALTER COLUMN "); try identifier(name); try append(" DROP NOT NULL")
        case .addConstraint(let value): try append("ADD "); try ddlConstraint(value, column: nil)
        case .dropConstraint(let name, let conditional, let behavior):
            try append("DROP CONSTRAINT "); try ddlConditional(conditional); try identifier(name); try ddlBehavior(behavior)
        case .renameTable(let name): try append("RENAME TO "); try identifier(name)
        }
    }

    private mutating func ddlSequenceOptions(_ value: SQLSequenceOptions, leadingSpace: Bool = true) throws(SQLSerializationError) {
        var separator = leadingSpace ? " " : ""
        if let type = value.type { try append(separator + "AS "); try self.type(type); separator = " " }
        if let start = value.start { try append(separator + "START WITH \(start)"); separator = " " }
        if let increment = value.increment { try append(separator + "INCREMENT BY \(increment)"); separator = " " }
        if let minimum = value.minimum {
            switch minimum {
            case .none: try append(separator + "NO MINVALUE")
            case .value(let minimum): try append(separator + "MINVALUE \(minimum)")
            }
            separator = " "
        }
        if let maximum = value.maximum {
            switch maximum {
            case .none: try append(separator + "NO MAXVALUE")
            case .value(let maximum): try append(separator + "MAXVALUE \(maximum)")
            }
            separator = " "
        }
        if let cycle = value.cycle { try append(separator + (cycle ? "CYCLE" : "NO CYCLE")) }
    }

    private mutating func ddlOwnership(_ value: SQLSequenceOwnership?) throws(SQLSerializationError) {
        guard let value else { return }
        try append(" OWNED BY ")
        switch value {
        case .none: try append("NONE")
        case .column(let table, let column): try self.table(table); try append("."); try identifier(column)
        }
    }
}
