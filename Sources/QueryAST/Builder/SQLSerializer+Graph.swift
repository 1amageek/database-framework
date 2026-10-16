import DatabaseKit

extension SQLTextWriter {
    mutating func match(_ value: MatchPattern) throws(SQLSerializationError) {
        try enter(); defer { depth -= 1 }
        guard !value.paths.isEmpty else { throw .invalidStructure("Empty MATCH pattern") }
        try append("MATCH ")
        try list(value.paths) { (writer: inout SQLTextWriter, value: PathPattern) throws(SQLSerializationError) -> Void in try writer.path(value) }
        if let condition = value.where { try append(" WHERE "); try expression(condition) }
    }

    mutating func path(_ value: PathPattern) throws(SQLSerializationError) {
        try enter(); defer { depth -= 1 }
        guard !value.elements.isEmpty else { throw .invalidStructure("Empty path pattern") }
        if let name = value.pathVariable { try identifier(name); try append(" = ") }
        if let mode = value.mode { try self.mode(mode); try append(" ") }
        try list(value.elements, separator: "") { (writer: inout SQLTextWriter, value: PathElement) throws(SQLSerializationError) -> Void in try writer.pathElement(value) }
    }

    mutating func pathElement(_ value: PathElement) throws(SQLSerializationError) {
        try enter(); defer { depth -= 1 }
        switch value {
        case .node(let value): try node(value)
        case .edge(let value): try edge(value)
        case .quantified(let value, let quantifier): try append("("); try path(value); try append(")"); try self.quantifier(quantifier)
        case .alternation(let values):
            guard !values.isEmpty else { throw .invalidStructure("Empty path alternation") }
            try append("("); try list(values, separator: " | ") { (writer: inout SQLTextWriter, value: PathPattern) throws(SQLSerializationError) -> Void in try writer.path(value) }; try append(")")
        }
    }

    mutating func node(_ value: NodePattern) throws(SQLSerializationError) {
        try enter(); defer { depth -= 1 }
        try append("("); try patternContents(variable: value.variable, labels: value.labels, properties: value.properties); try append(")")
    }

    mutating func edge(_ value: EdgePattern) throws(SQLSerializationError) {
        try enter(); defer { depth -= 1 }
        try append(value.direction == .incoming || value.direction == .any ? "<-[" : "-[")
        try patternContents(variable: value.variable, labels: value.labels, properties: value.properties)
        try append(value.direction == .outgoing || value.direction == .any ? "]->" : "]-")
    }

    private mutating func patternContents(variable: String?, labels: [String]?, properties: [PropertyBinding]?) throws(SQLSerializationError) {
        if let variable { try identifier(variable) }
        if let labels, !labels.isEmpty {
            try append(":"); try list(labels, separator: ":") { (writer: inout SQLTextWriter, value: String) throws(SQLSerializationError) -> Void in try writer.identifier(value) }
        }
        if let properties, !properties.isEmpty {
            try append(" {")
            try list(properties) { (writer: inout SQLTextWriter, property: PropertyBinding) throws(SQLSerializationError) -> Void in try writer.identifier(property.key); try writer.append(": "); try writer.expression(property.value) }
            try append("}")
        }
    }

    mutating func quantifier(_ value: PathQuantifier) throws(SQLSerializationError) {
        switch value {
        case .exactly(let count):
            guard count >= 0 else { throw .invalidStructure("Negative path quantifier") }
            try append("{\(count)}")
        case .range(let minimum, let maximum):
            guard minimum != nil || maximum != nil, minimum == nil || minimum! >= 0,
                  maximum == nil || maximum! >= 0, minimum == nil || maximum == nil || minimum! <= maximum! else {
                throw .invalidStructure("Invalid path quantifier range")
            }
            try append("{")
            if let minimum { try append(String(minimum)) }
            try append(",")
            if let maximum { try append(String(maximum)) }
            try append("}")
        case .zeroOrMore: try append("*")
        case .oneOrMore: try append("+")
        case .zeroOrOne: try append("?")
        }
    }

    mutating func mode(_ value: PathMode) throws(SQLSerializationError) {
        switch value {
        case .walk: try append("WALK")
        case .trail: try append("TRAIL")
        case .acyclic: try append("ACYCLIC")
        case .simple: try append("SIMPLE")
        case .anyShortest: try append("ANY SHORTEST")
        case .allShortest: try append("ALL SHORTEST")
        case .shortestK(let count):
            guard count > 0 else { throw .invalidStructure("Nonpositive shortest path count") }
            try append("SHORTEST \(count)")
        }
    }

    mutating func graphTable(_ value: GraphTableSource) throws(SQLSerializationError) {
        try enter(); defer { depth -= 1 }
        try append("GRAPH_TABLE("); try identifier(value.graphName); try append(", "); try match(value.matchPattern)
        if let columns = value.columns {
            guard !columns.isEmpty else { throw .invalidStructure("Empty GRAPH_TABLE COLUMNS") }
            try append(" COLUMNS (")
            try list(columns) { (writer: inout SQLTextWriter, column: GraphTableColumn) throws(SQLSerializationError) -> Void in try writer.expression(column.expression); try writer.append(" AS "); try writer.identifier(column.alias) }
            try append(")")
        }
        try append(")")
    }

    mutating func createGraph(_ value: CreateGraphStatement) throws(SQLSerializationError) {
        try enter(); defer { depth -= 1 }
        guard !value.vertexTables.isEmpty else { throw .invalidStructure("Property graph requires vertex tables") }
        try append("CREATE PROPERTY GRAPH ")
        if value.ifNotExists { try append("IF NOT EXISTS ") }
        try identifier(value.graphName); try append(" VERTEX TABLES (")
        try list(value.vertexTables) { (writer: inout SQLTextWriter, value: VertexTableDefinition) throws(SQLSerializationError) -> Void in try writer.vertex(value) }; try append(")")
        if !value.edgeTables.isEmpty { try append(" EDGE TABLES ("); try list(value.edgeTables) { (writer: inout SQLTextWriter, value: EdgeTableDefinition) throws(SQLSerializationError) -> Void in try writer.edgeDefinition(value) }; try append(")") }
    }

    mutating func vertex(_ value: VertexTableDefinition) throws(SQLSerializationError) {
        try enter(); defer { depth -= 1 }
        try identifier(value.tableName)
        if let alias = value.alias { try append(" AS "); try identifier(alias) }
        try append(" KEY ("); try identifiers(value.keyColumns); try append(")")
        if let label = value.labelExpression { try append(" LABEL "); try self.label(label) }
        if let properties = value.propertiesSpec { try append(" "); try self.properties(properties) }
    }

    mutating func edgeDefinition(_ value: EdgeTableDefinition) throws(SQLSerializationError) {
        try enter(); defer { depth -= 1 }
        try identifier(value.tableName)
        if let alias = value.alias { try append(" AS "); try identifier(alias) }
        try append(" KEY ("); try identifiers(value.keyColumns); try append(") SOURCE "); try reference(value.sourceVertex)
        try append(" DESTINATION "); try reference(value.destinationVertex)
        if let label = value.labelExpression { try append(" LABEL "); try self.label(label) }
        if let properties = value.propertiesSpec { try append(" "); try self.properties(properties) }
    }

    mutating func reference(_ value: VertexReference) throws(SQLSerializationError) {
        guard !value.keyColumns.isEmpty else { throw .invalidStructure("Empty vertex key mapping") }
        try append("KEY ("); try identifiers(value.keyColumns.map { $0.source }); try append(") REFERENCES "); try identifier(value.tableName)
        try append(" ("); try identifiers(value.keyColumns.map { $0.target }); try append(")")
    }

    mutating func label(_ value: LabelExpression) throws(SQLSerializationError) {
        try enter(); defer { depth -= 1 }
        switch value {
        case .single(let value): try identifier(value)
        case .column(let value): try append("("); try identifier(value); try append(")")
        case .or(let values):
            guard !values.isEmpty else { throw .invalidStructure("Empty graph label alternative") }
            try append("("); try list(values, separator: " | ") { (writer: inout SQLTextWriter, value: LabelExpression) throws(SQLSerializationError) -> Void in try writer.label(value) }; try append(")")
        case .and(let values):
            guard !values.isEmpty else { throw .invalidStructure("Empty graph label conjunction") }
            try append("("); try list(values, separator: " & ") { (writer: inout SQLTextWriter, value: LabelExpression) throws(SQLSerializationError) -> Void in try writer.label(value) }; try append(")")
        }
    }

    mutating func properties(_ value: PropertiesSpec) throws(SQLSerializationError) {
        switch value {
        case .all: try append("PROPERTIES ALL COLUMNS")
        case .none: try append("NO PROPERTIES")
        case .columns(let columns): try append("PROPERTIES ("); try identifiers(columns); try append(")")
        case .allExcept(let columns): try append("PROPERTIES ALL COLUMNS EXCEPT ("); try identifiers(columns); try append(")")
        }
    }
}
