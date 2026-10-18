import DatabaseKit
import DatabaseTypes

extension SQLTextWriter {
    mutating func blockStep(_ step: SQLBlockStep) throws(SQLSerializationError) {
        try enter(); defer { depth -= 1 }
        switch step {
        case .statement(let value): try statement(value)
        case .transaction(let value):
            switch value { case .begin: try append("BEGIN"); case .commit: try append("COMMIT"); case .rollback: try append("ROLLBACK") }
        case .savepoint(let name): try append("SAVEPOINT "); try identifier(name)
        case .rollbackTo(let name): try append("ROLLBACK TO SAVEPOINT "); try identifier(name)
        case .release(let name): try append("RELEASE SAVEPOINT "); try identifier(name)
        case .setConstraints(let value):
            try append("SET CONSTRAINTS ")
            switch value.selection { case .all: try append("ALL"); case .named(let names): try identifiers(names) }
            try append(value.deferred ? " DEFERRED" : " IMMEDIATE")
        case .setting(let command):
            switch command {
            case .set(let name, let value, let scope):
                try append(scope == .local ? "SET LOCAL " : "SET SESSION ")
                try settingName(name); try append(" TO ")
                if name == .searchPath, case .array(let values) = value {
                    try list(values) { (writer: inout SQLTextWriter, item: Expression) throws(SQLSerializationError) -> Void in
                        guard case .literal(.sqlUnknownString(let text)) = item else { throw .invalidStructure("Search path entries must be names") }
                        try writer.identifier(text)
                    }
                } else { try expression(value) }
            case .show(let name):
                try append("SHOW "); if let name { try settingName(name) } else { try append("ALL") }
            case .reset(let name, let scope):
                try append(scope == .local ? "RESET LOCAL " : "RESET SESSION ")
                if let name { try settingName(name) } else { try append("ALL") }
            }
        case .prepare(let value):
            try append("PREPARE "); try identifier(value.name)
            if !value.parameterTypes.isEmpty {
                try append(" (")
                try list(value.parameterTypes) { (writer: inout SQLTextWriter, value: DataType) throws(SQLSerializationError) -> Void in try writer.type(value) }
                try append(")")
            }
            try append(" AS "); try statement(value.statement)
        case .execute(let value):
            try append("EXECUTE "); try identifier(value.name)
            if !value.arguments.isEmpty {
                try append(" (")
                try list(value.arguments) { (writer: inout SQLTextWriter, value: Expression) throws(SQLSerializationError) -> Void in try writer.expression(value) }
                try append(")")
            }
        case .deallocate(let name):
            try append("DEALLOCATE "); if let name { try identifier(name) } else { try append("ALL") }
        case .explain(let value):
            try append("EXPLAIN (ANALYZE "); try append(value.options.analyze ? "TRUE" : "FALSE")
            try append(", VERBOSE "); try append(value.options.verbose ? "TRUE" : "FALSE")
            try append(value.options.format == .text ? ", FORMAT TEXT) " : ", FORMAT JSON) ")
            try statement(value.statement)
        case .copy(let value):
            try append("COPY ")
            switch value.target {
            case .table(let name, let columns):
                try table(name)
                if !columns.isEmpty { try append(" ("); try identifiers(columns); try append(")") }
            case .query(let query): try append("("); try select(query); try append(")")
            }
            try append(value.direction == .from ? " FROM STDIN" : " TO STDOUT")
            let options = value.options
            try append(options.format == .text ? " WITH (FORMAT TEXT, DELIMITER " : " WITH (FORMAT CSV, DELIMITER ")
            try string(options.delimiter); try append(", NULL "); try string(options.null)
            if options.format == .csv {
                try append(options.header ? ", HEADER TRUE, QUOTE " : ", HEADER FALSE, QUOTE ")
                try string(options.quote); try append(", ESCAPE "); try string(options.escape)
            }
            try append(")")
            if let data = value.data {
                // Inline syntax requires complete lines and reserves an exact terminator line.
                guard data.isEmpty || data.last == 10 else { throw .invalidStructure("Inline COPY data must end with a newline") }
                var line = data.startIndex
                for index in data.indices where data[index] == 10 {
                    let end = index > line && data[index - 1] == 13 ? index - 1 : index
                    guard end - line != 2 || data[line] != 92 || data[line + 1] != 46 else {
                        throw .invalidStructure("COPY data contains the inline terminator")
                    }
                    line = index + 1
                }
                guard let text = String(validating: data, as: UTF8.self) else { throw .invalidStructure("Inline COPY data is not UTF-8") }
                try append(";\n"); try append(text); try append("\\.\n")
                return
            }
        case .sequence:
            throw .invalidStructure("Catalog sequence identities have no lossless SQL text representation")
        }
        try append(";\n")
    }

    private mutating func settingName(_ name: SQLSettingName) throws(SQLSerializationError) {
        switch name { case .searchPath: try append("search_path"); case .timeZone: try append("TimeZone"); case .statementTimeout: try append("statement_timeout") }
    }
}
