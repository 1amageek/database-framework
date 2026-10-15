import DatabaseKit

extension SelectQuery {
    package func requiredOrderByColumnNames(entity: Schema.Entity?) throws -> [String]? {
        guard let orderBy else { return nil }
        let items: [ProjectionItem]?
        switch projection {
        case .items(let value), .distinctItems(let value): items = value
        case .all, .allFrom: items = nil
        }
        func outputName(_ item: ProjectionItem, index: Int) -> String {
            if let alias = item.alias { return alias }
            if case .column(let column) = item.expression { return column.column }
            return "column\(index)"
        }
        func conservativeInputs() -> [Expression] {
            if let entity { return entity.allFields.map { .column(ColumnRef(column: $0)) } }
            return items?.map { $0.expression } ?? []
        }
        func orderingInputs(_ expression: Expression) -> [Expression] {
            if case .literal(.int(let position)) = expression {
                guard position > 0, let ordinal = Int(exactly: position) else {
                    return conservativeInputs()
                }
                if let items {
                    guard ordinal <= items.count else { return conservativeInputs() }
                    return [items[ordinal - 1].expression]
                }
                guard let entity else { return conservativeInputs() }
                if case .table(let table) = source {
                    guard ordinal <= entity.allFields.count else { return conservativeInputs() }
                    return [.column(ColumnRef(table: table.effectiveName, column: entity.allFields[ordinal - 1]))]
                }
                // A composite wildcard reads every projected entity field.
                // Preserve that authority until the binding owner selects slots.
                return conservativeInputs()
            }
            if case .column(let column) = expression, column.table == nil, let items {
                let matches = items.enumerated().filter { outputName($0.element, index: $0.offset) == column.column }
                if let first = matches.first {
                    if matches.allSatisfy({ $0.element.expression == first.element.expression }) {
                        return [first.element.expression]
                    }
                    // Ambiguity is a located binding failure. Admission keeps
                    // every candidate's dependencies until that check runs.
                    return matches.map { $0.element.expression }
                }
            }
            return [expression]
        }
        var fields: [String] = []
        for sortKey in orderBy {
            for expression in orderingInputs(sortKey.expression) {
                // Reuse QueryIR's complete expression dependency traversal.
                // The empty source contributes no unrelated projected fields.
                let dependencies = SelectQuery(projection: .items([ProjectionItem(expression)]),
                    source: .values([[]], columnNames: [])).referencedColumns
                fields.append(contentsOf: dependencies.map { $0.column }.sorted())
            }
        }
        return fields
    }
}
