import DatabaseKit

extension SelectQuery {
    package func requiredOrderByColumnNames(entity: Schema.Entity?, schema: Schema,
        sqlResolution: SQLReadResolution? = nil) throws -> [String]? {
        guard let orderBy else { return nil }
        let items: [(expression: Expression, alias: String?)]?
        if let sqlResolution {
            items = try DatabaseFieldReadAuthorizationPlan.orderingProjection(
                query: self, schema: schema, sqlResolution: sqlResolution)
        } else if projection.contains(where: { if case .wildcard = $0 { return true }; return false }) {
            if let entity {
                var expanded: [(expression: Expression, alias: String?)] = []
                for item in projection {
                    switch item {
                    case .expression(let expression, let alias): expanded.append((expression, alias))
                    case .wildcard(let qualifier):
                        expanded.append(contentsOf: entity.allFields.map {
                            (Expression.column(ColumnRef(table: qualifier, column: $0)), nil)
                        })
                    }
                }
                items = expanded
            } else { items = nil }
        } else {
            items = projection.compactMap { item in
                guard case .expression(let expression, let alias) = item else { return nil }
                return (expression, alias)
            }
        }
        func outputName(_ item: (expression: Expression, alias: String?), index: Int) -> String {
            if let alias = item.alias { return alias }
            return CanonicalQueryTypeBinder.outputName(item.expression, index: index)
        }
        func conservativeInputs() -> [Expression] {
            if sqlResolution != nil { return items?.map { $0.expression } ?? [] }
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
                let matches = items.enumerated().filter { CanonicalSourceName.equal(outputName($0.element, index: $0.offset), column.column) }
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
                let dependencies = try DatabaseFieldReadAuthorizationPlan.orderingFields(
                    query: self, inputs: [expression], schema: schema, sqlResolution: sqlResolution)
                if let entity { fields.append(contentsOf: (dependencies[entity.name] ?? []).sorted()) }
                else { fields.append(contentsOf: dependencies.values.flatMap { $0 }.sorted()) }
            }
        }
        return fields
    }
}
