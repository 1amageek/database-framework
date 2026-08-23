import DatabaseKit

extension SelectQuery {
    package func requiredOrderByColumnNames() throws -> [String]? {
        try orderBy?.requiredColumnNames()
    }

    package func orderByVariableNamesForAuthorization() -> [String]? {
        orderBy?.variableNamesForAuthorization()
    }
}

extension SPARQLSolutionModifiers {
    package func orderByVariableNamesForAuthorization() -> [String]? {
        orderBy.variableNamesForAuthorization()
    }
}

private extension Collection where Element == SortKey {
    func requiredColumnNames() throws -> [String] {
        var fields: [String] = []
        fields.reserveCapacity(count)
        for sortKey in self {
            guard case .column(let column) = sortKey.expression else {
                throw CanonicalReadError.unsupportedExpression
            }
            fields.append(column.column)
        }
        return fields
    }
}

private extension Collection where Element == SortKey {
    func variableNamesForAuthorization() -> [String]? {
        guard !isEmpty else { return nil }
        var names: [String] = []
        var seen: Set<String> = []
        for sortKey in self {
            for name in sortKey.expression.referencedVariables.sorted()
            where seen.insert(name).inserted {
                names.append(name)
            }
        }
        return names
    }
}
