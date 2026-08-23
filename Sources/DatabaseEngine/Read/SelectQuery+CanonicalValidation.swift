import DatabaseKit

extension SelectQuery {
    package func requiredOrderByColumnNames() throws -> [String]? {
        try orderBy?.requiredColumnNames()
    }
}

extension SPARQLSolutionModifiers {
    package func requiredOrderByColumnNames() throws -> [String]? {
        guard !orderBy.isEmpty else { return nil }
        return try orderBy.requiredColumnNames()
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
