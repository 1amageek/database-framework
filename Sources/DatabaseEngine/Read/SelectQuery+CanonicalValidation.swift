import DatabaseKit

extension SelectQuery {
    package func requiredOrderByColumnNames() throws -> [String]? {
        guard let orderBy else { return nil }
        var fields: [String] = []
        fields.reserveCapacity(orderBy.count)
        for sortKey in orderBy {
            guard case .column(let column) = sortKey.expression else {
                throw CanonicalReadError.unsupportedExpression
            }
            fields.append(column.column)
        }
        return fields
    }
}
