import DatabaseKit

extension SQLDDLExecutor {
    func renamePolicyColumns(_ relation: SQLRelation, from: String, to: String) throws -> SQLRelation {
        try mapPolicyExpressions(relation) { expression in
            let unqualified = try QueryColumnRenaming.rename(expression,
                from: ColumnRef(column: from), to: ColumnRef(column: to), limits: options.structuralLimits)
            return try QueryColumnRenaming.rename(unqualified,
                from: ColumnRef(table: relation.name, column: from), to: ColumnRef(table: relation.name, column: to), limits: options.structuralLimits)
        }
    }

    func renamePolicyTable(_ relation: SQLRelation, from: String, to: String) throws -> SQLRelation {
        try mapPolicyExpressions(relation) { expression in
            var result = expression
            for column in relation.columns {
                result = try QueryColumnRenaming.rename(result,
                    from: ColumnRef(table: from, column: column.name), to: ColumnRef(table: to, column: column.name), limits: options.structuralLimits)
            }
            return result
        }
    }

    private func mapPolicyExpressions(_ relation: SQLRelation,
        transform: (Expression) throws -> Expression) throws -> SQLRelation {
        let columns = try relation.columns.map { column in
            let policy: SQLColumnPolicy
            switch column.policy {
            case .input(let expression): policy = .input(defaultExpression: try expression.map(transform))
            case .generated(let expression): policy = .generated(try transform(expression))
            case .identity: return column
            }
            return try SQLColumn(id: column.id, name: column.name, backing: column.backing,
                policy: policy, sequenceBindings: column.sequenceBindings)
        }
        let constraints = try relation.constraints.map { constraint in
            guard case .check(let expression) = constraint.kind else { return constraint }
            return try SQLConstraint(attributes: constraint.attributes, kind: .check(transform(expression)))
        }
        return try replacing(relation, columns: columns, constraints: constraints)
    }
}
