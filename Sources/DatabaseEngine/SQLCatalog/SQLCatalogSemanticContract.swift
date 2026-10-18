import DatabaseKit

/// Compares column-bound meaning independently of a relation's logical names.
enum SQLCatalogSemanticContract {
    static func expression(_ expression: Expression, relation: SQLRelation) throws -> Expression {
        var normalized = expression
        for column in relation.columns {
            let stable = ColumnRef("\u{0}sql-column:" + column.id)
            normalized = try QueryColumnRenaming.rename(normalized,
                from: ColumnRef(column.name), to: stable)
            normalized = try QueryColumnRenaming.rename(normalized,
                from: ColumnRef(table: relation.name, column: column.name), to: stable)
        }
        return normalized
    }

    static func policy(_ policy: SQLColumnPolicy, relation: SQLRelation) throws -> SQLColumnPolicy {
        switch policy {
        case .generated(let expression): return try .generated(Self.expression(expression, relation: relation))
        case .input, .identity: return policy
        }
    }

    static func constraint(_ kind: SQLConstraintKind, relation: SQLRelation) throws -> SQLConstraintKind {
        if case .check(let expression) = kind {
            return try .check(Self.expression(expression, relation: relation))
        }
        return kind
    }

    static func constraints(_ relation: SQLRelation) throws -> [SQLConstraint] {
        try relation.constraints.map {
            try SQLConstraint(attributes: $0.attributes, kind: constraint($0.kind, relation: relation))
        }
    }
}
