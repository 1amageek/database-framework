import DatabaseKit

public enum SQLConstraintError: Error, Sendable, Equatable {
    case invalidDeclaration(relation: String, reason: String)
    case violation(kind: SQLConstraintViolationKind, relation: String, constraint: String?,
        schema: String? = nil, table: String? = nil, column: String? = nil)
    case unknownConstraint(String)
    case notDeferrable(String)
    case generatedAssignment(relation: String, column: String)
    case identityAssignment(relation: String, column: String)
    /// RESTRICT truncation of a relation that another relation references.
    case truncateReferenced(relation: String, constraint: String?, schema: String? = nil, table: String? = nil)
}


extension SQLConstraintError {
    package static func violation(kind: SQLConstraintViolationKind, relation: SQLRelation,
        constraint: String?, column: String? = nil, catalog: SQLCatalog) -> Self {
        .violation(kind: kind, relation: relation.id, constraint: constraint,
            schema: catalog.namespaces.first { SQLConstraintCompiler.same($0.id, relation.namespaceID) }?.name,
            table: relation.name, column: column)
    }

    package static func truncateReferenced(by relation: SQLRelation, constraint: String?,
        catalog: SQLCatalog) -> Self {
        .truncateReferenced(relation: relation.id, constraint: constraint,
            schema: catalog.namespaces.first { SQLConstraintCompiler.same($0.id, relation.namespaceID) }?.name,
            table: relation.name)
    }
}
