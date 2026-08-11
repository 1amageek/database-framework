import DatabaseKit

/// A statement admitted through canonical parsing, binding, and validation.
public struct ValidatedDatabaseStatement: Sendable {
    public let statement: QueryStatement
    public let structuralLimits: QueryStructuralLimits

    init(
        statement: QueryStatement,
        structuralLimits: QueryStructuralLimits
    ) {
        self.statement = statement
        self.structuralLimits = structuralLimits
    }
}
