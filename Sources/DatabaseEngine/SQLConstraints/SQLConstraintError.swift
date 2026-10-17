import DatabaseKit

public enum SQLConstraintError: Error, Sendable, Equatable {
    case invalidDeclaration(relation: String, reason: String)
    case violation(relation: String, constraint: String)
    case unknownConstraint(String)
    case notDeferrable(String)
    case generatedAssignment(relation: String, column: String)
    case identityAssignment(relation: String, column: String)
}
