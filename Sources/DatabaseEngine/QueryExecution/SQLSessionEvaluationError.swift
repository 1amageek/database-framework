public enum SQLSessionEvaluationError: Error, Sendable, Equatable {
    case missingNamespaceAdmission
    case missingDatabaseIdentity
    case unauthenticatedPrincipal
    case invalidArguments(function: String)
}
