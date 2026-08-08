public enum SchemaDrivenDatabaseApplicationError: Error, Sendable,
    Equatable {
    case compiledContainerDefinition
    case schemaExecutionUnavailable
}
