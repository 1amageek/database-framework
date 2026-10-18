/// Typed failures from logical SQL object definition and dependency resolution.
enum SQLDDLExecutionError: Error, Sendable, Equatable {
    case missingObject(String)
    case duplicateObject(String)
    case dependentObject(String)
    case invalidDefinition(String)
    case protectedObject(String)
}
