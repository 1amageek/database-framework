public enum ContainerSchemaError: Error, Sendable, Equatable {
    case entityNotFound(String)
    case entitySchemaMismatch(String)
}
