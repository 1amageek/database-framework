/// Failures in durable SQL view binding and materialization.
public enum SQLViewExecutionError: Error, Sendable, Equatable {
    case readOnly(String)
    case notPopulated(String)
    case missingDependency(String)
    case missingColumn(relation: String, column: String)
    case incompatibleColumn(relation: String, column: String)
    case cyclicDependency(String)
    case invalidDefinition(String)
    case incompatibleReplacement(String)
}
