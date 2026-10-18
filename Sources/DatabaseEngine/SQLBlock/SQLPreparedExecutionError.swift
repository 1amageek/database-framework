/// Request-local prepared statement contract failures.
public enum SQLPreparedExecutionError: Error, Sendable, Equatable {
    case duplicateName(String)
    case unknownName(String)
    case arity(name: String, expected: Int, actual: Int)
    case indeterminateParameter(UInt32)
    case namedParameter(String)
    case staleBinding(String)
    case argumentType(position: UInt32)
}
