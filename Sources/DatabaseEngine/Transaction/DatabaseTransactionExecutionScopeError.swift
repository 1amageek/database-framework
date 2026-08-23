/// Failure to reuse a storage transaction outside its admitted database scope.
public enum DatabaseTransactionExecutionScopeError:
    Error,
    Sendable,
    Equatable
{
    case containerMismatch
    case schemaGenerationMismatch
    case dataRootMismatch
    case storageDomainMismatch
}
