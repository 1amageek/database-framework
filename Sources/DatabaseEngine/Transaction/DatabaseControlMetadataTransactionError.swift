/// Failures produced while composing one data-root transaction with the
/// container's control-metadata root.
@_spi(DatabaseExecution)
public enum DatabaseControlMetadataTransactionError:
    Error,
    Sendable,
    Equatable
{
    /// Atomic data and control mutations require one physical transaction
    /// domain. Cross-domain work must use an explicitly checkpointed protocol.
    case storageDomainMismatch

    /// A different transaction is already bound to the current task. Opening a
    /// second root through that attenuated capability would bypass its scope.
    case requiresIndependentTransaction
}
