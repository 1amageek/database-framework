import DatabaseKit

/// Failures specific to semantic SQL block admission and checkpoint lookup.
public enum SQLBlockExecutionError: Error, Sendable, Equatable {
    case unknownCheckpoint(String)
    case invalidTransactionControl
    case invalidSettingValue(SQLSettingName)
    case statementTimeout
    case unsupportedStatement
    case mutationLimitOutOfRange(UInt32)
}
