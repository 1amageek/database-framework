import DatabaseKit

/// Restricts continuation delivery without granting any execution authority.
@_spi(DatabaseExecution)
public enum SQLReadOnlyQueryAdmission {
    public static func validate(_ statement: QueryStatement,
                                workMeter: DatabaseWorkMeter) throws {
        guard case .select = statement,
              try !SQLSequenceEffectAnalysis.containsSequence(statement, workMeter: workMeter)
        else { throw SQLBlockExecutionError.unsupportedPagedQuery }
    }
}
