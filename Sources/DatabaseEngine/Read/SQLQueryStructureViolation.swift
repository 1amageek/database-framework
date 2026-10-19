/// Classifies a SQL structural rule violated by a parsed query.
///
/// The read path owns the meaning of each rule; the SQL diagnostic owner maps
/// the class to its SQLSTATE.
public enum SQLQueryStructureViolation: Sendable, Equatable {
    /// A column alias list, CTE column list, output ordinal or DISTINCT ON
    /// key does not reference the query's output columns.
    case columnReference
    /// Combined query terms or VALUES rows disagree in column count.
    case columnCount
    /// A recursive CTE term violates the recursive query rules.
    case recursion
    /// A LIMIT row count is negative.
    case limitRowCount
    /// An OFFSET row count is negative.
    case offsetRowCount
    /// Two sources in one scope share an alias.
    case duplicateAlias
}
