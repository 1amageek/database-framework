#if DATABASE_MULTI_BASE
import DatabaseKit

/// A caller-owned Composition result with its complete ordered output schema.
public struct CompositionQueryResponse: Sendable {
    public let columns: [QueryColumn]
    public let rows: [CompositionResult<QueryRow>]

    public init(columns: [QueryColumn], rows: [CompositionResult<QueryRow>]) throws {
        try QueryColumn.validate(columns)
        for result in rows { try result.value.validate(columns: columns) }
        self.columns = columns
        self.rows = rows
    }
}
#endif
