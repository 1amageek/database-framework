import DatabaseKit

/// A semantic SELECT and its independent, invocation-owned syntax annotations.
public struct SQLParsedSelect: Sendable {
    public let query: SelectQuery
    public let occurrences: QueryOccurrenceArena
    public let root: QueryOccurrenceID
    public let sourceMap: SQLSourceMap

    init(query: SelectQuery, occurrences: QueryOccurrenceArena, root: QueryOccurrenceID, sourceMap: SQLSourceMap) {
        self.query = query
        self.occurrences = occurrences
        self.root = root
        self.sourceMap = sourceMap
    }
}
