import DatabaseKit

/// One complete semantic statement with syntax annotations from the same parse.
public struct SQLParsedStatement: Sendable {
    public let statement: QueryStatement
    public let occurrences: QueryOccurrenceArena
    public let root: QueryOccurrenceID
    public let sourceMap: SQLSourceMap

    init(statement: QueryStatement, occurrences: QueryOccurrenceArena, root: QueryOccurrenceID, sourceMap: SQLSourceMap) {
        self.statement = statement
        self.occurrences = occurrences
        self.root = root
        self.sourceMap = sourceMap
    }
}
