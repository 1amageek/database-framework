import DatabaseKit

/// A statement batch whose source ranges remain absolute in the original input.
public struct SQLParsedStatements: Sendable {
    public let statements: [QueryStatement]
    public let occurrences: QueryOccurrenceArena
    public let roots: [QueryOccurrenceID]
    public let sourceMap: SQLSourceMap

    init(statements: [QueryStatement], occurrences: QueryOccurrenceArena, roots: [QueryOccurrenceID], sourceMap: SQLSourceMap) {
        self.statements = statements
        self.occurrences = occurrences
        self.roots = roots
        self.sourceMap = sourceMap
    }
}
