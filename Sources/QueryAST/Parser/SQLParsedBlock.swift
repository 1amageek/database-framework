import DatabaseKit

/// A request block with absolute UTF-8 ranges and occurrence roots aligned to steps.
public struct SQLParsedBlock: Sendable {
    public let block: SQLBlock
    public let occurrences: QueryOccurrenceArena
    /// Ordinary statements inside statement, PREPARE, EXPLAIN, and query COPY steps.
    public let roots: [QueryOccurrenceID?]
    public let sourceMap: SQLSourceMap
    public let stepRanges: [SQLSourceRange]
    public let copyDataRanges: [SQLSourceRange?]

    init(block: SQLBlock, occurrences: QueryOccurrenceArena, roots: [QueryOccurrenceID?],
         sourceMap: SQLSourceMap, stepRanges: [SQLSourceRange], copyDataRanges: [SQLSourceRange?]) {
        self.block = block; self.occurrences = occurrences; self.roots = roots
        self.sourceMap = sourceMap; self.stepRanges = stepRanges; self.copyDataRanges = copyDataRanges
    }
}
