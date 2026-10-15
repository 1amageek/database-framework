import DatabaseKit

/// An immutable sparse map keyed by occurrence identity, never AST value equality.
public struct SQLSourceMap: Sendable {
    public let annotations: [QueryOccurrenceID: SQLSourceAnnotation]

    init(annotations: [QueryOccurrenceID: SQLSourceAnnotation]) {
        self.annotations = annotations
    }

    public subscript(_ occurrence: QueryOccurrenceID) -> SQLSourceAnnotation? {
        annotations[occurrence]
    }

}
