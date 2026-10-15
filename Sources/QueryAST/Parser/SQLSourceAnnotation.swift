/// Syntax provenance for one semantic occurrence; identifiers may have a narrower range.
public struct SQLSourceAnnotation: Sendable, Equatable, Hashable {
    public let range: SQLSourceRange
    public let identifierRange: SQLSourceRange?

    public init(range: SQLSourceRange, identifierRange: SQLSourceRange? = nil) {
        self.range = range
        self.identifierRange = identifierRange
    }
}
