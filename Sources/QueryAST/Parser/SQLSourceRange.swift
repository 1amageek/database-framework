/// A zero-based, half-open UTF-8 range in the original SQL input.
public struct SQLSourceRange: Sendable, Equatable, Hashable {
    public let start: Int
    public let end: Int

    public init(start: Int, end: Int) {
        precondition(start >= 0 && end >= start)
        self.start = start
        self.end = end
    }
}
