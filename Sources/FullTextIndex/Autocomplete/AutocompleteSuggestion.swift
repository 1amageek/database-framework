public struct AutocompleteSuggestion: Sendable, Hashable {
    public let term: String
    public let score: Int64

    public init(term: String, score: Int64) {
        self.term = term
        self.score = score
    }
}
