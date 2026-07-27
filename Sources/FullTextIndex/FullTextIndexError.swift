public enum FullTextIndexError: Error, Sendable {
    case invalidConfiguration(String)
    case invalidQuery(String)
}
