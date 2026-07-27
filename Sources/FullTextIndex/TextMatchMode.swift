/// Defines how a full-text query combines multiple search terms.
public enum TextMatchMode: Sendable {
    /// Matches documents containing at least one term.
    case any

    /// Matches documents containing every term.
    case all

    /// Matches documents containing the terms as an exact phrase.
    case phrase
}
