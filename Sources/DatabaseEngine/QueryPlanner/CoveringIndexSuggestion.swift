/// A schema change that makes an index projection complete.
public struct CoveringIndexSuggestion: Sendable {
    public enum SuggestionType: Sendable {
        case newIndex
        case extendExisting
    }

    public let type: SuggestionType
    public let indexName: String?
    public let keyFields: [String]
    public let storedFields: [String]
    public let reason: String
}
