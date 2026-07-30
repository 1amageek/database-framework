
public enum FullTextStorageError: Error, Sendable, CustomStringConvertible, Equatable {
    case corruptedPosting(term: String)
    case corruptedCorpusStatistics
    case missingDocumentMetadata
    case corruptedDocumentMetadata
    case corruptedFacetKey(field: String)
    case corruptedDocumentFacetValues(field: String)
    case corruptedAutocompleteSuggestionKey(field: String, prefix: String)
    case corruptedAutocompleteTermKey(field: String)

    public var description: String {
        switch self {
        case .corruptedPosting(let term):
            return "Corrupted full-text posting for term '\(term)'"
        case .corruptedCorpusStatistics:
            return "Corrupted full-text corpus statistics"
        case .missingDocumentMetadata:
            return "Full-text document metadata is missing"
        case .corruptedDocumentMetadata:
            return "Corrupted full-text document metadata"
        case .corruptedFacetKey(let field):
            return "Corrupted full-text facet key for field '\(field)'"
        case .corruptedDocumentFacetValues(let field):
            return "Corrupted full-text document facet values for field '\(field)'"
        case .corruptedAutocompleteSuggestionKey(let field, let prefix):
            return "Corrupted autocomplete suggestion key for field '\(field)' and prefix '\(prefix)'"
        case .corruptedAutocompleteTermKey(let field):
            return "Corrupted autocomplete term key for field '\(field)'"
        }
    }
}
