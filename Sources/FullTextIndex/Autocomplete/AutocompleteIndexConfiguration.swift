import DatabaseKit

struct AutocompleteIndexConfiguration: Sendable, Hashable {
    let minPrefixLength: Int
    let maxPrefixLength: Int

    init(metadata: IndexKindMetadata) throws {
        guard case .autocomplete(
            let minPrefixLength,
            let maxPrefixLength
        ) = try IndexDefinition(metadata: metadata) else {
            throw AutocompleteError.invalidIndexConfiguration
        }
        self.minPrefixLength = minPrefixLength
        self.maxPrefixLength = maxPrefixLength
    }
}
