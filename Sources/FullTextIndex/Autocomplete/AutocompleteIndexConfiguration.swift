import DatabaseKit

struct AutocompleteIndexConfiguration: Sendable, Hashable {
    let minPrefixLength: Int
    let maxPrefixLength: Int

    init(definition: IndexDefinition<FieldIdentity>) throws {
        guard
            case .text(
                _,
                .autocomplete(
            let minPrefixLength,
            let maxPrefixLength
                )
            ) = definition
        else {
            throw AutocompleteError.invalidIndexConfiguration
        }
        self.minPrefixLength = minPrefixLength
        self.maxPrefixLength = maxPrefixLength
    }
}
