import DatabaseKit

struct FullTextIndexConfiguration: Sendable, Hashable {
    let tokenizer: TokenizationStrategy
    let storePositions: Bool
    let ngramSize: Int
    let minTermLength: Int

    init(metadata: IndexKindMetadata) throws {
        guard case .fullText(
            let tokenizer,
            let storePositions,
            let ngramSize,
            let minTermLength
        ) = try IndexDefinition(metadata: metadata) else {
            throw FullTextIndexError.invalidConfiguration(
                "Index metadata does not describe a full-text index"
            )
        }
        self.tokenizer = tokenizer
        self.storePositions = storePositions
        self.ngramSize = ngramSize
        self.minTermLength = minTermLength
    }

    init(metadata: PolymorphicIndexMetadata) throws {
        guard metadata.kindIdentifier == "fulltext",
              metadata.subspaceStructure == .hierarchical,
              !metadata.fields.isEmpty,
              Set(metadata.metadata.keys) == [
                  "tokenizer",
                  "storePositions",
                  "ngramSize",
                  "minTermLength",
              ],
              let tokenizerValue = metadata.metadata["tokenizer"]?.stringValue,
              let tokenizer = TokenizationStrategy(rawValue: tokenizerValue),
              let storePositions = metadata.metadata[
                  "storePositions"
              ]?.boolValue,
              let ngramValue = metadata.metadata["ngramSize"]?.int64Value,
              let ngramSize = Int(exactly: ngramValue),
              ngramSize > 0,
              let minimumValue = metadata.metadata[
                  "minTermLength"
              ]?.int64Value,
              let minTermLength = Int(exactly: minimumValue),
              minTermLength > 0 else {
            throw FullTextIndexError.invalidConfiguration(
                "Invalid polymorphic full-text index metadata"
            )
        }
        self.tokenizer = tokenizer
        self.storePositions = storePositions
        self.ngramSize = ngramSize
        self.minTermLength = minTermLength
    }
}
