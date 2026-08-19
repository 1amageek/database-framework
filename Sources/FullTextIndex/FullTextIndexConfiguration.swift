import DatabaseKit
import DatabaseTypes

struct FullTextIndexConfiguration: Sendable, Hashable {
    let tokenizer: TokenizationStrategy
    let storePositions: Bool
    let ngramSize: Int
    let minTermLength: Int

    init(definition: IndexDefinition<FieldIdentity>) throws {
        guard
            case .text(
                _,
                .fullText(
            let tokenizer,
            let storePositions,
            let ngramSize,
                    let minimumTermLength
                )
            ) = definition
        else {
            throw FullTextIndexError.invalidConfiguration(
                "Index definition does not describe a full-text index"
            )
        }
        self.tokenizer = tokenizer
        self.storePositions = storePositions
        self.ngramSize = ngramSize
        self.minTermLength = minimumTermLength
    }

    init(definition: IndexDefinition<String>) throws {
        guard
            case .text(
                _,
                .fullText(
                    let tokenizer,
                    let storePositions,
                    let ngramSize,
                    let minimumTermLength
                )
            ) = definition
        else {
            throw FullTextIndexError.invalidConfiguration(
                "Index definition does not describe a polymorphic full-text index"
            )
        }
        self.tokenizer = tokenizer
        self.storePositions = storePositions
        self.ngramSize = ngramSize
        self.minTermLength = minimumTermLength
    }
}
