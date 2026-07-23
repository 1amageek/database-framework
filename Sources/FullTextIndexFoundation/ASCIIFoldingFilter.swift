import Foundation
import FullTextIndex

/// Foundation-backed diacritic folding for native text-analysis pipelines.
public struct ASCIIFoldingFilter: TokenFilter {
    public static var identifier: String { "ascii_folding" }

    public init() {}

    public func filter(_ tokens: [AnalyzedToken]) -> [AnalyzedToken] {
        tokens.map { token in
            AnalyzedToken(
                text: token.text.folding(
                    options: .diacriticInsensitive,
                    locale: nil
                ),
                position: token.position,
                startOffset: token.startOffset,
                endOffset: token.endOffset,
                type: token.type
            )
        }
    }
}
