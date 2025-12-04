// TextAnalyzer.swift
// FullTextIndex - Text analysis protocols and types
//
// Reference: Lucene Analyzer, Elasticsearch Analysis

import Foundation

// MARK: - AnalyzedToken

/// A token produced by text analysis
///
/// Contains the token text along with position and offset information
/// for phrase queries and highlighting.
public struct AnalyzedToken: Sendable, Hashable {
    /// The token text (after normalization)
    public let text: String

    /// Position in the token stream (for phrase queries)
    ///
    /// Starts at 0, increments for each token.
    /// Synonyms may share the same position.
    public let position: Int

    /// Start offset in original text (for highlighting)
    public let startOffset: Int

    /// End offset in original text (for highlighting)
    public let endOffset: Int

    /// Token type (e.g., "word", "alphanum", "num")
    public let type: String

    public init(
        text: String,
        position: Int,
        startOffset: Int,
        endOffset: Int,
        type: String = "word"
    ) {
        self.text = text
        self.position = position
        self.startOffset = startOffset
        self.endOffset = endOffset
        self.type = type
    }
}

// MARK: - TextAnalyzer Protocol

/// Protocol for text analyzers
///
/// An analyzer transforms input text into a stream of tokens.
/// The analysis process typically involves:
/// 1. Tokenization - splitting text into tokens
/// 2. Filtering - transforming/filtering tokens (lowercase, stemming, etc.)
///
/// **Reference**: Lucene Analyzer
public protocol TextAnalyzer: Sendable {
    /// Unique identifier for this analyzer
    static var identifier: String { get }

    /// Analyze text into tokens
    ///
    /// - Parameter text: The input text to analyze
    /// - Returns: Array of analyzed tokens
    func analyze(_ text: String) -> [AnalyzedToken]

    /// Analyze a query string
    ///
    /// Query analysis may differ from index analysis
    /// (e.g., no synonym expansion at query time).
    ///
    /// Default implementation calls `analyze(_:)`.
    func analyzeQuery(_ text: String) -> [AnalyzedToken]
}

extension TextAnalyzer {
    /// Default query analysis uses the same logic as index analysis
    public func analyzeQuery(_ text: String) -> [AnalyzedToken] {
        analyze(text)
    }
}

// MARK: - TokenFilter Protocol

/// Protocol for token filters
///
/// A token filter transforms a stream of tokens, potentially:
/// - Modifying token text (lowercase, stemming)
/// - Removing tokens (stopwords)
/// - Adding tokens (synonyms)
///
/// **Reference**: Lucene TokenFilter
public protocol TokenFilter: Sendable {
    /// Unique identifier for this filter
    static var identifier: String { get }

    /// Filter the token stream
    ///
    /// - Parameter tokens: Input tokens
    /// - Returns: Filtered tokens
    func filter(_ tokens: [AnalyzedToken]) -> [AnalyzedToken]
}

// MARK: - Built-in Token Filters

/// Lowercase filter - converts all tokens to lowercase
///
/// **Reference**: Lucene LowercaseFilter
public struct LowercaseFilter: TokenFilter {
    public static var identifier: String { "lowercase" }

    public init() {}

    public func filter(_ tokens: [AnalyzedToken]) -> [AnalyzedToken] {
        tokens.map { token in
            AnalyzedToken(
                text: token.text.lowercased(),
                position: token.position,
                startOffset: token.startOffset,
                endOffset: token.endOffset,
                type: token.type
            )
        }
    }
}

/// Minimum length filter - removes tokens shorter than minimum
public struct MinLengthFilter: TokenFilter {
    public static var identifier: String { "min_length" }

    public let minLength: Int

    public init(minLength: Int = 2) {
        self.minLength = minLength
    }

    public func filter(_ tokens: [AnalyzedToken]) -> [AnalyzedToken] {
        tokens.filter { $0.text.count >= minLength }
    }
}

/// Maximum length filter - removes tokens longer than maximum
public struct MaxLengthFilter: TokenFilter {
    public static var identifier: String { "max_length" }

    public let maxLength: Int

    public init(maxLength: Int = 255) {
        self.maxLength = maxLength
    }

    public func filter(_ tokens: [AnalyzedToken]) -> [AnalyzedToken] {
        tokens.filter { $0.text.count <= maxLength }
    }
}

/// ASCII folding filter - converts Unicode to ASCII equivalents
///
/// Removes diacritics: é→e, ü→u, ñ→n
///
/// **Reference**: Lucene ASCIIFoldingFilter
public struct ASCIIFoldingFilter: TokenFilter {
    public static var identifier: String { "ascii_folding" }

    public init() {}

    public func filter(_ tokens: [AnalyzedToken]) -> [AnalyzedToken] {
        tokens.map { token in
            let folded = token.text.folding(options: .diacriticInsensitive, locale: nil)
            return AnalyzedToken(
                text: folded,
                position: token.position,
                startOffset: token.startOffset,
                endOffset: token.endOffset,
                type: token.type
            )
        }
    }
}

/// Trim filter - trims whitespace from tokens
public struct TrimFilter: TokenFilter {
    public static var identifier: String { "trim" }

    public init() {}

    public func filter(_ tokens: [AnalyzedToken]) -> [AnalyzedToken] {
        tokens.map { token in
            AnalyzedToken(
                text: token.text.trimmingCharacters(in: .whitespaces),
                position: token.position,
                startOffset: token.startOffset,
                endOffset: token.endOffset,
                type: token.type
            )
        }
    }
}

/// Stopword filter - removes common words
///
/// **Reference**: Lucene StopFilter
public struct StopwordFilter: TokenFilter {
    public static var identifier: String { "stopword" }

    /// The set of stopwords to remove
    public let stopwords: Set<String>

    /// Default English stopwords (Lucene default list)
    public static let englishStopwords: Set<String> = [
        "a", "an", "and", "are", "as", "at", "be", "but", "by",
        "for", "if", "in", "into", "is", "it", "no", "not", "of",
        "on", "or", "such", "that", "the", "their", "then", "there",
        "these", "they", "this", "to", "was", "will", "with"
    ]

    public init(stopwords: Set<String> = StopwordFilter.englishStopwords) {
        self.stopwords = stopwords
    }

    public func filter(_ tokens: [AnalyzedToken]) -> [AnalyzedToken] {
        tokens.filter { !stopwords.contains($0.text.lowercased()) }
    }
}

// MARK: - Tokenizer Protocol

/// Protocol for tokenizers
///
/// A tokenizer splits input text into tokens.
///
/// **Reference**: Lucene Tokenizer
public protocol Tokenizer: Sendable {
    /// Unique identifier for this tokenizer
    static var identifier: String { get }

    /// Tokenize input text
    ///
    /// - Parameter text: The input text to tokenize
    /// - Returns: Array of tokens with position/offset information
    func tokenize(_ text: String) -> [AnalyzedToken]
}

// MARK: - Built-in Tokenizers

/// Whitespace tokenizer - splits on whitespace
public struct WhitespaceTokenizer: Tokenizer {
    public static var identifier: String { "whitespace" }

    public init() {}

    public func tokenize(_ text: String) -> [AnalyzedToken] {
        var tokens: [AnalyzedToken] = []
        var position = 0
        var currentIndex = text.startIndex

        while currentIndex < text.endIndex {
            // Skip whitespace
            while currentIndex < text.endIndex && text[currentIndex].isWhitespace {
                currentIndex = text.index(after: currentIndex)
            }

            guard currentIndex < text.endIndex else { break }

            // Find end of token
            let tokenStart = currentIndex
            while currentIndex < text.endIndex && !text[currentIndex].isWhitespace {
                currentIndex = text.index(after: currentIndex)
            }

            let tokenText = String(text[tokenStart..<currentIndex])
            let startOffset = text.distance(from: text.startIndex, to: tokenStart)
            let endOffset = text.distance(from: text.startIndex, to: currentIndex)

            tokens.append(AnalyzedToken(
                text: tokenText,
                position: position,
                startOffset: startOffset,
                endOffset: endOffset
            ))
            position += 1
        }

        return tokens
    }
}

/// Standard tokenizer - splits on word boundaries
///
/// Handles punctuation, contractions, and Unicode word boundaries.
///
/// **Reference**: Lucene StandardTokenizer
public struct StandardTokenizer: Tokenizer {
    public static var identifier: String { "standard" }

    public init() {}

    public func tokenize(_ text: String) -> [AnalyzedToken] {
        var tokens: [AnalyzedToken] = []
        var position = 0

        // Use word boundary enumeration for proper Unicode handling
        text.enumerateSubstrings(in: text.startIndex..., options: .byWords) { substring, range, _, _ in
            guard let word = substring else { return }

            let startOffset = text.distance(from: text.startIndex, to: range.lowerBound)
            let endOffset = text.distance(from: text.startIndex, to: range.upperBound)

            tokens.append(AnalyzedToken(
                text: word,
                position: position,
                startOffset: startOffset,
                endOffset: endOffset
            ))
            position += 1
        }

        return tokens
    }
}

/// N-gram tokenizer - generates character n-grams
///
/// Useful for fuzzy matching and CJK languages.
public struct NGramTokenizer: Tokenizer {
    public static var identifier: String { "ngram" }

    /// Minimum n-gram size
    public let minGram: Int

    /// Maximum n-gram size
    public let maxGram: Int

    public init(minGram: Int = 2, maxGram: Int = 3) {
        self.minGram = max(1, minGram)
        self.maxGram = max(self.minGram, maxGram)
    }

    public func tokenize(_ text: String) -> [AnalyzedToken] {
        var tokens: [AnalyzedToken] = []
        var position = 0

        let chars = Array(text)
        for i in 0..<chars.count {
            for n in minGram...maxGram {
                guard i + n <= chars.count else { continue }

                let gram = String(chars[i..<(i + n)])
                tokens.append(AnalyzedToken(
                    text: gram,
                    position: position,
                    startOffset: i,
                    endOffset: i + n,
                    type: "ngram"
                ))
            }
            position += 1
        }

        return tokens
    }
}

// MARK: - Composite Analyzer

/// A customizable analyzer that combines a tokenizer with filters
///
/// **Usage**:
/// ```swift
/// let analyzer = CompositeAnalyzer(
///     tokenizer: StandardTokenizer(),
///     filters: [
///         LowercaseFilter(),
///         StopwordFilter(),
///         SnowballStemmer()
///     ]
/// )
/// ```
public struct CompositeAnalyzer: TextAnalyzer {
    public static var identifier: String { "composite" }

    private let tokenizer: any Tokenizer
    private let filters: [any TokenFilter]

    public init(tokenizer: any Tokenizer, filters: [any TokenFilter]) {
        self.tokenizer = tokenizer
        self.filters = filters
    }

    public func analyze(_ text: String) -> [AnalyzedToken] {
        var tokens = tokenizer.tokenize(text)
        for filter in filters {
            tokens = filter.filter(tokens)
        }
        return tokens
    }
}

// MARK: - Standard Analyzers

/// Simple analyzer - whitespace tokenization + lowercase
public struct SimpleAnalyzer: TextAnalyzer {
    public static var identifier: String { "simple" }

    private let tokenizer = WhitespaceTokenizer()
    private let lowercaseFilter = LowercaseFilter()
    private let minLengthFilter: MinLengthFilter

    public init(minLength: Int = 1) {
        self.minLengthFilter = MinLengthFilter(minLength: minLength)
    }

    public func analyze(_ text: String) -> [AnalyzedToken] {
        var tokens = tokenizer.tokenize(text)
        tokens = lowercaseFilter.filter(tokens)
        tokens = minLengthFilter.filter(tokens)
        return tokens
    }
}

/// Standard analyzer - standard tokenization + lowercase + stopwords
public struct StandardAnalyzer: TextAnalyzer {
    public static var identifier: String { "standard" }

    private let tokenizer = StandardTokenizer()
    private let lowercaseFilter = LowercaseFilter()
    private let stopwordFilter: StopwordFilter
    private let minLengthFilter: MinLengthFilter

    public init(
        stopwords: Set<String> = StopwordFilter.englishStopwords,
        minLength: Int = 2
    ) {
        self.stopwordFilter = StopwordFilter(stopwords: stopwords)
        self.minLengthFilter = MinLengthFilter(minLength: minLength)
    }

    public func analyze(_ text: String) -> [AnalyzedToken] {
        var tokens = tokenizer.tokenize(text)
        tokens = lowercaseFilter.filter(tokens)
        tokens = stopwordFilter.filter(tokens)
        tokens = minLengthFilter.filter(tokens)
        return tokens
    }
}

/// Keyword analyzer - treats entire input as single token
public struct KeywordAnalyzer: TextAnalyzer {
    public static var identifier: String { "keyword" }

    private let lowercase: Bool

    public init(lowercase: Bool = true) {
        self.lowercase = lowercase
    }

    public func analyze(_ text: String) -> [AnalyzedToken] {
        let processedText = lowercase ? text.lowercased() : text
        return [AnalyzedToken(
            text: processedText,
            position: 0,
            startOffset: 0,
            endOffset: text.count
        )]
    }
}
