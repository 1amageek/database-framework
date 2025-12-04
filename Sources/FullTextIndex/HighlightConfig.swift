// HighlightConfig.swift
// FullTextIndex - Highlighting configuration and result types
//
// Reference: Elasticsearch Highlighting, Lucene Highlighter

import Foundation

// MARK: - HighlightConfig

/// Configuration for search result highlighting
///
/// Controls how matching terms are highlighted in search results.
///
/// **Usage**:
/// ```swift
/// let config = HighlightConfig(
///     preTag: "<em>",
///     postTag: "</em>",
///     fragmentSize: 150,
///     numberOfFragments: 3
/// )
/// ```
///
/// **Reference**: Elasticsearch highlight API
public struct HighlightConfig: Sendable, Hashable {
    /// Tag inserted before highlighted term
    ///
    /// Default: `<em>`
    public var preTag: String

    /// Tag inserted after highlighted term
    ///
    /// Default: `</em>`
    public var postTag: String

    /// Maximum size of each fragment in characters
    ///
    /// Default: 150
    public var fragmentSize: Int

    /// Maximum number of fragments to return
    ///
    /// Default: 3
    public var numberOfFragments: Int

    /// Character to use for fragment boundary
    ///
    /// Fragments try to break on this character.
    /// Default: `.` (sentence boundary)
    public var fragmentBoundary: Character

    /// Whether to merge overlapping highlights
    ///
    /// Default: true
    public var mergeOverlapping: Bool

    /// Fields to highlight (empty = all searched fields)
    public var fields: [String]

    /// Highlighter type
    public var highlighterType: HighlighterType

    public init(
        preTag: String = "<em>",
        postTag: String = "</em>",
        fragmentSize: Int = 150,
        numberOfFragments: Int = 3,
        fragmentBoundary: Character = ".",
        mergeOverlapping: Bool = true,
        fields: [String] = [],
        highlighterType: HighlighterType = .unified
    ) {
        self.preTag = preTag
        self.postTag = postTag
        self.fragmentSize = fragmentSize
        self.numberOfFragments = numberOfFragments
        self.fragmentBoundary = fragmentBoundary
        self.mergeOverlapping = mergeOverlapping
        self.fields = fields
        self.highlighterType = highlighterType
    }

    /// Default HTML highlighting
    public static let html = HighlightConfig()

    /// Plain text highlighting with asterisks
    public static let plainText = HighlightConfig(
        preTag: "**",
        postTag: "**"
    )

    /// ANSI terminal highlighting
    public static let terminal = HighlightConfig(
        preTag: "\u{001B}[1;33m",  // Bold yellow
        postTag: "\u{001B}[0m"     // Reset
    )
}

// MARK: - HighlighterType

/// Type of highlighter algorithm
public enum HighlighterType: String, Sendable, Hashable, Codable {
    /// Unified highlighter (default, best for most cases)
    ///
    /// Uses term vectors or re-analysis for highlighting.
    /// **Reference**: Elasticsearch unified highlighter
    case unified

    /// Fast vector highlighter
    ///
    /// Requires term vectors with positions and offsets.
    /// Faster for large documents.
    case fvh

    /// Plain highlighter
    ///
    /// Simple highlighting without term vectors.
    /// Works on any field but slower for large documents.
    case plain
}

// MARK: - FieldHighlight

/// Highlighted fragments for a single field
public struct FieldHighlight: Sendable, Hashable {
    /// Field name
    public let field: String

    /// Highlighted fragments
    public let fragments: [HighlightFragment]

    public init(field: String, fragments: [HighlightFragment]) {
        self.field = field
        self.fragments = fragments
    }
}

// MARK: - HighlightFragment

/// A single highlighted fragment
public struct HighlightFragment: Sendable, Hashable {
    /// The fragment text with highlight tags
    public let text: String

    /// Offset of this fragment in the original text
    public let offset: Int

    /// Relevance score of this fragment
    public let score: Float

    /// Highlighted spans within this fragment
    public let spans: [HighlightSpan]

    public init(
        text: String,
        offset: Int = 0,
        score: Float = 1.0,
        spans: [HighlightSpan] = []
    ) {
        self.text = text
        self.offset = offset
        self.score = score
        self.spans = spans
    }
}

// MARK: - HighlightSpan

/// A span within a fragment that was highlighted
public struct HighlightSpan: Sendable, Hashable {
    /// Start offset within the fragment
    public let start: Int

    /// End offset within the fragment
    public let end: Int

    /// The matched term
    public let term: String

    public init(start: Int, end: Int, term: String) {
        self.start = start
        self.end = end
        self.term = term
    }
}

// MARK: - FullTextSearchResult

/// Result from a full-text search with optional highlighting
public struct FullTextSearchResult<T: Sendable>: Sendable {
    /// The matching item
    public let item: T

    /// Relevance score
    public let score: Double

    /// Highlighted fields (empty if highlighting not requested)
    public let highlights: [FieldHighlight]

    /// Explanation of score calculation (for debugging)
    public let explanation: ScoreExplanation?

    public init(
        item: T,
        score: Double,
        highlights: [FieldHighlight] = [],
        explanation: ScoreExplanation? = nil
    ) {
        self.item = item
        self.score = score
        self.highlights = highlights
        self.explanation = explanation
    }

    /// Get highlights for a specific field
    public func highlights(for field: String) -> [HighlightFragment] {
        highlights.first { $0.field == field }?.fragments ?? []
    }
}

// MARK: - ScoreExplanation

/// Explanation of how a score was calculated
///
/// Useful for debugging relevance issues.
///
/// **Reference**: Lucene Explanation
public struct ScoreExplanation: Sendable, Hashable {
    /// Human-readable description
    public let description: String

    /// Score value
    public let value: Float

    /// Sub-explanations (for compound scores)
    public let details: [ScoreExplanation]

    public init(
        description: String,
        value: Float,
        details: [ScoreExplanation] = []
    ) {
        self.description = description
        self.value = value
        self.details = details
    }

    /// Format as indented string
    public func format(indent: Int = 0) -> String {
        let prefix = String(repeating: "  ", count: indent)
        var result = "\(prefix)\(value) = \(description)"
        for detail in details {
            result += "\n" + detail.format(indent: indent + 1)
        }
        return result
    }
}

// MARK: - ScoringModel

/// Scoring model for relevance calculation
///
/// **Reference**: Lucene Similarity, BM25
public enum ScoringModel: String, Sendable, Hashable, Codable {
    /// BM25 (default in modern search engines)
    ///
    /// Probabilistic model with good performance on short queries.
    /// Parameters: k1 (term saturation), b (length normalization)
    ///
    /// **Reference**: Robertson & Walker, "Some Simple Effective
    /// Approximations to the 2-Poisson Model"
    case bm25

    /// TF-IDF (classic model)
    ///
    /// Term Frequency - Inverse Document Frequency.
    /// Simpler than BM25 but less effective.
    case tfidf

    /// Boolean model
    ///
    /// No scoring - all matching documents have score 1.0.
    /// Useful for filtering without ranking.
    case boolean
}

// MARK: - BM25Parameters

/// Parameters for BM25 scoring
///
/// **Reference**: Lucene BM25Similarity
public struct BM25Parameters: Sendable, Hashable {
    /// Term saturation parameter
    ///
    /// Controls how quickly term frequency saturates.
    /// Higher values = term frequency continues to matter.
    /// Default: 1.2 (Lucene default)
    public let k1: Float

    /// Length normalization parameter
    ///
    /// Controls impact of document length on score.
    /// 0 = no length normalization
    /// 1 = full length normalization
    /// Default: 0.75 (Lucene default)
    public let b: Float

    public init(k1: Float = 1.2, b: Float = 0.75) {
        self.k1 = k1
        self.b = b
    }

    /// Lucene defaults
    public static let `default` = BM25Parameters()

    /// No length normalization
    public static let noLengthNorm = BM25Parameters(k1: 1.2, b: 0)

    /// Strong length normalization
    public static let strongLengthNorm = BM25Parameters(k1: 1.2, b: 1.0)
}
